#!/usr/bin/env bash
# experiments/build.sh for a Claude Code cloud session (linux/amd64 host whose
# outbound HTTPS goes through a TLS-intercepting proxy on 127.0.0.1, and whose
# egress policy denies the Debian mirrors). Builds the same four images
# (hbdb-build, hbdb-bft2-build, hbdb-node, hbdb-kafka) from the UNCHANGED
# experiments/docker/Dockerfile.*, but:
#   - every build runs with --network host and the proxy as build arg, so
#     go mod download, go install, apt and curl reach the internet;
#   - each FROM image is replaced (--build-context) by a local image that
#     trusts the proxy CA (Dockerfile.ca); Ubuntu bases fetch apt over HTTPS;
#   - redis:6.2-bookworm (base of hbdb-node) is replaced by
#     Dockerfile.redis-ubuntu: the same Redis 6.2 binaries on Ubuntu 24.04,
#     because its Debian packages (iproute2, iptables) cannot be fetched.
# The runtime containers get no proxy settings (the cluster needs no internet).
#
#   experiments/cloud/build.sh
#
# Env: CCR_CA=/root/.ccr/ca-bundle.crt, HTTPS_PROXY (from the session).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HERE="$ROOT/experiments/cloud"
cd "$ROOT"

CCR_CA="${CCR_CA:-/root/.ccr/ca-bundle.crt}"
[ -f "$CCR_CA" ] || { echo "cloud/build.sh: CA file $CCR_CA not found" >&2; exit 1; }
[ -n "${HTTPS_PROXY:-}" ] || { echo "cloud/build.sh: HTTPS_PROXY is not set" >&2; exit 1; }

ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT
cp "$CCR_CA" "$ctx/ccr-proxy.crt"

net=(--network host
  --build-arg "HTTPS_PROXY=$HTTPS_PROXY" --build-arg "https_proxy=$HTTPS_PROXY"
  --build-arg "NO_PROXY=localhost,127.0.0.1" --build-arg "no_proxy=localhost,127.0.0.1")

# base <source image> <local tag>
base() {
  docker build "${net[@]}" -f "$HERE/Dockerfile.ca" --build-arg "BASE=$1" -t "$2" "$ctx"
}

base golang:1.20-bullseye hbdb-cloud-base:golang1.20
base golang:1.23-bookworm hbdb-cloud-base:golang1.23
base eclipse-temurin:11-jre hbdb-cloud-base:temurin11
base ubuntu:24.04 hbdb-cloud-base:ubuntu24.04
docker build "${net[@]}" -f "$HERE/Dockerfile.redis-ubuntu" \
  --build-arg BASE=hbdb-cloud-base:ubuntu24.04 -t hbdb-cloud-base:redis6.2-ubuntu "$ctx"

docker build "${net[@]}" -f experiments/docker/Dockerfile.build \
  --build-context golang:1.20-bullseye=docker-image://hbdb-cloud-base:golang1.20 -t hbdb-build .
docker build "${net[@]}" -f experiments/docker/Dockerfile.bft2 \
  --build-context golang:1.23-bookworm=docker-image://hbdb-cloud-base:golang1.23 -t hbdb-bft2-build .

rm -rf bin && mkdir -p bin
cid=""
cid2=""
trap 'rm -rf "$ctx"; for c in $cid $cid2; do docker rm "$c" >/dev/null; done' EXIT
cid="$(docker create hbdb-build)"
docker cp "$cid:/out/." bin/
cid2="$(docker create hbdb-bft2-build)"
docker cp "$cid2:/out/bft2-node" bin/bft2-node

docker build "${net[@]}" -f experiments/docker/Dockerfile.node \
  --build-context redis:6.2-bookworm=docker-image://hbdb-cloud-base:redis6.2-ubuntu -t hbdb-node .
docker build "${net[@]}" -f experiments/docker/Dockerfile.kafka \
  --build-context eclipse-temurin:11-jre=docker-image://hbdb-cloud-base:temurin11 -t hbdb-kafka .
