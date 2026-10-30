#!/usr/bin/env bash
# Build hbdb-build (main module, golang:1.20) and hbdb-bft2-build (the bft2node module,
# golang:1.23: relab/hotstuff needs Go >= 1.23.4), extract their linux/arm64 binaries
# into ./bin, then build hbdb-node and hbdb-kafka.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker build -f experiments/docker/Dockerfile.build -t hbdb-build .
docker build -f experiments/docker/Dockerfile.bft2 -t hbdb-bft2-build .

rm -rf bin && mkdir -p bin
cid=""
cid2=""
trap 'for c in $cid $cid2; do docker rm "$c" >/dev/null; done' EXIT
cid="$(docker create hbdb-build)"
docker cp "$cid:/out/." bin/
cid2="$(docker create hbdb-bft2-build)"
docker cp "$cid2:/out/bft2-node" bin/bft2-node

docker build -f experiments/docker/Dockerfile.node -t hbdb-node .
docker build -f experiments/docker/Dockerfile.kafka -t hbdb-kafka .
