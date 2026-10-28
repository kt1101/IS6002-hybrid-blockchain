#!/usr/bin/env bash
# Build hbdb-build, extract linux/arm64 binaries into ./bin, then build hbdb-node and hbdb-kafka.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker build -f experiments/docker/Dockerfile.build -t hbdb-build .

mkdir -p bin
cid="$(docker create hbdb-build)"
trap 'docker rm "$cid" >/dev/null' EXIT
docker cp "$cid:/out/." bin/

docker build -f experiments/docker/Dockerfile.node -t hbdb-node .
docker build -f experiments/docker/Dockerfile.kafka -t hbdb-kafka .
