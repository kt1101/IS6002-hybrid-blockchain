#!/usr/bin/env bash
# Regenerate proto/veritas/veritas.pb.go with the generator the committed
# file was made with (protoc-gen-go v1.3.2, plugins=grpc: APIv1,
# grpc.SupportPackageIsVersion4), so the diff only adds the new messages.
# Runs in golang:1.20 (Docker); protoc 3.21.12 from the GitHub release.
#
#   scripts/genproto_docker.sh
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
docker run --rm -v "$root:/src" -w /src golang:1.20 bash -euc '
  apt-get update -qq && apt-get install -y -qq unzip >/dev/null
  curl -fsSL -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protoc-21.12-linux-$(uname -m | sed s/aarch64/aarch_64/).zip
  unzip -q -o /tmp/protoc.zip -d /usr/local
  GOFLAGS=-mod=mod GO111MODULE=on go install github.com/golang/protobuf/protoc-gen-go@v1.3.2
  protoc --plugin=protoc-gen-go=$(go env GOPATH)/bin/protoc-gen-go --go_out=plugins=grpc:. ./proto/veritas/veritas.proto
'
git -C "$root" diff --stat -- proto/veritas/veritas.pb.go
