module hybrid/bft2node

go 1.23.4

// go.sum is produced by `go mod tidy` in a golang:1.23 container (see
// experiments/docker/Dockerfile.bft2); the versions below are the ones
// github.com/relab/hotstuff v0.5.0 itself requires.
require (
	github.com/relab/gorums v0.7.1-0.20220818130557-8533cb369cd6
	github.com/relab/hotstuff v0.5.0
	google.golang.org/grpc v1.69.2
)
