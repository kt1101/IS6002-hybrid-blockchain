// Package consensus defines the ordering service that Veritas nodes use to
// agree on a single sequence of blocks. Backends (Kafka, Raft, Tendermint, ...)
// implement Consensus; consensus/memnet provides an in-memory one for tests.
package consensus

import "context"

// Consensus is a total-order broadcast service as seen by one node.
// Every node of a deployment must observe the same sequence of blocks on
// Deliver, including the blocks it submitted itself.
type Consensus interface {
	// Submit proposes a block for ordering. The implementation must not
	// retain blk after Submit returns, so callers may reuse the buffer.
	// A nil error means the block was accepted for ordering, not that it was
	// delivered: it may still be lost (leader change, timeout) or, if the
	// caller retries, delivered twice. Callers must tolerate both.
	Submit(ctx context.Context, blk []byte) error
	// Deliver returns the channel of ordered blocks. Every call returns the
	// same channel, and it is closed by Close or when the backend fails.
	// Each received slice belongs to the receiver, which may modify it.
	Deliver() <-chan []byte
	// Close releases the backend and closes the Deliver channel. It is safe
	// to call more than once.
	Close() error
}
