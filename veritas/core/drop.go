package core

import (
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"

	pbv "hybrid/proto/veritas"
)

// AbortBlock tells the server that the consensus backend dropped block raw
// for good: it will never be delivered, on any node (e.g. the Kafka backend
// read an Abort vote for it, or evicted it from its pending bound because a
// party stopped voting). Backends call it through a callback such as
// consensus/kafka Config.OnDrop. The cause is not passed: clients get the
// same single answer, ErrAborted, for every kind of drop.
//
// If raw is one of THIS node's blocks (its Signature is this node's), the
// block is counted in Breakdown.Dropped, its pending Consensus sample is
// discarded, and in CommitSync mode the sync waiter of every SharedLog in it
// is resolved at once with ErrAborted (gRPC code Aborted) instead of waiting
// for SyncTimeout. CommitAsync has no waiters, so only the count changes.
//
// Blocks of other proposers, undecodable bytes, empty blocks and seqs with
// no waiter (unknown, or already resolved by ctx/timeout/stop) are ignored.
// It never touches the store or the ledger, and it is safe to call
// concurrently with everything else, including the apply loop. It does not
// retain raw.
func (s *Server) AbortBlock(raw []byte) {
	var blk pbv.Block
	if err := proto.Unmarshal(raw, &blk); err != nil {
		log.Printf("core: node %d: AbortBlock: undecodable block ignored: %v", s.cfg.NodeID, err)
		return
	}
	if blk.GetSignature() != s.cfg.Signature || len(blk.GetTxs()) == 0 {
		return
	}
	s.metrics.dropped(blk.GetTxs()[0].GetSeq())
	if s.cfg.Commit != CommitSync {
		return
	}
	for _, sl := range blk.GetTxs() {
		seq := sl.GetSeq()
		s.waiters.resolve(seq, &rpcError{codes.Aborted, fmt.Errorf("%w: seq %d (block dropped by consensus)", ErrAborted, seq)})
	}
}

// dropped counts one of this node's blocks as dropped by consensus and
// forgets its Consensus sample (it will never arrive).
func (m *metrics) dropped(id int64) {
	m.mu.Lock()
	delete(m.pending, id)
	m.b.Dropped++
	m.mu.Unlock()
}
