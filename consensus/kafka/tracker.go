package kafka

import (
	"container/list"
	"log"

	pbv "hybrid/proto/veritas"
)

// Message encoding on the topic (the proto is unchanged; MessageType only has
// Approve and Abort):
//
//   - proposal: pbv.Block{Type: Approve, Signature: proposer, Txs: [...]}
//     where at least one SharedLog carries Sets. This is exactly what
//     core.Server.submitBlock produces (Type Approve, as the original server's
//     block log), so the proposer's raw bytes go on the topic unchanged and
//     are delivered unchanged. The proposal itself counts as the proposer's
//     Approve.
//   - vote: pbv.Block{Type: Approve|Abort, Signature: voter,
//     Txs: [{Seq: <proposal's Txs[0].Seq>}]} with no Sets.
//
// So an Approve message is a proposal iff it carries a Set; an Abort is always
// a vote. A block whose SharedLogs all have zero Sets would be read as a vote;
// core.Server never produces one (Set has one Set, BatchSet rejects empty
// requests).

const (
	// defaultMaxPending bounds the number of blocks waiting for votes. When a
	// new proposal would exceed it, the OLDEST pending block is dropped (as if
	// aborted: logged and reported through onDrop). Every node reads the same
	// topic, so every node evicts the same block at the same message.
	// Normally a block waits a few milliseconds;
	// the bound only matters when a party stops voting (crash, ShouldVote).
	// ~1024 blocks x ~110 KB (100 Sets of 1 KB) = ~110 MB worst case.
	defaultMaxPending = 1024
	// defaultDoneWindow is how many finished (delivered, aborted or evicted)
	// block seqs are remembered, so a duplicate proposal of a recently
	// finished block (e.g. a producer retry) is ignored instead of being
	// voted on and delivered a second time. Seqs are 8 bytes, so the window
	// costs well under 1 MB.
	defaultDoneWindow = 1 << 16
)

// entry is one pending block: the proposer's raw bytes and the parties whose
// Approve has been read from the topic (the proposer's is its proposal).
type entry struct {
	proposer string
	raw      []byte
	approved map[string]struct{}
	elem     *list.Element // in tracker.order
}

// tracker is the pure, deterministic approve-all state machine of the Kafka
// backend. It is fed every topic message in topic order (single partition)
// and decides, from the messages alone, which blocks are delivered and when.
// Since every node reads the same messages in the same order, every node
// delivers the same blocks in the same order (the completion order in the
// topic), whatever its own identity.
//
// Rules (onMessage):
//   - Messages from a signature outside parties are ignored entirely.
//   - Malformed messages (no Txs, nil Txs[0]) are ignored.
//   - A proposal for an unknown seq opens a pending entry, keyed by
//     Txs[0].Seq (globally unique: NodeID<<40|counter), approved by its
//     proposer. If this node is not the proposer it must vote (needVote).
//   - A node's own vote counts only when it is read back from the topic,
//     like everybody else's; nothing is counted at send time.
//   - A block is delivered at the message that completes its approvals from
//     ALL parties, and then forgotten.
//   - An Abort from any party for a pending block drops it: it is never
//     delivered, on any node. The drop is reported through onDrop.
//   - Duplicate proposals: a proposal whose seq is pending is ignored (also
//     if its signature differs from the pending proposer: the first proposal
//     read wins); a proposal whose seq finished recently (done window) is
//     ignored.
//   - Duplicate votes (same party, same seq) are idempotent.
//   - Votes (Approve or Abort) for an unknown seq, or for a block already
//     delivered, dropped or evicted, are ignored and NOT stored, so late
//     votes cost no memory. A vote read before its proposal would therefore
//     be lost; on a single-partition topic that cannot happen, because a
//     party only votes after reading the proposal.
//
// Memory: an entry exists only while its block is pending; it is removed
// when the block is delivered, aborted, or evicted by the maxPending bound.
// The done window is a fixed-size ring.
//
// tracker is not safe for concurrent use; the Kafka backend calls it from its
// single consumer goroutine.
type tracker struct {
	self    string
	parties map[string]struct{}
	pending map[int64]*entry
	order   *list.List // seqs (int64) of pending entries, oldest first

	maxPending int

	// onDrop, if set, is called once for every pending block that will never
	// be delivered: dropped by an Abort, or evicted by the maxPending bound
	// (also logged). It runs right after the drop, with the proposer's raw
	// bytes (a copy now owned by the callee).
	onDrop func(raw []byte)

	done     map[int64]struct{} // seqs in doneRing
	doneRing []int64
	doneNext int // next slot of doneRing to overwrite
	doneLen  int // filled slots of doneRing
}

// newTracker builds a tracker for node self. parties is copied; self is
// always added to it (as cmd/veritas does for the original server), so a
// node's own votes are never treated as coming from an unknown party.
func newTracker(self string, parties map[string]struct{}) *tracker {
	ps := make(map[string]struct{}, len(parties)+1)
	for p := range parties {
		ps[p] = struct{}{}
	}
	ps[self] = struct{}{}
	return &tracker{
		self:       self,
		parties:    ps,
		pending:    make(map[int64]*entry),
		order:      list.New(),
		maxPending: defaultMaxPending,
		done:       make(map[int64]struct{}),
		doneRing:   make([]int64, defaultDoneWindow),
	}
}

// isProposal reports whether m is a proposal (see the encoding above).
func isProposal(m *pbv.Block) bool {
	if m.GetType() != pbv.MessageType_Approve {
		return false
	}
	for _, sl := range m.GetTxs() {
		if len(sl.GetSets()) > 0 {
			return true
		}
	}
	return false
}

// onMessage processes one topic message m (raw is its wire encoding), in
// topic order. It returns the blocks completed by this message, as the
// proposers' raw bytes in completion order (at most one per call today), and
// whether this node must now vote on m (m is a proposal seen for the first
// time and this node is not its proposer). The returned slices are copies
// owned by the caller; raw is not retained.
func (t *tracker) onMessage(m *pbv.Block, raw []byte) (delivered [][]byte, needVote bool) {
	if m == nil || len(m.GetTxs()) == 0 || m.GetTxs()[0] == nil {
		return nil, false
	}
	sig := m.GetSignature()
	if _, ok := t.parties[sig]; !ok {
		return nil, false
	}
	seq := m.GetTxs()[0].GetSeq()

	switch {
	case m.GetType() == pbv.MessageType_Abort:
		if e, ok := t.pending[seq]; ok {
			t.finish(seq, e)
			if t.onDrop != nil {
				t.onDrop(e.raw)
			}
		}
		return nil, false

	case isProposal(m):
		if _, ok := t.pending[seq]; ok {
			return nil, false // duplicate or conflicting proposal: first wins
		}
		if _, ok := t.done[seq]; ok {
			return nil, false // duplicate of a recently finished block
		}
		e := &entry{
			proposer: sig,
			raw:      append([]byte(nil), raw...),
			approved: map[string]struct{}{sig: {}},
		}
		e.elem = t.order.PushBack(seq)
		t.pending[seq] = e
		for len(t.pending) > t.maxPending {
			oldest := t.order.Front().Value.(int64)
			old := t.pending[oldest]
			t.finish(oldest, old)
			log.Printf("kafka consensus %s: evicting pending block %#x (pending %d > max %d): never delivered", t.self, oldest, len(t.pending)+1, t.maxPending)
			if t.onDrop != nil {
				t.onDrop(old.raw)
			}
		}
		if _, ok := t.pending[seq]; !ok {
			return nil, false // evicted at once (maxPending < 1)
		}
		if len(e.approved) == len(t.parties) { // single-party deployment
			t.finish(seq, e)
			return [][]byte{e.raw}, false
		}
		return nil, sig != t.self

	case m.GetType() == pbv.MessageType_Approve: // vote
		e, ok := t.pending[seq]
		if !ok {
			return nil, false
		}
		e.approved[sig] = struct{}{}
		if len(e.approved) < len(t.parties) {
			return nil, false
		}
		t.finish(seq, e)
		return [][]byte{e.raw}, false

	default: // unknown MessageType
		return nil, false
	}
}

// finish forgets pending entry e (delivered, aborted or evicted) and records
// seq in the done window.
func (t *tracker) finish(seq int64, e *entry) {
	delete(t.pending, seq)
	t.order.Remove(e.elem)
	if len(t.doneRing) == 0 {
		return
	}
	if t.doneLen == len(t.doneRing) {
		delete(t.done, t.doneRing[t.doneNext])
	} else {
		t.doneLen++
	}
	t.doneRing[t.doneNext] = seq
	t.done[seq] = struct{}{}
	t.doneNext = (t.doneNext + 1) % len(t.doneRing)
}
