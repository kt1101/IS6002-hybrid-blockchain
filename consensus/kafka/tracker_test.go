package kafka

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	pbv "hybrid/proto/veritas"
)

// testTimeout bounds every test of this package (see guard).
const testTimeout = 10 * time.Second

// guard makes the test binary fail loudly if the test runs longer than
// testTimeout (also for tests that never block, so every test has one).
func guard(t *testing.T) {
	t.Helper()
	name := t.Name()
	timer := time.AfterFunc(testTimeout, func() {
		panic(fmt.Sprintf("%s: timed out after %v", name, testTimeout))
	})
	t.Cleanup(func() { timer.Stop() })
}

// msg is one topic message: the decoded block and its wire bytes.
type msg struct {
	m   *pbv.Block
	raw []byte
}

func mustMarshal(t *testing.T, b *pbv.Block) []byte {
	t.Helper()
	raw, err := proto.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// proposal builds a one-SharedLog proposal from signer with block id seq.
func proposal(t *testing.T, signer string, seq int64) msg {
	t.Helper()
	b := &pbv.Block{
		Type:      pbv.MessageType_Approve,
		Signature: signer,
		Txs: []*pbv.SharedLog{{
			Seq:  seq,
			Sets: []*pbv.SetRequest{{Key: fmt.Sprintf("k%d", seq), Value: "v", Version: seq}},
		}},
	}
	return msg{m: b, raw: mustMarshal(t, b)}
}

// vote builds an Approve or Abort vote from voter for block seq.
func vote(t *testing.T, voter string, seq int64, typ pbv.MessageType) msg {
	t.Helper()
	b := &pbv.Block{Type: typ, Signature: voter, Txs: []*pbv.SharedLog{{Seq: seq}}}
	return msg{m: b, raw: mustMarshal(t, b)}
}

func approve(t *testing.T, voter string, seq int64) msg {
	return vote(t, voter, seq, pbv.MessageType_Approve)
}

func abort(t *testing.T, voter string, seq int64) msg {
	return vote(t, voter, seq, pbv.MessageType_Abort)
}

func parties(names ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[n] = struct{}{}
	}
	return m
}

var three = []string{"n1", "n2", "n3"}

// feed runs msgs through tr and returns everything delivered, in order, and
// for each message whether a vote was requested.
func feed(tr *tracker, msgs []msg) (delivered [][]byte, votes []bool) {
	for _, x := range msgs {
		d, v := tr.onMessage(x.m, x.raw)
		delivered = append(delivered, d...)
		votes = append(votes, v)
	}
	return delivered, votes
}

func TestTrackerDeliversAfterAllApprovesReadFromTopic(t *testing.T) {
	guard(t)
	p := proposal(t, "n1", 101)
	for _, self := range three {
		tr := newTracker(self, parties(three...))

		d, need := tr.onMessage(p.m, p.raw)
		if len(d) != 0 {
			t.Fatalf("self=%s: delivered at the proposal", self)
		}
		if want := self != "n1"; need != want {
			t.Fatalf("self=%s: needVote=%v, want %v", self, need, want)
		}
		// n3's approve first: n2's (possibly our own) vote is not read yet,
		// so nothing is delivered even if n2 already sent it.
		a3 := approve(t, "n3", 101)
		if d, need = tr.onMessage(a3.m, a3.raw); len(d) != 0 || need {
			t.Fatalf("self=%s: delivered=%d needVote=%v after 2 of 3 approvals", self, len(d), need)
		}
		a2 := approve(t, "n2", 101)
		d, need = tr.onMessage(a2.m, a2.raw)
		if need || len(d) != 1 || !bytes.Equal(d[0], p.raw) {
			t.Fatalf("self=%s: want exactly the proposal delivered at the last approve, got %d blocks needVote=%v", self, len(d), need)
		}
		// Exactly once: a repeated approve delivers nothing.
		if d, _ = tr.onMessage(a2.m, a2.raw); len(d) != 0 {
			t.Fatalf("self=%s: delivered twice", self)
		}
		if len(tr.pending) != 0 || tr.order.Len() != 0 {
			t.Fatalf("self=%s: pending not empty after delivery: %d/%d", self, len(tr.pending), tr.order.Len())
		}
	}
}

func TestTrackerSameOrderForEveryNode(t *testing.T) {
	guard(t)
	// Three blocks proposed in the order 1, 2, 3 but completed in the order
	// 3, 1 (2 is aborted); one unknown-party message and a late vote mixed in.
	msgs := []msg{
		proposal(t, "n1", 1),
		proposal(t, "n2", 2),
		approve(t, "n2", 1),
		proposal(t, "n3", 3),
		approve(t, "x9", 2),
		approve(t, "n1", 3),
		approve(t, "n2", 3), // completes 3
		abort(t, "n1", 2),   // drops 2
		approve(t, "n3", 2), // late, ignored
		approve(t, "n3", 1), // completes 1
		proposal(t, "n2", 4),
		approve(t, "n3", 4),
		approve(t, "n1", 4), // completes 4
	}
	want := [][]byte{msgs[3].raw, msgs[0].raw, msgs[10].raw}

	var first [][]byte
	for i, self := range three {
		tr := newTracker(self, parties(three...))
		got, _ := feed(tr, msgs)
		if len(got) != len(want) {
			t.Fatalf("self=%s: delivered %d blocks, want %d", self, len(got), len(want))
		}
		for j := range want {
			if !bytes.Equal(got[j], want[j]) {
				t.Fatalf("self=%s: block %d differs from the expected completion order", self, j)
			}
		}
		if i == 0 {
			first = got
			continue
		}
		for j := range first {
			if !bytes.Equal(got[j], first[j]) {
				t.Fatalf("self=%s: block %d differs from self=n1", self, j)
			}
		}
		if len(tr.pending) != 0 {
			t.Fatalf("self=%s: %d blocks still pending", self, len(tr.pending))
		}
	}
}

func TestTrackerDropsBlockOnAbort(t *testing.T) {
	guard(t)
	for _, self := range three {
		tr := newTracker(self, parties(three...))
		msgs := []msg{
			proposal(t, "n1", 7),
			approve(t, "n2", 7),
			abort(t, "n3", 7),
			approve(t, "n3", 7), // after the abort: ignored
		}
		got, _ := feed(tr, msgs)
		if len(got) != 0 {
			t.Fatalf("self=%s: aborted block delivered", self)
		}
		if len(tr.pending) != 0 || tr.order.Len() != 0 {
			t.Fatalf("self=%s: aborted block still pending", self)
		}
		// A re-sent proposal of the aborted block is not voted on again.
		p := proposal(t, "n1", 7)
		if d, need := tr.onMessage(p.m, p.raw); len(d) != 0 || need {
			t.Fatalf("self=%s: duplicate proposal of an aborted block reopened it", self)
		}
	}

	// This node's own Abort drops the block once it is read back from the
	// topic, like any other party's.
	tr := newTracker("n2", parties(three...))
	got, need := feed(tr, []msg{proposal(t, "n1", 8), abort(t, "n2", 8), approve(t, "n2", 8), approve(t, "n3", 8)})
	if len(got) != 0 {
		t.Fatal("block aborted by self delivered")
	}
	if !need[0] {
		t.Fatal("n2 was not asked to vote on n1's proposal")
	}
}

func TestTrackerIgnoresUnknownParty(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties("n1", "n2"))

	// A proposal from an outsider opens nothing and asks for no vote.
	px := proposal(t, "x9", 1)
	if d, need := tr.onMessage(px.m, px.raw); len(d) != 0 || need || len(tr.pending) != 0 {
		t.Fatalf("outsider proposal: delivered=%d needVote=%v pending=%d", len(d), need, len(tr.pending))
	}

	// Outsider votes neither complete nor abort a real block.
	p := proposal(t, "n1", 2)
	got, _ := feed(tr, []msg{p, approve(t, "x9", 2), abort(t, "x9", 2)})
	if len(got) != 0 || len(tr.pending) != 1 {
		t.Fatalf("outsider votes changed the block: delivered=%d pending=%d", len(got), len(tr.pending))
	}
	got, _ = feed(tr, []msg{approve(t, "n2", 2)})
	if len(got) != 1 || !bytes.Equal(got[0], p.raw) {
		t.Fatal("block not delivered after all real parties approved")
	}
}

func TestTrackerDuplicateVotesCountOnce(t *testing.T) {
	guard(t)
	tr := newTracker("n3", parties(three...))
	got, _ := feed(tr, []msg{
		proposal(t, "n1", 5),
		approve(t, "n2", 5),
		approve(t, "n2", 5),
		approve(t, "n2", 5),
		approve(t, "n1", 5), // the proposer's explicit approve: already counted
	})
	if len(got) != 0 {
		t.Fatal("duplicate votes completed the block")
	}
	got, _ = feed(tr, []msg{approve(t, "n3", 5)})
	if len(got) != 1 {
		t.Fatalf("delivered %d, want 1", len(got))
	}
}

func TestTrackerLateVotesAfterDelivery(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties(three...))
	p := proposal(t, "n1", 9)
	got, _ := feed(tr, []msg{p, approve(t, "n2", 9), approve(t, "n3", 9)})
	if len(got) != 1 {
		t.Fatalf("delivered %d, want 1", len(got))
	}
	// Late votes, a late abort and a duplicate proposal change nothing and
	// store nothing.
	got, votes := feed(tr, []msg{approve(t, "n2", 9), abort(t, "n3", 9), p, approve(t, "n2", 9), approve(t, "n3", 9)})
	if len(got) != 0 {
		t.Fatal("late messages delivered the block again")
	}
	for i, v := range votes {
		if v {
			t.Fatalf("message %d asked for a vote", i)
		}
	}
	if len(tr.pending) != 0 || tr.order.Len() != 0 {
		t.Fatalf("late messages left state behind: pending=%d", len(tr.pending))
	}
}

func TestTrackerVotesForUnknownSeqAreIgnored(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties(three...))
	got, votes := feed(tr, []msg{approve(t, "n2", 42), approve(t, "n3", 42), abort(t, "n2", 43)})
	if len(got) != 0 || len(tr.pending) != 0 || tr.order.Len() != 0 || len(tr.done) != 0 {
		t.Fatalf("votes for unknown seqs left state: delivered=%d pending=%d done=%d", len(got), len(tr.pending), len(tr.done))
	}
	for i, v := range votes {
		if v {
			t.Fatalf("vote %d asked for a vote", i)
		}
	}
	// A proposal for that seq read later is a fresh block: the earlier
	// (premature) votes did not count.
	p := proposal(t, "n2", 42)
	if d, need := tr.onMessage(p.m, p.raw); len(d) != 0 || !need {
		t.Fatalf("proposal after stray votes: delivered=%d needVote=%v", len(d), need)
	}
}

func TestTrackerConflictingProposalFirstWins(t *testing.T) {
	guard(t)
	tr := newTracker("n3", parties(three...))
	p1 := proposal(t, "n1", 11)
	p2 := proposal(t, "n2", 11) // same seq, other proposer
	got, votes := feed(tr, []msg{p1, p2, approve(t, "n2", 11), approve(t, "n3", 11)})
	if !votes[0] || votes[1] {
		t.Fatalf("needVote = %v, want only for the first proposal", votes)
	}
	if len(got) != 1 || !bytes.Equal(got[0], p1.raw) {
		t.Fatal("want the first proposal delivered once")
	}
}

func TestTrackerSinglePartyDeliversAtProposal(t *testing.T) {
	guard(t)
	tr := newTracker("n1", nil) // self is always a party
	p := proposal(t, "n1", 1)
	d, need := tr.onMessage(p.m, p.raw)
	if need || len(d) != 1 || !bytes.Equal(d[0], p.raw) {
		t.Fatalf("single party: delivered=%d needVote=%v", len(d), need)
	}
	if len(tr.pending) != 0 {
		t.Fatal("single party: block left pending")
	}
}

func TestTrackerEvictsOldestWhenFull(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties(three...))
	tr.maxPending = 2
	feed(tr, []msg{proposal(t, "n2", 1), proposal(t, "n2", 2), proposal(t, "n2", 3)})
	if len(tr.pending) != 2 || tr.order.Len() != 2 {
		t.Fatalf("pending=%d order=%d, want 2", len(tr.pending), tr.order.Len())
	}
	if _, ok := tr.pending[1]; ok {
		t.Fatal("oldest block not evicted")
	}
	// The evicted block can no longer complete.
	got, _ := feed(tr, []msg{approve(t, "n1", 1), approve(t, "n3", 1)})
	if len(got) != 0 {
		t.Fatal("evicted block delivered")
	}
	got, _ = feed(tr, []msg{approve(t, "n1", 2), approve(t, "n3", 2)})
	if len(got) != 1 {
		t.Fatalf("delivered %d, want 1 (block 2)", len(got))
	}
}

func TestTrackerDoneWindowIsBounded(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties(three...))
	tr.doneRing = make([]int64, 4)
	for seq := int64(1); seq <= 10; seq++ {
		feed(tr, []msg{proposal(t, "n2", seq), abort(t, "n3", seq)})
	}
	if len(tr.done) != 4 {
		t.Fatalf("done set has %d seqs, want 4", len(tr.done))
	}
	for seq := int64(7); seq <= 10; seq++ {
		if _, ok := tr.done[seq]; !ok {
			t.Fatalf("seq %d missing from the done window", seq)
		}
	}
}

func TestTrackerDeliveredBytesAreCopies(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties("n1", "n2"))
	p := proposal(t, "n1", 3)
	raw := append([]byte(nil), p.raw...)
	tr.onMessage(p.m, raw)
	for i := range raw { // the caller reuses its buffer
		raw[i] = 0
	}
	a := approve(t, "n2", 3)
	got, _ := tr.onMessage(a.m, a.raw)
	if len(got) != 1 || !bytes.Equal(got[0], p.raw) {
		t.Fatal("tracker kept a reference to the caller's buffer")
	}
}

func TestTrackerIgnoresMalformedMessages(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties(three...))
	for _, b := range []*pbv.Block{
		nil,
		{Type: pbv.MessageType_Approve, Signature: "n2"},
		{Type: pbv.MessageType_Approve, Signature: "n2", Txs: []*pbv.SharedLog{nil}},
		{Type: pbv.MessageType(7), Signature: "n2", Txs: []*pbv.SharedLog{{Seq: 1}}},
	} {
		if d, need := tr.onMessage(b, nil); len(d) != 0 || need {
			t.Fatalf("malformed message %v: delivered=%d needVote=%v", b, len(d), need)
		}
	}
	if len(tr.pending) != 0 {
		t.Fatal("malformed message opened a block")
	}
}

func TestTrackerReportsEachDropOnce(t *testing.T) {
	guard(t)
	tr := newTracker("n1", parties(three...))
	tr.maxPending = 2
	var dropped [][]byte
	tr.onDrop = func(raw []byte) { dropped = append(dropped, raw) }

	p1, p2, p3 := proposal(t, "n2", 1), proposal(t, "n3", 2), proposal(t, "n2", 3)
	feed(tr, []msg{
		p1,
		abort(t, "n3", 1), // drops 1: reported
		abort(t, "n1", 1), // late abort: not reported again
		abort(t, "n2", 5), // unknown seq: not reported
		p2,
		approve(t, "n1", 2),
		approve(t, "n2", 2), // delivered, not dropped
		abort(t, "n1", 2),   // late abort after delivery: not reported
		p3,
		proposal(t, "n2", 4),
		proposal(t, "n2", 6), // evicts 3 (oldest pending): reported
		abort(t, "n3", 3),    // late abort of the evicted block: not reported again
		approve(t, "n3", 3),
	})
	if len(dropped) != 2 || !bytes.Equal(dropped[0], p1.raw) || !bytes.Equal(dropped[1], p3.raw) {
		t.Fatalf("onDrop called %d times, want exactly block 1 (abort) then block 3 (eviction)", len(dropped))
	}
}
