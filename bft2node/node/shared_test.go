package node

import (
	"context"
	"testing"
	"time"

	"github.com/relab/hotstuff"
)

func TestQueueInflightRequeue(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	q.requeueViews = 8
	_ = q.push("a")
	_ = q.push("b")
	id := cmdID([]byte("b"))
	if !q.markInflight(id, 10) {
		t.Fatal("markInflight of a queued id returned false")
	}
	if q.len() != 1 {
		t.Fatalf("len after markInflight = %d, want 1", q.len())
	}
	if n := q.requeueStale(17); n != 0 {
		t.Fatalf("requeueStale(17) = %d, want 0 (only 7 views)", n)
	}
	if n := q.requeueStale(19); n != 1 {
		t.Fatalf("requeueStale(19) = %d, want 1", n)
	}
	if cmd, ok := q.pop(context.Background(), time.Millisecond); !ok || cmd != "b" {
		t.Fatalf("pop after requeue = %q, %v; want b at the front", cmd, ok)
	}
}

func TestQueueCommitRemovesQueuedAndInflight(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	_ = q.push("a")
	_ = q.push("b")
	q.markInflight(cmdID([]byte("a")), 1)
	q.commit(cmdID([]byte("a")))
	q.commit(cmdID([]byte("b")))
	if q.len() != 0 || q.inflightLen() != 0 {
		t.Fatalf("after commit: len %d inflight %d, want 0 0", q.len(), q.inflightLen())
	}
	if n := q.requeueStale(1000); n != 0 {
		t.Fatalf("committed id requeued (%d)", n)
	}
}

func TestQueuePushIgnoresKnownAndCommittedIDs(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	committed := map[[32]byte]bool{cmdID([]byte("old")): true}
	q.seen = func(id [32]byte) bool { return committed[id] }
	_ = q.push("x")
	_ = q.push("x") // a forwarded copy of a queued block
	q.markInflight(cmdID([]byte("x")), 3)
	_ = q.push("x") // and of an inflight one
	_ = q.push("old")
	if q.len() != 0 || q.inflightLen() != 1 {
		t.Fatalf("len %d inflight %d, want 0 1", q.len(), q.inflightLen())
	}
}

func TestCommitLogSkipsDuplicateIDs(t *testing.T) {
	l := NewCommitLog(1 << 20)
	l.dedup = newIDRing(4)
	id := cmdID([]byte("blk"))
	if !l.appendOnce(id, []byte("blk")) {
		t.Fatal("first appendOnce returned false")
	}
	if l.appendOnce(id, []byte("blk")) {
		t.Fatal("second appendOnce returned true")
	}
	_, items, _ := l.Peek(context.Background(), 10)
	if len(items) != 1 {
		t.Fatalf("log has %d items, want 1", len(items))
	}
	// The window keeps the last 4 ids: after 4 others, blk is forgotten.
	for _, s := range []string{"1", "2", "3", "4"} {
		l.appendOnce(cmdID([]byte(s)), []byte(s))
	}
	if !l.appendOnce(id, []byte("blk")) {
		t.Fatal("id outside the dedup window still refused")
	}
}

// Exec of the same block twice (two leaders proposed one shared block)
// delivers it once and removes it from the queue of every replica.
func TestAdapterExecDeliversOnceAndAcceptMarksInflight(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	l := NewCommitLog(1 << 20)
	a := &adapter{id: 1, queue: q, log: l, idle: time.Millisecond, view: func() uint64 { return 7 }}
	q.seen = l.seen
	_ = q.push("shared")
	if !a.Accept("shared") {
		t.Fatal("Accept returned false")
	}
	if q.len() != 0 || q.inflightLen() != 1 {
		t.Fatalf("after Accept: len %d inflight %d, want 0 1", q.len(), q.inflightLen())
	}
	var qc hotstuff.QuorumCert
	a.Exec(hotstuff.NewBlock(hotstuff.Hash{}, qc, "shared", 8, 2))
	a.Exec(hotstuff.NewBlock(hotstuff.Hash{}, qc, "shared", 9, 3))
	_, items, _ := l.Peek(context.Background(), 10)
	if len(items) != 1 {
		t.Fatalf("delivered %d, want 1", len(items))
	}
	if a.duplicates.Load() != 1 {
		t.Fatalf("duplicates = %d, want 1", a.duplicates.Load())
	}
	if q.inflightLen() != 0 {
		t.Fatal("committed block still inflight")
	}
	_ = q.push("shared") // a late forward of a committed block
	if q.len() != 0 {
		t.Fatal("late forward of a committed block queued again")
	}
}

// Fork of another replica's block that is inflight here puts it back in the
// queue (every replica holds every pending block).
func TestAdapterForkRequeuesInflightBlocksOfOthers(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	a := &adapter{id: 1, queue: q, log: NewCommitLog(1 << 20), idle: time.Millisecond, view: func() uint64 { return 1 }}
	_ = q.push("theirs")
	a.Accept("theirs")
	var qc hotstuff.QuorumCert
	a.Fork(hotstuff.NewBlock(hotstuff.Hash{}, qc, "theirs", 4, 3))
	if cmd, ok := q.pop(context.Background(), time.Millisecond); !ok || cmd != "theirs" {
		t.Fatalf("pop after Fork = %q, %v; want theirs", cmd, ok)
	}
}

// TestForwardedBlockCommitsFromAnyLeader: 100 commands submitted on replica
// 2 only are delivered on every replica, each exactly once, and other
// leaders than replica 2 proposed some of them (Stats.Proposed).
func TestForwardedBlockCommitsFromAnyLeader(t *testing.T) {
	const n, total = 4, 100
	nodes := testClusterOpts(t, n, true)
	cs := make([]*collector, n)
	for i, nd := range nodes {
		cs[i] = collect(nd)
	}
	var all [][]byte
	for k := 0; k < total; k++ {
		p := payload(2, k, 32)
		all = append(all, p)
		for {
			err := nodes[1].Submit(p)
			if err == nil {
				break
			}
			if err != ErrQueueFull {
				t.Fatalf("Submit %d: %v", k, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitCount(t, cs, total, 90*time.Second)
	time.Sleep(500 * time.Millisecond)
	seqs := make([][][]byte, n)
	for i, c := range cs {
		seqs[i], _ = c.snapshot()
	}
	sameSequence(t, seqs)
	exactlyOnce(t, seqs[0], all)
	var others uint64
	for i, nd := range nodes {
		if i != 1 {
			others += nd.Stats().Proposed
		}
	}
	if others == 0 {
		t.Fatal("no leader other than replica 2 proposed a command of replica 2")
	}
}

// TestForwardSurvivesOneCrash: replica 4 stops after 20 commands; the
// commands submitted on replica 1 all commit on the three survivors.
func TestForwardSurvivesOneCrash(t *testing.T) {
	const n = 4
	nodes := testClusterOpts(t, n, true)
	cs := make([]*collector, n)
	for i, nd := range nodes {
		cs[i] = collect(nd)
	}
	var all [][]byte
	submit := func(k int) {
		p := payload(1, k, 32)
		all = append(all, p)
		if err := nodes[0].Submit(p); err != nil {
			t.Fatalf("Submit %d: %v", k, err)
		}
	}
	for k := 0; k < 20; k++ {
		submit(k)
	}
	waitCount(t, cs, 20, 60*time.Second)
	nodes[3].Stop()
	for k := 20; k < 60; k++ {
		submit(k)
	}
	waitCount(t, cs[:3], 60, 90*time.Second)
	time.Sleep(500 * time.Millisecond)
	seqs := make([][][]byte, 3)
	for i := range seqs {
		seqs[i], _ = cs[i].snapshot()
	}
	sameSequence(t, seqs)
	exactlyOnce(t, seqs[0], all)
}
