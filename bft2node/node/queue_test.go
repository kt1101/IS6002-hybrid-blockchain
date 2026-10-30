package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/relab/hotstuff"
)

func TestQueueFIFOAndLimits(t *testing.T) {
	q := newCmdQueue(2, 10)
	if err := q.push("abcd"); err != nil {
		t.Fatal(err)
	}
	if err := q.push("efgh"); err != nil {
		t.Fatal(err)
	}
	if err := q.push("i"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third push (count limit): err = %v, want ErrQueueFull", err)
	}
	cmd, ok := q.pop(context.Background(), time.Second)
	if !ok || cmd != "abcd" {
		t.Fatalf("pop = %q, %v; want abcd", cmd, ok)
	}
	if err := q.push("ijklmnopq"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("push over the byte limit: err = %v, want ErrQueueFull", err)
	}
	// A forked command goes back to the front, even over the limits.
	q.pushFront("forked-command")
	if cmd, _ := q.pop(context.Background(), time.Second); cmd != "forked-command" {
		t.Fatalf("pop after pushFront = %q, want forked-command", cmd)
	}
	if cmd, _ := q.pop(context.Background(), time.Second); cmd != "efgh" {
		t.Fatalf("pop = %q, want efgh", cmd)
	}
}

func TestQueuePopWaitsAtMostIdle(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	start := time.Now()
	if cmd, ok := q.pop(context.Background(), 30*time.Millisecond); ok {
		t.Fatalf("pop on an empty queue = %q, true; want false", cmd)
	}
	if d := time.Since(start); d < 25*time.Millisecond || d > 2*time.Second {
		t.Fatalf("empty pop took %v, want about the idle wait (30ms)", d)
	}
	// A push wakes a waiting pop.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = q.push("late")
	}()
	if cmd, ok := q.pop(context.Background(), 5*time.Second); !ok || cmd != "late" {
		t.Fatalf("pop = %q, %v; want late", cmd, ok)
	}
	// A cancelled ctx ends the wait early.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start = time.Now()
	if _, ok := q.pop(ctx, 5*time.Second); ok || time.Since(start) > time.Second {
		t.Fatalf("pop with a cancelled ctx: ok=%v after %v", ok, time.Since(start))
	}
}

func TestQueueCloseWakesPopAndRefusesPush(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	done := make(chan bool)
	go func() {
		_, ok := q.pop(context.Background(), time.Minute)
		done <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	q.close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("pop returned a command after close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not wake pop")
	}
	if err := q.push("x"); !errors.Is(err, ErrStopped) {
		t.Fatalf("push after close: err = %v, want ErrStopped", err)
	}
}

// The leader's Get never stalls the protocol: with an empty queue it returns
// an empty filler command after the idle wait (ok=true, so the library still
// proposes a block and earlier blocks keep committing), ok=false only when
// its ctx ends (view change or timeout) or the node stops.
func TestAdapterGet(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	a := &adapter{id: 1, queue: q, log: NewCommitLog(1 << 20), idle: 10 * time.Millisecond}
	if cmd, ok := a.Get(context.Background()); !ok || cmd != "" {
		t.Fatalf("Get on an empty queue = %q, %v; want the empty filler, true", cmd, ok)
	}
	_ = q.push("blk")
	if cmd, ok := a.Get(context.Background()); !ok || cmd != "blk" {
		t.Fatalf("Get = %q, %v; want blk, true", cmd, ok)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := a.Get(ctx); ok {
		t.Fatal("Get with a cancelled ctx returned ok=true")
	}
	q.close()
	if _, ok := a.Get(context.Background()); ok {
		t.Fatal("Get after close returned ok=true")
	}
}

func TestAdapterExecSkipsFillerAndForkRequeuesOwnBlocks(t *testing.T) {
	q := newCmdQueue(10, 1<<20)
	l := NewCommitLog(1 << 20)
	a := &adapter{id: 2, queue: q, log: l, idle: time.Millisecond}
	var qc hotstuff.QuorumCert
	a.Exec(hotstuff.NewBlock(hotstuff.Hash{}, qc, "", 1, 1))
	a.Exec(hotstuff.NewBlock(hotstuff.Hash{}, qc, "one", 2, 2))
	a.Exec(hotstuff.NewBlock(hotstuff.Hash{}, qc, "two", 3, 3))
	first, items, err := l.Peek(context.Background(), 10)
	if err != nil || first != 0 || len(items) != 2 || string(items[0]) != "one" || string(items[1]) != "two" {
		t.Fatalf("Peek = %d, %q, %v; want 0, [one two]", first, items, err)
	}
	a.Fork(hotstuff.NewBlock(hotstuff.Hash{}, qc, "other's", 4, 3)) // not ours: dropped
	a.Fork(hotstuff.NewBlock(hotstuff.Hash{}, qc, "", 5, 2))        // filler: dropped
	a.Fork(hotstuff.NewBlock(hotstuff.Hash{}, qc, "mine", 6, 2))
	if cmd, ok := q.pop(context.Background(), time.Millisecond); !ok || cmd != "mine" {
		t.Fatalf("after Fork: pop = %q, %v; want mine", cmd, ok)
	}
	if cmd, ok := q.pop(context.Background(), time.Millisecond); ok {
		t.Fatalf("after Fork: second pop = %q, want nothing", cmd)
	}
}
