package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"hybrid/consensus"
	"hybrid/consensus/memnet"
)

const testTimeout = 5 * time.Second

// recvWithin reads one block from ch or fails the test after d.
func recvWithin(t *testing.T, ch <-chan []byte, d time.Duration) []byte {
	t.Helper()
	select {
	case b, ok := <-ch:
		if !ok {
			t.Fatal("deliver channel closed")
		}
		return b
	case <-time.After(d):
		t.Fatalf("no block delivered within %v", d)
		return nil
	}
}

// expectNone fails if ch yields a block within d.
func expectNone(t *testing.T, ch <-chan []byte, d time.Duration) {
	t.Helper()
	select {
	case b := <-ch:
		t.Fatalf("unexpected delivery %q", b)
	case <-time.After(d):
	}
}

// newTestNet returns a 2-node hub where node 0 is a plain memnet node whose
// deliveries are drained in the background, and node 1 is wrapped in a
// faultyNet.
func newTestNet(t *testing.T) (plain *memnetDrainer, fn *faultyNet) {
	t.Helper()
	hub := memnet.NewHub(2)
	fn = newFaultyNet(hub.Node(1), 50*time.Millisecond)
	t.Cleanup(func() { fn.Close() })
	plain = &memnetDrainer{c: hub.Node(0), done: make(chan struct{})}
	go plain.run()
	t.Cleanup(plain.close)
	return plain, fn
}

type memnetDrainer struct {
	c    consensus.Consensus
	done chan struct{}
}

func (d *memnetDrainer) run() {
	defer close(d.done)
	for range d.c.Deliver() {
	}
}

func (d *memnetDrainer) close() {
	d.c.Close()
	<-d.done
}

func submit(t *testing.T, c consensus.Consensus, blk string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := c.Submit(ctx, []byte(blk)); err != nil {
		t.Fatalf("Submit(%q): %v", blk, err)
	}
}

func TestFaultyNetNormalPassesThrough(t *testing.T) {
	plain, fn := newTestNet(t)
	submit(t, plain.c, "a")
	submit(t, fn, "b")
	if got := string(recvWithin(t, fn.Deliver(), testTimeout)); got != "a" {
		t.Fatalf("first block = %q, want a", got)
	}
	if got := string(recvWithin(t, fn.Deliver(), testTimeout)); got != "b" {
		t.Fatalf("second block = %q, want b", got)
	}
}

func TestFaultyNetCrashBuffersAndReplays(t *testing.T) {
	plain, fn := newTestNet(t)
	fn.setState(netCrashed)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fn.Submit(ctx, []byte("x")); err == nil {
		t.Fatal("Submit on a crashed net succeeded")
	}

	// 100 > memnet.NodeBuffer: without the always-draining goroutine the
	// peer's Submit would block once the crashed node's buffer filled.
	const blocks = 100
	for i := 0; i < blocks; i++ {
		submit(t, plain.c, fmt.Sprint(i))
	}
	expectNone(t, fn.Deliver(), 100*time.Millisecond)

	fn.setState(netNormal)
	for i := 0; i < blocks; i++ {
		if got, want := string(recvWithin(t, fn.Deliver(), testTimeout)), fmt.Sprint(i); got != want {
			t.Fatalf("replayed block %d = %q, want %q", i, got, want)
		}
	}
}

func TestFaultyNetSilentDropsSubmit(t *testing.T) {
	plain, fn := newTestNet(t)
	fn.setState(netSilent)

	submit(t, fn, "dropped") // accepted but never ordered
	submit(t, plain.c, "kept")
	if got := string(recvWithin(t, fn.Deliver(), testTimeout)); got != "kept" {
		t.Fatalf("silent node delivered %q, want kept (its own block must be swallowed)", got)
	}
	expectNone(t, fn.Deliver(), 100*time.Millisecond)
}

func TestFaultyNetSlowDelays(t *testing.T) {
	plain, fn := newTestNet(t)
	fn.setState(netSlow)
	start := time.Now()
	submit(t, plain.c, "late")
	recvWithin(t, fn.Deliver(), testTimeout)
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Fatalf("slow delivery took %v, want >= 50ms", d)
	}
}

func TestFaultyNetCloseClosesDeliver(t *testing.T) {
	_, fn := newTestNet(t)
	fn.setState(netCrashed)
	fn.Close()
	select {
	case _, ok := <-fn.Deliver():
		if ok {
			t.Fatal("block delivered after Close")
		}
	case <-time.After(testTimeout):
		t.Fatal("Deliver not closed by Close")
	}
}
