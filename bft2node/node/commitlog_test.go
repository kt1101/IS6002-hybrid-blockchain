package node

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommitLogPeekAdvance(t *testing.T) {
	l := NewCommitLog(1 << 20)
	l.Add([]byte("a"))
	l.Add([]byte("b"))
	l.Add([]byte("c"))
	first, items, err := l.Peek(context.Background(), 2)
	if err != nil || first != 0 || len(items) != 2 || string(items[0]) != "a" || string(items[1]) != "b" {
		t.Fatalf("Peek(2) = %d, %q, %v", first, items, err)
	}
	// Peek does not consume: the same items come back until Advance.
	if first, items, _ = l.Peek(context.Background(), 10); first != 0 || len(items) != 3 {
		t.Fatalf("second Peek = %d, %d items; want 0, 3", first, len(items))
	}
	l.Advance(2)
	first, items, err = l.Peek(context.Background(), 10)
	if err != nil || first != 2 || len(items) != 1 || string(items[0]) != "c" {
		t.Fatalf("Peek after Advance(2) = %d, %q, %v; want 2, [c]", first, items, err)
	}
	l.Advance(1)
	// Empty: Peek waits for the next append, which gets seq 3.
	go func() {
		time.Sleep(20 * time.Millisecond)
		l.Add([]byte("d"))
	}()
	first, items, err = l.Peek(context.Background(), 10)
	if err != nil || first != 3 || len(items) != 1 || string(items[0]) != "d" {
		t.Fatalf("Peek after wait = %d, %q, %v; want 3, [d]", first, items, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	l.Advance(1)
	if _, _, err := l.Peek(ctx, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Peek on an empty log with a short ctx: err = %v, want DeadlineExceeded", err)
	}
}

// Add blocks while the undelivered bytes are over the limit (backpressure
// on the event loop), Advance releases it, and Close releases it for good
// (the command is then dropped).
func TestCommitLogBackpressureAndClose(t *testing.T) {
	l := NewCommitLog(10)
	l.Add(make([]byte, 8))
	done := make(chan struct{})
	go func() {
		l.Add(make([]byte, 8)) // 16 > 10: waits
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("append did not wait for room")
	case <-time.After(50 * time.Millisecond):
	}
	l.Advance(1)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Advance did not release the waiting append")
	}
	// A single command larger than the limit still gets in when the log is empty.
	l.Advance(1)
	l.Add(make([]byte, 100))

	blocked := make(chan struct{})
	go func() {
		l.Add(make([]byte, 1))
		close(blocked)
	}()
	time.Sleep(20 * time.Millisecond)
	l.Close()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release the waiting append")
	}
	if _, _, err := l.Peek(context.Background(), 10); !errors.Is(err, ErrStopped) {
		t.Fatalf("Peek after Close: err = %v, want ErrStopped", err)
	}
	l.Close() // idempotent
	l.Add([]byte("dropped"))
}
