package memnet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

// recv reads one block or fails the test after testTimeout.
func recv(t *testing.T, ch <-chan []byte) ([]byte, bool) {
	t.Helper()
	select {
	case b, ok := <-ch:
		return b, ok
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Deliver")
		return nil, false
	}
}

// pending reports whether a block is immediately readable from ch.
func pending(ch <-chan []byte) bool {
	select {
	case _, ok := <-ch:
		return ok
	default:
		return false
	}
}

func TestMemnetSameOrderAllNodes(t *testing.T) {
	const nodes, perNode = 3, 100
	const total = nodes * perNode
	h := NewHub(nodes)

	got := make([][]string, nodes)
	var readers sync.WaitGroup
	for i := 0; i < nodes; i++ {
		readers.Add(1)
		go func(i int) {
			defer readers.Done()
			ch := h.Node(i).Deliver()
			for len(got[i]) < total {
				select {
				case b := <-ch:
					got[i] = append(got[i], string(b))
				case <-time.After(testTimeout):
					return
				}
			}
		}(i)
	}

	var writers sync.WaitGroup
	errs := make([]error, nodes)
	for i := 0; i < nodes; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			buf := make([]byte, 0, 16)
			for j := 0; j < perNode; j++ {
				buf = append(buf[:0], fmt.Sprintf("n%d-b%03d", i, j)...)
				if err := h.Node(i).Submit(context.Background(), buf); err != nil {
					errs[i] = err
					return
				}
			}
		}(i)
	}
	writers.Wait()
	readers.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("node %d Submit: %v", i, err)
		}
	}
	for i := 0; i < nodes; i++ {
		if len(got[i]) != total {
			t.Fatalf("node %d delivered %d blocks, want %d", i, len(got[i]), total)
		}
	}
	for i := 1; i < nodes; i++ {
		for k := range got[0] {
			if got[i][k] != got[0][k] {
				t.Fatalf("order differs at %d: node 0 %q, node %d %q", k, got[0][k], i, got[i][k])
			}
		}
	}

	// Every block appears once, and each submitter's blocks keep their order.
	next := make([]int, nodes)
	for _, s := range got[0] {
		var from, seq int
		if _, err := fmt.Sscanf(s, "n%d-b%d", &from, &seq); err != nil {
			t.Fatalf("bad block %q: %v", s, err)
		}
		if seq != next[from] {
			t.Fatalf("node %d block %d arrived where %d was expected", from, seq, next[from])
		}
		next[from]++
	}
	for i, n := range next {
		if n != perNode {
			t.Fatalf("node %d: %d blocks delivered, want %d", i, n, perNode)
		}
	}

	for i := 0; i < nodes; i++ {
		h.Node(i).Close()
	}
}

func TestMemnetSubmitCopiesBuffer(t *testing.T) {
	h := NewHub(2)
	buf := []byte("hello")
	if err := h.Node(0).Submit(context.Background(), buf); err != nil {
		t.Fatal(err)
	}
	copy(buf, "XXXXX")
	a, _ := recv(t, h.Node(0).Deliver())
	b, _ := recv(t, h.Node(1).Deliver())
	if string(a) != "hello" || string(b) != "hello" {
		t.Fatalf("got %q and %q, want hello", a, b)
	}
	a[0] = 'J' // receivers must not share storage
	if string(b) != "hello" {
		t.Fatalf("nodes share a buffer: %q", b)
	}
}

func TestMemnetDeliverSameChannel(t *testing.T) {
	h := NewHub(1)
	if h.Node(0).Deliver() != h.Node(0).Deliver() {
		t.Fatal("Deliver returned different channels")
	}
}

func TestMemnetCloseClosesDeliver(t *testing.T) {
	h := NewHub(2)
	n := h.Node(0)
	for i := 0; i < 3; i++ {
		if err := n.Submit(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Blocks buffered before Close are still readable, then the channel ends.
	for i := 0; i < 3; i++ {
		b, ok := recv(t, n.Deliver())
		if !ok || len(b) != 1 || b[0] != byte(i) {
			t.Fatalf("block %d: got %v ok=%v", i, b, ok)
		}
	}
	if _, ok := recv(t, n.Deliver()); ok {
		t.Fatal("Deliver channel still open after Close")
	}

	if err := n.Submit(context.Background(), []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close: got %v, want ErrClosed", err)
	}

	// The remaining node keeps working and no longer feeds the closed one.
	other := h.Node(1)
	for i := 0; i < 3; i++ { // skip the blocks submitted before the Close
		recv(t, other.Deliver())
	}
	if err := other.Submit(context.Background(), []byte("y")); err != nil {
		t.Fatalf("Submit on open node: %v", err)
	}
	if b, _ := recv(t, other.Deliver()); string(b) != "y" {
		t.Fatalf("got %q, want y", b)
	}
	other.Close()
}

func TestMemnetDrop(t *testing.T) {
	h := NewHub(3)
	h.Drop(1, true)
	for i := 0; i < 5; i++ {
		if err := h.Node(0).Submit(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, i := range []int{0, 2} {
		for k := 0; k < 5; k++ {
			if b, _ := recv(t, h.Node(i).Deliver()); b[0] != byte(k) {
				t.Fatalf("node %d block %d: got %v", i, k, b)
			}
		}
	}
	if pending(h.Node(1).Deliver()) {
		t.Fatal("dropped node received a block")
	}

	// Dropped blocks are gone for good; only new ones arrive.
	h.Drop(1, false)
	if err := h.Node(2).Submit(context.Background(), []byte{9}); err != nil {
		t.Fatal(err)
	}
	if b, _ := recv(t, h.Node(1).Deliver()); b[0] != 9 {
		t.Fatalf("got %v, want [9]", b)
	}
	if pending(h.Node(1).Deliver()) {
		t.Fatal("dropped blocks were replayed")
	}

	// A dropped node queues nothing, so it never stalls Submit.
	h.Drop(1, true)
	for i := 0; i < 2*NodeBuffer; i++ {
		if err := h.Node(0).Submit(context.Background(), []byte{1}); err != nil {
			t.Fatal(err)
		}
		recv(t, h.Node(0).Deliver())
		recv(t, h.Node(2).Deliver())
	}
}

func TestMemnetSubmitCancelledContext(t *testing.T) {
	h := NewHub(2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Node(0).Submit(ctx, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if pending(h.Node(0).Deliver()) || pending(h.Node(1).Deliver()) {
		t.Fatal("block delivered despite cancelled context")
	}
}

func TestMemnetSubmitUnblocksOnCancel(t *testing.T) {
	h := NewHub(2)
	bg := context.Background()
	// Fill both buffers; nobody reads, so the next Submit must block.
	for i := 0; i < NodeBuffer; i++ {
		if err := h.Node(0).Submit(bg, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Free node 0 only. A failed Submit must then reach neither node.
	for i := 0; i < NodeBuffer; i++ {
		recv(t, h.Node(0).Deliver())
	}

	ctx, cancel := context.WithCancel(bg)
	result := make(chan error, 1)
	go func() { result <- h.Node(0).Submit(ctx, []byte("blocked")) }()

	select {
	case err := <-result:
		t.Fatalf("Submit returned %v while node 1 buffer is full", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Submit did not return after cancel")
	}
	if pending(h.Node(0).Deliver()) {
		t.Fatal("cancelled Submit reached node 0 but not node 1")
	}

	// Once the consumer catches up, Submit works again.
	for i := 0; i < NodeBuffer; i++ {
		recv(t, h.Node(1).Deliver())
	}
	if err := h.Node(0).Submit(bg, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if b, _ := recv(t, h.Node(i).Deliver()); string(b) != "ok" {
			t.Fatalf("node %d got %q, want ok", i, b)
		}
	}
}

func TestMemnetSubmitUnblocksOnConsumer(t *testing.T) {
	h := NewHub(1)
	bg := context.Background()
	for i := 0; i < NodeBuffer; i++ {
		if err := h.Node(0).Submit(bg, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	result := make(chan error, 1)
	go func() { result <- h.Node(0).Submit(bg, []byte("late")) }()
	recv(t, h.Node(0).Deliver()) // make room for one block
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Submit still blocked after the consumer made room")
	}
}

func TestMemnetCloseUnblocksSubmit(t *testing.T) {
	h := NewHub(2)
	bg := context.Background()
	for i := 0; i < NodeBuffer; i++ {
		if err := h.Node(0).Submit(bg, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	result := make(chan error, 1)
	go func() { result <- h.Node(0).Submit(bg, []byte("stuck")) }()
	time.Sleep(10 * time.Millisecond)
	// Close must not wait for the blocked Submit. It takes node 1 out of the
	// fan-out; node 0 is then the only full buffer, so drain one block there.
	if err := h.Node(1).Close(); err != nil {
		t.Fatal(err)
	}
	recv(t, h.Node(0).Deliver())
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Submit stayed blocked after Close")
	}
}
