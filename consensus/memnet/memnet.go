// Package memnet is an in-memory consensus backend for tests. A Hub fans every
// submitted block out to all of its nodes under one mutex, so all nodes see the
// same total order. It is not meant for production use.
package memnet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"hybrid/consensus"
)

// NodeBuffer is the capacity of each node's Deliver channel. A goroutine that
// both drains a node's Deliver and calls Submit can block itself once that
// buffer is full; only ctx cancellation releases it.
const NodeBuffer = 64

// pollInterval is how often a blocked Submit rechecks for free buffer space.
const pollInterval = 200 * time.Microsecond

// ErrClosed is returned by Submit on a node that has been closed.
var ErrClosed = errors.New("memnet: node closed")

// Hub connects n nodes through a single total order.
type Hub struct {
	lock  chan struct{} // capacity 1; used as a mutex that honours ctx
	nodes []*node
}

// NewHub creates a hub with n nodes.
func NewHub(n int) *Hub {
	h := &Hub{lock: make(chan struct{}, 1), nodes: make([]*node, n)}
	for i := range h.nodes {
		h.nodes[i] = &node{
			hub:  h,
			out:  make(chan []byte, NodeBuffer),
			done: make(chan struct{}),
		}
	}
	return h
}

// Node returns the consensus endpoint of node i. It panics if i is out of range.
func (h *Hub) Node(i int) consensus.Consensus {
	return h.nodes[i]
}

// Drop makes node i silently discard delivered blocks while on is true.
// Dropped blocks are not queued and are not replayed when on becomes false.
func (h *Hub) Drop(i int, on bool) {
	var v int32
	if on {
		v = 1
	}
	atomic.StoreInt32(&h.nodes[i].dropped, v)
}

// acquire takes the hub mutex, giving up if ctx is cancelled first.
func (h *Hub) acquire(ctx context.Context) error {
	select {
	case h.lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Hub) release() { <-h.lock }

// targets returns the nodes that should receive the next block, and the first
// of them whose buffer is full (nil if all have room).
func (h *Hub) targets() (live []*node, full *node) {
	for _, n := range h.nodes {
		if n.isClosed() || atomic.LoadInt32(&n.dropped) != 0 {
			continue
		}
		live = append(live, n)
		if full == nil && len(n.out) >= cap(n.out) {
			full = n
		}
	}
	return live, full
}

// broadcast delivers blk to every live node as one atomic step. If a live
// node's buffer is full it waits for the consumer, or returns ctx.Err() when
// ctx ends first. In that case no node has received the block, so the nodes
// never diverge. The caller must hold the hub mutex.
func (h *Hub) broadcast(ctx context.Context, blk []byte) error {
	for {
		live, full := h.targets()
		if full == nil {
			// Only the mutex holder sends, so every buffer that had room
			// above still has it and the sends below never block.
			for _, n := range live {
				n.push(append([]byte(nil), blk...))
			}
			return nil
		}
		t := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

type node struct {
	hub     *Hub
	out     chan []byte
	done    chan struct{} // closed when Close is first called
	once    sync.Once
	dropped int32 // accessed atomically

	mu   sync.Mutex // guards shut and the close of out against push
	shut bool
}

var _ consensus.Consensus = (*node)(nil)

func (n *node) isClosed() bool {
	select {
	case <-n.done:
		return true
	default:
		return false
	}
}

// push appends blk to the node's buffer unless the node is already shut.
func (n *node) push(blk []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.shut {
		n.out <- blk
	}
}

// Submit copies blk and delivers it to all nodes in the hub's total order.
// When a node's buffer (NodeBuffer blocks) is full, Submit blocks until the
// consumer makes room or ctx ends, and then returns ctx.Err() with the block
// delivered to no one.
func (n *node) Submit(ctx context.Context, blk []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.isClosed() {
		return ErrClosed
	}
	if err := n.hub.acquire(ctx); err != nil {
		return err
	}
	defer n.hub.release()
	if n.isClosed() {
		return ErrClosed
	}
	return n.hub.broadcast(ctx, blk)
}

// Deliver returns the same channel on every call.
func (n *node) Deliver() <-chan []byte { return n.out }

// Close closes the Deliver channel; blocks already buffered stay readable.
// It is idempotent.
func (n *node) Close() error {
	n.once.Do(func() {
		close(n.done) // lets a Submit that is waiting on us move on
		n.mu.Lock()
		n.shut = true
		close(n.out)
		n.mu.Unlock()
	})
	return nil
}
