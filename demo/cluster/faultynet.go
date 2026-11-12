package cluster

import (
	"context"
	"sync"
	"time"

	"hybrid/consensus"
)

// netState is the network fault a faultyNet is simulating.
type netState int

const (
	netNormal  netState = iota // pass through
	netCrashed                 // hold deliveries, reject Submit
	netSilent                  // swallow Submit, deliver normally
	netSlow                    // delay Submit and every delivery
)

// faultyNet wraps one consensus.Consensus endpoint and injects network-level
// faults. A drain goroutine ALWAYS reads the inner Deliver channel into an
// unbounded queue, whatever the state, so a faulty node never blocks its
// peers on a shared hub (memnet.NodeBuffer is only 64). A forward goroutine
// moves the queue to Deliver() in order, except while crashed: then it holds
// the queue, and switching back to another state replays it, so the node
// catches up.
type faultyNet struct {
	inner consensus.Consensus
	delay time.Duration // the slow fault's extra latency
	out   chan []byte

	mu          sync.Mutex
	state       netState
	queue       []queued
	innerClosed bool

	wake      chan struct{} // capacity 1: the queue or the state changed
	done      chan struct{} // closed by Close
	closeOnce sync.Once
	closeErr  error
	drained   chan struct{} // closed when the drain goroutine returns
	forwarded chan struct{} // closed when the forward goroutine returns
}

type queued struct {
	blk []byte
	at  time.Time // when the inner backend delivered it
}

var _ consensus.Consensus = (*faultyNet)(nil)

// newFaultyNet wraps inner, which it owns from now on (Close closes it).
// slowDelay is the extra latency of the slow state.
func newFaultyNet(inner consensus.Consensus, slowDelay time.Duration) *faultyNet {
	n := &faultyNet{
		inner:     inner,
		delay:     slowDelay,
		out:       make(chan []byte),
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		drained:   make(chan struct{}),
		forwarded: make(chan struct{}),
	}
	go n.drain()
	go n.forward()
	return n
}

// setState switches the simulated fault; leaving netCrashed replays the
// blocks held meanwhile.
func (n *faultyNet) setState(s netState) {
	n.mu.Lock()
	n.state = s
	n.mu.Unlock()
	n.signal()
}

func (n *faultyNet) getState() netState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *faultyNet) signal() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// Submit implements consensus.Consensus: crashed fails with ErrNodeDown,
// silent returns nil without ordering the block, slow waits the delay first.
func (n *faultyNet) Submit(ctx context.Context, blk []byte) error {
	switch n.getState() {
	case netCrashed:
		return ErrNodeDown
	case netSilent:
		return nil
	case netSlow:
		t := time.NewTimer(n.delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
	return n.inner.Submit(ctx, blk)
}

// Deliver implements consensus.Consensus.
func (n *faultyNet) Deliver() <-chan []byte { return n.out }

// Close closes the inner backend and the Deliver channel (blocks still
// queued are discarded) and waits for both goroutines. It is idempotent.
func (n *faultyNet) Close() error {
	n.closeOnce.Do(func() {
		close(n.done)
		n.closeErr = n.inner.Close()
		<-n.drained
		<-n.forwarded
	})
	return n.closeErr
}

// drain moves every inner delivery into the queue until the inner Deliver
// channel closes.
func (n *faultyNet) drain() {
	defer close(n.drained)
	for blk := range n.inner.Deliver() {
		n.mu.Lock()
		n.queue = append(n.queue, queued{blk: blk, at: time.Now()})
		n.mu.Unlock()
		n.signal()
	}
	n.mu.Lock()
	n.innerClosed = true
	n.mu.Unlock()
	n.signal()
}

// forward passes queued blocks to out in order, honouring the state. It
// closes out on Close, or once the inner backend closed and the queue is
// empty.
func (n *faultyNet) forward() {
	defer close(n.forwarded)
	defer close(n.out)
	for {
		blk, ready, closed := n.next()
		if closed {
			return
		}
		if blk == nil {
			// Nothing to deliver now: wait for a change, or until the
			// head block's slow delay has elapsed.
			var timeout <-chan time.Time
			var t *time.Timer
			if !ready.IsZero() {
				t = time.NewTimer(time.Until(ready))
				timeout = t.C
			}
			stopped := false
			select {
			case <-n.wake:
			case <-timeout:
			case <-n.done:
				stopped = true
			}
			if t != nil {
				t.Stop()
			}
			if stopped {
				return
			}
			continue
		}
		select {
		case n.out <- blk:
		case <-n.done:
			return
		}
	}
}

// next pops the head block if it may be delivered now. Otherwise it returns
// blk=nil and, if the head is only waiting for its slow delay, the time it
// becomes ready. closed reports that the inner backend is closed and
// nothing is left to deliver.
func (n *faultyNet) next() (blk []byte, ready time.Time, closed bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.queue) == 0 || n.state == netCrashed {
		return nil, time.Time{}, n.innerClosed && len(n.queue) == 0
	}
	head := n.queue[0]
	if n.state == netSlow {
		if r := head.at.Add(n.delay); time.Now().Before(r) {
			return nil, r, false
		}
	}
	n.queue[0] = queued{}
	n.queue = n.queue[1:]
	if len(n.queue) == 0 {
		n.queue = nil // let the backing array go
	}
	return head.blk, time.Time{}, false
}
