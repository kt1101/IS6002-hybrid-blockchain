package node

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/relab/hotstuff"
)

var (
	// ErrQueueFull: Submit refused, the proposal queue is at its limit.
	ErrQueueFull = errors.New("bft2-node: proposal queue full")
	// ErrStopped: the node is stopping or stopped.
	ErrStopped = errors.New("bft2-node: stopped")
	// ErrEmpty: an empty command (reserved for the filler blocks).
	ErrEmpty = errors.New("bft2-node: empty command")
	// ErrTooLarge: the command is over Config.MaxCommandBytes.
	ErrTooLarge = errors.New("bft2-node: command too large")
)

// cmdQueue holds the commands submitted on this replica, in order, until
// this replica is leader and proposes them, one per view. It is bounded in
// commands and bytes (push fails with ErrQueueFull); pushFront (re-queue of
// a forked block) ignores the limits.
type cmdQueue struct {
	mu       sync.Mutex
	items    []hotstuff.Command
	bytes    int64
	maxItems int
	maxBytes int64
	wake     chan struct{} // closed and replaced whenever items are added or the queue closes
	closed   bool
}

func newCmdQueue(maxItems int, maxBytes int64) *cmdQueue {
	return &cmdQueue{maxItems: maxItems, maxBytes: maxBytes, wake: make(chan struct{})}
}

// signal wakes every waiting pop. Called with mu held.
func (q *cmdQueue) signal() {
	close(q.wake)
	q.wake = make(chan struct{})
}

func (q *cmdQueue) push(cmd hotstuff.Command) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrStopped
	}
	if len(q.items) >= q.maxItems || q.bytes+int64(len(cmd)) > q.maxBytes {
		return ErrQueueFull
	}
	q.items = append(q.items, cmd)
	q.bytes += int64(len(cmd))
	q.signal()
	return nil
}

func (q *cmdQueue) pushFront(cmd hotstuff.Command) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.items = append([]hotstuff.Command{cmd}, q.items...)
	q.bytes += int64(len(cmd))
	q.signal()
}

// pop returns the oldest command, waiting at most idle for one. It returns
// ok=false after idle, when ctx ends or when the queue is closed.
func (q *cmdQueue) pop(ctx context.Context, idle time.Duration) (cmd hotstuff.Command, ok bool) {
	var timer *time.Timer
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return "", false
		}
		if len(q.items) > 0 {
			cmd = q.items[0]
			q.items[0] = ""
			q.items = q.items[1:]
			if len(q.items) == 0 {
				q.items = nil // drop the backing array
			}
			q.bytes -= int64(len(cmd))
			q.mu.Unlock()
			return cmd, true
		}
		wake := q.wake
		q.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(idle)
			defer timer.Stop()
		}
		select {
		case <-wake:
		case <-timer.C:
			return "", false
		case <-ctx.Done():
			return "", false
		}
	}
}

func (q *cmdQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *cmdQueue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// close drops the queued commands, refuses later pushes and wakes pop.
func (q *cmdQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.items = nil
	q.bytes = 0
	q.signal()
}
