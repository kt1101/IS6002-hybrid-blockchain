package node

import (
	"context"
	"sync"
)

// CommitLog is the ordered backlog of committed commands of this replica
// that the local subscriber (the veritas server, over GET /deliver) has not
// taken yet. Commands are numbered from 0 in commit order. The executor
// appends; the subscriber reads with Peek and removes with Advance once the
// frames are written, so a command is dropped only after it was sent.
//
// Add blocks while the backlog is over maxBytes (and not empty): that
// holds the replica's event loop, i.e. backpressure on consensus when the
// local veritas server falls behind, instead of unbounded memory.
type CommitLog struct {
	mu       sync.Mutex
	items    [][]byte
	first    uint64 // seq of items[0]
	bytes    int64
	maxBytes int64
	changed  chan struct{} // closed and replaced on append, Advance and Close
	closed   bool
}

func NewCommitLog(maxBytes int64) *CommitLog {
	return &CommitLog{maxBytes: maxBytes, changed: make(chan struct{})}
}

func (l *CommitLog) signal() {
	close(l.changed)
	l.changed = make(chan struct{})
}

// Add adds b (owned by the log from now on; the executor calls it), first
// waiting while the backlog is non-empty and adding b would exceed maxBytes.
// After Close it drops b.
func (l *CommitLog) Add(b []byte) {
	l.mu.Lock()
	for !l.closed && l.bytes > 0 && l.bytes+int64(len(b)) > l.maxBytes {
		ch := l.changed
		l.mu.Unlock()
		<-ch
		l.mu.Lock()
	}
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.items = append(l.items, b)
	l.bytes += int64(len(b))
	l.signal()
}

// Peek returns up to max of the oldest commands and the seq of the first,
// waiting until there is at least one. It does not remove them (Advance
// does). It fails with ErrStopped after Close and with ctx.Err() when ctx
// ends. The returned slices must not be modified.
func (l *CommitLog) Peek(ctx context.Context, max int) (first uint64, items [][]byte, err error) {
	l.mu.Lock()
	for !l.closed && len(l.items) == 0 {
		ch := l.changed
		l.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
		l.mu.Lock()
	}
	defer l.mu.Unlock()
	if l.closed {
		return 0, nil, ErrStopped
	}
	n := len(l.items)
	if n > max {
		n = max
	}
	return l.first, append([][]byte(nil), l.items[:n]...), nil
}

// Advance removes the n oldest commands (all of them if fewer).
func (l *CommitLog) Advance(n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n > len(l.items) {
		n = len(l.items)
	}
	for i := 0; i < n; i++ {
		l.bytes -= int64(len(l.items[i]))
		l.items[i] = nil
	}
	l.items = l.items[n:]
	if len(l.items) == 0 {
		l.items = nil
	}
	l.first += uint64(n)
	l.signal()
}

// Close drops the backlog, releases a waiting Add and fails Peek. It is
// idempotent.
func (l *CommitLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	l.items = nil
	l.bytes = 0
	l.signal()
}
