package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
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

// cmdID identifies a command (one Veritas block) by the SHA-256 of its bytes.
func cmdID(cmd []byte) [32]byte { return sha256.Sum256(cmd) }

// inflightCmd is a pending command seen in a proposal at view.
type inflightCmd struct {
	cmd  hotstuff.Command
	view uint64
}

// cmdQueue holds the pending commands of this replica, in order, until
// this replica is leader and proposes them, one per view. With forwarding
// (Config.Peers[i].ForwardAddr) every replica holds every pending command,
// so any leader can propose it: a command seen in a proposal moves to
// inflight (markInflight), leaves on commit (commit), and goes back to the
// front if it is still not committed RequeueViews views later
// (requeueStale). A command already queued, inflight or committed (seen) is
// not queued again. It is bounded in commands and bytes (push fails with
// ErrQueueFull); pushFront (re-queue of a forked or stale block) ignores
// the limits.
type cmdQueue struct {
	mu       sync.Mutex
	items    []hotstuff.Command
	ids      [][32]byte // ids[i] = cmdID(items[i])
	bytes    int64
	maxItems int
	maxBytes int64
	wake     chan struct{} // closed and replaced whenever items are added or the queue closes
	closed   bool

	inflight     map[[32]byte]inflightCmd
	requeueViews uint64
	seen         func(id [32]byte) bool // committed recently (nil: none)
}

func newCmdQueue(maxItems int, maxBytes int64) *cmdQueue {
	return &cmdQueue{maxItems: maxItems, maxBytes: maxBytes, wake: make(chan struct{}),
		inflight: make(map[[32]byte]inflightCmd), requeueViews: DefaultRequeueViews}
}

// known reports whether id is queued, inflight or committed. Called with mu held.
func (q *cmdQueue) known(id [32]byte) bool {
	if _, ok := q.inflight[id]; ok {
		return true
	}
	if q.seen != nil && q.seen(id) {
		return true
	}
	return q.indexOf(id) >= 0
}

// indexOf is the position of id in items, or -1. Called with mu held.
func (q *cmdQueue) indexOf(id [32]byte) int {
	for i := range q.ids {
		if q.ids[i] == id {
			return i
		}
	}
	return -1
}

// removeAt drops items[i]. Called with mu held.
func (q *cmdQueue) removeAt(i int) hotstuff.Command {
	cmd := q.items[i]
	q.items = append(q.items[:i], q.items[i+1:]...)
	q.ids = append(q.ids[:i], q.ids[i+1:]...)
	if len(q.items) == 0 {
		q.items, q.ids = nil, nil
	}
	q.bytes -= int64(len(cmd))
	return cmd
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
	id := cmdID([]byte(cmd))
	if q.known(id) {
		return nil // a forwarded copy of a block this replica already has
	}
	if len(q.items) >= q.maxItems || q.bytes+int64(len(cmd)) > q.maxBytes {
		return ErrQueueFull
	}
	q.items = append(q.items, cmd)
	q.ids = append(q.ids, id)
	q.bytes += int64(len(cmd))
	q.signal()
	return nil
}

// pushFront puts cmd at the front (a forked or stale block), unless it is
// queued or committed already; it leaves inflight.
func (q *cmdQueue) pushFront(cmd hotstuff.Command) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pushFrontLocked(cmdID([]byte(cmd)), cmd)
}

func (q *cmdQueue) pushFrontLocked(id [32]byte, cmd hotstuff.Command) bool {
	if q.closed {
		return false
	}
	delete(q.inflight, id)
	if (q.seen != nil && q.seen(id)) || q.indexOf(id) >= 0 {
		return false
	}
	q.items = append([]hotstuff.Command{cmd}, q.items...)
	q.ids = append([][32]byte{id}, q.ids...)
	q.bytes += int64(len(cmd))
	q.signal()
	return true
}

// markInflight records that a proposal at view carries id: a queued
// command leaves the queue for inflight; an inflight one gets the newer
// view. It returns whether id was queued or inflight.
func (q *cmdQueue) markInflight(id [32]byte, view uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if e, ok := q.inflight[id]; ok {
		if view > e.view {
			e.view = view
			q.inflight[id] = e
		}
		return true
	}
	i := q.indexOf(id)
	if i < 0 {
		return false
	}
	q.inflight[id] = inflightCmd{cmd: q.removeAt(i), view: view}
	return true
}

// setInflight records cmd (popped by this replica as leader) as inflight at view.
func (q *cmdQueue) setInflight(cmd hotstuff.Command, view uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.inflight[cmdID([]byte(cmd))] = inflightCmd{cmd: cmd, view: view}
}

// isInflight reports whether id is inflight.
func (q *cmdQueue) isInflight(id [32]byte) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.inflight[id]
	return ok
}

// commit drops id from the queue and from inflight (it was committed).
func (q *cmdQueue) commit(id [32]byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inflight, id)
	if i := q.indexOf(id); i >= 0 {
		q.removeAt(i)
	}
}

// requeueStale puts every inflight command seen last at a view at least
// requeueViews before curView back at the front of the queue (oldest
// first) and returns how many.
func (q *cmdQueue) requeueStale(curView uint64) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	var stale []inflightCmd
	var ids [][32]byte
	for id, e := range q.inflight {
		if curView >= e.view+q.requeueViews {
			stale = append(stale, e)
			ids = append(ids, id)
		}
	}
	idx := make([]int, len(stale))
	for i := range idx {
		idx[i] = i
	}
	// Front-insert newest first so the oldest ends up at the very front.
	sort.Slice(idx, func(a, b int) bool { return stale[idx[a]].view > stale[idx[b]].view })
	n := 0
	for _, i := range idx {
		if q.pushFrontLocked(ids[i], stale[i].cmd) {
			n++
		}
	}
	return n
}

func (q *cmdQueue) inflightLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.inflight)
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
			q.ids = q.ids[1:]
			if len(q.items) == 0 {
				q.items, q.ids = nil, nil // drop the backing arrays
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
	q.items, q.ids = nil, nil
	q.inflight = map[[32]byte]inflightCmd{}
	q.bytes = 0
	q.signal()
}
