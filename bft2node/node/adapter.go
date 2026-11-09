package node

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/modules"
)

// adapter connects the HotStuff module system to this replica's command
// queue and commit log. It replaces the library's replica.clientSrv and
// cmdCache (which batch protobuf client commands and wait for a full batch):
// one HotStuff command is one Veritas block, exactly as submitted.
type adapter struct {
	id    hotstuff.ID
	queue *cmdQueue
	log   *CommitLog
	idle  time.Duration // how long the leader waits for a command before proposing a filler block
	view  func() uint64 // current view (nil: 0)

	committed  atomic.Uint64 // non-empty commands delivered
	proposed   atomic.Uint64 // non-empty commands returned by Get
	requeued   atomic.Uint64 // forked or stale commands re-queued
	duplicates atomic.Uint64 // committed commands not delivered again (dedup)
	failRun    atomic.Int64  // "failed to commit" since the last Exec
}

var (
	_ modules.CommandQueue   = (*adapter)(nil)
	_ modules.Acceptor       = (*adapter)(nil)
	_ modules.ExecutorExt    = (*adapter)(nil)
	_ modules.ForkHandlerExt = (*adapter)(nil)
)

// Get is called by the consensus module when this replica is leader
// (consensus.Propose, with a ctx that ends at the view's timeout or view
// change). It returns the next submitted command; with none queued it waits
// at most a.idle and then returns the EMPTY command with ok=true, so the
// leader still proposes a (filler) block. Without filler blocks a quiet
// leader would stall its view until the timeout, and chained HotStuff only
// commits a block once three more blocks extend it, so the last submitted
// block would never commit. ok=false (no proposal this view) only when ctx
// ended or the node stopped.
func (a *adapter) Get(ctx context.Context) (hotstuff.Command, bool) {
	if cmd, ok := a.queue.pop(ctx, a.idle); ok {
		a.proposed.Add(1)
		a.queue.setInflight(cmd, a.curView())
		return cmd, true
	}
	if ctx.Err() != nil || a.queue.isClosed() {
		return "", false
	}
	return "", true
}

func (a *adapter) curView() uint64 {
	if a.view == nil {
		return 0
	}
	return a.view()
}

// Accept accepts every command (veritas validates blocks when it applies
// them). It is called on every replica, the leader included, for each
// proposal it votes on: a pending command it carries moves to inflight.
func (a *adapter) Accept(cmd hotstuff.Command) bool {
	if cmd != "" {
		a.queue.markInflight(cmdID([]byte(cmd)), a.curView())
	}
	return true
}

// Proposed is a no-op (de-duplication is in Exec, by command id).
func (a *adapter) Proposed(hotstuff.Command) {}

// Exec is called by the consensus module, on the event loop, for every
// committed block in commit order (the same order on every replica). It
// appends the block's command, copied, to the commit log unless the same
// command was committed within the dedup window (two leaders proposed one
// shared block: deterministic, so every replica skips the same copy);
// filler blocks (empty command) are skipped on every replica alike. It
// blocks while the commit log is over its byte limit (backpressure).
func (a *adapter) Exec(b *hotstuff.Block) {
	a.failRun.Store(0) // a commit succeeded (fillers count)
	cmd := b.Command()
	if cmd == "" {
		return
	}
	id := cmdID([]byte(cmd))
	a.queue.commit(id)
	if !a.log.appendOnce(id, []byte(cmd)) {
		a.duplicates.Add(1)
		return
	}
	a.committed.Add(1)
}

// Fork is called for blocks pruned off the committed chain: they can never
// commit (HotStuff safety), so a block this replica proposed, or a pending
// block inflight here (forwarding: every replica holds it), goes back to
// the front of the queue and is proposed again by the next leader that
// has it. A block committed meanwhile is not re-queued.
func (a *adapter) Fork(b *hotstuff.Block) {
	cmd := b.Command()
	if cmd == "" {
		return
	}
	if b.Proposer() != a.id && !a.queue.isInflight(cmdID([]byte(cmd))) {
		return
	}
	a.requeued.Add(1)
	a.queue.pushFront(cmd)
}
