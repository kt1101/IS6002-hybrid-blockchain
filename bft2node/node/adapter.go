package node

import (
	"context"
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
		return cmd, true
	}
	if ctx.Err() != nil || a.queue.isClosed() {
		return "", false
	}
	return "", true
}

// Accept accepts every command: veritas validates blocks when it applies them.
func (a *adapter) Accept(hotstuff.Command) bool { return true }

// Proposed is a no-op (no de-duplication: the consensus contract lets a
// block be delivered twice).
func (a *adapter) Proposed(hotstuff.Command) {}

// Exec is called by the consensus module, on the event loop, for every
// committed block in commit order (the same order on every replica). It
// appends the block's command, copied, to the commit log; filler blocks
// (empty command) are skipped on every replica alike. It blocks while the
// commit log is over its byte limit (backpressure).
func (a *adapter) Exec(b *hotstuff.Block) {
	cmd := b.Command()
	if cmd == "" {
		return
	}
	a.log.Add([]byte(cmd))
}

// Fork is called for blocks pruned off the committed chain: they can never
// commit (HotStuff safety), so a block this replica proposed goes back to
// the front of its queue and is proposed again in its next view as leader.
// Other replicas' forked blocks are their proposers' to re-queue.
func (a *adapter) Fork(b *hotstuff.Block) {
	if b.Proposer() != a.id || b.Command() == "" {
		return
	}
	a.queue.pushFront(b.Command())
}
