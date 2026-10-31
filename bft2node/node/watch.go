package node

import (
	"fmt"
	"strings"

	"github.com/relab/hotstuff/logging"
)

// watchLogger is the library's logger with two of its warnings turned into
// signals; the library has no other way to report them:
//
//   - consensus.commit: Warnf("failed to commit: %v", err), when a block to
//     commit has an ancestor this replica neither has nor can fetch (e.g. it
//     fell behind past the blockchain's retention): the replica then delivers
//     nothing more while it still looks alive;
//   - eventloop.AddEvent: Warnf("event queue is full, dropped event: %v"),
//     the usual way a replica starts falling behind.
type watchLogger struct {
	logging.Logger
	onCommitFailure func(msg string)
	onDroppedEvent  func()
}

var _ logging.Logger = (*watchLogger)(nil)

func (w *watchLogger) Warnf(template string, args ...any) {
	switch {
	case strings.HasPrefix(template, "failed to commit"):
		if w.onCommitFailure != nil {
			w.onCommitFailure(fmt.Sprintf(template, args...))
		}
	case strings.HasPrefix(template, "event queue is full"):
		if w.onDroppedEvent != nil {
			w.onDroppedEvent()
		}
	}
	w.Logger.Warnf(template, args...)
}
