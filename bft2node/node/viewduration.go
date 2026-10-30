package node

import (
	"time"

	"github.com/relab/hotstuff/synchronizer"
)

// boundedViewDuration clamps the library's adaptive view timeout
// (synchronizer.NewViewDuration: mean + 1.96 sd of recent successful views,
// x multiplier after a timeout) to [min, max]. Without the floor the timeout
// drops to a few ms after the first fast views (the mean is reset to the
// first sample), and any hiccup of a 0.5-CPU container (a large Veritas
// block, GC) would time the view out and waste it; without the ceiling a
// crashed leader's view grows by the multiplier on every timeout.
type boundedViewDuration struct {
	inner    synchronizer.ViewDuration
	min, max time.Duration
}

var _ synchronizer.ViewDuration = boundedViewDuration{}

func (b boundedViewDuration) Duration() time.Duration {
	d := b.inner.Duration()
	if d < b.min {
		d = b.min
	}
	if b.max > 0 && d > b.max {
		d = b.max
	}
	return d
}

func (b boundedViewDuration) ViewStarted()   { b.inner.ViewStarted() }
func (b boundedViewDuration) ViewSucceeded() { b.inner.ViewSucceeded() }
func (b boundedViewDuration) ViewTimeout()   { b.inner.ViewTimeout() }
