package node

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/relab/hotstuff/synchronizer"
)

// maxBackoffSteps caps the consecutive-timeout exponent (the result is
// clamped to max anyway; this only keeps the float finite).
const maxBackoffSteps = 64

// boundedViewDuration is the view timeout:
//
//	min(max(inner.Duration(), min) * mult^k, max)
//
// where inner is the library's estimator (synchronizer.NewViewDuration: mean
// + 1.96 sd of recent SUCCESSFUL views) and k is the number of consecutive
// timed-out views, reset by the next successful view.
//
// ViewTimeout is NOT forwarded to inner: the library's estimator multiplies
// its mean by the multiplier on every timeout but, once it has more than
// `samples` measurements, moves the mean only 1/samples of the way back on a
// successful view. With a crashed replica each of its leader slots costs two
// timed-out views (the view before it, whose votes go to the dead leader,
// and its own), so the mean grew without bound and every timeout settled at
// max for the rest of the run (review I1). Here a timeout only backs off
// until the next successful view.
//
// The floor matters too: after the first fast views inner's mean is a few
// ms, and any hiccup of a 0.5-CPU container would time views out.
//
// The synchronizer calls these methods on its event loop only; last is
// atomic so Stats can read it from other goroutines.
type boundedViewDuration struct {
	inner    synchronizer.ViewDuration
	min, max time.Duration
	mult     float64

	k    int          // consecutive timeouts
	last atomic.Int64 // last Duration result, ns (Stats)
}

var _ synchronizer.ViewDuration = (*boundedViewDuration)(nil)

func newBoundedViewDuration(inner synchronizer.ViewDuration, min, max time.Duration, mult float64) *boundedViewDuration {
	return &boundedViewDuration{inner: inner, min: min, max: max, mult: mult}
}

func (b *boundedViewDuration) Duration() time.Duration {
	base := float64(b.inner.Duration())
	if base < float64(b.min) {
		base = float64(b.min)
	}
	d := base * math.Pow(b.mult, float64(b.k))
	if b.max > 0 && d > float64(b.max) {
		d = float64(b.max)
	}
	out := time.Duration(math.Round(d))
	b.last.Store(int64(out))
	return out
}

func (b *boundedViewDuration) ViewStarted() { b.inner.ViewStarted() }

func (b *boundedViewDuration) ViewSucceeded() {
	b.k = 0
	b.inner.ViewSucceeded()
}

func (b *boundedViewDuration) ViewTimeout() {
	if b.k < maxBackoffSteps {
		b.k++
	}
}

// lastDuration is the most recent view timeout handed to the synchronizer.
func (b *boundedViewDuration) lastDuration() time.Duration { return time.Duration(b.last.Load()) }
