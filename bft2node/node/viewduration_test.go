package node

import (
	"testing"
	"time"

	"github.com/relab/hotstuff/synchronizer"
)

// fixedInner is an estimator that always says d and records timeouts.
type fixedInner struct {
	d        time.Duration
	timeouts int
}

func (f *fixedInner) Duration() time.Duration { return f.d }
func (f *fixedInner) ViewStarted()            {}
func (f *fixedInner) ViewSucceeded()          {}
func (f *fixedInner) ViewTimeout()            { f.timeouts++ }

// The pattern of a cluster with one crashed replica: every few views two
// consecutive timeouts (the dead leader's predecessor and its own view),
// then successful views. The timeout must return to the floor after each
// successful view instead of drifting to max (review I1), and timeouts must
// not reach the library's estimator.
func TestViewDurationBacksOffOnlyUntilTheNextSuccess(t *testing.T) {
	inner := &fixedInner{d: 5 * time.Millisecond}
	min, max := 100*time.Millisecond, 2*time.Second
	b := newBoundedViewDuration(inner, min, max, 1.2)
	for epoch := 0; epoch < 500; epoch++ {
		b.ViewStarted()
		b.ViewTimeout()
		if d := b.Duration(); d != 120*time.Millisecond {
			t.Fatalf("epoch %d: after 1 timeout = %v, want 120ms", epoch, d)
		}
		b.ViewStarted()
		b.ViewTimeout()
		if d := b.Duration(); d != 144*time.Millisecond {
			t.Fatalf("epoch %d: after 2 timeouts = %v, want 144ms", epoch, d)
		}
		for s := 0; s < 2; s++ {
			b.ViewStarted()
			b.ViewSucceeded()
			if d := b.Duration(); d != min {
				t.Fatalf("epoch %d: after a success = %v, want the floor %v", epoch, d, min)
			}
		}
	}
	if inner.timeouts != 0 {
		t.Fatalf("%d timeouts forwarded to the library's estimator, want 0", inner.timeouts)
	}
	if b.lastDuration() != min {
		t.Fatalf("lastDuration = %v, want %v", b.lastDuration(), min)
	}
	// Many consecutive timeouts (e.g. f+1 replicas down) cap at max.
	for i := 0; i < 200; i++ {
		b.ViewTimeout()
	}
	if d := b.Duration(); d != max {
		t.Fatalf("after 200 timeouts = %v, want max %v", d, max)
	}
	b.ViewSucceeded()
	if d := b.Duration(); d != min {
		t.Fatalf("after recovery = %v, want %v", d, min)
	}
}

// Same with the library's real estimator (its successful views take ~0 ms
// here, so it says a few ms at most and the floor applies).
func TestViewDurationWithLibraryEstimatorStaysAtTheFloor(t *testing.T) {
	min, max := 100*time.Millisecond, 2*time.Second
	b := newBoundedViewDuration(synchronizer.NewViewDuration(1000, 500, 2000, 1.2), min, max, 1.2)
	b.ViewStarted()
	b.ViewSucceeded()
	for epoch := 0; epoch < 2000; epoch++ {
		b.ViewStarted()
		b.ViewTimeout()
		b.ViewStarted()
		b.ViewTimeout()
		b.ViewStarted()
		b.ViewSucceeded()
		b.ViewStarted()
		b.ViewSucceeded()
	}
	if d := b.Duration(); d > 2*min {
		t.Fatalf("after 2000 epochs of T,T,S,S: %v, want near the floor %v", d, min)
	}
}
