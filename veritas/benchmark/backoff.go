package benchmark

import (
	"context"
	"time"
)

// Backoff is the error backoff of one closed-loop worker: after a failed
// request the worker pauses Next() before its next request, starting at Min
// and doubling up to Max on consecutive failures; a success resets it. A
// worker bound to a stopped or unreachable node therefore does not busy-loop
// (thousands of failing requests per second that load the client container,
// the TSO and the errors count). Not safe for concurrent use: one per worker.
type Backoff struct {
	Min, Max time.Duration
	cur      time.Duration
}

// Failure records a failed request and returns how long to wait before the
// next one: Min after a success (or at first), then twice the previous wait,
// at most Max.
func (b *Backoff) Failure() time.Duration {
	switch {
	case b.cur <= 0:
		b.cur = b.Min
	case b.cur < b.Max:
		b.cur *= 2
	}
	if b.cur > b.Max {
		b.cur = b.Max
	}
	return b.cur
}

// Success records a successful request: the next Failure waits Min again.
func (b *Backoff) Success() {
	b.cur = 0
}

// SleepCtx waits d or until ctx is done, whichever comes first; it reports
// whether the full d elapsed (false: ctx is done).
func SleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
