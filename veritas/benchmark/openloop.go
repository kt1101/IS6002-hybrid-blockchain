package benchmark

import (
	"context"
	"sync"
	"time"
)

// OpenLoop issues requests at a fixed total Rate (requests per second),
// independent of how fast they complete: operation i is scheduled at
// start + i/Rate. At most MaxInflight operations run at once; an operation
// whose slot comes while MaxInflight are still running is dropped (counted,
// never queued), so a slow server cannot lower the offered rate.
type OpenLoop struct {
	Rate        float64
	MaxInflight int
}

// DefaultMaxInflight is the default cap on outstanding open-loop requests.
const DefaultMaxInflight = 4096

// Run issues next() at the scheduled times until ctx is done or next returns
// nil. Each operation runs do in its own goroutine with its scheduled time
// (latency should be measured from it, not from the actual send, to avoid
// coordinated omission). Run returns once no more operations are issued and
// every do call it started has returned (do gets ctx, so a cancelled ctx
// should end it). sent counts operations handed to do,
// dropped those skipped because MaxInflight were running.
func (o OpenLoop) Run(ctx context.Context, next func() *Request, do func(ctx context.Context, r *Request, scheduled time.Time)) (sent, dropped int64) {
	if o.Rate <= 0 {
		return 0, 0
	}
	maxIn := o.MaxInflight
	if maxIn <= 0 {
		maxIn = DefaultMaxInflight
	}
	sem := make(chan struct{}, maxIn)
	var wg sync.WaitGroup
	defer wg.Wait()
	interval := float64(time.Second) / o.Rate
	start := time.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C
	for i := int64(0); ; i++ {
		at := start.Add(time.Duration(float64(i) * interval))
		if d := time.Until(at); d > 0 {
			timer.Reset(d)
			select {
			case <-ctx.Done():
				return sent, dropped
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return sent, dropped
		}
		r := next()
		if r == nil {
			return sent, dropped
		}
		select {
		case sem <- struct{}{}:
			sent++
			wg.Add(1)
			go func(r *Request, at time.Time) {
				defer wg.Done()
				defer func() { <-sem }()
				do(ctx, r, at)
			}(r, at)
		default:
			dropped++
		}
	}
}
