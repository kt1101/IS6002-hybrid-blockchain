package benchmark

import (
	"context"
	"time"
)

// RetryWithTimeout calls op up to attempts times (at least once) until it
// returns nil. Each call gets its own context derived from ctx with a deadline
// of perAttempt, so ctx still bounds every attempt; op must honour its
// context. Between a failed call and the next one it waits backoff (no wait
// if backoff <= 0, none after the last call). Retrying stops early, without a
// further call, once ctx is done, also during the wait.
// It returns the number of retries made (calls after the first) and the
// error of the last call, or ctx's error if ctx was done before any call.
func RetryWithTimeout(ctx context.Context, attempts int, perAttempt, backoff time.Duration, op func(context.Context) error) (int, error) {
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			if err == nil {
				err = ctx.Err()
			}
			return retriesMade(i), err
		}
		actx, cancel := context.WithTimeout(ctx, perAttempt)
		err = op(actx)
		cancel()
		if err == nil {
			return retriesMade(i + 1), nil
		}
		if i < attempts-1 && backoff > 0 {
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return retriesMade(i + 1), err
			case <-t.C:
			}
		}
	}
	return attempts - 1, err
}

// retriesMade converts a number of calls into a number of retries.
func retriesMade(calls int) int {
	if calls < 1 {
		return 0
	}
	return calls - 1
}
