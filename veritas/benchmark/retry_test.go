package benchmark

import (
	"context"
	"errors"
	"testing"
	"time"
)

// testBackoff keeps the tests fast; ycsbbench waits 1 s between attempts.
const testBackoff = time.Millisecond

func TestRetryWithTimeoutFirstCallSucceeds(t *testing.T) {
	calls := 0
	retries, err := RetryWithTimeout(context.Background(), 3, time.Second, testBackoff, func(context.Context) error {
		calls++
		return nil
	})
	if err != nil || calls != 1 || retries != 0 {
		t.Fatalf("err=%v calls=%d retries=%d, want nil 1 0", err, calls, retries)
	}
}

func TestRetryWithTimeoutSucceedsAfterRetry(t *testing.T) {
	calls := 0
	retries, err := RetryWithTimeout(context.Background(), 3, time.Second, testBackoff, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("EOF")
		}
		return nil
	})
	if err != nil || calls != 3 || retries != 2 {
		t.Fatalf("err=%v calls=%d retries=%d, want nil 3 2", err, calls, retries)
	}
}

func TestRetryWithTimeoutFailsAfterAllAttempts(t *testing.T) {
	calls := 0
	retries, err := RetryWithTimeout(context.Background(), 3, time.Second, testBackoff, func(context.Context) error {
		calls++
		return errors.New("attempt failed")
	})
	if err == nil || err.Error() != "attempt failed" || calls != 3 || retries != 2 {
		t.Fatalf("err=%v calls=%d retries=%d, want 'attempt failed' 3 2", err, calls, retries)
	}
}

func TestRetryWithTimeoutAttemptsBelowOneMeansOnce(t *testing.T) {
	calls := 0
	retries, err := RetryWithTimeout(context.Background(), 0, time.Second, testBackoff, func(context.Context) error {
		calls++
		return errors.New("attempt failed")
	})
	if err == nil || calls != 1 || retries != 0 {
		t.Fatalf("err=%v calls=%d retries=%d, want error 1 0", err, calls, retries)
	}
}

func TestRetryWithTimeoutHonoursPerAttemptTimeout(t *testing.T) {
	const perAttempt = 50 * time.Millisecond
	calls := 0
	start := time.Now()
	retries, err := RetryWithTimeout(context.Background(), 2, perAttempt, testBackoff, func(ctx context.Context) error {
		calls++
		dl, ok := ctx.Deadline()
		if !ok {
			t.Errorf("attempt %d: context has no deadline", calls)
		} else if until := time.Until(dl); until > perAttempt {
			t.Errorf("attempt %d: deadline in %v, want <= %v", calls, until, perAttempt)
		}
		<-ctx.Done() // a call that hangs until its own deadline
		return ctx.Err()
	})
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) || calls != 2 || retries != 1 {
		t.Fatalf("err=%v calls=%d retries=%d, want DeadlineExceeded 2 1", err, calls, retries)
	}
	if elapsed < 2*perAttempt || elapsed > 2*time.Second {
		t.Fatalf("took %v, want about %v", elapsed, 2*perAttempt)
	}
}

func TestRetryWithTimeoutStopsWhenParentCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	retries, err := RetryWithTimeout(ctx, 3, time.Second, testBackoff, func(context.Context) error {
		calls++
		cancel() // e.g. --load-timeout passes during the first call
		return errors.New("attempt failed")
	})
	if err == nil || err.Error() != "attempt failed" || calls != 1 || retries != 0 {
		t.Fatalf("err=%v calls=%d retries=%d, want 'attempt failed' 1 0", err, calls, retries)
	}
}

func TestRetryWithTimeoutParentDoneBeforeFirstCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	retries, err := RetryWithTimeout(ctx, 3, time.Second, testBackoff, func(context.Context) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 0 || retries != 0 {
		t.Fatalf("err=%v calls=%d retries=%d, want Canceled 0 0", err, calls, retries)
	}
}

func TestRetryWithTimeoutParentDeadlineBoundsAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	start := time.Now()
	_, err := RetryWithTimeout(ctx, 3, time.Hour, testBackoff, func(actx context.Context) error {
		calls++
		<-actx.Done()
		return actx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want DeadlineExceeded 1", err, calls)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %v: the parent deadline did not bound the attempt", elapsed)
	}
}

func TestRetryWithTimeoutWaitsBackoffBetweenAttempts(t *testing.T) {
	const backoff = 30 * time.Millisecond
	var calls []time.Time
	retries, err := RetryWithTimeout(context.Background(), 3, time.Second, backoff, func(context.Context) error {
		calls = append(calls, time.Now())
		return errors.New("attempt failed")
	})
	if err == nil || len(calls) != 3 || retries != 2 {
		t.Fatalf("err=%v calls=%d retries=%d, want error 3 2", err, len(calls), retries)
	}
	for i := 1; i < len(calls); i++ {
		if gap := calls[i].Sub(calls[i-1]); gap < backoff {
			t.Fatalf("gap before call %d is %v, want >= %v", i+1, gap, backoff)
		}
	}
}

func TestRetryWithTimeoutNoBackoffAfterLastAttempt(t *testing.T) {
	start := time.Now()
	_, err := RetryWithTimeout(context.Background(), 1, time.Second, time.Hour, func(context.Context) error {
		return errors.New("attempt failed")
	})
	if err == nil {
		t.Fatal("want an error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %v: waited after the last attempt", elapsed)
	}
}

func TestRetryWithTimeoutBackoffStopsWhenParentDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	start := time.Now()
	retries, err := RetryWithTimeout(ctx, 3, time.Second, time.Hour, func(context.Context) error {
		calls++
		return errors.New("attempt failed")
	})
	if err == nil || err.Error() != "attempt failed" || calls != 1 || retries != 0 {
		t.Fatalf("err=%v calls=%d retries=%d, want 'attempt failed' 1 0", err, calls, retries)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %v: the wait ignored the parent deadline", elapsed)
	}
}
