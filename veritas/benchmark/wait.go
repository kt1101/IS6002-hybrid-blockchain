package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WaitWithGrace waits for wg. Once ctx is done it waits at most grace more
// and then returns an error, so a goroutine stuck in a call that ignores its
// context (e.g. a hung TSO request) cannot block the caller forever. It
// returns nil as soon as wg is done. On error the goroutine waiting for wg is
// left behind; callers are expected to exit.
func WaitWithGrace(ctx context.Context, wg *sync.WaitGroup, grace time.Duration) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-done:
		return nil
	case <-t.C:
		return fmt.Errorf("workers still busy %v after the phase deadline (%v); a call is ignoring its context, e.g. a hung TSO request", grace, ctx.Err())
	}
}
