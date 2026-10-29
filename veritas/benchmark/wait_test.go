package benchmark

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestWaitWithGraceReturnsWhenWorkersFinish(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		wg.Done()
	}()
	if err := WaitWithGrace(context.Background(), &wg, time.Hour); err != nil {
		t.Fatalf("err=%v, want nil", err)
	}
}

func TestWaitWithGraceKeepsWaitingBeforeDeadline(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	res := make(chan error, 1)
	go func() { res <- WaitWithGrace(context.Background(), &wg, time.Millisecond) }()
	select {
	case err := <-res:
		t.Fatalf("returned %v while the context is not done and a worker is busy", err)
	case <-time.After(50 * time.Millisecond):
	}
	wg.Done()
	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("err=%v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not return after the worker finished")
	}
}

func TestWaitWithGraceAllowsFinishWithinGrace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		wg.Done()
	}()
	if err := WaitWithGrace(ctx, &wg, time.Hour); err != nil {
		t.Fatalf("err=%v, want nil", err)
	}
}

func TestWaitWithGraceFailsWhenWorkerHangs(t *testing.T) {
	const grace = 30 * time.Millisecond
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1) // never done: a worker stuck in a call that ignores ctx
	err := WaitWithGrace(ctx, &wg, grace)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want an error for a worker that never finishes")
	}
	if elapsed < 20*time.Millisecond+grace || elapsed > 2*time.Second {
		t.Fatalf("returned after %v, want about deadline + grace (%v)", elapsed, 20*time.Millisecond+grace)
	}
}
