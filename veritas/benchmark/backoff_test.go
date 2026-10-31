package benchmark

import (
	"context"
	"testing"
	"time"
)

func TestBackoffDoublesToMaxAndResets(t *testing.T) {
	b := Backoff{Min: 50 * time.Millisecond, Max: time.Second}
	want := []time.Duration{50, 100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := b.Failure(); got != w*time.Millisecond {
			t.Fatalf("failure %d: wait %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
	b.Success()
	if got := b.Failure(); got != 50*time.Millisecond {
		t.Fatalf("after Success: wait %v, want 50ms", got)
	}
}

func TestBackoffMaxBelowMin(t *testing.T) {
	b := Backoff{Min: time.Second, Max: 100 * time.Millisecond}
	if got := b.Failure(); got != 100*time.Millisecond {
		t.Fatalf("wait %v, want the Max 100ms", got)
	}
}

func TestSleepCtx(t *testing.T) {
	if !SleepCtx(context.Background(), time.Millisecond) {
		t.Fatal("a 1ms sleep with a live context should complete")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t0 := time.Now()
	if SleepCtx(ctx, time.Hour) {
		t.Fatal("a sleep on a done context should report false")
	}
	if time.Since(t0) > time.Second {
		t.Fatal("a sleep on a done context should return at once")
	}
	if SleepCtx(ctx, 0) {
		t.Fatal("a zero sleep on a done context should report false")
	}
}
