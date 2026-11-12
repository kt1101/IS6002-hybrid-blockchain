package hub

import (
	"errors"
	"testing"
	"time"
)

func TestMetricsTPSAndPercentiles(t *testing.T) {
	now := time.Unix(1000, 0)
	m := newMetricsTracker()

	// second 1000: 5 ops of 10..50ms; second 1001: 3 ops of 60..80ms + 1 failure.
	for i := 1; i <= 5; i++ {
		m.record(now, time.Duration(i*10)*time.Millisecond, nil)
	}
	now = time.Unix(1001, 0).Add(100 * time.Millisecond)
	for i := 6; i <= 8; i++ {
		m.record(now, time.Duration(i*10)*time.Millisecond, nil)
	}
	m.record(now, 5*time.Millisecond, errors.New("boom"))
	m.mark(now, "crash node3")

	now = time.Unix(1002, 0).Add(500 * time.Millisecond) // last full second is 1001
	got := m.snapshot(now)
	if got.TPS != 3 {
		t.Errorf("TPS = %v, want 3", got.TPS)
	}
	if got.Errors != 1 {
		t.Errorf("Errors = %d, want 1", got.Errors)
	}
	// samples 10..80 ms (8 of them): nearest-rank p50 = 4th = 40, p99 = 8th = 80.
	if got.P50Ms != 40 || got.P99Ms != 80 {
		t.Errorf("p50/p99 = %v/%v, want 40/80", got.P50Ms, got.P99Ms)
	}
	if len(got.Series) != 60 {
		t.Fatalf("len(Series) = %d, want 60", len(got.Series))
	}
	if got.Series[59] != 3 || got.Series[58] != 5 || got.Series[57] != 0 {
		t.Errorf("Series tail = %v, want [... 0 5 3]", got.Series[55:])
	}
	if len(got.Markers) != 1 || got.Markers[0].Label != "crash node3" || got.Markers[0].AgoS != 1 {
		t.Errorf("Markers = %+v, want one 'crash node3' 1s ago", got.Markers)
	}

	// 11s later the 10s latency window is empty, and the marker is still within 60s.
	now = time.Unix(1013, 0)
	got = m.snapshot(now)
	if got.TPS != 0 || got.P50Ms != 0 || got.P99Ms != 0 {
		t.Errorf("after idle: TPS/p50/p99 = %v/%v/%v, want 0", got.TPS, got.P50Ms, got.P99Ms)
	}
	if got.Errors != 1 {
		t.Errorf("Errors must be cumulative, got %d", got.Errors)
	}

	// 61s after the marker it is gone.
	got = m.snapshot(time.Unix(1070, 0))
	if len(got.Markers) != 0 {
		t.Errorf("stale markers kept: %+v", got.Markers)
	}
}
