package benchmark

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func constNext() func() *Request {
	return func() *Request { return &Request{ReqType: GetOp, Key: "k"} }
}

func TestOpenLoopRate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	o := OpenLoop{Rate: 1000, MaxInflight: 64}
	sent, dropped := o.Run(ctx, constNext(), func(context.Context, *Request, time.Time) {})
	if sent < 1900 || sent > 2100 {
		t.Fatalf("sent = %d, want in [1900, 2100]", sent)
	}
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
}

func TestOpenLoopDropsAboveInflightCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var cur, max int64
	o := OpenLoop{Rate: 1000, MaxInflight: 10}
	release := make(chan struct{})
	sent, dropped := o.Run(ctx, constNext(), func(context.Context, *Request, time.Time) {
		n := atomic.AddInt64(&cur, 1)
		for {
			m := atomic.LoadInt64(&max)
			if n <= m || atomic.CompareAndSwapInt64(&max, m, n) {
				break
			}
		}
		select {
		case <-time.After(time.Second):
		case <-release:
		}
		atomic.AddInt64(&cur, -1)
	})
	close(release)
	for atomic.LoadInt64(&cur) != 0 {
		time.Sleep(time.Millisecond)
	}
	if dropped < 400 {
		t.Fatalf("dropped = %d (sent %d), want >= 400", dropped, sent)
	}
	if m := atomic.LoadInt64(&max); m > 10 {
		t.Fatalf("max concurrent do = %d, want <= 10", m)
	}
}

func TestOpenLoopLatencyFromSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	var mu sync.Mutex
	var lats []time.Duration
	o := OpenLoop{Rate: 200, MaxInflight: 1}
	o.Run(ctx, constNext(), func(_ context.Context, _ *Request, scheduled time.Time) {
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		lats = append(lats, time.Since(scheduled))
		mu.Unlock()
	})
	time.Sleep(100 * time.Millisecond) // the last do may still run
	mu.Lock()
	defer mu.Unlock()
	if len(lats) == 0 {
		t.Fatal("no operation ran")
	}
	for _, l := range lats {
		if l < 50*time.Millisecond {
			t.Fatalf("latency %v < 50ms", l)
		}
	}
}

func TestOpenLoopStopsWhenNextIsNil(t *testing.T) {
	n := 0
	next := func() *Request {
		if n == 5 {
			return nil
		}
		n++
		return &Request{}
	}
	sent, _ := OpenLoop{Rate: 10000, MaxInflight: 4}.Run(context.Background(), next, func(context.Context, *Request, time.Time) {})
	if sent != 5 {
		t.Fatalf("sent = %d, want 5", sent)
	}
}

func TestAppendCSVRowOfferedRps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	if err := AppendCSVRow(path, map[string]string{"backend": "raft", "offered_rps": "500"}, Summary{TPS: 1}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows[0]) != 23 || rows[0][22] != "offered_rps" {
		t.Fatalf("header = %v, want 23 columns ending offered_rps", rows[0])
	}
	if rows[1][22] != "500" {
		t.Fatalf("offered_rps = %q, want 500", rows[1][22])
	}
}

func TestRecorderDropped(t *testing.T) {
	r := NewRecorder(0, time.Second, time.Now())
	r.ObserveDropped()
	r.ObserveDropped()
	if s := r.Summary(); s.Dropped != 2 {
		t.Fatalf("Dropped = %d, want 2", s.Dropped)
	}
}
