package benchmark

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRecorderCountsOnlySuccessInWindow(t *testing.T) {
	s0 := time.Unix(0, 0)
	r := NewRecorder(10*time.Second, 60*time.Second, s0)
	r.Observe(s0.Add(5*time.Second), time.Millisecond, nil) // warmup: ignored
	for i := 0; i < 600; i++ {
		r.Observe(s0.Add(11*time.Second), 2*time.Millisecond, nil)
	}
	r.Observe(s0.Add(12*time.Second), time.Second, errors.New("timeout")) // error
	r.Observe(s0.Add(71*time.Second), time.Millisecond, nil)              // after window: ignored
	s := r.Summary()
	if s.TPS != 10 {
		t.Fatalf("TPS=%v want 10", s.TPS) // 600 / 60s
	}
	if s.Errors != 1 {
		t.Fatalf("Errors=%v", s.Errors)
	}
	if s.LatP99Ms != 2 {
		t.Fatalf("p99=%v", s.LatP99Ms) // errors are not part of the latency distribution
	}
}

func TestRecorderCountsWrongGets(t *testing.T) {
	s0 := time.Unix(0, 0)
	r := NewRecorder(10*time.Second, 60*time.Second, s0)
	r.ObserveGet(s0.Add(11*time.Second), time.Millisecond, "v-CORRUPT", nil)             // counted
	r.ObserveGet(s0.Add(12*time.Second), time.Millisecond, "v", nil)                     // good value
	r.ObserveGet(s0.Add(13*time.Second), time.Millisecond, "v-CORRUPT", errors.New("x")) // failed get
	r.ObserveGet(s0.Add(5*time.Second), time.Millisecond, "v-CORRUPT", nil)              // warmup
	r.ObserveGet(s0.Add(71*time.Second), time.Millisecond, "v-CORRUPT", nil)             // after window
	s := r.Summary()
	if s.WrongGets != 1 {
		t.Fatalf("WrongGets=%v want 1", s.WrongGets)
	}
	if s.Errors != 1 {
		t.Fatalf("Errors=%v want 1", s.Errors)
	}
}

func TestTimelineBucketsPerSecond(t *testing.T) {
	s0 := time.Unix(0, 0)
	r := NewRecorder(10*time.Second, 60*time.Second, s0)
	for i := 0; i < 3; i++ {
		r.Observe(s0.Add(11*time.Second+time.Duration(i)*100*time.Millisecond), time.Millisecond, nil)
	}
	r.Observe(s0.Add(12*time.Second), time.Millisecond, nil)
	r.Observe(s0.Add(3*time.Second), time.Millisecond, nil)                    // warmup counts in timeline
	r.Observe(s0.Add(13*time.Second), time.Millisecond, errors.New("timeout")) // errors do not
	tl := r.Timeline()
	if len(tl) != 70 {
		t.Fatalf("len(Timeline)=%d want 70", len(tl))
	}
	if tl[11] != 3 || tl[12] != 1 {
		t.Fatalf("Timeline[11]=%d [12]=%d want 3,1", tl[11], tl[12])
	}
	if tl[3] != 1 {
		t.Fatalf("Timeline[3]=%d want 1", tl[3])
	}
	if tl[13] != 0 {
		t.Fatalf("Timeline[13]=%d want 0", tl[13])
	}
}

func TestAppendCSVRowWritesHeaderOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	tags := map[string]string{"exp": "e1", "backend": "kafka", "n": "4"}
	s := Summary{TPS: 10, LatMeanMs: 2, LatP50Ms: 2, LatP99Ms: 3, Errors: 1, WrongGets: 0}
	if err := AppendCSVRow(path, tags, s); err != nil {
		t.Fatal(err)
	}
	if err := AppendCSVRow(path, tags, s); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3:\n%s", len(lines), data)
	}
	const header = "ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,host_cpu_max_pct,saturated,net_bytes_per_block"
	if lines[0] != header {
		t.Fatalf("header=%q", lines[0])
	}
	if lines[1] != lines[2] {
		t.Fatalf("rows differ: %q vs %q", lines[1], lines[2])
	}
	if want := ",e1,kafka,4,,,,,,,,,10.00,2.000,2.000,3.000,1,0,,,"; !strings.Contains(lines[1], want) {
		t.Fatalf("row=%q does not contain %q", lines[1], want)
	}
	if len(strings.Split(lines[1], ",")) != 22 {
		t.Fatalf("row has wrong column count: %q", lines[1])
	}
	if err := AppendCSVRow(path, map[string]string{"bogus": "1"}, s); err == nil {
		t.Fatal("unknown tag should be rejected")
	}
	if err := AppendCSVRow(path, map[string]string{"tps": "1"}, s); err == nil {
		t.Fatal("metric column as tag should be rejected")
	}
}
