package benchmark

import (
	"errors"
	"fmt"
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

func TestErrorKindsCapAndOther(t *testing.T) {
	s0 := time.Unix(0, 0)
	r := NewRecorder(10*time.Second, 60*time.Second, s0)
	in := s0.Add(11 * time.Second)
	r.Observe(s0.Add(5*time.Second), time.Millisecond, errors.New("warmup only")) // not counted
	// 25 distinct messages; message i occurs i+1 times.
	total := int64(0)
	for i := 0; i < 25; i++ {
		for j := 0; j <= i; j++ {
			r.Observe(in, time.Millisecond, fmt.Errorf("err-%02d", i))
			total++
		}
	}
	// A message seen before the cap was reached keeps being counted by name.
	r.Observe(in, time.Millisecond, errors.New("err-00"))
	total++
	// Two more distinct (long) messages arrive after the cap: they go to other.
	long := strings.Repeat("x", 1000)
	r.Observe(in, time.Millisecond, errors.New(long+"a"))
	r.Observe(in, time.Millisecond, errors.New(long+"b"))
	total += 2

	kinds := r.ErrorKinds()
	if len(kinds) != MaxErrorKinds+1 {
		t.Fatalf("got %d kinds, want %d named + other: %+v", len(kinds), MaxErrorKinds, kinds)
	}
	other := kinds[len(kinds)-1]
	if other.Msg != ErrorKindOther {
		t.Fatalf("last kind=%q want %q", other.Msg, ErrorKindOther)
	}
	// Messages err-20..err-24 (21+22+23+24+25 = 115) and the two long ones overflow.
	if other.Count != 115+2 {
		t.Fatalf("other=%d want 117", other.Count)
	}
	sum := int64(0)
	byMsg := map[string]int64{}
	for i, k := range kinds {
		sum += k.Count
		byMsg[k.Msg] = k.Count
		if i > 0 && i < MaxErrorKinds && kinds[i-1].Count < k.Count {
			t.Fatalf("kinds not sorted by count: %+v", kinds)
		}
	}
	if sum != total || sum != r.Summary().Errors {
		t.Fatalf("sum of kinds=%d, total=%d, Summary.Errors=%d", sum, total, r.Summary().Errors)
	}
	if byMsg["err-00"] != 2 || byMsg["err-19"] != 20 {
		t.Fatalf("err-00=%d (want 2) err-19=%d (want 20)", byMsg["err-00"], byMsg["err-19"])
	}
	if _, ok := byMsg["warmup only"]; ok {
		t.Fatal("warmup error must not be counted")
	}
	if kinds[0].Msg != "err-19" {
		t.Fatalf("most frequent kind=%q want err-19", kinds[0].Msg)
	}
}

func TestErrorKindsTruncatesLongMessages(t *testing.T) {
	s0 := time.Unix(0, 0)
	r := NewRecorder(0, 60*time.Second, s0)
	long := strings.Repeat("y", 1000)
	r.Observe(s0.Add(time.Second), time.Millisecond, errors.New(long+"a"))
	r.Observe(s0.Add(time.Second), time.Millisecond, errors.New(long+"b"))
	kinds := r.ErrorKinds()
	if len(kinds) != 1 || kinds[0].Count != 2 || len(kinds[0].Msg) != maxErrorMsgLen {
		t.Fatalf("kinds=%d first count=%d len=%d", len(kinds), kinds[0].Count, len(kinds[0].Msg))
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
