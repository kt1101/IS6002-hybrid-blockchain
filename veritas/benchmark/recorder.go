package benchmark

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CorruptMarker is the suffix a faulty replica appends to a value; a Get whose
// value ends with it is counted as a wrong read.
const CorruptMarker = "-CORRUPT"

// Summary holds the aggregated metrics of one run (measurement window only).
type Summary struct {
	TPS       float64
	LatMeanMs float64
	LatP50Ms  float64
	LatP99Ms  float64
	Errors    int64
	WrongGets int64
}

// Recorder collects per-request observations of a time-based run. Only
// observations inside [start+warmup, start+warmup+duration) contribute to the
// Summary; the Timeline additionally covers the warmup phase. Safe for
// concurrent use.
type Recorder struct {
	mu        sync.Mutex
	warmup    time.Duration
	duration  time.Duration
	start     time.Time
	lats      []time.Duration // successful ops inside the window
	errors    int64
	wrongGets int64
	timeline  []int64 // successful ops per second since start, incl. warmup
}

// NewRecorder creates a Recorder for a run that began at start.
func NewRecorder(warmup, duration time.Duration, start time.Time) *Recorder {
	total := warmup + duration
	secs := int((total + time.Second - 1) / time.Second)
	if secs < 0 {
		secs = 0
	}
	return &Recorder{
		warmup:   warmup,
		duration: duration,
		start:    start,
		timeline: make([]int64, secs),
	}
}

// Observe records one finished request: at is its completion time, lat its
// latency, err its outcome.
func (r *Recorder) Observe(at time.Time, lat time.Duration, err error) {
	r.observe(at, lat, err, false)
}

// ObserveGet is Observe for a Get; a successful Get whose value ends with
// CorruptMarker is also counted as a wrong get.
func (r *Recorder) ObserveGet(at time.Time, lat time.Duration, val string, err error) {
	r.observe(at, lat, err, err == nil && strings.HasSuffix(val, CorruptMarker))
}

func (r *Recorder) observe(at time.Time, lat time.Duration, err error, wrong bool) {
	off := at.Sub(r.start)
	if off < 0 || off >= r.warmup+r.duration {
		return
	}
	inWindow := off >= r.warmup

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		if inWindow {
			r.errors++
		}
		return
	}
	if sec := int(off / time.Second); sec < len(r.timeline) {
		r.timeline[sec]++
	}
	if inWindow {
		r.lats = append(r.lats, lat)
		if wrong {
			r.wrongGets++
		}
	}
}

// Summary aggregates the observations recorded so far.
func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	lats := make([]time.Duration, len(r.lats))
	copy(lats, r.lats)
	s := Summary{Errors: r.errors, WrongGets: r.wrongGets}
	r.mu.Unlock()

	n := len(lats)
	if r.duration > 0 {
		s.TPS = float64(n) / r.duration.Seconds()
	}
	if n == 0 {
		return s
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	s.LatMeanMs = toMs(sum) / float64(n)
	s.LatP50Ms = toMs(percentile(lats, 0.50))
	s.LatP99Ms = toMs(percentile(lats, 0.99))
	return s
}

// Timeline returns the number of successful requests per second since start
// (index 0 is the first second), including the warmup phase.
func (r *Recorder) Timeline() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, len(r.timeline))
	copy(out, r.timeline)
	return out
}

func toMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// percentile expects a sorted, non-empty slice; index is ceil(q*n)-1.
func percentile(sorted []time.Duration, q float64) time.Duration {
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// csvColumns is the fixed column order of the results CSV.
var csvColumns = []string{
	"ts", "commit", "exp", "backend", "n", "f", "clients", "block_size",
	"workload", "rtt_ms", "fault", "rep", "commit_mode",
	"tps", "lat_mean_ms", "lat_p50_ms", "lat_p99_ms", "errors", "wrong_gets",
	"host_cpu_max_pct", "saturated", "net_bytes_per_block",
}

// metricColumns are filled from Summary and may not be given as tags.
var metricColumns = map[string]bool{
	"tps": true, "lat_mean_ms": true, "lat_p50_ms": true, "lat_p99_ms": true,
	"errors": true, "wrong_gets": true,
}

// AppendCSVRow appends one result row to the CSV at path, writing the header
// first if the file is missing or empty. Metric columns come from s, all other
// columns from tags (empty when absent). A tag that is not a non-metric column
// is rejected.
func AppendCSVRow(path string, tags map[string]string, s Summary) error {
	known := make(map[string]bool, len(csvColumns))
	for _, c := range csvColumns {
		known[c] = true
	}
	for k := range tags {
		if !known[k] || metricColumns[k] {
			return fmt.Errorf("benchmark: invalid csv tag %q", k)
		}
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w := csv.NewWriter(f)
	if st.Size() == 0 {
		if err := w.Write(csvColumns); err != nil {
			f.Close()
			return err
		}
	}

	metrics := map[string]string{
		"tps":         strconv.FormatFloat(s.TPS, 'f', 2, 64),
		"lat_mean_ms": strconv.FormatFloat(s.LatMeanMs, 'f', 3, 64),
		"lat_p50_ms":  strconv.FormatFloat(s.LatP50Ms, 'f', 3, 64),
		"lat_p99_ms":  strconv.FormatFloat(s.LatP99Ms, 'f', 3, 64),
		"errors":      strconv.FormatInt(s.Errors, 10),
		"wrong_gets":  strconv.FormatInt(s.WrongGets, 10),
	}
	row := make([]string, len(csvColumns))
	for i, c := range csvColumns {
		if v, ok := metrics[c]; ok {
			row[i] = v
		} else {
			row[i] = tags[c]
		}
	}
	if err := w.Write(row); err != nil {
		f.Close()
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
