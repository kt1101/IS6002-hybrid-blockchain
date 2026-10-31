package benchmark

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// OpKind classifies an observation for the per-kind statistics (OpSummaries,
// SetTimeline). Every kind counts in Summary and Timeline.
type OpKind int

const (
	// OpOther is an observation of Observe: counted in Summary and Timeline
	// only, in no per-kind statistic.
	OpOther OpKind = iota
	// OpRead is an observation of ObserveGet.
	OpRead
	// OpWrite is an observation of ObserveSet.
	OpWrite
	numOpKinds
)

// kindStats are the in-window observations of one OpKind.
type kindStats struct {
	lats    []time.Duration // successful ops inside the window
	errors  int64           // failed ops inside the window (aborted ones included)
	aborted int64           // of errors: gRPC codes.Aborted (a by-design abort, e.g. a stale version)
}

// Recorder collects per-request observations of a time-based run. Only
// observations inside [start+warmup, start+warmup+duration) contribute to the
// Summary and the OpSummaries; the Timeline and the SetTimeline additionally
// cover the warmup phase. Safe for concurrent use.
type Recorder struct {
	mu        sync.Mutex
	warmup    time.Duration
	duration  time.Duration
	start     time.Time
	kinds     [numOpKinds]kindStats
	wrongGets int64
	errKinds  map[string]int64 // in-window errors by message, at most MaxErrorKinds keys
	errOther  int64            // in-window errors whose message did not fit in errKinds
	timeline  []int64          // successful ops per second since start, incl. warmup
	wtimeline []int64          // successful Sets (ObserveSet) per second since start, incl. warmup
}

// NewRecorder creates a Recorder for a run that began at start.
func NewRecorder(warmup, duration time.Duration, start time.Time) *Recorder {
	total := warmup + duration
	secs := int((total + time.Second - 1) / time.Second)
	if secs < 0 {
		secs = 0
	}
	return &Recorder{
		warmup:    warmup,
		duration:  duration,
		start:     start,
		timeline:  make([]int64, secs),
		wtimeline: make([]int64, secs),
	}
}

// Observe records one finished request of no particular kind (OpOther): at
// is its completion time, lat its latency, err its outcome. It counts in
// Summary and Timeline, not in OpSummaries.
func (r *Recorder) Observe(at time.Time, lat time.Duration, err error) {
	r.observe(OpOther, at, lat, err, false)
}

// ObserveGet records a finished Get (OpRead); a successful Get whose value
// ends with CorruptMarker is also counted as a wrong get.
func (r *Recorder) ObserveGet(at time.Time, lat time.Duration, val string, err error) {
	r.observe(OpRead, at, lat, err, err == nil && strings.HasSuffix(val, CorruptMarker))
}

// ObserveSet records a finished Set (OpWrite); a successful one also counts
// in SetTimeline.
func (r *Recorder) ObserveSet(at time.Time, lat time.Duration, err error) {
	r.observe(OpWrite, at, lat, err, false)
}

func (r *Recorder) observe(kind OpKind, at time.Time, lat time.Duration, err error, wrong bool) {
	off := at.Sub(r.start)
	if off < 0 || off >= r.warmup+r.duration {
		return
	}
	inWindow := off >= r.warmup

	r.mu.Lock()
	defer r.mu.Unlock()
	k := &r.kinds[kind]
	if err != nil {
		if inWindow {
			k.errors++
			if status.Code(err) == codes.Aborted {
				k.aborted++
			}
			r.countErrorKind(err.Error())
		}
		return
	}
	if sec := int(off / time.Second); sec < len(r.timeline) {
		r.timeline[sec]++
		if kind == OpWrite {
			r.wtimeline[sec]++
		}
	}
	if inWindow {
		k.lats = append(k.lats, lat)
		if wrong {
			r.wrongGets++
		}
	}
}

// MaxErrorKinds is the number of distinct error messages a Recorder keeps;
// further distinct messages are counted under ErrorKindOther.
const MaxErrorKinds = 20

// ErrorKindOther is the Msg of the bucket for messages beyond MaxErrorKinds.
const ErrorKindOther = "(other)"

// maxErrorMsgLen bounds the length of a kept error message.
const maxErrorMsgLen = 300

// ErrorKind is the number of in-window errors with one message.
type ErrorKind struct {
	Msg   string
	Count int64
}

// seqRE matches the per-request "seq <n>" of the core's error messages; it is
// replaced by "seq N" so that messages of the same cause count together.
var seqRE = regexp.MustCompile(`\bseq [0-9]+`)

// countErrorKind must be called with r.mu held.
func (r *Recorder) countErrorKind(msg string) {
	msg = seqRE.ReplaceAllString(msg, "seq N")
	if len(msg) > maxErrorMsgLen {
		msg = msg[:maxErrorMsgLen]
	}
	if r.errKinds == nil {
		r.errKinds = make(map[string]int64)
	}
	if _, ok := r.errKinds[msg]; ok || len(r.errKinds) < MaxErrorKinds {
		r.errKinds[msg]++
		return
	}
	r.errOther++
}

// ErrorKinds returns the in-window errors grouped by message, most frequent
// first (ties by message), followed by an ErrorKindOther entry if any error
// did not fit into the first MaxErrorKinds distinct messages.
func (r *Recorder) ErrorKinds() []ErrorKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ErrorKind, 0, len(r.errKinds)+1)
	for m, c := range r.errKinds {
		out = append(out, ErrorKind{Msg: m, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Msg < out[j].Msg
	})
	if r.errOther > 0 {
		out = append(out, ErrorKind{Msg: ErrorKindOther, Count: r.errOther})
	}
	return out
}

// latStats are the count and latency statistics of a set of latencies.
type latStats struct {
	n                    int
	meanMs, p50Ms, p99Ms float64
}

// summarize sorts lats in place and returns their statistics (zero
// latencies when lats is empty).
func summarize(lats []time.Duration) latStats {
	st := latStats{n: len(lats)}
	if st.n == 0 {
		return st
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	st.meanMs = toMs(sum) / float64(st.n)
	st.p50Ms = toMs(percentile(lats, 0.50))
	st.p99Ms = toMs(percentile(lats, 0.99))
	return st
}

// Summary aggregates the observations recorded so far over every kind (the
// CSV row: reads, writes and OpOther together).
func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	var lats []time.Duration
	s := Summary{WrongGets: r.wrongGets}
	for i := range r.kinds {
		lats = append(lats, r.kinds[i].lats...)
		s.Errors += r.kinds[i].errors
	}
	r.mu.Unlock()

	st := summarize(lats)
	if r.duration > 0 {
		s.TPS = float64(st.n) / r.duration.Seconds()
	}
	s.LatMeanMs, s.LatP50Ms, s.LatP99Ms = st.meanMs, st.p50Ms, st.p99Ms
	return s
}

// OpSummary is the measurement-window result of one operation kind: Count
// successful ops (TPS = Count / duration), their latency mean, p50 and p99,
// Errors failed ops other than aborts, and Aborted failed ops with gRPC code
// Aborted (by design: a stale version, or a block the consensus dropped,
// e.g. a Kafka Abort vote). Errors + Aborted = every failed op of the kind
// (Summary.Errors, the CSV errors column, counts both).
type OpSummary struct {
	Op        string // "read" or "write"
	Count     int64
	TPS       float64
	LatMeanMs float64
	LatP50Ms  float64
	LatP99Ms  float64
	Errors    int64
	Aborted   int64
}

// OpSummaries returns the read (ObserveGet) and the write (ObserveSet)
// summaries, in that order; OpOther observations (Observe) are in neither.
func (r *Recorder) OpSummaries() []OpSummary {
	kinds := []struct {
		kind OpKind
		name string
	}{{OpRead, "read"}, {OpWrite, "write"}}
	out := make([]OpSummary, 0, len(kinds))
	for _, k := range kinds {
		r.mu.Lock()
		lats := make([]time.Duration, len(r.kinds[k.kind].lats))
		copy(lats, r.kinds[k.kind].lats)
		errs := r.kinds[k.kind].errors
		aborted := r.kinds[k.kind].aborted
		r.mu.Unlock()
		st := summarize(lats)
		o := OpSummary{Op: k.name, Count: int64(st.n), Errors: errs - aborted, Aborted: aborted,
			LatMeanMs: st.meanMs, LatP50Ms: st.p50Ms, LatP99Ms: st.p99Ms}
		if r.duration > 0 {
			o.TPS = float64(st.n) / r.duration.Seconds()
		}
		out = append(out, o)
	}
	return out
}

// OpsCSVHeader is the header line of the per-run ops sidecar (WriteOpsCSV).
const OpsCSVHeader = "op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted"

// WriteOpsCSV writes ops (OpSummaries) to path as a small CSV: the
// OpsCSVHeader line, then one line per kind (experiments/run_one.sh keeps it
// as results/raw/<EXP>/<tag>.ops.csv next to the 22-column row).
func WriteOpsCSV(path string, ops []OpSummary) error {
	var sb strings.Builder
	sb.WriteString(OpsCSVHeader)
	sb.WriteByte('\n')
	for _, o := range ops {
		fmt.Fprintf(&sb, "%s,%d,%s,%s,%s,%s,%d,%d\n", o.Op, o.Count,
			strconv.FormatFloat(o.TPS, 'f', 2, 64),
			strconv.FormatFloat(o.LatMeanMs, 'f', 3, 64),
			strconv.FormatFloat(o.LatP50Ms, 'f', 3, 64),
			strconv.FormatFloat(o.LatP99Ms, 'f', 3, 64),
			o.Errors, o.Aborted)
	}
	return os.WriteFile(path, []byte(sb.String()), 0o644)
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

// SetTimeline is Timeline restricted to successful Sets (ObserveSet): same
// length, same t=0 (index 0 is the first second of the warmup).
func (r *Recorder) SetTimeline() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, len(r.wtimeline))
	copy(out, r.wtimeline)
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
