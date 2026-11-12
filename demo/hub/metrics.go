package hub

import (
	"math"
	"sort"
	"time"
)

const (
	seriesLen     = 60 // seconds of throughput history
	latencyWindow = 10 // seconds of latency samples behind p50/p99
)

// Metrics is the throughput/latency panel.
type Metrics struct {
	TPS     float64   `json:"tps"` // successful ops in the last full second
	P50Ms   float64   `json:"p50_ms"`
	P99Ms   float64   `json:"p99_ms"`
	Errors  int64     `json:"errors"`  // cumulative failed ops
	Series  []float64 `json:"series"`  // ops/s for the last 60 full seconds, oldest first
	Markers []Marker  `json:"markers"` // fault markers within the series window
}

// Marker labels a moment on the sparkline.
type Marker struct {
	AgoS  int    `json:"ago_s"`
	Label string `json:"label"`
}

type sample struct {
	at  time.Time
	lat time.Duration
}

type marker struct {
	at    time.Time
	label string
}

// metricsTracker keeps per-second op counts and recent latencies. It is not
// safe for concurrent use; Hub guards it. Time is always passed in so tests
// can drive a fake clock.
type metricsTracker struct {
	buckets map[int64]int // unix second → successful ops
	samples []sample      // successful ops in the last latencyWindow seconds
	markers []marker
	errors  int64
}

func newMetricsTracker() *metricsTracker {
	return &metricsTracker{buckets: map[int64]int{}}
}

// record notes one finished operation. Failed operations only count towards
// Errors, so TPS and the percentiles describe work that succeeded.
func (m *metricsTracker) record(now time.Time, lat time.Duration, err error) {
	if err != nil {
		m.errors++
		return
	}
	m.buckets[now.Unix()]++
	m.samples = append(m.samples, sample{at: now, lat: lat})
}

func (m *metricsTracker) mark(now time.Time, label string) {
	m.markers = append(m.markers, marker{at: now, label: label})
}

func (m *metricsTracker) snapshot(now time.Time) Metrics {
	m.prune(now)
	last := now.Unix() - 1 // the last full second
	out := Metrics{
		TPS:     float64(m.buckets[last]),
		Errors:  m.errors,
		Series:  make([]float64, seriesLen),
		Markers: []Marker{},
	}
	for i := range out.Series {
		out.Series[i] = float64(m.buckets[last-int64(seriesLen-1-i)])
	}

	lats := make([]float64, 0, len(m.samples))
	for _, s := range m.samples {
		lats = append(lats, float64(s.lat)/float64(time.Millisecond))
	}
	sort.Float64s(lats)
	out.P50Ms = nearestRank(lats, 0.50)
	out.P99Ms = nearestRank(lats, 0.99)

	for _, mk := range m.markers {
		out.Markers = append(out.Markers, Marker{AgoS: int(now.Sub(mk.at) / time.Second), Label: mk.label})
	}
	return out
}

// prune drops data older than what snapshot can show.
func (m *metricsTracker) prune(now time.Time) {
	oldest := now.Unix() - seriesLen
	for s := range m.buckets {
		if s < oldest {
			delete(m.buckets, s)
		}
	}
	cut := now.Add(-latencyWindow * time.Second)
	i := 0
	for i < len(m.samples) && m.samples[i].at.Before(cut) {
		i++
	}
	m.samples = m.samples[i:]

	cut = now.Add(-seriesLen * time.Second)
	j := 0
	for j < len(m.markers) && m.markers[j].at.Before(cut) {
		j++
	}
	m.markers = m.markers[j:]
}

// nearestRank is the p-th percentile of sorted by the nearest-rank method.
func nearestRank(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}
