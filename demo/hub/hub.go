// Package hub aggregates the live demo's state: node views, the consistency
// verdict, metrics and an event log, and publishes changes to UI subscribers.
package hub

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"hybrid/demo/cluster"
)

const (
	rootHistory   = 64  // heights of roots kept per node
	blockHistory  = 30  // blocks shown in a snapshot
	logHistory    = 200 // log lines kept
	subscriberCap = 64  // pending updates per subscriber before drops
	getTimeout    = 3 * time.Second
	tickEvery     = time.Second
)

// NodeView is one node as the UI shows it.
type NodeView struct {
	Name   string   `json:"name"`
	Status string   `json:"status"` // up | down | divergent
	Height int64    `json:"height"`
	Root   string   `json:"root"`  // hex ledger root at Height
	Group  string   `json:"group"` // A, B, ..; "" when down
	Value  string   `json:"value"` // tracked key's value on this node
	Faults []string `json:"faults"`
}

// BlockView is one block as seen by the reference node.
type BlockView struct {
	Height   int64  `json:"height"`
	Proposer string `json:"proposer"`
	Txs      int    `json:"txs"`
	Aborted  int    `json:"aborted"`
}

// LogLine is one line of the event log.
type LogLine struct {
	T     string `json:"t"`     // HH:MM:SS
	Level string `json:"level"` // info | ok | warn | error
	Text  string `json:"text"`
}

// LoadState describes the load generator, as reported to the hub.
type LoadState struct {
	Running    bool   `json:"running"`
	Phase      string `json:"phase"` // "preloading" or "running" while running; "" otherwise
	Clients    int    `json:"clients"`
	Workload   string `json:"workload"`
	RemainingS int    `json:"remaining_s"`
}

// Snapshot is the hub's whole state.
type Snapshot struct {
	Info        cluster.ClusterInfo `json:"info"`
	Nodes       []NodeView          `json:"nodes"`
	Consistency Consistency         `json:"consistency"`
	Blocks      []BlockView         `json:"blocks"`
	Metrics     Metrics             `json:"metrics"`
	Log         []LogLine           `json:"log"`
	Load        LoadState           `json:"load"`
	TrackedKey  string              `json:"tracked_key"`
	Step        int                 `json:"scenario_step"`
}

// Update is one change pushed to subscribers. Data is the new value of the
// part of the Snapshot that Kind names: nodes → []NodeView, consistency →
// Consistency, block → BlockView, metrics → Metrics, log → LogLine, load →
// LoadState.
type Update struct {
	Kind string
	Data interface{}
}

// Hub aggregates a cluster's state. Create with New, then call Run.
type Hub struct {
	c     cluster.Cluster
	clock func() time.Time

	mu      sync.Mutex
	infos   []cluster.NodeInfo          // last Nodes() reading
	values  map[string]string           // node → tracked key's value
	roots   map[string]map[int64]string // node → height → hex root
	blocks  map[string][]BlockView      // node → last blockHistory blocks
	metrics *metricsTracker
	log     []LogLine
	load    LoadState
	tracked string
	step    int
	last    Consistency // last verdict that had a common height
	logged  string      // verdict last reported to the log

	subMu sync.Mutex
	subs  map[chan Update]struct{}
}

// New returns a Hub for c. clock supplies timestamps for the log and metrics
// (nil means time.Now).
func New(c cluster.Cluster, clock func() time.Time) *Hub {
	if clock == nil {
		clock = time.Now
	}
	h := &Hub{
		c:       c,
		clock:   clock,
		values:  map[string]string{},
		roots:   map[string]map[int64]string{},
		blocks:  map[string][]BlockView{},
		metrics: newMetricsTracker(),
		subs:    map[chan Update]struct{}{},
	}
	h.infos = c.Nodes()
	return h
}

// Run consumes the cluster's block events and refreshes the hub every second
// until ctx is done.
func (h *Hub) Run(ctx context.Context) {
	events := h.c.Events()
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				events = nil // cluster closed
				continue
			}
			h.onBlock(ev)
		case <-ticker.C:
			h.tick(ctx)
		}
	}
}

func (h *Hub) onBlock(ev cluster.BlockEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.roots[ev.Node]
	if r == nil {
		r = map[int64]string{}
		h.roots[ev.Node] = r
	}
	r[ev.Height] = hex.EncodeToString(ev.Root)
	for old := range r {
		if old <= ev.Height-rootHistory {
			delete(r, old)
		}
	}
	bv := BlockView{Height: ev.Height, Proposer: ev.Proposer, Txs: ev.Txs, Aborted: ev.Aborted}
	bl := append(h.blocks[ev.Node], bv)
	if len(bl) > blockHistory {
		bl = bl[len(bl)-blockHistory:]
	}
	h.blocks[ev.Node] = bl
	if ev.Node == h.referenceLocked() {
		h.publish(Update{"block", bv})
	}
}

// tick refreshes node status and tracked values, then publishes the derived
// state.
func (h *Hub) tick(ctx context.Context) {
	infos := h.c.Nodes()
	h.mu.Lock()
	key := h.tracked
	h.mu.Unlock()

	values := h.readValues(ctx, infos, key)

	h.mu.Lock()
	defer h.mu.Unlock()
	if key != h.tracked { // changed (or reset) while reading
		values = map[string]string{}
	}
	h.infos, h.values = infos, values
	nodes, cons := h.nodeViewsLocked()
	if cons.Verdict != h.logged && (cons.Verdict == "DIVERGENT" || h.logged == "DIVERGENT") {
		if cons.Verdict == "DIVERGENT" {
			h.logLocked("warn", fmt.Sprintf("Replicas diverged at height %d: %v", cons.Height, cons.Divergent))
		} else if cons.Verdict == "CONSISTENT" {
			h.logLocked("ok", fmt.Sprintf("Replicas consistent again at height %d", cons.Height))
		}
	}
	if cons.Verdict != "UNKNOWN" {
		h.logged = cons.Verdict
	}
	h.publish(Update{"nodes", nodes})
	h.publish(Update{"consistency", cons})
	h.publish(Update{"metrics", h.metrics.snapshot(h.clock())})
}

// readValues reads key from every up node in parallel; missing keys and
// failed reads give "".
func (h *Hub) readValues(ctx context.Context, infos []cluster.NodeInfo, key string) map[string]string {
	values := map[string]string{}
	if key == "" {
		return values
	}
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for _, n := range infos {
		if !n.Up {
			continue
		}
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			gctx, cancel := context.WithTimeout(ctx, getTimeout)
			defer cancel()
			kv, ok, err := h.c.Get(gctx, name, key)
			if err != nil || !ok {
				return
			}
			mu.Lock()
			values[name] = kv.Value
			mu.Unlock()
		}(n.Name)
	}
	wg.Wait()
	return values
}

// Snapshot returns the hub's current state.
func (h *Hub) Snapshot() Snapshot {
	info := h.c.Info()
	h.mu.Lock()
	defer h.mu.Unlock()
	nodes, cons := h.nodeViewsLocked()
	var blocks []BlockView
	if ref := h.referenceLocked(); ref != "" {
		blocks = append(blocks, h.blocks[ref]...)
	}
	if blocks == nil {
		blocks = []BlockView{}
	}
	return Snapshot{
		Info:        info,
		Nodes:       nodes,
		Consistency: cons,
		Blocks:      blocks,
		Metrics:     h.metrics.snapshot(h.clock()),
		Log:         append([]LogLine{}, h.log...),
		Load:        h.load,
		TrackedKey:  h.tracked,
		Step:        h.step,
	}
}

// nodeViewsLocked derives the node views and verdict from the current roots
// and node infos.
func (h *Hub) nodeViewsLocked() ([]NodeView, Consistency) {
	var up, down []string
	for _, n := range h.infos {
		if n.Up {
			up = append(up, n.Name)
		} else {
			down = append(down, n.Name)
		}
	}
	sort.Strings(up)
	cons := Judge(h.roots, up)
	cons.Down = append([]string{}, down...)
	sort.Strings(cons.Down)
	if cons.Verdict == "UNKNOWN" && h.last.Verdict != "" {
		// No common height right now (e.g. a node is catching up): keep the
		// previous verdict, minus nodes that have since gone down.
		cons = h.keepLastLocked(cons)
	} else if cons.Verdict != "UNKNOWN" {
		h.last = cons
	}

	divergent := map[string]bool{}
	for _, n := range cons.Divergent {
		divergent[n] = true
	}
	groups := groupLetters(h.roots, up, cons.Height)

	nodes := make([]NodeView, 0, len(h.infos))
	for _, info := range h.infos {
		v := NodeView{Name: info.Name, Status: "up", Faults: []string{}, Group: groups[info.Name], Value: h.values[info.Name]}
		for _, f := range info.Faults {
			v.Faults = append(v.Faults, string(f))
		}
		if height, ok := latest(h.roots[info.Name]); ok {
			v.Height, v.Root = height, h.roots[info.Name][height]
		}
		switch {
		case !info.Up:
			v.Status, v.Group, v.Value = "down", "", ""
		case divergent[info.Name]:
			v.Status = "divergent"
		}
		nodes = append(nodes, v)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes, cons
}

func (h *Hub) keepLastLocked(cur Consistency) Consistency {
	isDown := map[string]bool{}
	for _, n := range cur.Down {
		isDown[n] = true
	}
	kept := h.last
	kept.Down, kept.Up = cur.Down, cur.Up
	kept.Divergent = []string{}
	for _, n := range h.last.Divergent {
		if !isDown[n] {
			kept.Divergent = append(kept.Divergent, n)
		}
	}
	return kept
}

func latest(r map[int64]string) (int64, bool) {
	var best int64
	found := false
	for h := range r {
		if !found || h > best {
			best, found = h, true
		}
	}
	return best, found
}

// referenceLocked is the lowest-numbered up node, whose blocks the UI shows.
func (h *Hub) referenceLocked() string {
	ref := ""
	for _, n := range h.infos {
		if n.Up && (ref == "" || n.Name < ref) {
			ref = n.Name
		}
	}
	return ref
}

// RecordOp notes one finished client operation (called by the load generator
// and the transaction handler).
func (h *Hub) RecordOp(lat time.Duration, err error) {
	now := h.clock()
	h.mu.Lock()
	h.metrics.record(now, lat, err)
	h.mu.Unlock()
}

// Logf appends a line to the event log. level is info, ok, warn or error.
func (h *Hub) Logf(level, format string, args ...interface{}) {
	h.mu.Lock()
	h.logLocked(level, fmt.Sprintf(format, args...))
	h.mu.Unlock()
}

func (h *Hub) logLocked(level, text string) {
	line := LogLine{T: h.clock().Format("15:04:05"), Level: level, Text: text}
	h.log = append(h.log, line)
	if len(h.log) > logHistory {
		h.log = h.log[len(h.log)-logHistory:]
	}
	h.publish(Update{"log", line})
}

// Mark puts a labelled fault marker on the sparkline.
func (h *Hub) Mark(label string) {
	now := h.clock()
	h.mu.Lock()
	h.metrics.mark(now, label)
	h.mu.Unlock()
}

// SetTrackedKey chooses the key whose value each node row shows.
func (h *Hub) SetTrackedKey(k string) {
	h.mu.Lock()
	h.tracked = k
	h.mu.Unlock()
}

// SetStep records the scenario step the UI is on.
func (h *Hub) SetStep(n int) {
	h.mu.Lock()
	h.step = n
	h.mu.Unlock()
}

// SetLoad records the load generator's state and publishes it.
func (h *Hub) SetLoad(l LoadState) {
	h.mu.Lock()
	h.load = l
	h.publish(Update{"load", l})
	h.mu.Unlock()
}

// Reset forgets everything learned from the cluster. Call it after the
// cluster itself was reset: events still buffered from before are discarded
// without blocking, then roots, blocks, metrics, log, tracked key, scenario
// step and load state are cleared.
func (h *Hub) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	events := h.c.Events()
drain:
	for {
		select {
		case _, ok := <-events:
			if !ok {
				break drain
			}
		default:
			break drain
		}
	}
	h.roots = map[string]map[int64]string{}
	h.blocks = map[string][]BlockView{}
	h.values = map[string]string{}
	h.metrics = newMetricsTracker()
	h.log = nil
	h.load = LoadState{}
	h.tracked = ""
	h.step = 0
	h.last = Consistency{}
	h.logged = ""
	h.infos = h.c.Nodes()
	h.logLocked("info", "Cluster reset")
}

// Subscribe returns a channel of updates and a function that cancels the
// subscription (safe to call more than once). A subscriber that does not keep
// up loses updates rather than slowing the hub.
func (h *Hub) Subscribe() (<-chan Update, func()) {
	ch := make(chan Update, subscriberCap)
	h.subMu.Lock()
	h.subs[ch] = struct{}{}
	h.subMu.Unlock()
	return ch, func() {
		h.subMu.Lock()
		defer h.subMu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
}

func (h *Hub) publish(u Update) {
	h.subMu.Lock()
	defer h.subMu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- u:
		default: // subscriber is behind; drop
		}
	}
}
