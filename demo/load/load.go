// Package load is the live demo's closed-loop YCSB-style load generator: N
// clients, each issuing one Get or Set at a time against a demo cluster, and
// reporting every operation's latency and error to a recorder.
package load

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hybrid/demo/cluster"
)

// ErrRunning is returned by Start while a run is active.
var ErrRunning = errors.New("load: already running")

const (
	minClients  = 1
	maxClients  = 256
	minDuration = 5 * time.Second
	maxDuration = 300 * time.Second

	defaultKeys      = 1000
	defaultValueSize = 100

	setTimeout = 10 * time.Second
	getTimeout = 3 * time.Second

	preloadWriters = 16
	tickEvery      = time.Second
	upRefreshEvery = 250 * time.Millisecond
	failBackoff    = time.Millisecond
)

// Config describes one load run.
type Config struct {
	Clients   int
	Workload  string // "a" (50% reads), "b" (95% reads) or "c" (100% reads)
	Duration  time.Duration
	Keys      int // key space ycsb0..ycsb{Keys-1}; default 1000
	ValueSize int // bytes per written value; default 100
}

// readFraction maps a YCSB workload letter to its fraction of reads.
var readFraction = map[string]float64{"a": 0.5, "b": 0.95, "c": 1.0}

// Validate checks the config's bounds.
func (c Config) Validate() error {
	if c.Clients < minClients || c.Clients > maxClients {
		return fmt.Errorf("load: clients must be %d..%d, got %d", minClients, maxClients, c.Clients)
	}
	if _, ok := readFraction[c.Workload]; !ok {
		return fmt.Errorf("load: workload must be a, b or c, got %q", c.Workload)
	}
	if c.Duration < minDuration || c.Duration > maxDuration {
		return fmt.Errorf("load: duration must be %v..%v, got %v", minDuration, maxDuration, c.Duration)
	}
	if c.Keys < 0 || c.ValueSize < 0 {
		return errors.New("load: keys and value size must not be negative")
	}
	return nil
}

func (c Config) withDefaults() Config {
	if c.Keys == 0 {
		c.Keys = defaultKeys
	}
	if c.ValueSize == 0 {
		c.ValueSize = defaultValueSize
	}
	return c
}

// keyName is the i-th load key. The ycsb prefix keeps the load clear of the
// keys the UI scenario writes and verifies (user42).
func keyName(i int) string { return fmt.Sprintf("ycsb%d", i) }

// Runner runs at most one load run at a time against a cluster.
type Runner struct {
	cl  cluster.Cluster
	rec func(time.Duration, error)

	mu        sync.Mutex
	running   bool
	preloaded bool // keys are in the cluster; cleared by ResetPreload
	cancel    context.CancelFunc
	done      chan struct{} // closed when the latest run has fully ended
}

// NewRunner returns a Runner that reports every operation to rec.
func NewRunner(c cluster.Cluster, rec func(time.Duration, error)) *Runner {
	return &Runner{cl: c, rec: rec}
}

// Running reports whether a run is active.
func (r *Runner) Running() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

// ResetPreload makes the next Start preload the keys again. Call it after the
// cluster has been reset.
func (r *Runner) ResetPreload() {
	r.mu.Lock()
	r.preloaded = false
	r.mu.Unlock()
}

// Stop ends the active run, if any, and returns once it has fully stopped.
func (r *Runner) Stop() {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// Start begins a run in the background: it preloads the keys if this
// cluster generation has not been preloaded yet (unrecorded), then runs the
// closed loop for cfg.Duration. onState (may be nil) is called with
// (true, remaining) about once a second while running and (false, 0) exactly
// once when the run ends, by duration or Stop.
func (r *Runner) Start(cfg Config, onState func(running bool, remaining time.Duration)) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	cfg = cfg.withDefaults()
	if onState == nil {
		onState = func(bool, time.Duration) {}
	}

	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrRunning
	}
	ctx, cancel := context.WithCancel(context.Background())
	done, prev := make(chan struct{}), r.done
	r.running, r.cancel, r.done = true, cancel, done
	needPreload := !r.preloaded
	r.mu.Unlock()

	go r.run(ctx, cancel, prev, done, cfg, needPreload, onState)
	return nil
}

func (r *Runner) run(ctx context.Context, cancel context.CancelFunc, prev, done chan struct{}, cfg Config, needPreload bool, onState func(bool, time.Duration)) {
	// The previous run may still be delivering its (false, 0); wait so that
	// state callbacks never arrive out of order.
	if prev != nil {
		<-prev
	}
	defer func() {
		cancel()
		r.mu.Lock()
		r.running, r.cancel = false, nil
		r.mu.Unlock()
		onState(false, 0)
		close(done)
	}()

	onState(true, cfg.Duration)
	if needPreload && r.preload(ctx, cfg) {
		r.mu.Lock()
		r.preloaded = true
		r.mu.Unlock()
	}
	if ctx.Err() != nil {
		return
	}

	end := time.Now().Add(cfg.Duration)
	tctx, tcancel := context.WithDeadline(ctx, end)
	defer tcancel()

	view := newNodeView(r.cl)
	view.refresh()
	var wg sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.client(tctx, cfg, i, view)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		view.keepFresh(tctx)
	}()

	tick := time.NewTicker(tickEvery)
	defer tick.Stop()
loop:
	for {
		select {
		case <-tctx.Done():
			break loop
		case <-tick.C:
			if rem := time.Until(end); rem > 0 {
				onState(true, rem)
			}
		}
	}
	tcancel()
	wg.Wait()
}

// preload writes keys ycsb0..ycsb{Keys-1} to node1..N round-robin with up to
// preloadWriters concurrent writers. It reports whether every key was stored.
func (r *Runner) preload(ctx context.Context, cfg Config) bool {
	view := newNodeView(r.cl)
	view.refresh()
	value := strings.Repeat("x", cfg.ValueSize)
	var failed int32
	var wg sync.WaitGroup
	sem := make(chan struct{}, preloadWriters)
	for i := 0; i < cfg.Keys && ctx.Err() == nil; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			cctx, cancel := context.WithTimeout(ctx, setTimeout)
			defer cancel()
			if err := r.cl.Set(cctx, view.target(i), keyName(i), value); err != nil {
				atomic.StoreInt32(&failed, 1)
			}
		}(i)
	}
	wg.Wait()
	return ctx.Err() == nil && atomic.LoadInt32(&failed) == 0
}

// client is one closed-loop client: it issues operations back to back until
// ctx ends. Client i targets node (i mod N)+1, or the next up node if its own
// is down.
func (r *Runner) client(ctx context.Context, cfg Config, i int, view *nodeView) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
	reads := readFraction[cfg.Workload]
	value := strings.Repeat("v", cfg.ValueSize)
	for ctx.Err() == nil {
		key := keyName(rng.Intn(cfg.Keys))
		node := view.target(i)
		start := time.Now()
		var err error
		if rng.Float64() < reads {
			cctx, cancel := context.WithTimeout(ctx, getTimeout)
			// A missing key (ok=false) is a read result, not an error.
			_, _, err = r.cl.Get(cctx, node, key)
			cancel()
		} else {
			cctx, cancel := context.WithTimeout(ctx, setTimeout)
			err = r.cl.Set(cctx, node, key, value)
			cancel()
		}
		if err != nil && ctx.Err() != nil {
			return // aborted by the end of the run, not a failed op
		}
		r.rec(time.Since(start), err)
		if err != nil {
			// Back off briefly so clients of a dead cluster (instant
			// failures) do not spin and flood the recorder.
			select {
			case <-ctx.Done():
			case <-time.After(failBackoff):
			}
		}
	}
}

// nodeView is a periodically refreshed snapshot of which nodes are up, so
// clients do not query the cluster's node list on every operation.
type nodeView struct {
	cl    cluster.Cluster
	nodes atomic.Value // []cluster.NodeInfo
}

func newNodeView(cl cluster.Cluster) *nodeView { return &nodeView{cl: cl} }

func (v *nodeView) refresh() { v.nodes.Store(v.cl.Nodes()) }

func (v *nodeView) keepFresh(ctx context.Context) {
	t := time.NewTicker(upRefreshEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			v.refresh()
		}
	}
}

// target returns the node for client i: node (i mod N), or the next up node
// when that one is down. If every node is down it returns the client's own.
func (v *nodeView) target(i int) string {
	nodes, _ := v.nodes.Load().([]cluster.NodeInfo)
	if len(nodes) == 0 {
		return "node1"
	}
	own := i % len(nodes)
	for k := 0; k < len(nodes); k++ {
		if n := nodes[(own+k)%len(nodes)]; n.Up {
			return n.Name
		}
	}
	return nodes[own].Name
}
