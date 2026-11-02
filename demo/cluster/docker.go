package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"hybrid/veritas/driver"
	"hybrid/veritas/observe"
)

// ErrNoNodes: NewDocker found no node answering at the configured addresses.
var ErrNoNodes = errors.New("cluster: no node reachable")

const (
	defaultGRPCBase       = 19900 // gen_compose.py HOST_GRPC_BASE
	defaultEventsBase     = 19800 // gen_compose.py HOST_EVENTS_BASE
	defaultConnectTimeout = 10 * time.Second
	defaultFaultScript    = "experiments/demo_fault.sh"
	dockerDriverSignature = "demo"

	faultScriptTimeout = 30 * time.Second
	faultScriptNoSupp  = 4 // exit code: fault not supported on that node
	sseRetryInterval   = time.Second
	sseDownAfter       = 2 // consecutive failures before a node counts as down
	httpTimeout        = 10 * time.Second
	healthzTimeout     = 500 * time.Millisecond
	healthzPoll        = 100 * time.Millisecond
	sseMaxLine         = 1 << 20
)

// DockerConfig configures a Docker cluster: a published hbdb cluster started
// by experiments/demo_up.sh. Nodes are numbered from 1 (GRPCAddr(1) is node1).
// Zero fields take their defaults (see applyDefaults) except TSOAddr, which is
// used as given ("" = no TSO: versions are assigned by the nodes; cmd/demo
// picks 127.0.0.1:19870 for kafka and raft).
type DockerConfig struct {
	Backend string
	N       int
	// GRPCAddr and EventsURL give node i's (1-based) gRPC address and the
	// base URL of its observer. Default 127.0.0.1:(19900+i) and
	// http://127.0.0.1:(19800+i).
	GRPCAddr  func(i int) string
	EventsURL func(i int) string
	TSOAddr   string
	// FaultScript is run as `bash FaultScript <action> <node>`. Default
	// experiments/demo_fault.sh; a relative path is looked up from the
	// current directory upwards, so it works from any repo subdirectory.
	FaultScript string
	// ArmedCorrupt and ArmedSilent are the nodes whose veritas was started
	// with --fault armed (demo_up.sh). nil takes the default (node3 for
	// corrupt; node4 for silent on kafka, none otherwise); an empty non-nil
	// slice means none.
	ArmedCorrupt, ArmedSilent []string
	ConnectTimeout            time.Duration // default 10s
}

func (c *DockerConfig) applyDefaults() {
	if c.GRPCAddr == nil {
		c.GRPCAddr = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", defaultGRPCBase+i) }
	}
	if c.EventsURL == nil {
		c.EventsURL = func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", defaultEventsBase+i) }
	}
	if c.FaultScript == "" {
		c.FaultScript = defaultFaultScript
	}
	if c.ArmedCorrupt == nil {
		c.ArmedCorrupt = []string{"node3"}
	}
	if c.ArmedSilent == nil && c.Backend == "kafka" {
		c.ArmedSilent = []string{"node4"}
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
}

// Docker is a Cluster over a real hbdb cluster: Set goes through the node's
// gRPC (veritas/driver), Get and Prove through the node's observer, block
// events through its SSE stream, and faults through the fault script.
//
// A node is Up while its SSE stream is connected; two consecutive failed
// connects mark it down, and it is retried every second. A Set, Get or Prove
// that cannot reach the node marks it down at once; its stream marks it up
// again when it reconnects or delivers a block. NodeInfo.Faults
// lists the faults this Docker injected and has not healed: crash and
// corrupt stay (they cannot be healed), and so does silent on kafka.
type Docker struct {
	cfg    DockerConfig
	ctx    context.Context
	cancel context.CancelFunc
	events chan BlockEvent
	http   *http.Client // Get, Prove, healthz
	nodes  []*dockerNode

	readers   sync.WaitGroup
	closeOnce sync.Once
}

var _ Cluster = (*Docker)(nil)

type dockerNode struct {
	name string
	idx  int // 1-based

	openMu sync.Mutex // serializes opening drv, outside mu

	mu       sync.Mutex
	up       bool
	failures int // consecutive failed SSE connects
	faults   []Fault
	drv      *driver.Driver
	closed   bool // Close ran: no driver may be installed any more
}

// markDown marks n down after a request to it could not reach it. failures is
// raised too, so that if the stream also ends it does not wait for a second
// failed connect.
func (n *dockerNode) markDown() {
	n.mu.Lock()
	n.up, n.failures = false, sseDownAfter
	n.mu.Unlock()
}

// NewDocker connects to the cluster and starts one SSE reader per node. It
// returns ErrNoNodes if no node's /healthz answers within cfg.ConnectTimeout.
func NewDocker(ctx context.Context, cfg DockerConfig) (*Docker, error) {
	if cfg.N < 1 {
		return nil, fmt.Errorf("cluster: docker: N must be at least 1, got %d", cfg.N)
	}
	cfg.applyDefaults()
	cfg.FaultScript = resolveScript(cfg.FaultScript)
	// Fail at startup, not at the first fault button.
	if _, err := os.Stat(cfg.FaultScript); err != nil {
		return nil, fmt.Errorf("cluster: docker: fault script: %w", err)
	}

	dctx, cancel := context.WithCancel(ctx)
	d := &Docker{
		cfg:    cfg,
		ctx:    dctx,
		cancel: cancel,
		events: make(chan BlockEvent, eventBuffer),
		http:   &http.Client{Timeout: httpTimeout},
	}
	for i := 1; i <= cfg.N; i++ {
		d.nodes = append(d.nodes, &dockerNode{name: fmt.Sprintf("node%d", i), idx: i})
	}
	if err := d.waitAnyNode(ctx); err != nil {
		cancel()
		return nil, err
	}
	for _, n := range d.nodes {
		d.readers.Add(1)
		go d.readEvents(n)
	}
	return d, nil
}

// waitAnyNode returns nil once some node's /healthz answers.
func (d *Docker) waitAnyNode(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, d.cfg.ConnectTimeout)
	defer cancel()
	found := make(chan struct{}, len(d.nodes))
	var wg sync.WaitGroup
	for _, n := range d.nodes {
		wg.Add(1)
		go func(n *dockerNode) {
			defer wg.Done()
			for cctx.Err() == nil {
				if d.healthy(cctx, n) {
					found <- struct{}{}
					return
				}
				select {
				case <-cctx.Done():
				case <-time.After(healthzPoll):
				}
			}
		}(n)
	}
	// Stop the pollers of unreachable nodes as soon as we return.
	defer func() {
		cancel()
		wg.Wait()
	}()
	select {
	case <-found:
		return nil
	case <-cctx.Done():
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("%w: none of %s..%s answered within %v", ErrNoNodes,
			d.cfg.EventsURL(1), d.cfg.EventsURL(d.cfg.N), d.cfg.ConnectTimeout)
	}
}

func (d *Docker) healthy(ctx context.Context, n *dockerNode) bool {
	ctx, cancel := context.WithTimeout(ctx, healthzTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.cfg.EventsURL(n.idx)+"/healthz", nil)
	if err != nil {
		return false
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

// ---- Events ----

// readEvents keeps n's SSE stream connected until Close.
func (d *Docker) readEvents(n *dockerNode) {
	defer d.readers.Done()
	for d.ctx.Err() == nil {
		d.stream(n)
		if d.ctx.Err() != nil {
			return
		}
		n.mu.Lock()
		n.failures++
		if n.failures >= sseDownAfter {
			n.up = false
		}
		n.mu.Unlock()
		select {
		case <-d.ctx.Done():
			return
		case <-time.After(sseRetryInterval):
		}
	}
}

// stream connects to n's /events and forwards its blocks until the stream
// ends or fails.
func (d *Docker) stream(n *dockerNode) {
	req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, d.cfg.EventsURL(n.idx)+"/events", nil)
	if err != nil {
		return
	}
	req.Header.Set("Accept", "text/event-stream")
	// No client timeout: the stream is long-lived; Close cancels d.ctx.
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	n.mu.Lock()
	n.up, n.failures = true, 0
	n.mu.Unlock()

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), sseMaxLine)
	var event, data string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if data != "" && (event == "" || event == "block") {
				// A block shows the node is alive, even if a failed
				// request marked it down while the stream stayed up.
				n.mu.Lock()
				n.up = true
				n.mu.Unlock()
				d.publish(n, data)
			}
			event, data = "", ""
		case strings.HasPrefix(line, ":"): // comment (ping)
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(line[len("data:"):])
		}
	}
}

// publish decodes one observer event and queues it; when the channel is full
// the event is dropped.
func (d *Docker) publish(n *dockerNode, data string) {
	var ev observe.Event
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return
	}
	root, err := hex.DecodeString(ev.Root)
	if err != nil {
		return
	}
	at := time.Now()
	if ev.AtMs > 0 {
		at = time.Unix(0, ev.AtMs*int64(time.Millisecond))
	}
	select {
	case d.events <- BlockEvent{
		Node:     n.name,
		Height:   ev.Height,
		Proposer: ev.Proposer,
		Txs:      ev.Txs,
		Aborted:  ev.Aborted,
		Root:     root,
		At:       at,
	}:
	default:
	}
}

// Events implements Cluster. The channel is closed by Close.
func (d *Docker) Events() <-chan BlockEvent { return d.events }

// ---- Info and nodes ----

// kafkaSilent: on kafka silent is a veritas FaultPolicy, armed at start and
// not undoable; on the other backends it is iptables rules the script removes.
func (d *Docker) kafkaSilent() bool { return d.cfg.Backend == "kafka" }

// Info implements Cluster.
func (d *Docker) Info() ClusterInfo {
	faults := []Fault{FaultCrash, FaultSlow}
	healable := []Fault{FaultSlow}
	if !d.kafkaSilent() {
		faults = append(faults, FaultSilent)
		healable = append(healable, FaultSilent)
	} else if len(d.cfg.ArmedSilent) > 0 {
		faults = append(faults, FaultSilent)
	}
	if len(d.cfg.ArmedCorrupt) > 0 {
		faults = append(faults, FaultCorrupt)
	}
	return ClusterInfo{
		Mode:     "docker",
		Backend:  d.cfg.Backend,
		N:        d.cfg.N,
		F:        (d.cfg.N - 1) / 3,
		Faults:   faults,
		Healable: healable,
		CanReset: false,
	}
}

// Nodes implements Cluster.
func (d *Docker) Nodes() []NodeInfo {
	out := make([]NodeInfo, len(d.nodes))
	for i, n := range d.nodes {
		n.mu.Lock()
		out[i] = NodeInfo{Name: n.name, Up: n.up, Faults: append([]Fault{}, n.faults...)}
		n.mu.Unlock()
	}
	return out
}

func (d *Docker) node(name string) (*dockerNode, error) {
	for _, n := range d.nodes {
		if n.name == name {
			return n, nil
		}
	}
	return nil, fmt.Errorf("cluster: unknown node %q", name)
}

// ---- Set, Get, Prove ----

// driverFor returns node n's driver, opening it on first use. The open (whose
// TSO dial has no timeout) runs outside n.mu, so Nodes and the SSE reader
// never wait for it.
func (d *Docker) driverFor(n *dockerNode) (*driver.Driver, error) {
	current := func() (*driver.Driver, error) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.closed {
			return nil, fmt.Errorf("cluster: %s: cluster closed", n.name)
		}
		return n.drv, nil
	}
	if drv, err := current(); drv != nil || err != nil {
		return drv, err
	}
	n.openMu.Lock()
	defer n.openMu.Unlock()
	if drv, err := current(); drv != nil || err != nil {
		return drv, err // opened by a concurrent caller, or closed meanwhile
	}
	drv, err := driver.Open(d.cfg.GRPCAddr(n.idx), d.cfg.TSOAddr, dockerDriverSignature)
	if err != nil {
		return nil, fmt.Errorf("cluster: %s: %w", n.name, err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed { // Close ran during the open: do not leak the connection
		_ = drv.Close()
		return nil, fmt.Errorf("cluster: %s: cluster closed", n.name)
	}
	n.drv = drv
	return drv, nil
}

// Set implements Cluster. A node that cannot be reached fails with ErrNodeDown.
func (d *Docker) Set(ctx context.Context, node, key, value string) error {
	n, err := d.node(node)
	if err != nil {
		return err
	}
	drv, err := d.driverFor(n)
	if err != nil {
		return err
	}
	if err := drv.Set(ctx, key, value); err != nil {
		if status.Code(err) == codes.Unavailable {
			n.markDown()
			return fmt.Errorf("%w: %s: %v", ErrNodeDown, node, err)
		}
		return err
	}
	return nil
}

// Get implements Cluster, through the node's observer GET /kv.
func (d *Docker) Get(ctx context.Context, node, key string) (KV, bool, error) {
	var out struct {
		Value   string `json:"value"`
		Version int64  `json:"version"`
	}
	found, err := d.observerGet(ctx, node, "/kv", key, &out)
	if err != nil || !found {
		return KV{}, false, err
	}
	return KV{Value: out.Value, Version: out.Version}, true, nil
}

// Prove implements Cluster, through the node's observer GET /prove.
func (d *Docker) Prove(ctx context.Context, node, key string) (Proof, error) {
	var out struct {
		Root                  string   `json:"root"`
		SideNodes             []string `json:"side_nodes"`
		NonMembershipLeafData string   `json:"non_membership_leaf_data"`
	}
	found, err := d.observerGet(ctx, node, "/prove", key, &out)
	if err != nil {
		return Proof{}, err
	}
	if !found {
		return Proof{}, fmt.Errorf("cluster: %s: no proof for %q", node, key)
	}
	p := Proof{}
	if p.Root, err = hex.DecodeString(out.Root); err != nil {
		return Proof{}, fmt.Errorf("cluster: %s: bad root: %w", node, err)
	}
	for _, s := range out.SideNodes {
		b, err := hex.DecodeString(s)
		if err != nil {
			return Proof{}, fmt.Errorf("cluster: %s: bad side node: %w", node, err)
		}
		p.SideNodes = append(p.SideNodes, b)
	}
	if p.NonMembershipLeafData, err = hex.DecodeString(out.NonMembershipLeafData); err != nil {
		return Proof{}, fmt.Errorf("cluster: %s: bad leaf data: %w", node, err)
	}
	return p, nil
}

// observerGet GETs path?key= on node's observer and decodes the JSON body into
// v. found=false means 404. An unreachable node fails with ErrNodeDown.
func (d *Docker) observerGet(ctx context.Context, node, path, key string, v interface{}) (found bool, err error) {
	n, err := d.node(node)
	if err != nil {
		return false, err
	}
	u := d.cfg.EventsURL(n.idx) + path + "?key=" + url.QueryEscape(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, err
	}
	resp, err := d.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		n.markDown()
		return false, fmt.Errorf("%w: %s: %v", ErrNodeDown, node, err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return false, fmt.Errorf("cluster: %s: read %s: %w", node, path, err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.Unmarshal(body, v); err != nil {
			return false, fmt.Errorf("cluster: %s: decode %s: %w", node, path, err)
		}
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		var e struct {
			Error string `json:"error"`
		}
		msg := strings.TrimSpace(string(body))
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			msg = e.Error
		}
		return false, fmt.Errorf("cluster: %s: %s: %s: %s", node, path, resp.Status, msg)
	}
}

// ---- Faults ----

// Inject implements Cluster: it runs the fault script and, on success, records
// the fault on the node. A script exit code of 4 (fault not available on that
// node, e.g. corrupt on a node that was not armed) is ErrUnsupported.
func (d *Docker) Inject(ctx context.Context, node string, f Fault) error {
	n, err := d.node(node)
	if err != nil {
		return err
	}
	switch f {
	case FaultCrash, FaultSilent, FaultSlow, FaultCorrupt:
	default:
		return fmt.Errorf("%w: fault %q", ErrUnsupported, f)
	}
	if err := d.runScript(ctx, string(f), node); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, have := range n.faults {
		if have == f {
			return nil
		}
	}
	n.faults = append(n.faults, f)
	return nil
}

// healable reports whether the demo can undo f.
func (d *Docker) healable(f Fault) bool {
	return f == FaultSlow || (f == FaultSilent && !d.kafkaSilent())
}

// Heal implements Cluster: it undoes the node's slow and silent faults (the
// script removes the iptables rules and the netem delay). A node whose faults
// are all unhealable (crash, corrupt, silent on kafka) fails with
// ErrUnsupported without running the script.
func (d *Docker) Heal(ctx context.Context, node string) error {
	n, err := d.node(node)
	if err != nil {
		return err
	}
	n.mu.Lock()
	stuck := len(n.faults) > 0
	for _, f := range n.faults {
		if d.healable(f) {
			stuck = false
		}
	}
	faults := append([]Fault{}, n.faults...)
	n.mu.Unlock()
	if stuck {
		return fmt.Errorf("%w: %s has %v, which cannot be healed", ErrUnsupported, node, faults)
	}
	if err := d.runScript(ctx, "heal", node); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	kept := n.faults[:0]
	for _, f := range n.faults {
		if !d.healable(f) {
			kept = append(kept, f)
		}
	}
	n.faults = kept
	return nil
}

// Reset implements Cluster; a docker cluster is reset by restarting it.
func (d *Docker) Reset(ctx context.Context) error {
	return fmt.Errorf("%w: restart the cluster with experiments/demo_up.sh", ErrUnsupported)
}

// runScript runs `bash <FaultScript> <action> <node>` for at most 30 s, with
// HBDB_BACKEND and the armed nodes (DEMO_ARMED_CORRUPT, DEMO_ARMED_SILENT) in
// its environment. A failure carries the script's last stderr line.
func (d *Docker) runScript(ctx context.Context, action, node string) error {
	ctx, cancel := context.WithTimeout(ctx, faultScriptTimeout)
	defer cancel()
	// stderr goes to a file, not a pipe: a docker child that outlives the
	// killed script would otherwise keep Wait blocked on the pipe.
	errFile, err := ioutil.TempFile("", "demo-fault-")
	if err != nil {
		return fmt.Errorf("cluster: fault script: %w", err)
	}
	defer os.Remove(errFile.Name())
	defer errFile.Close()

	cmd := exec.CommandContext(ctx, "bash", d.cfg.FaultScript, action, node)
	cmd.Env = append(os.Environ(),
		"HBDB_BACKEND="+d.cfg.Backend,
		"DEMO_ARMED_CORRUPT="+strings.Join(d.cfg.ArmedCorrupt, ","),
		"DEMO_ARMED_SILENT="+strings.Join(d.cfg.ArmedSilent, ","))
	cmd.Stderr = errFile
	runErr := cmd.Run()
	stderr, _ := ioutil.ReadFile(errFile.Name())
	if runErr == nil {
		return nil
	}
	if out := bytes.TrimSpace(stderr); len(out) > 0 {
		log.Printf("demo: fault script %s %s failed:\n%s", action, node, out)
	}
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("cluster: fault script %s %s timed out after %v", action, node, faultScriptTimeout)
	}
	msg := lastLine(string(stderr))
	if msg == "" {
		msg = runErr.Error()
	}
	var exit *exec.ExitError
	if errors.As(runErr, &exit) && exit.ExitCode() == faultScriptNoSupp {
		return fmt.Errorf("%w: %s", ErrUnsupported, msg)
	}
	return fmt.Errorf("cluster: fault script %s %s: %s", action, node, msg)
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// resolveScript finds a relative script path from the current directory
// upwards (the demo may run from any directory of the repository).
func resolveScript(p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	dir, err := os.Getwd()
	if err != nil {
		return p
	}
	for {
		cand := filepath.Join(dir, p)
		if _, err := os.Stat(cand); err == nil {
			return cand
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return p
		}
		dir = parent
	}
}

// Close implements Cluster: it stops the readers, closes the drivers and the
// Events channel. It is idempotent.
func (d *Docker) Close() error {
	d.closeOnce.Do(func() {
		d.cancel()
		d.readers.Wait()
		close(d.events)
		for _, n := range d.nodes {
			n.mu.Lock()
			n.closed = true
			if n.drv != nil {
				_ = n.drv.Close()
				n.drv = nil
			}
			n.mu.Unlock()
		}
	})
	return nil
}
