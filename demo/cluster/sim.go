package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"hybrid/consensus/memnet"
	pbv "hybrid/proto/veritas"
	"hybrid/veritas"
	"hybrid/veritas/core"
	"hybrid/veritas/ledger"
)

// SimConfig configures a Sim. Zero fields take their defaults.
type SimConfig struct {
	N         int           // number of nodes; default 4
	SlowDelay time.Duration // extra latency of the slow fault; default 200 ms
}

const (
	defaultSimN      = 4
	defaultSlowDelay = 200 * time.Millisecond

	simBlockSize     = 100
	simFlushInterval = 200 * time.Millisecond // blocks come out fast for hand-typed Sets
	// corruptNever: the corrupt policy every node starts with only turns on
	// through Inject (Activate), never by its delay.
	corruptNever = 100 * 365 * 24 * time.Hour
)

// Sim is an in-process cluster: N core.Servers (CommitSync, VersionOrdered)
// on one memnet hub, each behind a faultyNet, with a map store and a ledger
// in a temporary directory (removed on Reset and Close).
//
// Faults: crash, silent and slow are network states of the node's faultyNet;
// a node has at most one of them, and injecting another replaces it. Set,
// Get and Prove on a crashed node fail with ErrNodeDown. corrupt is the
// executor's core corrupt policy, activated by Inject; it cannot be healed
// (the node's state is already wrong), only Reset clears it.
type Sim struct {
	cfg    SimConfig
	events chan BlockEvent // shared by every generation of nodes

	mu     sync.RWMutex // guards nodes and closed
	nodes  []*simNode
	closed bool
}

var _ Cluster = (*Sim)(nil)

type simNode struct {
	name    string
	dir     string
	net     *faultyNet
	store   core.KVStore
	ex      *core.Executor
	ledger  *ledger.LogLedger
	srv     *core.Server
	corrupt core.Activator

	mu       sync.Mutex
	netFault Fault // FaultCrash, FaultSilent, FaultSlow or "" (none)
}

// NewSim builds and starts a cluster of cfg.N nodes.
func NewSim(cfg SimConfig) (*Sim, error) {
	if cfg.N <= 0 {
		cfg.N = defaultSimN
	}
	if cfg.SlowDelay <= 0 {
		cfg.SlowDelay = defaultSlowDelay
	}
	s := &Sim{cfg: cfg, events: make(chan BlockEvent, eventBuffer)}
	nodes, err := s.build()
	if err != nil {
		return nil, err
	}
	s.nodes = nodes
	return s, nil
}

// build starts a fresh set of nodes on a new hub.
func (s *Sim) build() ([]*simNode, error) {
	hub := memnet.NewHub(s.cfg.N)
	nodes := make([]*simNode, 0, s.cfg.N)
	for i := 0; i < s.cfg.N; i++ {
		n, err := s.buildNode(i, hub)
		if err != nil {
			for j := i; j < s.cfg.N; j++ {
				hub.Node(j).Close() // nodes not started yet must not block the hub
			}
			closeNodes(nodes)
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func (s *Sim) buildNode(i int, hub *memnet.Hub) (*simNode, error) {
	name := fmt.Sprintf("node%d", i+1)
	dir, err := os.MkdirTemp("", "veritas-demo-"+name+"-")
	if err != nil {
		return nil, fmt.Errorf("cluster: %s: %w", name, err)
	}
	l, err := ledger.NewLedger(dir, true)
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("cluster: %s: ledger: %w", name, err)
	}
	fp, err := core.ParseFault(core.FaultCorrupt, time.Now(), corruptNever)
	if err != nil {
		l.Close()
		os.RemoveAll(dir)
		return nil, fmt.Errorf("cluster: %s: %w", name, err)
	}
	n := &simNode{
		name:    name,
		dir:     dir,
		net:     newFaultyNet(hub.Node(i), s.cfg.SlowDelay),
		store:   core.NewMapStore(),
		ledger:  l,
		corrupt: fp.(core.Activator),
	}
	n.ex = core.NewExecutor(n.store, l, core.VersionOrdered, fp)
	n.srv = core.NewServer(core.ServerConfig{
		NodeID:        i + 1,
		Signature:     name,
		BlockSize:     simBlockSize,
		Mode:          core.VersionOrdered,
		Commit:        core.CommitSync,
		FlushInterval: simFlushInterval,
		OnApplied:     s.onApplied(name),
	}, n.net, n.ex, l)
	return n, nil
}

// onApplied returns node name's OnApplied hook. It runs in the node's apply
// loop, so it never blocks: an event that does not fit is dropped.
func (s *Sim) onApplied(name string) func(core.AppliedEvent) {
	return func(ev core.AppliedEvent) {
		select {
		case s.events <- BlockEvent{
			Node:     name,
			Height:   ev.Height,
			Proposer: ev.Proposer,
			Txs:      ev.Txs,
			Aborted:  ev.Aborted,
			Root:     ev.Root,
			At:       time.Now(),
		}:
		default:
		}
	}
}

// close stops the node and removes its ledger directory.
func (n *simNode) close() {
	n.srv.Close() // also closes n.net
	n.ledger.Close()
	os.RemoveAll(n.dir)
}

func closeNodes(nodes []*simNode) {
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *simNode) {
			defer wg.Done()
			n.close()
		}(n)
	}
	wg.Wait()
}

// node looks up a node by name.
func (s *Sim) node(name string) (*simNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("cluster: closed")
	}
	for _, n := range s.nodes {
		if n.name == name {
			return n, nil
		}
	}
	return nil, fmt.Errorf("cluster: unknown node %q", name)
}

// upNode looks up a node by name and fails with ErrNodeDown if it is crashed.
func (s *Sim) upNode(name string) (*simNode, error) {
	n, err := s.node(name)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	down := n.netFault == FaultCrash
	n.mu.Unlock()
	if down {
		return nil, fmt.Errorf("%w: %s", ErrNodeDown, name)
	}
	return n, nil
}

// Info implements Cluster.
func (s *Sim) Info() ClusterInfo {
	return ClusterInfo{
		Mode:     "sim",
		Backend:  "memnet",
		N:        s.cfg.N,
		F:        (s.cfg.N - 1) / 3,
		Faults:   []Fault{FaultCrash, FaultSilent, FaultSlow, FaultCorrupt},
		Healable: []Fault{FaultCrash, FaultSilent, FaultSlow},
		CanReset: true,
	}
}

// Nodes implements Cluster.
func (s *Sim) Nodes() []NodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]NodeInfo, len(s.nodes))
	for i, n := range s.nodes {
		faults := []Fault{}
		if n.corrupt.Active() {
			faults = append(faults, FaultCorrupt)
		}
		n.mu.Lock()
		nf := n.netFault
		n.mu.Unlock()
		if nf != "" {
			faults = append(faults, nf)
		}
		out[i] = NodeInfo{Name: n.name, Up: nf != FaultCrash, Faults: faults}
	}
	return out
}

// Set implements Cluster. It returns the node's commit error unchanged
// (e.g. core.ErrCommitTimeout, a ctx error, core.ErrStopped after Reset).
func (s *Sim) Set(ctx context.Context, node, key, value string) error {
	n, err := s.upNode(node)
	if err != nil {
		return err
	}
	_, err = n.srv.Set(ctx, &pbv.SetRequest{Signature: node, Key: key, Value: value})
	return err
}

// Get implements Cluster. It reads the node's store directly, to return the
// version along with the value.
func (s *Sim) Get(ctx context.Context, node, key string) (KV, bool, error) {
	n, err := s.upNode(node)
	if err != nil {
		return KV{}, false, err
	}
	raw, ok, err := n.store.Get(ctx, key)
	if err != nil || !ok {
		return KV{}, false, err
	}
	v, err := veritas.Decode(raw)
	if err != nil {
		return KV{}, false, fmt.Errorf("cluster: %s: decode %q: %w", node, key, err)
	}
	return KV{Value: v.Val, Version: v.Version}, true, nil
}

// Prove implements Cluster.
func (s *Sim) Prove(ctx context.Context, node, key string) (Proof, error) {
	n, err := s.upNode(node)
	if err != nil {
		return Proof{}, err
	}
	root, p, err := n.ex.Prove([]byte(key))
	if err != nil {
		return Proof{}, err
	}
	return Proof{Root: root, SideNodes: p.SideNodes, NonMembershipLeafData: p.NonMembershipLeafData}, nil
}

// Events implements Cluster. The channel is closed by Close.
func (s *Sim) Events() <-chan BlockEvent { return s.events }

// Inject implements Cluster.
func (s *Sim) Inject(ctx context.Context, node string, f Fault) error {
	n, err := s.node(node)
	if err != nil {
		return err
	}
	var st netState
	switch f {
	case FaultCorrupt:
		n.corrupt.Activate()
		return nil
	case FaultCrash:
		st = netCrashed
	case FaultSilent:
		st = netSilent
	case FaultSlow:
		st = netSlow
	default:
		return fmt.Errorf("%w: fault %q", ErrUnsupported, f)
	}
	n.mu.Lock()
	n.netFault = f
	n.net.setState(st)
	n.mu.Unlock()
	return nil
}

// Heal implements Cluster: it clears the node's crash, silent or slow fault
// (a crashed node replays the blocks it missed). It is a no-op on a healthy
// node, and fails with ErrUnsupported on a node whose only fault is corrupt.
func (s *Sim) Heal(ctx context.Context, node string) error {
	n, err := s.node(node)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.netFault == "" {
		if n.corrupt.Active() {
			return fmt.Errorf("%w: %s is corrupt (only Reset clears it)", ErrUnsupported, node)
		}
		return nil
	}
	n.netFault = ""
	n.net.setState(netNormal)
	return nil
}

// Reset implements Cluster: it closes every node and starts a fresh cluster
// with empty stores and ledgers and no faults.
func (s *Sim) Reset(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("cluster: closed")
	}
	closeNodes(s.nodes)
	s.nodes = nil
	nodes, err := s.build()
	if err != nil {
		return err
	}
	s.nodes = nodes
	return nil
}

// Close implements Cluster: it stops every node, removes their directories
// and closes the Events channel. It is idempotent.
func (s *Sim) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	closeNodes(s.nodes)
	s.nodes = nil
	close(s.events)
	return nil
}
