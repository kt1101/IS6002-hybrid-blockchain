// Package node runs one HotStuff replica (github.com/relab/hotstuff v0.5.0,
// chained HotStuff, ECDSA; leaders in a per-epoch shuffled round robin, see
// leader.go) for the bft2 backend.
//
// The module stack is the one the library's own orchestration worker builds
// (internal/orchestration/worker.go createReplica), assembled here directly
// instead of through replica.New, because replica.New registers the
// library's gRPC client server as the command queue and executor. Ours is
// the adapter (adapter.go): Submit queues a command (one Veritas block, exact
// bytes), the leader proposes one command per view (an empty filler block
// when its queue stays empty for IdleWait), and every committed non-empty
// command is appended, in commit order, to the commit log read by the local
// API (package api). The blockchain module is a copy of the library's that
// also evicts old blocks (blockchain.go).
//
// Membership is static: replicas 1..N, all known up front, keys derived
// from a shared seed (keys.go). Replicas talk over the library's gorums/gRPC
// transport, without TLS.
package node

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/relab/gorums"
	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/backend"
	"github.com/relab/hotstuff/consensus"
	"github.com/relab/hotstuff/consensus/chainedhotstuff"
	"github.com/relab/hotstuff/crypto"
	hsecdsa "github.com/relab/hotstuff/crypto/ecdsa"
	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
	"github.com/relab/hotstuff/synchronizer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// Defaults of the Config fields left zero.
const (
	DefaultKeySeed          = "hbdb-bft2"
	DefaultViewTimeout      = 500 * time.Millisecond
	DefaultMinViewTimeout   = 100 * time.Millisecond
	DefaultMaxViewTimeout   = 2 * time.Second
	DefaultTimeoutMult      = 1.2
	DefaultDurationSamples  = 1000
	DefaultIdleWait         = 20 * time.Millisecond
	DefaultConnectTimeout   = 120 * time.Second
	DefaultDialTimeout      = 10 * time.Second
	DefaultMinRetainViews   = 64
	DefaultRetainBytes      = 64 << 20
	DefaultMaxCommitFails   = 50
	DefaultEventQueue       = 1000
	DefaultMaxQueue         = 1024
	DefaultMaxQueueBytes    = 16 << 20
	DefaultMaxBacklogBytes  = 32 << 20
	DefaultMaxCommandBytes  = 4 << 20
	DefaultRequeueViews     = 8
	DefaultDedupWindow      = 65536
	DefaultForwardPort      = 7202
	forwardQueueLen         = 1024
	forwardTimeout          = 2 * time.Second
	grpcMaxMsgBytes         = 4*DefaultMaxCommandBytes + 1<<20
	peerProbeInterval       = 200 * time.Millisecond
	peerProbeTimeout        = time.Second
	verifiedCryptoCacheSize = 100
)

// Peer is one replica of the static configuration.
type Peer struct {
	ID   hotstuff.ID
	Addr string // host:port of its replica (gorums) server
	// ForwardAddr is host:port of its forward endpoint (POST wire.ForwardPath,
	// served by api.ForwardHandler; port 7202 in the cluster). When set on
	// the other replicas, Submit forwards every command to them, so every
	// replica holds every pending command and any leader can propose it.
	ForwardAddr string
}

// Config configures one replica. Zero fields take the defaults above.
type Config struct {
	ID    hotstuff.ID
	Peers []Peer // every replica, this one included; IDs exactly 1..N, N >= 2
	// Listen is the replica server's listen address (default: this
	// replica's Peers address). Listener, if set, is used instead (tests).
	Listen   string
	Listener net.Listener
	KeySeed  string

	// View timeout: max(library estimate of successful views (starts at
	// ViewTimeout), MinViewTimeout) x TimeoutMultiplier^(consecutive
	// timeouts), at most MaxViewTimeout (viewduration.go).
	ViewTimeout       time.Duration
	MinViewTimeout    time.Duration
	MaxViewTimeout    time.Duration
	TimeoutMultiplier float64
	DurationSamples   int

	// IdleWait: how long a leader with an empty queue waits for a command
	// before it proposes an empty filler block.
	IdleWait time.Duration
	// ConnectTimeout bounds Start's wait for every peer's server.
	ConnectTimeout time.Duration
	// RetainBytes: after each commit the oldest blocks are evicted while
	// the stored blocks cost more than this (command bytes + ~0.5 KB each),
	// but never those of the last MinRetainViews views below the committed
	// height (blockchain.go). A replica lagging further cannot fetch them
	// any more and stops committing.
	RetainBytes    int64
	MinRetainViews int
	// MaxCommitFailures: after this many "failed to commit" in a row
	// (without a commit in between) the replica reports itself failed
	// (Failed, Stats.CommitFailures); bft2-node then exits.
	MaxCommitFailures int
	// EventQueue: capacity of the library's event loop (it drops the oldest
	// event when full).
	EventQueue int
	// MaxQueue, MaxQueueBytes bound the proposal queue (Submit fails with
	// ErrQueueFull); MaxBacklogBytes bounds the commit log (the executor
	// waits); MaxCommandBytes bounds one command (ErrTooLarge).
	MaxQueue        int
	MaxQueueBytes   int64
	MaxBacklogBytes int64
	MaxCommandBytes int
	// RequeueViews: a pending command seen in a proposal that has not
	// committed this many views later goes back to the front of the queue.
	// DedupWindow: how many committed command ids are remembered so that a
	// command committed twice (two leaders proposed the same shared block)
	// is delivered once.
	RequeueViews int
	DedupWindow  int
}

func (c *Config) setDefaults() {
	if c.KeySeed == "" {
		c.KeySeed = DefaultKeySeed
	}
	if c.ViewTimeout <= 0 {
		c.ViewTimeout = DefaultViewTimeout
	}
	if c.MinViewTimeout <= 0 {
		c.MinViewTimeout = DefaultMinViewTimeout
	}
	if c.MaxViewTimeout <= 0 {
		c.MaxViewTimeout = DefaultMaxViewTimeout
	}
	if c.TimeoutMultiplier <= 1 {
		c.TimeoutMultiplier = DefaultTimeoutMult
	}
	if c.DurationSamples <= 0 {
		c.DurationSamples = DefaultDurationSamples
	}
	if c.IdleWait <= 0 {
		c.IdleWait = DefaultIdleWait
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.RetainBytes <= 0 {
		c.RetainBytes = DefaultRetainBytes
	}
	if c.MinRetainViews <= 0 {
		c.MinRetainViews = DefaultMinRetainViews
	}
	if c.MaxCommitFailures <= 0 {
		c.MaxCommitFailures = DefaultMaxCommitFails
	}
	if c.EventQueue <= 0 {
		c.EventQueue = DefaultEventQueue
	}
	if c.MaxQueue <= 0 {
		c.MaxQueue = DefaultMaxQueue
	}
	if c.MaxQueueBytes <= 0 {
		c.MaxQueueBytes = DefaultMaxQueueBytes
	}
	if c.MaxBacklogBytes <= 0 {
		c.MaxBacklogBytes = DefaultMaxBacklogBytes
	}
	if c.MaxCommandBytes <= 0 {
		c.MaxCommandBytes = DefaultMaxCommandBytes
	}
	if c.RequeueViews <= 0 {
		c.RequeueViews = DefaultRequeueViews
	}
	if c.DedupWindow <= 0 {
		c.DedupWindow = DefaultDedupWindow
	}
}

// validatePeers checks that the IDs are exactly 1..N (the leader rotation
// assumes it), N >= 2 (gorums needs at least one other node), every
// address is set, and id is one of them. It returns the peers sorted by ID.
func validatePeers(id hotstuff.ID, peers []Peer) ([]Peer, error) {
	if len(peers) < 2 {
		return nil, fmt.Errorf("bft2-node: need at least 2 replicas, got %d", len(peers))
	}
	sorted := append([]Peer(nil), peers...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	for i, p := range sorted {
		if p.ID != hotstuff.ID(i+1) {
			return nil, fmt.Errorf("bft2-node: replica IDs must be exactly 1..%d (got %v)", len(sorted), idsOf(sorted))
		}
		if p.Addr == "" {
			return nil, fmt.Errorf("bft2-node: replica %d has no address", p.ID)
		}
	}
	if id < 1 || int(id) > len(sorted) {
		return nil, fmt.Errorf("bft2-node: own ID %d is not in the replica set 1..%d", id, len(sorted))
	}
	return sorted, nil
}

func idsOf(peers []Peer) []hotstuff.ID {
	ids := make([]hotstuff.ID, len(peers))
	for i, p := range peers {
		ids[i] = p.ID
	}
	return ids
}

// Node is one running HotStuff replica.
type Node struct {
	cfg   Config
	peers []Peer // sorted by ID
	infos []backend.ReplicaInfo

	core    *modules.Core
	hscfg   *backend.Config
	server  *backend.Server
	queue   *cmdQueue
	log     *CommitLog
	adapter *adapter

	viewDuration *boundedViewDuration
	forwarders   []*forwarder

	view           atomic.Uint64 // current view (from the synchronizer's view-change events)
	timeouts       atomic.Uint64 // views ended by a timeout certificate
	commitFailures atomic.Uint64 // "failed to commit" warnings of the consensus module
	dropped        atomic.Uint64 // events the event loop dropped (queue full)
	failed         chan struct{} // closed once MaxCommitFailures commits failed in a row
	failOnce       sync.Once

	mu        sync.Mutex
	started   bool
	connected bool
	stopped   bool
	cancel    context.CancelFunc
	done      chan struct{} // closed when the event loop has ended
	stopCh    chan struct{} // closed by Stop (ends Start's wait for peers)
}

// New builds the replica and starts its replica server (it accepts peer
// connections from now on, so peers can connect before this node's Start).
// Submit works from now on (commands wait in the queue); call Start to join
// the protocol.
func New(cfg Config) (*Node, error) {
	cfg.setDefaults()
	peers, err := validatePeers(cfg.ID, cfg.Peers)
	if err != nil {
		return nil, err
	}
	priv, err := DeriveKey(cfg.KeySeed, cfg.ID)
	if err != nil {
		return nil, err
	}
	infos := make([]backend.ReplicaInfo, len(peers))
	for i, p := range peers {
		k, err := DeriveKey(cfg.KeySeed, p.ID)
		if err != nil {
			return nil, err
		}
		infos[i] = backend.ReplicaInfo{ID: p.ID, Address: p.Addr, PubKey: &k.PublicKey}
	}

	lis := cfg.Listener
	if lis == nil {
		addr := cfg.Listen
		if addr == "" {
			addr = peers[cfg.ID-1].Addr
		}
		if lis, err = net.Listen("tcp", addr); err != nil {
			return nil, fmt.Errorf("bft2-node: listen %s: %w", addr, err)
		}
	}

	n := &Node{
		cfg:    cfg,
		peers:  peers,
		infos:  infos,
		queue:  newCmdQueue(cfg.MaxQueue, cfg.MaxQueueBytes),
		log:    NewCommitLog(cfg.MaxBacklogBytes),
		done:   make(chan struct{}),
		stopCh: make(chan struct{}),
		failed: make(chan struct{}),
	}

	// Transport: gorums over gRPC, no TLS (backend.NewConfig uses insecure
	// credentials for nil). The message limits fit the largest command plus
	// the QC; reconnects to a crashed peer back off at most 2 s (the gRPC
	// default goes up to 120 s).
	n.hscfg = backend.NewConfig(nil,
		gorums.WithDialTimeout(DefaultDialTimeout),
		gorums.WithBackoff(backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second}),
		gorums.WithGrpcDialOptions(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcMaxMsgBytes),
			grpc.MaxCallSendMsgSize(grpcMaxMsgBytes),
		)),
	)
	n.server = backend.NewServer(
		backend.WithLatencies(cfg.ID, nil), // as replica.New: no emulated latencies
		backend.WithGorumsServerOptions(gorums.WithGRPCServerOptions(
			grpc.MaxRecvMsgSize(grpcMaxMsgBytes),
			grpc.MaxSendMsgSize(grpcMaxMsgBytes),
		)),
	)

	n.queue.requeueViews = uint64(cfg.RequeueViews)
	n.log.dedup = newIDRing(cfg.DedupWindow)
	n.queue.seen = n.log.seen
	n.adapter = &adapter{id: cfg.ID, queue: n.queue, log: n.log, idle: cfg.IdleWait, view: n.view.Load}
	for _, p := range peers {
		if p.ID != cfg.ID && p.ForwardAddr != "" {
			n.forwarders = append(n.forwarders, newForwarder(cfg.ID, p))
		}
	}

	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	// The library's estimator only sees successful views: the wrapper does
	// the timeout back-off itself (viewduration.go).
	n.viewDuration = newBoundedViewDuration(
		synchronizer.NewViewDuration(uint64(cfg.DurationSamples),
			ms(cfg.ViewTimeout), ms(cfg.MaxViewTimeout), cfg.TimeoutMultiplier),
		cfg.MinViewTimeout, cfg.MaxViewTimeout, cfg.TimeoutMultiplier)

	builder := modules.NewBuilder(cfg.ID, priv)
	// Votes are verified on the event loop: the asynchronous path reads
	// the synchronizer's highQC from another goroutine without a lock.
	builder.Options().SetShouldVerifyVotesSync()
	builder.Add(
		eventloop.New(uint(cfg.EventQueue)),
		consensus.New(chainedhotstuff.New()),
		consensus.NewVotingMachine(),
		crypto.NewCache(hsecdsa.New(), verifiedCryptoCacheSize),
		newShuffledRoundRobin(len(peers), leaderSeed(cfg.KeySeed)),
		synchronizer.New(n.viewDuration),
		newBlockChain(hotstuff.View(cfg.MinRetainViews), cfg.RetainBytes),
		&watchLogger{
			Logger:          logging.New(fmt.Sprintf("hs%d", cfg.ID)),
			onCommitFailure: n.commitFailed,
			onDroppedEvent:  func() { n.dropped.Add(1) },
		},
		n.hscfg,
		n.server,
		n.adapter,
	)
	n.core = builder.Build()

	// Stats only: follow the view on the event loop (the synchronizer's
	// own View() is not safe to read from other goroutines).
	var eventLoop *eventloop.EventLoop
	n.core.Get(&eventLoop)
	eventLoop.RegisterHandler(synchronizer.ViewChangeEvent{}, func(event any) {
		e := event.(synchronizer.ViewChangeEvent)
		n.view.Store(uint64(e.View))
		if e.Timeout {
			n.timeouts.Add(1)
		}
		if k := n.queue.requeueStale(uint64(e.View)); k > 0 {
			n.adapter.requeued.Add(uint64(k))
		}
	})
	n.server.StartOnListener(lis)
	return n, nil
}

// Start waits (bounded by ctx and Config.ConnectTimeout) until every peer's
// replica server accepts TCP connections, connects to all of them and starts
// the protocol. It returns once the replica runs. Connecting needs every
// peer up: gorums fails a connection to a peer that is down, and the
// library's Config.Connect cannot be retried.
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.started {
		n.mu.Unlock()
		return errors.New("bft2-node: already started")
	}
	n.started = true
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, n.cfg.ConnectTimeout)
	defer cancel()
	go func() {
		select {
		case <-n.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	for _, p := range n.peers {
		if p.ID == n.cfg.ID {
			continue
		}
		if err := waitTCP(ctx, p.Addr); err != nil {
			return fmt.Errorf("bft2-node: replica %d (%s) not reachable: %w", p.ID, p.Addr, err)
		}
	}

	// Connect without n.mu (it may take up to the dial timeout), so Stop is
	// not held up; if Stop ran meanwhile, undo the connection here (Stop
	// saw connected == false and left hscfg alone).
	if err := n.hscfg.Connect(n.infos); err != nil {
		return fmt.Errorf("bft2-node: connect to replicas: %w", err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		n.hscfg.Close()
		return ErrStopped
	}
	n.connected = true

	var (
		synchro   modules.Synchronizer
		eventLoop *eventloop.EventLoop
	)
	n.core.Get(&synchro, &eventLoop)
	runCtx, stop := context.WithCancel(context.Background())
	n.cancel = stop
	go func() {
		defer close(n.done)
		// As replica.Run, but the synchronizer gets a context that is never
		// cancelled: on cancellation its Start spawns a goroutine that stops
		// the view timer, racing (unsynchronized) with the event loop that
		// replaces it. The timer then fires at most once after Stop, onto a
		// stopped loop.
		synchro.Start(context.Background())
		eventLoop.Run(runCtx)
	}()
	return nil
}

// waitTCP dials addr until it accepts a connection or ctx ends.
func waitTCP(ctx context.Context, addr string) error {
	var d net.Dialer
	for {
		dctx, cancel := context.WithTimeout(ctx, peerProbeTimeout)
		c, err := d.DialContext(dctx, "tcp", addr)
		cancel()
		if err == nil {
			c.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%v (last error: %v)", ctx.Err(), err)
		case <-time.After(peerProbeInterval):
		}
	}
}

// leaderSeed derives the leader schedule's seed from the key seed, so every
// replica of a deployment uses the same schedule.
func leaderSeed(keySeed string) int64 {
	h := sha256.Sum256([]byte("hbdb-bft2-leaders\x00" + keySeed))
	return int64(binary.BigEndian.Uint64(h[:8]) >> 1)
}

// Submit queues cmd (copied) for proposal and, with forwarding configured
// (Peer.ForwardAddr), sends it to every other replica's forward endpoint in
// the background (best effort: a failed forward is logged, the command stays
// queued here). A nil error means accepted, not committed: the command may
// still be lost (e.g. every replica holding it crashes). A command committed
// more than once (two leaders proposed it) is delivered once (dedup window).
// Errors: ErrEmpty, ErrTooLarge, ErrQueueFull, ErrStopped.
func (n *Node) Submit(cmd []byte) error {
	if err := n.enqueue(cmd); err != nil {
		return err
	}
	for _, f := range n.forwarders {
		f.send(cmd)
	}
	return nil
}

// Forward queues cmd received from another replica's Submit (POST
// wire.ForwardPath); it is not forwarded further. Same errors as Submit.
func (n *Node) Forward(cmd []byte) error { return n.enqueue(cmd) }

func (n *Node) enqueue(cmd []byte) error {
	if len(cmd) == 0 {
		return ErrEmpty
	}
	if len(cmd) > n.cfg.MaxCommandBytes {
		return ErrTooLarge
	}
	return n.queue.push(hotstuff.Command(cmd))
}

// Stats is a snapshot of a replica's progress (for logs and tests).
type Stats struct {
	View           uint64        // current view
	Timeouts       uint64        // views ended by a timeout certificate
	ViewTimeout    time.Duration // the view timeout last handed to the synchronizer
	Committed      uint64        // committed non-empty commands (= Veritas blocks)
	Proposed       uint64        // non-empty commands this replica proposed (re-proposals included)
	Requeued       uint64        // forked or stale commands put back in the queue
	Duplicates     uint64        // committed commands skipped as already delivered
	Queued         int           // commands waiting for proposal
	Inflight       int           // pending commands seen in a proposal, not committed yet
	CommitFailures uint64        // "failed to commit" (missing ancestor); see Failed
	Dropped        uint64        // events dropped by the full event queue
}

func (s Stats) String() string {
	return fmt.Sprintf("view=%d timeouts=%d view_timeout=%v committed=%d proposed=%d requeued=%d duplicates=%d queued=%d inflight=%d commit_failures=%d dropped_events=%d",
		s.View, s.Timeouts, s.ViewTimeout, s.Committed, s.Proposed, s.Requeued, s.Duplicates, s.Queued, s.Inflight, s.CommitFailures, s.Dropped)
}

// Stats returns a snapshot of the replica's progress.
func (n *Node) Stats() Stats {
	return Stats{
		View:           n.view.Load(),
		Timeouts:       n.timeouts.Load(),
		ViewTimeout:    n.viewDuration.lastDuration(),
		Committed:      n.adapter.committed.Load(),
		Proposed:       n.adapter.proposed.Load(),
		Requeued:       n.adapter.requeued.Load(),
		Duplicates:     n.adapter.duplicates.Load(),
		Queued:         n.queue.len(),
		Inflight:       n.queue.inflightLen(),
		CommitFailures: n.commitFailures.Load(),
		Dropped:        n.dropped.Load(),
	}
}

// commitFailed counts a "failed to commit" of the consensus module. After
// MaxCommitFailures in a row (adapter.Exec resets the run) the replica has
// lost blocks it cannot fetch any more and will never commit again: it logs
// an error and closes Failed.
func (n *Node) commitFailed(msg string) {
	n.commitFailures.Add(1)
	run := n.adapter.failRun.Add(1)
	if run >= int64(n.cfg.MaxCommitFailures) {
		n.failOnce.Do(func() {
			log.Printf("bft2-node %d: ERROR: %d commits failed in a row (last: %s); this replica can no longer commit (fell behind past the retained blocks?)",
				n.cfg.ID, run, msg)
			close(n.failed)
		})
	}
}

// Failed is closed when the replica can no longer commit (see
// Config.MaxCommitFailures). bft2-node exits non-zero on it.
func (n *Node) Failed() <-chan struct{} { return n.failed }

// Log returns the commit log: every committed command, in commit order.
func (n *Node) Log() *CommitLog { return n.log }

// MaxCommandBytes is the largest command Submit accepts.
func (n *Node) MaxCommandBytes() int { return n.cfg.MaxCommandBytes }

// Stop stops the replica (a crash from the other replicas' point of view):
// later Submits fail with ErrStopped, the commit log is closed, the event
// loop ends, connections and the server close. It is idempotent.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	cancel, connected := n.cancel, n.connected
	n.mu.Unlock()

	// First release anything the event loop may be blocked on (a leader's
	// Get, an Exec waiting for room in the commit log).
	n.queue.close()
	n.log.Close()
	for _, f := range n.forwarders {
		f.close()
	}
	if cancel != nil {
		cancel()
		<-n.done
	}
	if connected {
		n.hscfg.Close()
	}
	n.server.Stop()
}
