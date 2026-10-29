// Package raft is the Raft consensus backend (hashicorp/raft v1.3.2 with a
// raft-boltdb log store, as raftkv/peer.go).
//
// The cluster is static: Config.Peers lists every node, node1 (Bootstrap)
// bootstraps the full peer set and the others wait to be contacted (no Join).
// Every block is one raft log entry; the FSM pushes a copy of each committed
// entry's data to the Deliver channel, so every node, the proposer included,
// delivers the same blocks in the raft log order.
//
// Submit on the leader calls raft.Apply and returns as soon as the entry is
// handed to raft (ACCEPTANCE, not commit: the future is only watched, in a
// goroutine, to log errors). On a follower Submit POSTs the block to the
// leader's forward HTTP endpoint (http://<ForwardAddr>/submit), which calls
// Apply the same way; the leader is found with raft.Leader() -> RaftAddr ->
// ForwardAddr via Peers. A failed attempt is retried, after waiting for the
// leader to change, up to forwardRetries times. An accepted block can still
// be lost (leader change before commit) or, after a retry, delivered twice;
// the core tolerates both.
//
// Snapshots are no-ops: the core keeps its state in redis and the ledger, not
// in the FSM, so a snapshot persists nothing and Restore discards its input.
// Consequence: a node that falls behind the leader's compacted log (more than
// TrailingLogs entries) and gets an InstallSnapshot skips the blocks in the
// snapshot. The defaults (snapshot every 8192 entries, 10240 trailing logs
// kept) make that need a follower more than 10k blocks behind.
package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"hybrid/consensus"
)

var _ consensus.Consensus = (*Raft)(nil)

const (
	// DeliverBuffer is the capacity of the Deliver channel. When it is full
	// the FSM waits, which stalls this node's raft apply (backpressure).
	DeliverBuffer = 128
	// applyTimeout is the raft.Apply enqueue timeout.
	applyTimeout = 10 * time.Second
	// forwardRetries is how many times a failed Submit attempt (forward
	// refused, leader unreachable or not leader any more) is retried.
	forwardRetries = 3
	// forwardTimeout bounds one forward POST. It exceeds applyTimeout, since
	// the leader answers only once its Apply call has returned.
	forwardTimeout = applyTimeout + 5*time.Second
	// leaderWait bounds the wait for a known leader before an attempt.
	leaderWait = 10 * time.Second
	// leaderChangeWait bounds the wait, after a failed attempt, for the
	// leader to change before the next attempt (it is retried anyway).
	leaderChangeWait = 3 * time.Second
	// pollInterval is how often the leader waits re-read raft's state.
	pollInterval = 20 * time.Millisecond
	// maxBlockBytes bounds the body of a forward request.
	maxBlockBytes = 64 << 20
	// submitPath is the forward endpoint.
	submitPath = "/submit"
)

// ErrClosed is returned by Submit once Close has been called.
var ErrClosed = errors.New("raft consensus: closed")

// errNotLeader is the forward endpoint's refusal on a non-leader.
var errNotLeader = errors.New("raft consensus: not the leader")

// Peer is one member of the static raft cluster.
type Peer struct {
	// ID is the raft server ID (the node's signature in cmd/veritas).
	// RaftAddr is the raft transport address the other nodes dial; it is
	// also what raft.Leader() reports for this node. ForwardAddr is the
	// host:port of this node's forward HTTP endpoint.
	ID, RaftAddr, ForwardAddr string
}

// Config configures one node's Raft backend.
type Config struct {
	// ID is this node's server ID; it must be one of Peers. Bind is the
	// local address the TCP transport listens on (empty = this node's
	// RaftAddr; ignored with Transport). DataDir holds the bolt log store
	// and the snapshots (empty = in-memory stores, for tests).
	ID, Bind, DataDir string
	// Peers is the full, static cluster, this node included.
	Peers []Peer
	// Bootstrap is true on node1 only: it bootstraps the cluster with all
	// of Peers (no Join). A node that already has raft state ignores it.
	Bootstrap bool
	// Transport is the raft transport; nil = TCP on Bind, advertising this
	// node's RaftAddr. Tests pass a raft.InmemTransport whose address is
	// this node's RaftAddr.
	Transport hraft.Transport

	// raftConfig, if set, adjusts the raft config (tests: short timeouts).
	raftConfig func(*hraft.Config)
}

// Raft is one node's Raft backend. It implements consensus.Consensus.
type Raft struct {
	forwardsServed int64 // forward requests this node applied (atomic; tests)

	self   Peer
	byAddr map[string]Peer // RaftAddr -> peer

	raft    *hraft.Raft
	fsm     *fsm
	bolt    *raftboltdb.BoltStore // nil with DataDir == ""
	httpSrv *http.Server
	httpCli *http.Client
	served  chan struct{} // closed when the forward server's Serve returns

	closeCh   chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// New validates cfg, opens the stores, starts raft and the forward endpoint,
// and bootstraps the cluster when cfg.Bootstrap is set. It returns once the
// node is running; it does not wait for a leader.
func New(cfg Config) (*Raft, error) {
	self, byAddr, err := checkPeers(cfg)
	if err != nil {
		return nil, err
	}

	closeCh := make(chan struct{})
	f := &fsm{deliver: make(chan []byte, DeliverBuffer), closeCh: closeCh}
	r := &Raft{
		self:    self,
		byAddr:  byAddr,
		fsm:     f,
		closeCh: closeCh,
		served:  make(chan struct{}),
		httpCli: &http.Client{Timeout: forwardTimeout},
	}

	// cleanup undoes what New has built so far when a later step fails.
	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(cfg.ID)
	conf.LogOutput = os.Stderr
	conf.LogLevel = "INFO"
	if cfg.raftConfig != nil {
		cfg.raftConfig(conf)
	}

	var (
		logs   hraft.LogStore
		stable hraft.StableStore
		snaps  hraft.SnapshotStore
	)
	if cfg.DataDir == "" {
		mem := hraft.NewInmemStore()
		logs, stable, snaps = mem, mem, hraft.NewInmemSnapshotStore()
	} else {
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, fmt.Errorf("raft consensus: data dir: %w", err)
		}
		fss, err := hraft.NewFileSnapshotStore(cfg.DataDir, 1, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("raft consensus: snapshot store: %w", err)
		}
		bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return nil, fmt.Errorf("raft consensus: bolt store: %w", err)
		}
		cleanups = append(cleanups, func() { bolt.Close() })
		r.bolt = bolt
		logs, stable, snaps = bolt, bolt, fss
	}

	trans := cfg.Transport
	if trans == nil {
		bind := cfg.Bind
		if bind == "" {
			bind = self.RaftAddr
		}
		nt, err := newTCPTransport(bind, self.RaftAddr)
		if err != nil {
			cleanup()
			return nil, err
		}
		trans = nt
	}
	if closer, ok := trans.(hraft.WithClose); ok {
		// raft.Shutdown closes the transport; this is for a failed New.
		cleanups = append(cleanups, func() { closer.Close() })
	}
	if string(trans.LocalAddr()) != self.RaftAddr {
		cleanup()
		return nil, fmt.Errorf("raft consensus: transport address %q is not this node's RaftAddr %q", trans.LocalAddr(), self.RaftAddr)
	}

	// The forward endpoint listens before raft starts, so a bad address
	// fails New without a running raft to tear down.
	lis, err := net.Listen("tcp", self.ForwardAddr)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("raft consensus: forward listen %s: %w", self.ForwardAddr, err)
	}
	cleanups = append(cleanups, func() { lis.Close() })

	ra, err := hraft.NewRaft(conf, f, logs, stable, snaps, trans)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("raft consensus: start raft: %w", err)
	}
	r.raft = ra

	if cfg.Bootstrap {
		servers := make([]hraft.Server, len(cfg.Peers))
		for i, p := range cfg.Peers {
			servers[i] = hraft.Server{Suffrage: hraft.Voter, ID: hraft.ServerID(p.ID), Address: hraft.ServerAddress(p.RaftAddr)}
		}
		err := ra.BootstrapCluster(hraft.Configuration{Servers: servers}).Error()
		if err != nil && !errors.Is(err, hraft.ErrCantBootstrap) {
			ra.Shutdown().Error()
			cleanup()
			return nil, fmt.Errorf("raft consensus: bootstrap: %w", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc(submitPath, r.handleSubmit)
	r.httpSrv = &http.Server{Handler: mux}
	go func() {
		defer close(r.served)
		if err := r.httpSrv.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Printf("raft consensus: forward server: %v", err)
		}
	}()
	return r, nil
}

// checkPeers validates cfg's ID and Peers and returns this node's entry and
// the peers indexed by RaftAddr.
func checkPeers(cfg Config) (Peer, map[string]Peer, error) {
	if cfg.ID == "" {
		return Peer{}, nil, errors.New("raft consensus: empty ID")
	}
	ids := make(map[string]bool, len(cfg.Peers))
	byAddr := make(map[string]Peer, len(cfg.Peers))
	var self Peer
	found := false
	for _, p := range cfg.Peers {
		if p.ID == "" || p.RaftAddr == "" || p.ForwardAddr == "" {
			return Peer{}, nil, fmt.Errorf("raft consensus: peer %+v: ID, RaftAddr and ForwardAddr are required", p)
		}
		if ids[p.ID] {
			return Peer{}, nil, fmt.Errorf("raft consensus: duplicate peer ID %q", p.ID)
		}
		if _, dup := byAddr[p.RaftAddr]; dup {
			return Peer{}, nil, fmt.Errorf("raft consensus: duplicate peer RaftAddr %q", p.RaftAddr)
		}
		ids[p.ID] = true
		byAddr[p.RaftAddr] = p
		if p.ID == cfg.ID {
			self, found = p, true
		}
	}
	if !found {
		return Peer{}, nil, fmt.Errorf("raft consensus: ID %q is not one of the peers", cfg.ID)
	}
	return self, byAddr, nil
}

// Submit proposes blk (see the package comment). It never retains blk.
func (r *Raft) Submit(ctx context.Context, blk []byte) error {
	if r.isClosed() {
		return ErrClosed
	}
	data := append([]byte(nil), blk...)
	ctx, cancel := r.withClose(ctx)
	defer cancel()

	var lastErr error
	failedLeader := ""
	for attempt := 0; attempt <= forwardRetries; attempt++ {
		if attempt > 0 {
			r.waitLeaderChange(ctx, failedLeader)
		}
		if stop := r.stopErr(ctx); stop != nil {
			return r.wrapStop(stop, lastErr)
		}
		leader, err := r.waitLeader(ctx)
		if err != nil {
			if stop := r.stopErr(ctx); stop != nil {
				return r.wrapStop(stop, lastErr)
			}
			lastErr = err // no leader within leaderWait
			continue
		}
		if leader == "" { // this node is the leader
			r.apply(data)
			if r.isClosed() {
				// Close raced with Apply: raft may have refused the entry.
				return ErrClosed
			}
			return nil
		}
		failedLeader = leader
		p, ok := r.byAddr[leader]
		if !ok {
			lastErr = fmt.Errorf("raft consensus: leader %s is not one of the peers", leader)
			continue
		}
		if err := r.forward(ctx, p, data); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("raft consensus: submit failed after %d attempts: %w", forwardRetries+1, lastErr)
}

// apply hands data to raft (this node believes it is the leader) and returns
// without waiting for the commit; a goroutine logs a failed future.
func (r *Raft) apply(data []byte) {
	fut := r.raft.Apply(data, applyTimeout)
	go func() {
		if err := fut.Error(); err != nil && !r.isClosed() {
			log.Printf("raft consensus: %s: accepted block not committed: %v", r.self.ID, err)
		}
	}()
}

// waitLeader waits until a leader is known. It returns "" when this node is
// the leader, else the leader's raft address.
func (r *Raft) waitLeader(ctx context.Context) (string, error) {
	t := time.NewTimer(leaderWait)
	defer t.Stop()
	for {
		if r.raft.State() == hraft.Leader {
			return "", nil
		}
		if l := r.raft.Leader(); l != "" && string(l) != r.self.RaftAddr {
			return string(l), nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-t.C:
			return "", fmt.Errorf("raft consensus: no leader within %v", leaderWait)
		case <-time.After(pollInterval):
		}
	}
}

// waitLeaderChange waits, up to leaderChangeWait, until the known leader is
// not old (this node leading, or another node known as leader).
func (r *Raft) waitLeaderChange(ctx context.Context, old string) {
	t := time.NewTimer(leaderChangeWait)
	defer t.Stop()
	for {
		if r.raft.State() == hraft.Leader {
			return
		}
		if l := string(r.raft.Leader()); l != "" && l != old {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			return
		case <-time.After(pollInterval):
		}
	}
}

// forward POSTs data to p's forward endpoint.
func (r *Raft) forward(ctx context.Context, p Peer, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+p.ForwardAddr+submitPath, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("raft consensus: forward to %s: %w", p.ID, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := r.httpCli.Do(req)
	if err != nil {
		return fmt.Errorf("raft consensus: forward to %s: %w", p.ID, err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft consensus: forward to %s: %s: %s", p.ID, resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}

// handleSubmit is the forward endpoint: on the leader it calls Apply with
// the request body and answers 200 once Apply returned (not on commit);
// anywhere else it answers 503.
func (r *Raft) handleSubmit(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxBlockBytes))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if r.isClosed() {
		http.Error(w, ErrClosed.Error(), http.StatusServiceUnavailable)
		return
	}
	if r.raft.State() != hraft.Leader {
		http.Error(w, errNotLeader.Error(), http.StatusServiceUnavailable)
		return
	}
	r.apply(data)
	atomic.AddInt64(&r.forwardsServed, 1)
	w.WriteHeader(http.StatusOK)
}

// Deliver returns the channel of committed blocks, closed by Close.
func (r *Raft) Deliver() <-chan []byte { return r.fsm.deliver }

// Close stops the forward endpoint and raft, closes the stores and closes
// the Deliver channel. It is idempotent; later calls return the first
// call's result.
func (r *Raft) Close() error {
	r.closeOnce.Do(func() {
		close(r.closeCh) // fails Submits, releases an FSM Apply blocked on Deliver
		var errs []error
		if err := r.httpSrv.Close(); err != nil {
			errs = append(errs, fmt.Errorf("forward server: %w", err))
		}
		<-r.served
		if err := r.raft.Shutdown().Error(); err != nil {
			errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
		}
		r.fsm.close()
		if r.bolt != nil {
			if err := r.bolt.Close(); err != nil {
				errs = append(errs, fmt.Errorf("bolt store: %w", err))
			}
		}
		if len(errs) > 0 {
			r.closeErr = fmt.Errorf("raft consensus: close: %v", errs)
		}
	})
	return r.closeErr
}

func (r *Raft) isClosed() bool {
	select {
	case <-r.closeCh:
		return true
	default:
		return false
	}
}

// withClose derives a context that is also cancelled by Close.
func (r *Raft) withClose(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-r.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// stopErr is ErrClosed after Close, ctx's error once ctx is done, else nil.
func (r *Raft) stopErr(ctx context.Context) error {
	if r.isClosed() {
		return ErrClosed
	}
	return ctx.Err()
}

// wrapStop reports why Submit stopped, with the last attempt's error.
func (r *Raft) wrapStop(stop, last error) error {
	if last == nil || errors.Is(last, stop) {
		return stop
	}
	return fmt.Errorf("%w (last attempt: %v)", stop, last)
}

// ---- FSM ----

// fsm delivers every committed command entry. Its state lives outside raft
// (redis + ledger, applied by the core), so snapshots are empty.
type fsm struct {
	deliver chan []byte
	closeCh chan struct{} // the backend's; closed first by Close

	mu     sync.RWMutex // write-held only to close deliver
	closed bool
}

// Apply pushes a copy of the entry's data to Deliver, waiting for room.
// After Close it drops the entry: it never sends on the closed channel.
func (f *fsm) Apply(l *hraft.Log) interface{} {
	if l.Type != hraft.LogCommand {
		return nil
	}
	data := append([]byte(nil), l.Data...)
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return nil
	}
	select {
	case f.deliver <- data:
	case <-f.closeCh:
	}
	return nil
}

// close closes deliver exactly once. closeCh must already be closed, so an
// Apply blocked on a full deliver returns and releases the read lock.
func (f *fsm) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.deliver)
	}
}

func (f *fsm) Snapshot() (hraft.FSMSnapshot, error) { return emptySnapshot{}, nil }

// Restore discards the (empty) snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	_, err := io.Copy(io.Discard, rc)
	return err
}

type emptySnapshot struct{}

func (emptySnapshot) Persist(sink hraft.SnapshotSink) error { return sink.Close() }
func (emptySnapshot) Release()                              {}

// ---- TCP transport ----

// newTCPTransport is raft's TCP network transport listening on bind and
// advertising advertise verbatim. raft.NewTCPTransport would advertise a
// resolved IP:port, which raft.Leader() then reports, so it would not match
// the RaftAddr strings of Peers when they are host names.
func newTCPTransport(bind, advertise string) (*hraft.NetworkTransport, error) {
	lis, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("raft consensus: raft listen %s: %w", bind, err)
	}
	stream := &tcpStream{Listener: lis, advertise: tcpAddr(advertise)}
	return hraft.NewNetworkTransport(stream, 3, 10*time.Second, os.Stderr), nil
}

// tcpStream is a raft.StreamLayer over plain TCP whose Addr is the
// advertised address.
type tcpStream struct {
	net.Listener
	advertise net.Addr
}

func (s *tcpStream) Addr() net.Addr { return s.advertise }

func (s *tcpStream) Dial(addr hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(addr), timeout)
}

// tcpAddr is a net.Addr given as a host:port string.
type tcpAddr string

func (a tcpAddr) Network() string { return "tcp" }
func (a tcpAddr) String() string  { return string(a) }
