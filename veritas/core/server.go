package core

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"hybrid/consensus"
	pbv "hybrid/proto/veritas"
	"hybrid/veritas"
	"hybrid/veritas/ledger"
	"hybrid/veritas/sigs"
)

// CommitMode selects when Set returns.
type CommitMode int

const (
	// CommitAsync returns as soon as the SharedLog is queued in the batcher
	// (the original veritas behaviour).
	CommitAsync CommitMode = iota
	// CommitSync returns once the block containing the SharedLog has been
	// applied on THIS node (other nodes apply the same block independently
	// and may lag), or with an error.
	CommitSync
)

var (
	// ErrAborted: the SharedLog was applied as aborted (stale client
	// version, VersionClient mode). gRPC code Aborted.
	ErrAborted = errors.New("transaction aborted")
	// ErrStopped: the node is stopped (Close, a fatal Apply error, or the
	// consensus Deliver channel closed). gRPC code Unavailable.
	ErrStopped = errors.New("node stopped")
	// ErrCommitTimeout: a sync Set gave up waiting for its block (e.g. the
	// block was lost after Submit accepted it). gRPC code DeadlineExceeded.
	ErrCommitTimeout = errors.New("commit wait timed out")
	// ErrSubmitFailed: consensus.Submit rejected the block that carried
	// the SharedLog. gRPC code Unavailable.
	ErrSubmitFailed = errors.New("block submit failed")
)

const (
	// DefaultSyncTimeout bounds how long a sync Set waits for its block
	// when the request ctx has no earlier deadline, so a block lost after
	// Submit returned nil cannot leak its waiters.
	DefaultSyncTimeout = 30 * time.Second
	// submitTimeout bounds one consensus.Submit call.
	submitTimeout = 30 * time.Second
	// closeFlushTimeout is how long Close lets the final batcher flush run
	// before cancelling the server context.
	closeFlushTimeout = 2 * time.Second
	// seqNodeShift: SharedLog.Seq = NodeID<<seqNodeShift | counter.
	seqNodeShift = 40
	// maxNodeID is the exclusive upper bound on NodeID, so that
	// NodeID<<40 stays a positive int64.
	maxNodeID = 1 << (63 - seqNodeShift)
	// arrivalQueue is the capacity of the forwarder -> apply loop channel.
	arrivalQueue = 128
)

// ServerConfig configures a Server. NodeID, Signature, BlockSize, Mode and
// Commit are the fields of the plan; FlushInterval and SyncTimeout are
// optional additions (zero = default).
//
// NodeID must be in [0, 2^23) and unique in the deployment: it is the high
// part of every SharedLog.Seq this node creates. Signature must be non-empty
// and distinct per node: waiters are only resolved, and consensus time only
// measured, for delivered blocks carrying this node's signature. NewServer
// panics on an out-of-range NodeID, an empty Signature or an unknown Commit.
type ServerConfig struct {
	NodeID    int
	Signature string
	BlockSize int // < 1 means DefaultBlockSize
	Mode      VersionMode
	Commit    CommitMode

	FlushInterval time.Duration // <= 0 means DefaultFlushInterval (1 s)
	SyncTimeout   time.Duration // <= 0 means DefaultSyncTimeout

	// Sigs (--verify-sigs): SigIngress checks the client signature of every
	// Set at Set/BatchSet (codes.Unauthenticated on a bad or missing one);
	// SigAll also makes the executor check every Set before applying it
	// (a bad SharedLog is aborted). ClientKeys maps a SetRequest.ClientId
	// to its public key; it is required unless Sigs is SigNone.
	Sigs       sigs.SigMode
	ClientKeys func(id string) (ed25519.PublicKey, bool)

	// AttestEvery (--attest-every, phase 2 B2): > 0 = after every block at
	// a height divisible by it, sign (height, root) with NodeKey and certify
	// the attestations of all nodes (see attestor); 0 = off (phase 1, no
	// attestation ever, no extra block). N is the number of nodes (node ids
	// 1..N, keys NodeKeys); AttestQuorum 0 = 2f+1; AttestIdle 0 =
	// DefaultAttestIdle.
	AttestEvery  int
	N            int
	AttestQuorum int
	AttestIdle   time.Duration
	// CertifiedWait bounds a certified read's wait (0 = DefaultCertifiedWait).
	CertifiedWait time.Duration
	NodeKey       ed25519.PrivateKey
	NodeKeys      func(nodeID int32) (ed25519.PublicKey, bool)

	// OnApplied, if non-nil, is called synchronously from the apply loop
	// after every successfully applied block (empty blocks included) and
	// before the block's waiters are resolved. It MUST NOT block. nil = off.
	OnApplied func(AppliedEvent)
}

// AppliedEvent describes one block applied by a Server; see
// ServerConfig.OnApplied.
type AppliedEvent struct {
	Height   int64
	Proposer string // blk.GetSignature()
	Txs      int    // number of SetRequests in the block (sum over SharedLogs)
	Aborted  int    // number of aborted SharedLogs
	Root     []byte // copy of ex.RootDigest() after Apply
}

// SetVerifier is the executor check of --verify-sigs=all: the Set carries a
// known client id and that client's valid signature.
func SetVerifier(keys func(id string) (ed25519.PublicKey, bool)) func(*pbv.SetRequest) bool {
	return func(s *pbv.SetRequest) bool {
		pub, ok := keys(s.GetClientId())
		return ok && sigs.VerifySet(pub, s.GetKey(), s.GetValue(), s.GetVersion(), s.GetSig())
	}
}

// Server is the shared Veritas node core: it batches client Sets into
// blocks, submits them to a consensus.Consensus, applies every delivered
// block through the deterministic Executor and serves pbv.NodeServer.
//
// Goroutines: the batcher goroutine is the only caller of Submit. The
// forwarder goroutine reads Deliver, stamps each block's arrival time and
// hands it to the apply loop through a bounded channel (arrivalQueue); the
// apply loop is the only caller of Apply. Neither ever calls Submit (see the
// memnet self-block warning).
//
// Fatal errors: if Apply fails the store/ledger may be mid-block, so the
// node stops instead of continuing on diverged state: it logs, fails all
// waiters and every later RPC with ErrStopped, stops applying, and closes
// the consensus backend (crash semantics). The forwarder keeps draining
// Deliver (discarding blocks) until the channel closes, so peers sharing an
// in-memory hub, or a backend whose Close waits for its Deliver buffer to
// drain, are never blocked by a stopped node. SharedLogs already queued in
// the batcher when the node stops are still flushed and submitted (by the
// timer, or by Close), although their sync waiters have already been failed
// with ErrStopped. A block that fails to unmarshal is skipped (every node
// receives the same bytes, so skipping is deterministic); it still consumes
// a height.
type Server struct {
	counter int64 // SharedLog.Seq counter; atomic; first for alignment

	cfg         ServerConfig
	att         *attestor // nil unless AttestEvery > 0
	c           consensus.Consensus
	ex          *Executor
	batcher     *Batcher
	waiters     *waiterSet
	metrics     *metrics
	syncTimeout time.Duration
	arrivals    chan arrival // forwarder -> apply loop; closed by forwarder

	ctx    context.Context // cancelled by Close; parent of Submit contexts
	cancel context.CancelFunc

	stopMu  sync.Mutex
	stopErr error         // non-nil once stopped
	stopCh  chan struct{} // closed when stopped

	loopDone  chan struct{} // closed when the apply loop returns
	closeOnce sync.Once
	closeErr  error
}

// arrival is a delivered block stamped with the time the forwarder read it.
type arrival struct {
	raw []byte
	at  time.Time
}

var _ pbv.NodeServer = (*Server)(nil)

// NewServer starts a node over consensus c and executor ex and returns it;
// it owns c from now on (Close closes it). l must be ex's ledger, or nil; it
// is only checked, not kept: proofs are taken through ex.Prove, under the
// executor lock, never from the ledger directly. NewServer panics on a wiring
// bug: l is a different ledger, cfg.Mode differs from ex's mode, or cfg is
// invalid (see ServerConfig). The caller still owns and closes l and ex's
// store, after Close.
func NewServer(cfg ServerConfig, c consensus.Consensus, ex *Executor, l *ledger.LogLedger) *Server {
	if l != nil && l != ex.ledger {
		panic("core: NewServer: ledger is not the executor's ledger")
	}
	if cfg.Mode != ex.mode {
		panic(fmt.Sprintf("core: NewServer: config mode %d != executor mode %d", cfg.Mode, ex.mode))
	}
	if cfg.NodeID < 0 || int64(cfg.NodeID) >= maxNodeID {
		panic(fmt.Sprintf("core: NewServer: NodeID %d out of range [0, %d)", cfg.NodeID, int64(maxNodeID)))
	}
	if cfg.Signature == "" {
		panic("core: NewServer: empty Signature")
	}
	if cfg.Commit != CommitAsync && cfg.Commit != CommitSync {
		panic(fmt.Sprintf("core: NewServer: unknown commit mode %d", cfg.Commit))
	}
	if cfg.SyncTimeout <= 0 {
		cfg.SyncTimeout = DefaultSyncTimeout
	}
	if cfg.Sigs != sigs.SigNone && cfg.ClientKeys == nil {
		panic("core: NewServer: Sigs set without ClientKeys")
	}
	if cfg.Sigs == sigs.SigAll {
		ex.SetSetVerifier(SetVerifier(cfg.ClientKeys))
	}
	if cfg.AttestEvery < 0 {
		panic("core: NewServer: negative AttestEvery")
	}
	if cfg.AttestEvery > 0 && (cfg.N < 1 || cfg.NodeKey == nil || cfg.NodeKeys == nil) {
		panic("core: NewServer: AttestEvery needs N, NodeKey and NodeKeys")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:         cfg,
		c:           c,
		ex:          ex,
		waiters:     newWaiterSet(),
		metrics:     newMetrics(),
		syncTimeout: cfg.SyncTimeout,
		arrivals:    make(chan arrival, arrivalQueue),
		ctx:         ctx,
		cancel:      cancel,
		stopCh:      make(chan struct{}),
		loopDone:    make(chan struct{}),
	}
	if cfg.AttestEvery > 0 {
		s.att = newAttestor(cfg)
		s.batcher = newBatcherIdle(cfg.BlockSize, cfg.FlushInterval, s.submitBlock, s.metrics.addBatch,
			s.att.idle/2, s.att.idleBlock)
	} else {
		s.batcher = newBatcher(cfg.BlockSize, cfg.FlushInterval, s.submitBlock, s.metrics.addBatch)
	}
	go s.forwardLoop()
	go s.applyLoop()
	return s
}

// Close stops the node and releases the backend, in this order:
//
//  1. The node is stopped FIRST: new RPCs fail with ErrStopped and waiting
//     sync Sets are failed, including those whose SharedLog is still in the
//     batcher.
//  2. The batcher flushes what it holds (async Sets, and the SharedLogs of
//     the sync Sets just failed). The flush gets closeFlushTimeout; then the
//     server context is cancelled, which aborts its Submit if the backend
//     honours ctx. Close then waits for the flush to return, so this step is
//     bounded only for backends whose Submit honours ctx.
//  3. The consensus backend is closed and the apply loop is waited for.
//
// It returns c.Close()'s error and is idempotent.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.stop(errors.New("server closed"))
		flushed := make(chan struct{})
		go func() {
			s.batcher.Close()
			close(flushed)
		}()
		select {
		case <-flushed:
		case <-time.After(closeFlushTimeout):
		}
		s.cancel()
		<-flushed
		s.closeErr = s.c.Close()
		<-s.loopDone
	})
	return s.closeErr
}

// ---- gRPC handlers ----

// Get reads the key from the executor's store and returns its value, like
// the original server. A missing key is codes.NotFound (the original
// returned redis.Nil as an error). Reads are not synchronised with Apply, so
// a Get may observe a block half-applied (as in the original).
func (s *Server) Get(ctx context.Context, req *pbv.GetRequest) (*pbv.GetResponse, error) {
	if err := s.stoppedErr(); err != nil {
		return nil, err
	}
	if err := s.evictedErr(); err != nil {
		return nil, err
	}
	if err := s.divergedErr(); err != nil {
		return nil, err
	}
	raw, ok, err := s.ex.store.Get(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %q not found", req.GetKey())
	}
	v, err := veritas.Decode(raw)
	if err != nil {
		return nil, err
	}
	return &pbv.GetResponse{Value: s.onRead(req.GetKey(), v.Val, false)}, nil
}

// onRead passes a value this node returns through the node's ReadFault, if
// its FaultPolicy has one (F6); otherwise it returns val.
func (s *Server) onRead(key, val string, signed bool) string {
	if rf, ok := s.ex.fault.(ReadFault); ok {
		return rf.OnRead(key, val, signed)
	}
	return val
}

// Set submits one write as a SharedLog{Seq, Sets: [req]} and, in CommitSync
// mode, waits for its block (see commit). Unlike the original it does not
// take a per-key lock: that lock only serialised the channel send and gave
// no ordering guarantee.
func (s *Server) Set(ctx context.Context, req *pbv.SetRequest) (*pbv.SetResponse, error) {
	if err := s.checkSig(req); err != nil {
		return nil, err
	}
	if err := s.commit(ctx, []*pbv.SetRequest{copySet(req)}); err != nil {
		return nil, err
	}
	return &pbv.SetResponse{}, nil
}

// BatchSet mirrors the original: all Sets go into ONE SharedLog (staleness
// is decided per Set version, and the SharedLog commits or aborts as a
// whole); the commit mode applies as for Set. An empty request is
// codes.InvalidArgument (the original panicked).
func (s *Server) BatchSet(ctx context.Context, req *pbv.BatchSetRequest) (*pbv.BatchSetResponse, error) {
	if len(req.GetSets()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "BatchSet: no sets")
	}
	sets := make([]*pbv.SetRequest, len(req.GetSets()))
	for i, r := range req.GetSets() {
		if err := s.checkSig(r); err != nil {
			return nil, err
		}
		sets[i] = copySet(r)
	}
	if err := s.commit(ctx, sets); err != nil {
		return nil, err
	}
	return &pbv.BatchSetResponse{}, nil
}

// Verify returns the ledger root and a Merkle proof for the key, as the
// original server, but both come from Executor.Prove under one lock hold, so
// the proof always matches the returned root.
func (s *Server) Verify(ctx context.Context, req *pbv.VerifyRequest) (*pbv.VerifyResponse, error) {
	if err := s.stoppedErr(); err != nil {
		return nil, err
	}
	root, proof, err := s.ex.Prove([]byte(req.GetKey()))
	if err != nil {
		return nil, err
	}
	return &pbv.VerifyResponse{
		RootDigest:            root,
		SideNodes:             proof.SideNodes,
		NonMembershipLeafData: proof.NonMembershipLeafData,
	}, nil
}

// GetVerified returns key's value and version with a Merkle proof, the root
// and the height they all belong to (Executor.ProveValue, one lock hold), so
// the client can check the value against the root (driver.GetVerified). A
// missing key is codes.NotFound.
func (s *Server) GetVerified(ctx context.Context, req *pbv.GetVerifiedRequest) (*pbv.GetVerifiedResponse, error) {
	if err := s.stoppedErr(); err != nil {
		return nil, err
	}
	if err := s.evictedErr(); err != nil {
		return nil, err
	}
	if err := s.divergedErr(); err != nil {
		return nil, err
	}
	var resp *pbv.GetVerifiedResponse
	var err error
	if req.GetCertified() {
		resp, err = s.getCertified(ctx, []byte(req.GetKey()))
	} else {
		p := s.proveResp([]byte(req.GetKey()))
		resp, err = p.resp, p.err
	}
	if err != nil {
		return nil, err
	}
	// After the proof: proof, root and version are those of the real value.
	resp.Value = s.onRead(req.GetKey(), resp.GetValue(), req.GetSigned())
	if s.att != nil {
		resp.EvictedEpoch = s.att.acct.Epoch()
	}
	if req.GetSigned() {
		if s.cfg.NodeKey == nil {
			return nil, status.Error(codes.FailedPrecondition, "signed read: this node has no node key (--attest-every=0)")
		}
		// Signed over the value actually returned, so a wrong one is evidence.
		resp.NodeId = int32(s.cfg.NodeID)
		resp.NodeSig = sigs.SignRead(s.cfg.NodeKey, sigs.SignedReadOf(req.GetKey(), resp))
	}
	return resp, nil
}

// Evicted returns this node's evicted nodes and its eviction epoch (phase
// 3); none without attestations.
func (s *Server) Evicted(ctx context.Context, req *pbv.EvictedRequest) (*pbv.EvictedResponse, error) {
	if s.att == nil {
		return &pbv.EvictedResponse{}, nil
	}
	return &pbv.EvictedResponse{Epoch: s.att.acct.Epoch(), Nodes: s.att.acct.Nodes()}, nil
}

// ReportEvidence takes a client's evidence of a bad signed read (phase 3):
// a read signed by node ev.Culprit for ev.Height. A signature that does not
// verify is codes.InvalidArgument; otherwise the evidence goes on this
// node's next block and every node judges it in the apply loop
// (Accountant.CheckRead), so all of them evict the culprit at the same
// height or none does. Evidence on a node this node already evicted, or a
// copy of evidence this node queued lately (attestor.addEvidence), is
// acknowledged and not sent again; when maxPendingEvidence reports already
// wait for a block the answer is codes.ResourceExhausted.
func (s *Server) ReportEvidence(ctx context.Context, ev *pbv.Evidence) (*pbv.EvidenceAck, error) {
	if s.att == nil {
		return nil, status.Error(codes.FailedPrecondition, "ReportEvidence: attestations are off (--attest-every=0)")
	}
	r := ev.GetRead()
	if r == nil || r.GetNodeId() != ev.GetCulprit() || r.GetHeight() != ev.GetHeight() {
		return nil, status.Error(codes.InvalidArgument, "ReportEvidence: the read must be node culprit's, at height")
	}
	pub, ok := s.cfg.NodeKeys(ev.GetCulprit())
	if !ok || !sigs.VerifyRead(pub, r) {
		return nil, status.Errorf(codes.InvalidArgument, "ReportEvidence: no valid signature of node %d on the read", ev.GetCulprit())
	}
	if _, gone := s.att.acct.Height(ev.GetCulprit()); !gone {
		if s.att.addEvidence(ev) == evidenceFull {
			return nil, status.Errorf(codes.ResourceExhausted, "ReportEvidence: %d reports already wait for a block", maxPendingEvidence)
		}
	}
	return &pbv.EvidenceAck{}, nil
}

// evictedErr is codes.PermissionDenied once this node evicted itself (phase
// 3): the ordered log proved its state or reads wrong.
func (s *Server) evictedErr() error {
	if s.att != nil && s.att.acct.SelfEvicted() {
		h, _ := s.att.acct.Height(int32(s.cfg.NodeID))
		return status.Errorf(codes.PermissionDenied, "node %d was evicted at height %d", s.cfg.NodeID, h)
	}
	return nil
}

// DefaultCertifiedWait bounds how long a certified read waits.
const DefaultCertifiedWait = 5 * time.Second

// proveResp is GetVerified's proof (Executor.ProveValue) as a response.
func (s *Server) proveResp(key []byte) certProof {
	val, ver, h, root, proof, err := s.ex.ProveValue(key)
	if errors.Is(err, ErrKeyNotFound) {
		return certProof{err: status.Errorf(codes.NotFound, "key %q not found", key)}
	}
	if err != nil {
		return certProof{err: err}
	}
	return certProof{resp: &pbv.GetVerifiedResponse{
		Value:                 val,
		Version:               ver,
		Height:                h,
		RootDigest:            root,
		SideNodes:             proof.SideNodes,
		NonMembershipLeafData: proof.NonMembershipLeafData,
	}}
}

// getCertified is GetVerified(certified): a proof whose root is this node's
// root at an attested height h, returned with the quorum of attestations
// that certify (h, root). If the current root is one of an attested height
// (no block with txs since), the proof is taken now; otherwise the apply
// loop takes it right after the next attested height. Then it waits for
// the certification. The whole wait is bounded by CertifiedWait
// (codes.DeadlineExceeded); a certified root other than the proof's is
// codes.DataLoss (this node diverged).
func (s *Server) getCertified(ctx context.Context, key []byte) (*pbv.GetVerifiedResponse, error) {
	if s.att == nil {
		return nil, status.Error(codes.FailedPrecondition, "certified read: attestations are off (--attest-every=0)")
	}
	wait := s.cfg.CertifiedWait
	if wait <= 0 {
		wait = DefaultCertifiedWait
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	p := s.proveResp(key)
	if p.err != nil {
		return nil, p.err
	}
	h, ok := s.att.attestedHeight(p.resp.GetRootDigest())
	if ok {
		p.resp.Height = h
	} else {
		r := s.att.waitNext(key)
		select {
		case p = <-r.out:
		case <-ctx.Done():
			s.att.forget(r)
			return nil, status.Errorf(codes.DeadlineExceeded, "certified read of %q: no attested height within %v", key, wait)
		case <-s.stopCh:
			s.att.forget(r)
			return nil, s.stoppedErr()
		}
		if p.err != nil {
			return nil, p.err
		}
	}
	root, atts, err := s.att.cert.Wait(ctx, p.resp.GetHeight())
	if err != nil {
		return nil, status.Errorf(codes.DeadlineExceeded, "certified read of %q: height %d not certified within %v", key, p.resp.GetHeight(), wait)
	}
	if string(root) != string(p.resp.GetRootDigest()) {
		return nil, status.Errorf(codes.DataLoss, "certified read of %q: height %d certified with another root than this node's", key, p.resp.GetHeight())
	}
	if err := s.divergedErr(); err != nil {
		return nil, err
	}
	p.resp.Attestations = atts
	return p.resp, nil
}

// checkSig is the ingress check of --verify-sigs (SigIngress, SigAll).
func (s *Server) checkSig(r *pbv.SetRequest) error {
	if s.cfg.Sigs == sigs.SigNone {
		return nil
	}
	pub, ok := s.cfg.ClientKeys(r.GetClientId())
	if !ok {
		return status.Errorf(codes.Unauthenticated, "unknown client %q", r.GetClientId())
	}
	if !sigs.VerifySet(pub, r.GetKey(), r.GetValue(), r.GetVersion(), r.GetSig()) {
		return status.Errorf(codes.Unauthenticated, "bad signature of client %q on key %q", r.GetClientId(), r.GetKey())
	}
	return nil
}

func copySet(r *pbv.SetRequest) *pbv.SetRequest {
	return &pbv.SetRequest{
		Signature: r.GetSignature(),
		Key:       r.GetKey(),
		Value:     r.GetValue(),
		Version:   r.GetVersion(),
		ClientId:  r.GetClientId(),
		Sig:       append([]byte(nil), r.GetSig()...),
	}
}

// ---- commit path ----

// nextSeq returns a new SharedLog seq, NodeID<<40 | counter (counter from
// 1), in BOTH version modes. Seqs are therefore unique per deployment (given
// unique NodeIDs); they identify waiters and blocks, never versions:
// staleness (VersionClient) is decided by each Set's Version, and
// VersionOrdered assigns versions from the height.
func (s *Server) nextSeq() int64 {
	n := atomic.AddInt64(&s.counter, 1) & (int64(1)<<seqNodeShift - 1)
	return int64(s.cfg.NodeID)<<seqNodeShift | n
}

// commit queues SharedLog{seq, sets} with a fresh unique seq. CommitAsync
// returns once it is queued. CommitSync registers a waiter for the seq
// BEFORE queueing (the block can be applied before Submit even returns) and
// waits for the first of:
//
//   - its block applied on this node: nil, or ErrAborted if the SharedLog
//     was aborted;
//   - its block's Submit failed: ErrSubmitFailed;
//   - the node stopped: ErrStopped;
//   - ctx done: ctx.Err() (wrapped with gRPC code Canceled or
//     DeadlineExceeded);
//   - SyncTimeout: ErrCommitTimeout.
//
// The waiter is removed on every path, so none leaks. If the block was
// applied concurrently with ctx/timeout, the applied result wins. Two Sets
// with the same client version get distinct seqs and therefore their own
// results (e.g. one committed, the other aborted as stale).
func (s *Server) commit(ctx context.Context, sets []*pbv.SetRequest) error {
	if err := s.stoppedErr(); err != nil {
		return err
	}
	seq := s.nextSeq()
	sl := &pbv.SharedLog{Seq: seq, Sets: sets}

	if s.cfg.Commit != CommitSync {
		return s.addErr(s.batcher.addCtx(ctx, sl))
	}

	w, err := s.waiters.add(seq)
	if err != nil {
		return err
	}
	if err := s.batcher.addCtx(ctx, sl); err != nil {
		s.waiters.remove(seq, w)
		return s.addErr(err)
	}

	timer := time.NewTimer(s.syncTimeout)
	defer timer.Stop()
	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		if err, ok := s.waiters.cancel(seq, w); ok {
			return err
		}
		return ctxErr(ctx.Err())
	case <-timer.C:
		if err, ok := s.waiters.cancel(seq, w); ok {
			return err
		}
		return &rpcError{codes.DeadlineExceeded, fmt.Errorf("%w after %v (seq %d)", ErrCommitTimeout, s.syncTimeout, seq)}
	}
}

// ctxErr wraps a context error with its gRPC code, keeping errors.Is.
func ctxErr(err error) error {
	code := codes.Canceled
	if errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	return &rpcError{code, err}
}

// addErr maps a batcher addCtx error to an RPC error.
func (s *Server) addErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errBatcherClosed) {
		if serr := s.stoppedErr(); serr != nil {
			return serr
		}
		return &rpcError{codes.Unavailable, fmt.Errorf("%w: batcher closed", ErrStopped)}
	}
	return ctxErr(err) // the only other addCtx error is ctx.Err()
}

// submitBlock is the batcher's out: it stamps the block with this node's
// signature, marshals it and submits it. It runs on the batcher goroutine
// only. The block id used for the Consensus phase is Txs[0].Seq, unique per
// node (the block identity the original server and the Kafka tracker use).
// On failure the block's waiters get ErrSubmitFailed (even though the block
// might in rare cases still be delivered later).
func (s *Server) submitBlock(blk *pbv.Block) error {
	start := time.Now()
	blk.Signature = s.cfg.Signature
	blk.Type = pbv.MessageType_Approve
	if s.att != nil && blk.Attestations == nil {
		blk.Attestations = s.att.take()
	}
	if s.att != nil && blk.Evidence == nil {
		blk.Evidence = s.att.takeEvidence()
	}
	raw, err := proto.Marshal(blk)
	if err != nil {
		s.failBlock(blk, err)
		s.putBackAttestations(blk)
		return fmt.Errorf("marshal: %w", err)
	}
	var id int64
	if len(blk.Txs) > 0 { // an attestation-only block has no consensus sample
		id = blk.Txs[0].GetSeq()
		s.metrics.submitted(id, start)
	}
	ctx, cancel := context.WithTimeout(s.ctx, submitTimeout)
	err = s.c.Submit(ctx, raw)
	cancel()
	if err != nil {
		if len(blk.Txs) > 0 {
			s.metrics.unsubmitted(id)
		}
		s.failBlock(blk, err)
		s.putBackAttestations(blk)
		return fmt.Errorf("submit: %w", err)
	}
	return nil
}

func (s *Server) putBackAttestations(blk *pbv.Block) {
	if s.att != nil {
		s.att.putBack(blk.Attestations, blk.Evidence)
	}
}

// Diverged reports the first height at which a certified root differed
// from this node's own root (attestations, --attest-every > 0).
func (s *Server) Diverged() (height int64, ok bool) {
	if s.att == nil {
		return 0, false
	}
	h := s.att.diverged.Load()
	return h, h != 0
}

// divergedErr is codes.DataLoss once the node is diverged.
func (s *Server) divergedErr() error {
	if h, ok := s.Diverged(); ok {
		return status.Errorf(codes.DataLoss, "node %d diverged at height %d: its state disagrees with a certified root", s.cfg.NodeID, h)
	}
	return nil
}

// failBlock fails the waiters of every SharedLog of blk.
func (s *Server) failBlock(blk *pbv.Block, cause error) {
	if s.cfg.Commit != CommitSync {
		return
	}
	err := &rpcError{codes.Unavailable, fmt.Errorf("%w: %v", ErrSubmitFailed, cause)}
	for _, sl := range blk.GetTxs() {
		s.waiters.resolve(sl.GetSeq(), err)
	}
}

// ---- deliver path ----

// forwardLoop reads Deliver, stamps each block's arrival time and passes it
// to the apply loop. Once the node is stopped it discards blocks instead of
// waiting for room, so it always keeps draining Deliver. It closes
// s.arrivals when Deliver is closed.
func (s *Server) forwardLoop() {
	defer close(s.arrivals)
	for raw := range s.c.Deliver() {
		a := arrival{raw: raw, at: time.Now()}
		select {
		case s.arrivals <- a:
		case <-s.stopCh: // stopped: drop the block
		}
	}
}

// applyLoop is the only caller of Apply. height counts every block taken
// from the arrival queue, starting at 1, so all nodes agree on it.
func (s *Server) applyLoop() {
	defer close(s.loopDone)
	var height int64
	for a := range s.arrivals {
		height++
		if s.stoppedErr() != nil {
			continue // stopped: drain and discard
		}
		start := time.Now()
		s.metrics.queued(start.Sub(a.at))
		var blk pbv.Block
		if err := proto.Unmarshal(a.raw, &blk); err != nil {
			log.Printf("core: node %d: skipping undecodable block at height %d: %v", s.cfg.NodeID, height, err)
			continue
		}
		own := blk.GetSignature() == s.cfg.Signature && len(blk.GetTxs()) > 0
		if own {
			s.metrics.delivered(blk.Txs[0].GetSeq(), a.at)
		}
		_, aborted, err := s.ex.Apply(context.Background(), height, a.raw, &blk)
		ledgerTime := s.ex.TakeLedgerTime()
		if err != nil {
			log.Printf("core: node %d: FATAL apply at height %d, stopping node: %v", s.cfg.NodeID, height, err)
			s.stop(fmt.Errorf("apply at height %d: %v", height, err))
			go func() {
				if cerr := s.c.Close(); cerr != nil {
					log.Printf("core: node %d: closing consensus: %v", s.cfg.NodeID, cerr)
				}
			}()
			continue
		}
		s.metrics.applied(time.Since(start)-ledgerTime, ledgerTime)
		if s.cfg.OnApplied != nil {
			s.notifyApplied(height, &blk, aborted)
		}
		if s.att != nil {
			s.att.afterApply(height, s.ex.RootDigest(), &blk, s.cfg.NodeID, s.proveResp)
		}
		if own && s.cfg.Commit == CommitSync {
			s.resolveBlock(&blk, aborted)
		}
	}
	s.stop(errors.New("consensus delivery channel closed"))
}

// notifyApplied reports an applied block to cfg.OnApplied.
func (s *Server) notifyApplied(height int64, blk *pbv.Block, aborted []int64) {
	txs := 0
	for _, sl := range blk.GetTxs() {
		txs += len(sl.GetSets())
	}
	s.cfg.OnApplied(AppliedEvent{
		Height:   height,
		Proposer: blk.GetSignature(),
		Txs:      txs,
		Aborted:  len(aborted),
		Root:     s.ex.RootDigest(),
	})
}

// resolveBlock wakes the waiters of an applied own block. Waking a seq with
// no waiter (async Set, waiter already gone, duplicate delivery) is a no-op.
func (s *Server) resolveBlock(blk *pbv.Block, aborted []int64) {
	var ab map[int64]struct{}
	if len(aborted) > 0 {
		ab = make(map[int64]struct{}, len(aborted))
		for _, seq := range aborted {
			ab[seq] = struct{}{}
		}
	}
	for _, sl := range blk.GetTxs() {
		seq := sl.GetSeq()
		var err error
		if _, ok := ab[seq]; ok {
			err = &rpcError{codes.Aborted, fmt.Errorf("%w: seq %d", ErrAborted, seq)}
		}
		s.waiters.resolve(seq, err)
	}
}

// ---- stop state ----

// stop marks the node stopped (first cause wins), releases the forwarder
// and fails all waiters.
func (s *Server) stop(cause error) {
	s.stopMu.Lock()
	if s.stopErr != nil {
		s.stopMu.Unlock()
		return
	}
	err := &rpcError{codes.Unavailable, fmt.Errorf("%w: %v", ErrStopped, cause)}
	s.stopErr = err
	close(s.stopCh)
	s.stopMu.Unlock()
	s.waiters.failAll(err)
}

func (s *Server) stoppedErr() error {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	return s.stopErr
}

// ---- errors ----

// rpcError carries a gRPC code (via GRPCStatus, which grpc-go uses when the
// handler returns it) while still unwrapping to the sentinel errors above,
// so errors.Is works for in-process callers.
type rpcError struct {
	code codes.Code
	err  error
}

func (e *rpcError) Error() string              { return e.err.Error() }
func (e *rpcError) Unwrap() error              { return e.err }
func (e *rpcError) GRPCStatus() *status.Status { return status.New(e.code, e.err.Error()) }

// ---- waiters ----

// waiter receives exactly one result: it is sent at most once, under the
// waiterSet lock, at the moment the waiter is removed from the set.
type waiter struct {
	ch chan error // capacity 1
}

// waiterSet holds the sync Sets waiting for their block, keyed by their
// unique seq. Entries exist only while a Set is waiting, so the set is
// bounded by the number of in-flight sync Sets (no per-seq history is kept).
type waiterSet struct {
	mu     sync.Mutex
	m      map[int64]*waiter
	closed error // set by failAll; add fails afterwards
}

func newWaiterSet() *waiterSet {
	return &waiterSet{m: make(map[int64]*waiter)}
}

// add registers a waiter for seq, or returns the failAll error. seq must not
// already have a waiter (seqs are unique; a duplicate is a bug).
func (ws *waiterSet) add(seq int64) (*waiter, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closed != nil {
		return nil, ws.closed
	}
	if _, dup := ws.m[seq]; dup {
		panic(fmt.Sprintf("core: duplicate waiter for seq %#x", seq))
	}
	w := &waiter{ch: make(chan error, 1)}
	ws.m[seq] = w
	return w, nil
}

// remove deletes w if it is still registered; a no-op otherwise.
func (ws *waiterSet) remove(seq int64, w *waiter) {
	ws.mu.Lock()
	if ws.m[seq] == w {
		delete(ws.m, seq)
	}
	ws.mu.Unlock()
}

// cancel removes w and reports the result it had already been sent, if any
// (ok=true), so a result that raced with ctx/timeout is not lost.
func (ws *waiterSet) cancel(seq int64, w *waiter) (error, bool) {
	ws.remove(seq, w)
	select {
	case err := <-w.ch:
		return err, true
	default:
		return nil, false
	}
}

// resolve sends err to the waiter of seq, if any, and removes it.
func (ws *waiterSet) resolve(seq int64, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if w, ok := ws.m[seq]; ok {
		delete(ws.m, seq)
		w.ch <- err
	}
}

// failAll sends err to every waiter, empties the set and makes later adds
// fail with err.
func (ws *waiterSet) failAll(err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.closed = err
	for seq, w := range ws.m {
		w.ch <- err
		delete(ws.m, seq)
	}
}

// count returns the number of registered waiters (for tests).
func (ws *waiterSet) count() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return len(ws.m)
}
