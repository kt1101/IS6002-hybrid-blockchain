package core

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Breakdown is a node's cumulative per-phase time since NewServer. All
// durations are SUMS over blocks; divide by the matching count for a mean.
// For a block this node proposed, the phases follow one another in time:
//
//		Batch -> Consensus (incl. marshal) -> Queue -> Execute+Ledger -> [wake waiters]
//
//	  - Batch: for every block this node flushed, from the batcher
//	    goroutine's dequeue of the block's first SharedLog to the flush.
//	    Count: BatchBlocks. Not included anywhere: the time a SharedLog waits
//	    in the batcher's Add queue before being dequeued (it grows while the
//	    batcher goroutine is blocked in the previous block's Submit, i.e.
//	    overlaps that block's Consensus).
//	  - Consensus: for every block THIS node submitted, from just before it
//	    is marshalled (proto.Marshal + consensus.Submit) to its arrival on
//	    this node's Deliver channel, as stamped by the forwarder goroutine.
//	    Blocks of other nodes are not measured; own blocks that are lost, or
//	    whose Submit failed, are not counted. Count: ConsensusBlocks.
//	    Overlap: the forwarder reads Deliver as soon as the internal arrival
//	    queue (arrivalQueue blocks) has room; while that queue is full, time
//	    a block spends in the backend's Deliver buffer counts as Consensus
//	    instead of Queue.
//	  - Queue: for every block the apply loop takes while the node is running
//	    (own and others', including blocks that then fail to decode or
//	    apply), from arrival (forwarder stamp) to the apply loop starting on
//	    it. Count: QueueBlocks (>= Blocks).
//	  - Execute: for every applied block, proto.Unmarshal plus
//	    Executor.Apply MINUS the time Apply spent in ledger writes
//	    (store reads/writes, staleness checks, FaultPolicy, encoding, lock
//	    wait behind a concurrent Validate/Prove). Count: Blocks.
//	  - Ledger: for every applied block, the time Apply spent inside
//	    ledger.Append (SMT update + badger write, per applied Set) and
//	    ledger.AppendBlk (per block), from Executor.TakeLedgerTime.
//	    Count: Blocks. Execute + Ledger = decode + Apply wall time.
//	  - Blocks: number of blocks applied successfully on this node (all
//	    proposers; blocks that fail to decode are not counted).
//	  - Dropped: number of THIS node's blocks that the consensus backend
//	    reported as dropped (never delivered, e.g. a Kafka Abort vote), via
//	    AbortBlock (drop.go). Counted in both commit modes.
//
// Not measured: waking waiters after Apply and the gRPC round trip.
type Breakdown struct {
	Batch     time.Duration `json:"batch_ns"`
	Consensus time.Duration `json:"consensus_ns"`
	Queue     time.Duration `json:"queue_ns"`
	Execute   time.Duration `json:"execute_ns"`
	Ledger    time.Duration `json:"ledger_ns"`
	Blocks    int64         `json:"blocks"`

	BatchBlocks     int64 `json:"batch_blocks"`
	ConsensusBlocks int64 `json:"consensus_blocks"`
	QueueBlocks     int64 `json:"queue_blocks"`

	Dropped int64 `json:"dropped_blocks"`

	// Attestations (--attest-every > 0, else 0): AttestSent = own
	// attestations handed to Submit; AttestInvalid = attestations dropped
	// by the certifier (bad signature, unknown node); CertifyLag = sum
	// over the heights this node attested and saw certified of the time
	// from applying the height to its certification (CertifiedHeights of
	// them).
	AttestSent       int64         `json:"attest_sent"`
	AttestInvalid    int64         `json:"attest_invalid"`
	CertifyLag       time.Duration `json:"certify_lag_ns"`
	CertifiedHeights int64         `json:"certified_heights"`
	DivergedHeight   int64         `json:"diverged_height"`
}

const (
	// maxPending bounds the submitted-but-not-delivered map used for the
	// Consensus phase, so lost blocks cannot make it grow without bound.
	maxPending = 4096
	// pendingTTL is how long a submitted block is waited for before its
	// entry may be swept as lost.
	pendingTTL = 2 * time.Minute
)

// metrics accumulates a Breakdown. All methods are safe for concurrent use.
type metrics struct {
	mu sync.Mutex
	b  Breakdown
	// pending maps the id (Txs[0].Seq) of each of this node's submitted
	// blocks to the time its Submit started. Entries leave on delivery, on
	// Submit failure, or by the TTL sweep; size <= maxPending.
	pending map[int64]time.Time
}

func newMetrics() *metrics {
	return &metrics{pending: make(map[int64]time.Time)}
}

func (m *metrics) addBatch(d time.Duration) {
	m.mu.Lock()
	m.b.Batch += d
	m.b.BatchBlocks++
	m.mu.Unlock()
}

// submitted records that the block with this id is about to be submitted.
func (m *metrics) submitted(id int64, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) >= maxPending {
		for k, t := range m.pending {
			if at.Sub(t) > pendingTTL {
				delete(m.pending, k)
			}
		}
		if len(m.pending) >= maxPending {
			log.Printf("core: metrics: %d submitted blocks not delivered, dropping their consensus samples", len(m.pending))
			m.pending = make(map[int64]time.Time)
		}
	}
	m.pending[id] = at
}

// unsubmitted forgets id after a failed Submit.
func (m *metrics) unsubmitted(id int64) {
	m.mu.Lock()
	delete(m.pending, id)
	m.mu.Unlock()
}

// delivered closes the Consensus sample of id, if this node submitted it and
// it was not delivered before (a duplicate delivery is ignored).
func (m *metrics) delivered(id int64, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start, ok := m.pending[id]
	if !ok {
		return
	}
	delete(m.pending, id)
	m.b.Consensus += at.Sub(start)
	m.b.ConsensusBlocks++
}

func (m *metrics) queued(d time.Duration) {
	m.mu.Lock()
	m.b.Queue += d
	m.b.QueueBlocks++
	m.mu.Unlock()
}

func (m *metrics) applied(execute, ledger time.Duration) {
	m.mu.Lock()
	m.b.Execute += execute
	m.b.Ledger += ledger
	m.b.Blocks++
	m.mu.Unlock()
}

func (m *metrics) snapshot() Breakdown {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.b
}

// Breakdown returns a snapshot of the node's per-phase times. Safe to call
// at any time, concurrently with everything else.
func (s *Server) Breakdown() Breakdown {
	b := s.metrics.snapshot()
	if s.att != nil {
		b.AttestSent = s.att.sent.Load()
		b.AttestInvalid = s.att.cert.Invalid()
		b.CertifyLag = time.Duration(s.att.lag.Load())
		b.CertifiedHeights = s.att.certs.Load()
		b.DivergedHeight = s.att.diverged.Load()
	}
	return b
}

// breakdownFile is the JSON written by WriteBreakdownJSON.
type breakdownFile struct {
	Node int `json:"node"`
	Breakdown
}

// WriteBreakdownJSON writes {"node": NodeID, <Breakdown fields>} to path
// (durations in nanoseconds), via a temporary file and rename so a reader
// never sees a partial file. The file is mode 0644 (readable by the
// harness outside the container). For cmd/veritas's SIGTERM handler (Task 9).
func (s *Server) WriteBreakdownJSON(path string) error {
	data, err := json.MarshalIndent(breakdownFile{Node: s.cfg.NodeID, Breakdown: s.Breakdown()}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	if err := tmp.Chmod(0644); err != nil { // CreateTemp makes it 0600
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return nil
}
