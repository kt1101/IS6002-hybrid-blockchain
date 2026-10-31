package core

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas"
	"hybrid/veritas/ledger"
)

// VersionMode selects where the version of an applied Set comes from.
type VersionMode int

const (
	// VersionClient uses the client-supplied SetRequest.Version and applies
	// the original veritas staleness rule (stored version >= tx version is
	// stale). Stale SharedLogs are skipped and reported as aborted.
	VersionClient VersionMode = iota
	// VersionOrdered ignores the client version and assigns
	// height<<orderedIndexBits | index, where index is the SharedLog's
	// position in the block. The consensus order is the version order, so
	// nothing is ever aborted.
	VersionOrdered
)

// orderedIndexBits is the number of low bits of an ordered version that hold
// the SharedLog index within its block.
const orderedIndexBits = 20

// maxOrderedTxs is the largest number of SharedLogs a block may carry in
// VersionOrdered mode without the index overflowing into the height bits.
const maxOrderedTxs = 1 << orderedIndexBits

// maxOrderedHeight is the exclusive upper bound on block height in
// VersionOrdered mode: height<<orderedIndexBits must stay a positive int64.
const maxOrderedHeight = int64(1) << (63 - orderedIndexBits)

func orderedVersion(height int64, index int) int64 {
	return height<<orderedIndexBits | int64(index)
}

// Executor deterministically applies an ordered stream of blocks to a
// replica's KV store and ledger. Every backend shares it: given the same
// blocks (and heights, in VersionOrdered mode) every honest replica ends in
// the same store contents and the same ledger root digest.
//
// Concurrency: Apply must be driven from a single goroutine, the consensus
// backend's delivery loop, with blocks in order. Validate/ValidateErr and
// RootDigest may run on other goroutines concurrently with Apply (e.g. in
// the Kafka backend Validate runs on the consumer loop while Apply runs on
// the delivery loop; RootDigest is called by the gRPC Verify handler). The
// ledger's sparse Merkle tree is NOT thread-safe (its root is a plain slice
// field replaced on every Update) and the store may be mid-block during
// Apply, so the executor serialises Apply (write lock) against Validate and
// RootDigest (read lock) with an RWMutex. As a consequence Validate and
// RootDigest only ever observe state at block boundaries.
type Executor struct {
	mu     sync.RWMutex
	store  KVStore
	ledger *ledger.LogLedger
	mode   VersionMode
	fault  FaultPolicy

	// ledgerTime is the wall time spent inside ledger.Append and
	// ledger.AppendBlk by Apply since the last TakeLedgerTime. Guarded by
	// mu (written under the exclusive lock in Apply).
	ledgerTime time.Duration
}

// NewExecutor builds an executor. A nil fp means NoFault{}. The executor
// uses only fp.OnApply (every applied Set); fp.OnVote is the Kafka
// backend's vote hook (cmd/veritas wires both from --fault).
func NewExecutor(s KVStore, l *ledger.LogLedger, mode VersionMode, fp FaultPolicy) *Executor {
	if fp == nil {
		fp = NoFault{}
	}
	return &Executor{
		store:  s,
		ledger: l,
		mode:   mode,
		fault:  fp,
	}
}

// Validate is ValidateErr for callers that only need a vote: an error is
// logged and counts as "not valid" (false). Callers that must not turn a
// store failure into an Abort vote (the original server called log.Fatalf)
// should use ValidateErr.
func (e *Executor) Validate(ctx context.Context, blk *pbv.Block) bool {
	ok, err := e.ValidateErr(ctx, blk)
	if err != nil {
		log.Printf("executor: validate: %v", err)
		return false
	}
	return ok
}

// ValidateErr reports whether every Set in blk is fresh against the CURRENT
// store state, i.e. no stored version is >= the Set's client version (the
// original veritas rule from veritas/server.go). It is read-only. Store Get
// and Decode errors are returned (with ok=false).
//
// ValidateErr does not simulate the block: it does not account for writes
// made by earlier SharedLogs of the same block. Apply (VersionClient) does,
// so a block may validate true and still have SharedLogs aborted by Apply.
//
// In VersionOrdered mode Apply never aborts, so ValidateErr returns true
// without reading the store.
func (e *Executor) ValidateErr(ctx context.Context, blk *pbv.Block) (bool, error) {
	if e.mode == VersionOrdered {
		return true, nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, sl := range blk.GetTxs() {
		stale, err := e.sharedLogStale(ctx, sl)
		if err != nil {
			return false, err
		}
		if stale {
			return false, nil
		}
	}
	return true, nil
}

// Apply applies blk, delivered by consensus at the given height, and appends
// raw (the block's wire bytes) to the ledger block log.
//
// SharedLogs are processed strictly in block order; each Set is written to
// the store and the ledger immediately, so a later SharedLog in the same
// block that writes the same key overwrites the earlier one (last wins).
//
// VersionClient: a SharedLog is stale if ANY of its Sets is stale against
// the store state as already updated by the earlier SharedLogs of this block
// (equivalent to applying the SharedLogs one at a time). A stale SharedLog
// is skipped entirely (none of its Sets is applied) and its Seq is appended
// to aborted; otherwise all its Sets are applied and its Seq is appended to
// committed. Staleness of all Sets inside one SharedLog is decided before
// any of them is applied.
//
// VersionOrdered: every Set of the SharedLog at position index gets version
// height<<20 | index; every SharedLog is committed. A block with more than
// 2^20 SharedLogs, or a height outside [1, 2^43), is rejected with an error
// before anything is applied.
//
// Each applied Set writes veritas.Encode(v, version) to the store and
// ledger.Append(key, v+"-"+fmt.Sprintf("%v", version)), where
// v = FaultPolicy.OnApply(key, value). At the end of the block
// ledger.AppendBlk(raw) is called. The wall time of every ledger.Append and
// ledger.AppendBlk call (including failed ones) is added to the total read
// by TakeLedgerTime.
//
// Errors from the store or ledger are returned, never swallowed. Apply is NOT
// atomic: on a mid-block error the store/ledger keep whatever was written
// before the failure, and the returned committed/aborted describe the
// SharedLogs fully processed so far. The replica should treat such an error
// as fatal.
func (e *Executor) Apply(ctx context.Context, height int64, raw []byte, blk *pbv.Block) (committed, aborted []int64, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	txs := blk.GetTxs()
	switch e.mode {
	case VersionClient:
		for _, sl := range txs {
			stale, err := e.sharedLogStale(ctx, sl)
			if err != nil {
				return committed, aborted, err
			}
			if stale {
				aborted = append(aborted, sl.GetSeq())
				continue
			}
			for _, s := range sl.GetSets() {
				if err := e.applySet(ctx, s.GetKey(), s.GetValue(), s.GetVersion()); err != nil {
					return committed, aborted, err
				}
			}
			committed = append(committed, sl.GetSeq())
		}
	case VersionOrdered:
		if height < 1 || height >= maxOrderedHeight {
			return nil, nil, fmt.Errorf("executor: height %d out of range [1, %d) in ordered mode", height, maxOrderedHeight)
		}
		if len(txs) > maxOrderedTxs {
			return nil, nil, fmt.Errorf("executor: block at height %d has %d shared logs, max %d in ordered mode", height, len(txs), maxOrderedTxs)
		}
		for i, sl := range txs {
			version := orderedVersion(height, i)
			for _, s := range sl.GetSets() {
				if err := e.applySet(ctx, s.GetKey(), s.GetValue(), version); err != nil {
					return committed, aborted, err
				}
			}
			committed = append(committed, sl.GetSeq())
		}
	default:
		return nil, nil, fmt.Errorf("executor: unknown version mode %d", e.mode)
	}

	t0 := time.Now()
	err = e.ledger.AppendBlk(raw)
	e.ledgerTime += time.Since(t0)
	if err != nil {
		return committed, aborted, fmt.Errorf("executor: append block at height %d: %w", height, err)
	}
	return committed, aborted, nil
}

// RootDigest returns a copy of the ledger's current Merkle root. Safe to call
// concurrently with Apply.
//
// Note for Task 7: a Prove(key) (root, proof, err) method must take the
// EXCLUSIVE lock (e.mu.Lock), not RLock: the SMT's treeHasher shares one
// sha256 hash.Hash, so concurrent proofs would race on it. Only reading the
// root is safe under RLock. Prove must also read root and proof under one
// lock hold; never pair RootDigest() with a separate ledger.ProveKey call.
func (e *Executor) RootDigest() []byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	root := e.ledger.GetRootDigest()
	out := make([]byte, len(root))
	copy(out, root)
	return out
}

// TakeLedgerTime returns the wall time Apply has spent inside ledger.Append
// (SMT update + badger write, once per applied Set) and ledger.AppendBlk
// (once per block) since the previous call, and resets the total. The
// deliver loop calls it right after each Apply to split the block's apply
// time into ledger and non-ledger parts. It takes the exclusive lock, so it
// never observes a half-applied block's partial total.
func (e *Executor) TakeLedgerTime() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	d := e.ledgerTime
	e.ledgerTime = 0
	return d
}

// sharedLogStale reports whether any Set of sl is stale against the current
// store: a stored entry exists whose version is >= the Set's version.
func (e *Executor) sharedLogStale(ctx context.Context, sl *pbv.SharedLog) (bool, error) {
	for _, s := range sl.GetSets() {
		cur, ok, err := e.store.Get(ctx, s.GetKey())
		if err != nil {
			return false, fmt.Errorf("executor: get %q: %w", s.GetKey(), err)
		}
		if !ok {
			continue
		}
		v, err := veritas.Decode(cur)
		if err != nil {
			return false, fmt.Errorf("executor: decode %q: %w", s.GetKey(), err)
		}
		if v.Version >= s.GetVersion() {
			return true, nil
		}
	}
	return false, nil
}

// applySet writes one Set to the store and the ledger, in the same format as
// the original veritas server.
func (e *Executor) applySet(ctx context.Context, key, value string, version int64) error {
	val := e.fault.OnApply(key, value)
	entry, err := veritas.Encode(val, version)
	if err != nil {
		return fmt.Errorf("executor: encode %q: %w", key, err)
	}
	if err := e.store.Set(ctx, key, entry); err != nil {
		return fmt.Errorf("executor: set %q: %w", key, err)
	}
	t0 := time.Now()
	err = e.ledger.Append([]byte(key), []byte(val+"-"+fmt.Sprintf("%v", version)))
	e.ledgerTime += time.Since(t0)
	if err != nil {
		return fmt.Errorf("executor: ledger append %q: %w", key, err)
	}
	return nil
}
