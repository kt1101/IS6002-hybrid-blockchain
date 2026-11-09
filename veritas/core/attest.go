package core

import (
	"bytes"
	"crypto/ed25519"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/sigs"
)

// DefaultAttestIdle is how long a node's attestations wait for one of its
// data blocks before it submits a block carrying only attestations.
const DefaultAttestIdle = 100 * time.Millisecond

// attestor is the server side of root attestations (phase 2, B2): after
// applying a block with txs at a height h with h % every == 0 the node signs
// (h, root_h); the signature rides on its next submitted block, or on an
// attestation-only block (no txs) after DefaultAttestIdle. Every node feeds
// the attestations of every applied block, in block order, to its
// Certifier; when a height is certified with a root other than the node's
// own root at that height, the node is diverged: it logs
// "core: diverged at height <h>" and Get/GetVerified fail with DataLoss.
//
// The apply loop owns roots and appliedAt; pending is shared with the
// batcher goroutine (submitBlock, idleBlock) under mu.
type attestor struct {
	every  int64
	nodeID int32
	key    ed25519.PrivateKey
	idle   time.Duration
	cert   *Certifier
	acct   *Accountant
	keys   func(nodeID int32) (ed25519.PublicKey, bool)

	roots        map[int64][]byte    // own root at attested heights (apply loop only)
	appliedAt    map[int64]time.Time // when this node applied them (apply loop only)
	lastAttested []byte              // own root at the last attested height (apply loop only)
	conflicts    map[int64]bool      // heights whose certified root was logged (apply loop only)

	// unattested: the current root is not attested yet; lastApply: when the
	// apply loop last applied a block (unix ns). Read by idleBlock.
	unattested atomic.Bool
	lastApply  atomic.Int64

	mu           sync.Mutex
	pending      []*pbv.RootAttestation
	pendingEv    []*pbv.Evidence // read evidence to carry on the next block (phase 3)
	pendingSince time.Time       // oldest of pending and pendingEv

	// rootMu guards byRoot and readers, shared between the apply loop and
	// the GetVerified(certified) handlers.
	rootMu  sync.Mutex
	byRoot  map[string]int64 // own root -> latest attested height with it
	readers []*certReader    // certified reads waiting for the next attested height

	diverged atomic.Int64 // first diverged height, 0 = none
	sent     atomic.Int64
	certs    atomic.Int64
	lag      atomic.Int64 // sum over certified heights of certify lag, ns
}

func newAttestor(cfg ServerConfig) *attestor {
	idle := cfg.AttestIdle
	if idle <= 0 {
		idle = DefaultAttestIdle
	}
	acct := NewAccountant(int32(cfg.NodeID))
	return &attestor{
		acct:      acct,
		keys:      cfg.NodeKeys,
		every:     int64(cfg.AttestEvery),
		nodeID:    int32(cfg.NodeID),
		key:       cfg.NodeKey,
		idle:      idle,
		cert:      NewCertifier(cfg.N, cfg.AttestQuorum, cfg.NodeKeys, DefaultAttestKeep, acct.Skip),
		roots:     make(map[int64][]byte),
		appliedAt: make(map[int64]time.Time),
		byRoot:    make(map[string]int64),
		conflicts: make(map[int64]bool),
	}
}

// certReader is a certified read waiting for the next attested height:
// the apply loop proves its key right after that height (prove) and sends
// the result.
type certReader struct {
	key []byte
	out chan certProof // capacity 1
}

type certProof struct {
	resp *pbv.GetVerifiedResponse
	err  error
}

// attestedHeight returns the latest attested height whose own root is root.
func (a *attestor) attestedHeight(root []byte) (int64, bool) {
	a.rootMu.Lock()
	defer a.rootMu.Unlock()
	h, ok := a.byRoot[string(root)]
	return h, ok
}

// waitNext registers a certified read for the next attested height.
func (a *attestor) waitNext(key []byte) *certReader {
	r := &certReader{key: key, out: make(chan certProof, 1)}
	a.rootMu.Lock()
	a.readers = append(a.readers, r)
	a.rootMu.Unlock()
	return r
}

// forget drops r if it is still waiting.
func (a *attestor) forget(r *certReader) {
	a.rootMu.Lock()
	defer a.rootMu.Unlock()
	for i, x := range a.readers {
		if x == r {
			a.readers = append(a.readers[:i], a.readers[i+1:]...)
			return
		}
	}
}

// afterApply runs on the apply loop after the block at height h was
// applied with state root root: it certifies the block's attestations and,
// at an attested height, signs this node's own.
func (a *attestor) afterApply(h int64, root []byte, blk *pbv.Block, nodeID int, prove func(key []byte) certProof) {
	certified := false
	for _, at := range blk.GetAttestations() {
		res := a.cert.Add(at)
		if res.Certified {
			certified = true
			a.onCertified(at.GetHeight(), res.Root, nodeID)
		}
		if len(res.Divergent) > 0 {
			a.onDivergent(at.GetHeight(), h, res)
		}
	}
	// Phase 3: read evidence, judged against the certified roots in block
	// order (held evidence first, once this block certified a height).
	certRoot := func(ht int64) ([]byte, bool) {
		r, _, ok := a.cert.Certified(ht)
		return r, ok
	}
	if certified && a.acct.Held() > 0 {
		a.acct.RecheckHeld(certRoot, a.keys, a.cert.MinHeight(), h)
	}
	for _, ev := range blk.GetEvidence() {
		a.acct.CheckRead(ev, certRoot, a.keys, a.cert.MinHeight(), h)
	}
	a.lastApply.Store(time.Now().UnixNano())
	// Attested: a block with txs at a height divisible by every, or a block
	// without txs (attestation-only or idle tick) once the root changed
	// since the last attested height. Unchanged roots are never attested
	// again, so attestation-only blocks do not beget each other; the rule
	// only depends on the block sequence and the root, so honest nodes
	// attest the same heights.
	changed := !bytes.Equal(root, a.lastAttested)
	attest := changed && (len(blk.GetTxs()) == 0 || h%a.every == 0)
	if !attest {
		if changed {
			a.unattested.Store(true)
		}
		return
	}
	a.lastAttested = root
	a.unattested.Store(false)
	a.roots[h] = root
	a.appliedAt[h] = time.Now()
	a.rootMu.Lock()
	a.byRoot[string(root)] = h
	if old := h - a.every*int64(DefaultAttestKeep); old > 0 {
		if r, ok := a.roots[old]; ok && a.byRoot[string(r)] == old {
			delete(a.byRoot, string(r))
		}
		delete(a.roots, old)
		delete(a.appliedAt, old)
	}
	readers := a.readers
	a.readers = nil
	a.rootMu.Unlock()
	// The state is still exactly that of height h (the apply loop is the
	// only writer), so these proofs are for the attested root.
	for _, r := range readers {
		r.out <- prove(r.key)
	}
	att := &pbv.RootAttestation{Height: h, Root: root, NodeId: a.nodeID, Sig: sigs.SignAttest(a.key, h, root)}
	a.mu.Lock()
	if len(a.pending) == 0 {
		a.pendingSince = time.Now()
	}
	a.pending = append(a.pending, att)
	a.mu.Unlock()
}

func (a *attestor) onCertified(h int64, root []byte, nodeID int) {
	if t, ok := a.appliedAt[h]; ok {
		a.lag.Add(int64(time.Since(t)))
		a.certs.Add(1)
	}
	own, ok := a.roots[h]
	if !ok || bytes.Equal(own, root) {
		return
	}
	if a.diverged.CompareAndSwap(0, h) {
		log.Printf("core: diverged at height %d (node %d: own root %x, certified %x)", h, nodeID, own, root)
	}
}

// onDivergent (apply loop): height h has attestations for another root than
// its certified one, found while applying the block at blockHeight. Every
// divergent node is evicted at blockHeight (the same block on every node).
// The first time for a height it also logs the certified root ("core:
// certified height <h> root <hex>", read by run_one.sh into <tag>.certs),
// so an analysis can tell which root won.
func (a *attestor) onDivergent(h, blockHeight int64, res AddResult) {
	for _, n := range res.Divergent {
		a.acct.Evict(n, blockHeight, EvictDivergentRoot)
	}
	if a.conflicts[h] {
		return
	}
	a.conflicts[h] = true
	log.Printf("core: certified height %d root %x", h, res.Root)
	if len(a.conflicts) > DefaultAttestKeep {
		for old := range a.conflicts {
			if old <= h-int64(DefaultAttestKeep) {
				delete(a.conflicts, old)
			}
		}
	}
}

// take returns and clears the pending attestations (batcher goroutine).
func (a *attestor) take() []*pbv.RootAttestation {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.pending
	a.pending = nil
	if len(p) > 0 {
		a.sent.Add(int64(len(p)))
	}
	return p
}

// takeEvidence returns and clears the pending read evidence (batcher
// goroutine).
func (a *attestor) takeEvidence() []*pbv.Evidence {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.pendingEv
	a.pendingEv = nil
	return p
}

// addEvidence queues read evidence for this node's next block (phase 3,
// ReportEvidence).
func (a *attestor) addEvidence(ev *pbv.Evidence) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.pending) == 0 && len(a.pendingEv) == 0 {
		a.pendingSince = time.Now()
	}
	a.pendingEv = append(a.pendingEv, ev)
}

// putBack re-queues attestations and evidence whose block failed to submit.
func (a *attestor) putBack(atts []*pbv.RootAttestation, evs []*pbv.Evidence) {
	if len(atts) == 0 && len(evs) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sent.Add(-int64(len(atts)))
	if len(a.pending) == 0 && len(a.pendingEv) == 0 {
		a.pendingSince = time.Now()
	}
	a.pending = append(atts, a.pending...)
	a.pendingEv = append(evs, a.pendingEv...)
}

// idleBlock returns a block without txs (batcher goroutine) when this
// node's attestations or read evidence have waited at least idle for a
// data block (they go on it), or when no block was applied for idle while
// the current root is not attested (an idle tick: applying it makes every
// node attest that root, so certified reads on an idle cluster are served);
// else nil.
func (a *attestor) idleBlock() *pbv.Block {
	a.mu.Lock()
	due := (len(a.pending) > 0 || len(a.pendingEv) > 0) && time.Since(a.pendingSince) >= a.idle
	a.mu.Unlock()
	tick := a.unattested.Load() && time.Since(time.Unix(0, a.lastApply.Load())) >= a.idle
	if !due && !tick {
		return nil
	}
	atts := a.take()
	if atts == nil {
		atts = []*pbv.RootAttestation{} // non-nil: submitBlock sends it as a tick
	}
	return &pbv.Block{Type: pbv.MessageType_Approve, Attestations: atts, Evidence: a.takeEvidence()}
}
