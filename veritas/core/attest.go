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

	roots        map[int64][]byte    // own root at attested heights (apply loop only)
	appliedAt    map[int64]time.Time // when this node applied them (apply loop only)
	lastAttested []byte              // own root at the last attested height (apply loop only)

	// unattested: the current root is not attested yet; lastApply: when the
	// apply loop last applied a block (unix ns). Read by idleBlock.
	unattested atomic.Bool
	lastApply  atomic.Int64

	mu           sync.Mutex
	pending      []*pbv.RootAttestation
	pendingSince time.Time

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
	return &attestor{
		every:     int64(cfg.AttestEvery),
		nodeID:    int32(cfg.NodeID),
		key:       cfg.NodeKey,
		idle:      idle,
		cert:      NewCertifier(cfg.N, cfg.AttestQuorum, cfg.NodeKeys, DefaultAttestKeep),
		roots:     make(map[int64][]byte),
		appliedAt: make(map[int64]time.Time),
		byRoot:    make(map[string]int64),
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
	for _, at := range blk.GetAttestations() {
		if ok, croot := a.cert.Add(at); ok {
			a.onCertified(at.GetHeight(), croot, nodeID)
		}
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

// putBack re-queues attestations whose block failed to submit.
func (a *attestor) putBack(atts []*pbv.RootAttestation) {
	if len(atts) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sent.Add(-int64(len(atts)))
	if len(a.pending) == 0 {
		a.pendingSince = time.Now()
	}
	a.pending = append(atts, a.pending...)
}

// idleBlock returns a block without txs (batcher goroutine) when this
// node's attestations have waited at least idle for a data block (they go
// on it), or when no block was applied for idle while the current root is
// not attested (an idle tick: applying it makes every node attest that
// root, so certified reads on an idle cluster are served); else nil.
func (a *attestor) idleBlock() *pbv.Block {
	a.mu.Lock()
	due := len(a.pending) > 0 && time.Since(a.pendingSince) >= a.idle
	a.mu.Unlock()
	tick := a.unattested.Load() && time.Since(time.Unix(0, a.lastApply.Load())) >= a.idle
	if !due && !tick {
		return nil
	}
	atts := a.take()
	if atts == nil {
		atts = []*pbv.RootAttestation{} // non-nil: submitBlock sends it as a tick
	}
	return &pbv.Block{Type: pbv.MessageType_Approve, Attestations: atts}
}
