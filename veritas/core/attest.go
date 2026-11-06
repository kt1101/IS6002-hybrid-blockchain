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

	roots     map[int64][]byte    // own root at attested heights (apply loop only)
	appliedAt map[int64]time.Time // when this node applied them (apply loop only)

	mu           sync.Mutex
	pending      []*pbv.RootAttestation
	pendingSince time.Time

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
	}
}

// afterApply runs on the apply loop after the block at height h was
// applied with state root root: it certifies the block's attestations and,
// at an attested height, signs this node's own.
func (a *attestor) afterApply(h int64, root []byte, blk *pbv.Block, nodeID int) {
	for _, at := range blk.GetAttestations() {
		if ok, croot := a.cert.Add(at); ok {
			a.onCertified(at.GetHeight(), croot, nodeID)
		}
	}
	// Only blocks with txs are attested: an attestation-only block leaves
	// the root as it was (attesting it would make another such block, and
	// so on forever on an idle cluster).
	if h%a.every != 0 || len(blk.GetTxs()) == 0 {
		return
	}
	a.roots[h] = root
	a.appliedAt[h] = time.Now()
	if old := h - a.every*int64(DefaultAttestKeep); old > 0 {
		delete(a.roots, old)
		delete(a.appliedAt, old)
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

// idleBlock returns an attestation-only block when attestations have waited
// at least idle for a data block, else nil (batcher goroutine).
func (a *attestor) idleBlock() *pbv.Block {
	a.mu.Lock()
	due := len(a.pending) > 0 && time.Since(a.pendingSince) >= a.idle
	a.mu.Unlock()
	if !due {
		return nil
	}
	return &pbv.Block{Type: pbv.MessageType_Approve}
}
