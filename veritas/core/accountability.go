package core

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"log"
	"sort"
	"sync"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/ledger/merkletree"
	"hybrid/veritas/sigs"
)

// maxHeldEvidence bounds the evidence kept for heights not yet certified;
// beyond it the oldest is dropped (EvidenceDropped).
const maxHeldEvidence = 4096

// Eviction reasons (the <reason> of the "core: evicted node" log line).
const (
	EvictDivergentRoot = "divergent_root"
	EvictBadRead       = "bad_read"
)

// Accountant is a node's set of evicted nodes (phase 3, RQ6). The apply loop
// evicts a node when the ordered log proves it faulty (an attestation for
// another root than the certified one, a bad signed read), at the height of
// the block that holds the proof; every node applies the same blocks, so
// every honest node evicts the same nodes at the same heights. The
// attestations of an evicted node processed after its eviction are not
// counted (Skip), and a node that evicted itself refuses reads
// (SelfEvicted). Evictions are never undone.
// Safe for concurrent use.
type Accountant struct {
	self int32

	mu       sync.Mutex
	evicted  map[int32]int64  // node -> height of its eviction
	reasons  map[int32]string // node -> reason of its eviction
	epoch    int64
	held     []*pbv.Evidence // evidence of heights not yet certified (apply loop)
	rejected int64
	dropped  int64
}

// NewAccountant is the accountant of node self.
func NewAccountant(self int32) *Accountant {
	return &Accountant{self: self, evicted: make(map[int32]int64), reasons: make(map[int32]string)}
}

// Evict evicts node at height for reason. The first eviction of a node logs
// "core: evicted node <node> at height <height> (<reason>)" (run_one.sh keeps
// these lines in <tag>.evict), bumps the epoch and returns true; later ones
// return false.
func (a *Accountant) Evict(node int32, height int64, reason string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.evictLocked(node, height, reason)
}

func (a *Accountant) evictLocked(node int32, height int64, reason string) bool {
	if _, ok := a.evicted[node]; ok {
		return false
	}
	a.evicted[node] = height
	a.reasons[node] = reason
	a.epoch++
	log.Printf("core: evicted node %d at height %d (%s)", node, height, reason)
	return true
}

// Skip reports whether node is evicted (the Certifier's skip hook): none of
// its attestations processed from then on counts, whatever height they
// attest. Attestations reach the apply loop blocks after the height they
// sign, so a height-based cut would still count an evicted node's late
// attestations of earlier heights, and with quorum f+1 the other side could
// then certify too (both sides evicted). The cut is by block order, which is
// the same on every node.
func (a *Accountant) Skip(node int32, height int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.evicted[node]
	return ok
}

// Height returns the height node was evicted at.
func (a *Accountant) Height(node int32) (int64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	h, ok := a.evicted[node]
	return h, ok
}

// Epoch is the number of evictions so far.
func (a *Accountant) Epoch() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.epoch
}

// Nodes returns the evicted nodes in increasing order.
func (a *Accountant) Nodes() []int32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]int32, 0, len(a.evicted))
	for n := range a.evicted {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SelfEvicted reports whether this node evicted itself.
func (a *Accountant) SelfEvicted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.evicted[a.self]
	return ok
}

// Reason is the reason node was evicted for ("" if it is not evicted).
func (a *Accountant) Reason(node int32) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reasons[node]
}

// readVerdict is what one piece of read evidence proves.
type readVerdict int

const (
	verdictReject readVerdict = iota // forged, malformed, or a correct read
	verdictDrop                      // its height left the certifier window
	verdictHold                      // its height is not certified yet
	verdictBad                       // the culprit signed a wrong read
)

// judgeRead decides ev against the certified roots (cert) and the node keys.
// It takes no lock: cert and keys are called outside the Accountant's.
func judgeRead(ev *pbv.Evidence, cert func(h int64) ([]byte, bool),
	keys func(int32) (ed25519.PublicKey, bool), minHeight int64) readVerdict {
	r := ev.GetRead()
	if r == nil || r.GetNodeId() != ev.GetCulprit() || r.GetHeight() != ev.GetHeight() {
		return verdictReject
	}
	pub, ok := keys(ev.GetCulprit())
	if !ok || !sigs.VerifyRead(pub, r) {
		return verdictReject
	}
	if ev.GetHeight() < minHeight {
		return verdictDrop
	}
	root, ok := cert(ev.GetHeight())
	if !ok {
		return verdictHold
	}
	if !bytes.Equal(r.GetRoot(), root) {
		return verdictBad // signed a root that is not the certified one
	}
	proof := merkletree.SparseMerkleProof{SideNodes: r.GetSideNodes(), NonMembershipLeafData: r.GetNonMembershipLeafData()}
	leaf := []byte(fmt.Sprintf("%s-%d", r.GetValue(), r.GetVersion()))
	if !merkletree.VerifyProof(proof, root, []byte(r.GetKey()), leaf, sha256.New()) {
		return verdictBad // the value is not the one the certified root holds
	}
	return verdictReject // a correct read is no evidence
}

// CheckRead judges one piece of read evidence found in the block at
// curHeight (apply loop only): a bad signed read evicts its culprit at
// curHeight (EvictBadRead); evidence of a height that is not certified yet
// is held until RecheckHeld finds it certified; forged or malformed
// evidence, a correct read, or evidence on an already evicted node is
// rejected (EvidenceRejected); evidence below minHeight (out of the
// certifier window) is dropped (EvidenceDropped).
func (a *Accountant) CheckRead(ev *pbv.Evidence, cert func(h int64) ([]byte, bool),
	keys func(int32) (ed25519.PublicKey, bool), minHeight, curHeight int64) {
	a.mu.Lock()
	_, gone := a.evicted[ev.GetCulprit()]
	a.mu.Unlock()
	if gone {
		a.count(verdictReject)
		return
	}
	v := judgeRead(ev, cert, keys, minHeight)
	a.mu.Lock()
	defer a.mu.Unlock()
	switch v {
	case verdictBad:
		if !a.evictLocked(ev.GetCulprit(), curHeight, EvictBadRead) {
			a.rejected++
		}
	case verdictHold:
		a.held = append(a.held, ev)
		if len(a.held) > maxHeldEvidence {
			a.held = a.held[1:]
			a.dropped++
		}
	default:
		a.countLocked(v)
	}
}

// RecheckHeld judges the held evidence again (apply loop only), after a
// block certified a height; what is still not certified stays held.
func (a *Accountant) RecheckHeld(cert func(h int64) ([]byte, bool),
	keys func(int32) (ed25519.PublicKey, bool), minHeight, curHeight int64) {
	a.mu.Lock()
	held := a.held
	a.held = nil
	a.mu.Unlock()
	for _, ev := range held {
		a.CheckRead(ev, cert, keys, minHeight, curHeight)
	}
}

func (a *Accountant) count(v readVerdict) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.countLocked(v)
}

func (a *Accountant) countLocked(v readVerdict) {
	switch v {
	case verdictReject:
		a.rejected++
	case verdictDrop:
		a.dropped++
	}
}

// EvidenceRejected, EvidenceDropped and Held count the read evidence seen in
// the apply loop (see CheckRead).
func (a *Accountant) EvidenceRejected() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.rejected
}

func (a *Accountant) EvidenceDropped() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dropped
}

func (a *Accountant) Held() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.held)
}
