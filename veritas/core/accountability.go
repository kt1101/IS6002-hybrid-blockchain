package core

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/ledger/merkletree"
	"hybrid/veritas/sigs"
)

// maxHeldEvidence bounds the evidence kept for heights not yet certified;
// beyond it the evidence of the lowest held height (the next to leave the
// certifier window) is dropped first (EvidenceDropped).
const maxHeldEvidence = 4096

// evidenceKey names one piece of read evidence: the culprit and the digest
// of the read it signed (sigs.ReadDigest).
type evidenceKey struct {
	culprit int32
	digest  [sha256.Size]byte
}

func evidenceKeyOf(ev *pbv.Evidence) evidenceKey {
	return evidenceKey{culprit: ev.GetCulprit(), digest: sigs.ReadDigest(ev.GetRead())}
}

// heldEvidence is evidence whose signature was checked, waiting for its
// height to be certified.
type heldEvidence struct {
	ev  *pbv.Evidence
	key evidenceKey
}

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
	held     map[int64][]heldEvidence // by height: evidence of heights not yet certified (apply loop)
	heldKeys map[evidenceKey]struct{} // what held holds (a copy of held evidence is not held twice)
	heldN    int
	rejected int64
	dropped  int64
}

// NewAccountant is the accountant of node self.
func NewAccountant(self int32) *Accountant {
	return &Accountant{self: self, evicted: make(map[int32]int64), reasons: make(map[int32]string),
		held: make(map[int64][]heldEvidence), heldKeys: make(map[evidenceKey]struct{})}
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
	if !evidenceSigned(ev, keys) {
		return verdictReject
	}
	if ev.GetHeight() < minHeight {
		return verdictDrop
	}
	root, ok := cert(ev.GetHeight())
	if !ok {
		return verdictHold
	}
	return judgeAgainst(ev.GetRead(), root)
}

// evidenceSigned reports whether ev is well formed and its read carries the
// culprit's signature.
func evidenceSigned(ev *pbv.Evidence, keys func(int32) (ed25519.PublicKey, bool)) bool {
	r := ev.GetRead()
	if r == nil || r.GetNodeId() != ev.GetCulprit() || r.GetHeight() != ev.GetHeight() {
		return false
	}
	pub, ok := keys(ev.GetCulprit())
	return ok && sigs.VerifyRead(pub, r)
}

// judgeAgainst decides a signed read against the certified root of its
// height: verdictBad or verdictReject (a correct read).
func judgeAgainst(r *pbv.SignedRead, root []byte) readVerdict {
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
		a.holdLocked(ev)
	default:
		a.countLocked(v)
	}
}

// holdLocked keeps signature-checked evidence until its height is certified;
// a copy of evidence already held is rejected (no news).
func (a *Accountant) holdLocked(ev *pbv.Evidence) {
	k := evidenceKeyOf(ev)
	if _, dup := a.heldKeys[k]; dup {
		a.rejected++
		return
	}
	a.heldKeys[k] = struct{}{}
	h := ev.GetHeight()
	a.held[h] = append(a.held[h], heldEvidence{ev: ev, key: k})
	a.heldN++
	for a.heldN > maxHeldEvidence {
		low := int64(math.MaxInt64)
		for hh := range a.held {
			if hh < low {
				low = hh
			}
		}
		evs := a.held[low]
		delete(a.heldKeys, evs[0].key)
		if len(evs) == 1 {
			delete(a.held, low)
		} else {
			a.held[low] = evs[1:]
		}
		a.heldN--
		a.dropped++
	}
}

// RecheckHeld judges the held evidence of the heights certified by now
// (apply loop only, after a block certified a height), in increasing height
// order, so every node evicts in the same order. The signatures were checked
// when the evidence was held and are not checked again; only the heights are
// looked up (cert) and each read's proof checked against its certified root.
// Heights below minHeight (out of the certifier window) are dropped; the
// others stay held.
func (a *Accountant) RecheckHeld(cert func(h int64) ([]byte, bool), minHeight, curHeight int64) {
	a.mu.Lock()
	hs := make([]int64, 0, len(a.held))
	for h := range a.held {
		hs = append(hs, h)
	}
	a.mu.Unlock()
	sort.Slice(hs, func(i, j int) bool { return hs[i] < hs[j] })
	for _, h := range hs {
		var root []byte
		inWindow := h >= minHeight
		if inWindow {
			var ok bool
			if root, ok = cert(h); !ok {
				continue
			}
		}
		a.mu.Lock()
		evs := a.held[h]
		delete(a.held, h)
		a.heldN -= len(evs)
		for _, he := range evs {
			delete(a.heldKeys, he.key)
			if !inWindow {
				a.dropped++
				continue
			}
			if _, gone := a.evicted[he.ev.GetCulprit()]; gone || judgeAgainst(he.ev.GetRead(), root) != verdictBad {
				a.rejected++
				continue
			}
			if !a.evictLocked(he.ev.GetCulprit(), curHeight, EvictBadRead) {
				a.rejected++
			}
		}
		a.mu.Unlock()
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
	return a.heldN
}
