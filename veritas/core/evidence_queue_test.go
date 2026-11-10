package core

import (
	"fmt"
	"testing"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/sigs"
)

// signedEvidence is evidence of node culprit's signed read of key at height
// h, claiming root (any bytes: only the signature has to verify).
func signedEvidence(culprit int32, h int64, key string, root []byte) *pbv.Evidence {
	r := &pbv.SignedRead{Key: key, Value: "v", Version: 1, Height: h, Root: root, NodeId: culprit}
	r.Sig = sigs.SignRead(sigs.KeyFor(testSeed, sigs.NodeID(int(culprit))), r)
	return &pbv.Evidence{Culprit: culprit, Height: h, Read: r}
}

func TestAddEvidenceDropsCopies(t *testing.T) {
	a := &attestor{evSeen: make(map[evidenceKey]struct{})}
	ev := signedEvidence(4, 10, "k1", []byte("r"))
	if got := a.addEvidence(ev); got != evidenceQueued {
		t.Fatalf("first report: %v, want queued", got)
	}
	copyEv := signedEvidence(4, 10, "k1", []byte("r")) // same read, new message
	if got := a.addEvidence(copyEv); got != evidenceDuplicate {
		t.Fatalf("resent report: %v, want duplicate", got)
	}
	if n := len(a.takeEvidence()); n != 1 {
		t.Fatalf("%d evidence on the block, want 1", n)
	}
	if got := a.addEvidence(copyEv); got != evidenceDuplicate {
		t.Fatalf("report resent after its block: %v, want duplicate", got)
	}
	other := signedEvidence(4, 10, "k1", []byte("another root")) // another read of the same key and height
	if got := a.addEvidence(other); got != evidenceQueued {
		t.Fatalf("a different read of the same key and height: %v, want queued", got)
	}
}

func TestAddEvidenceBounded(t *testing.T) {
	a := &attestor{evSeen: make(map[evidenceKey]struct{})}
	for i := 0; i < maxPendingEvidence; i++ {
		if got := a.addEvidence(signedEvidence(4, 10, fmt.Sprint("k", i), nil)); got != evidenceQueued {
			t.Fatalf("report %d: %v, want queued", i, got)
		}
	}
	extra := signedEvidence(4, 10, "extra", nil)
	if got := a.addEvidence(extra); got != evidenceFull {
		t.Fatalf("report beyond maxPendingEvidence: %v, want full", got)
	}
	a.takeEvidence()
	if got := a.addEvidence(extra); got != evidenceQueued {
		t.Fatalf("the refused report once the queue emptied: %v, want queued", got)
	}
	// The remembered keys are bounded too: the oldest are forgotten.
	for i := 0; len(a.evSeenFIFO) < evidenceSeenKeep; i++ {
		a.takeEvidence()
		a.addEvidence(signedEvidence(4, 11, fmt.Sprint("m", i), nil))
	}
	a.takeEvidence()
	a.addEvidence(signedEvidence(4, 12, "last", nil))
	if len(a.evSeen) != evidenceSeenKeep || len(a.evSeenFIFO) != evidenceSeenKeep {
		t.Fatalf("remembered %d / %d keys, want %d", len(a.evSeen), len(a.evSeenFIFO), evidenceSeenKeep)
	}
	if got := a.addEvidence(signedEvidence(4, 10, "k0", nil)); got != evidenceQueued {
		t.Fatalf("the oldest key after %d newer ones: %v, want queued (forgotten)", evidenceSeenKeep, got)
	}
}

func TestReportEvidenceDeduplicates(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 3)
	ev := evidenceOf("k1", signedRead(t, nodes[3].srv, "k1"))
	for i := 0; i < 3; i++ {
		if err := report(t, nodes[0].srv, ev); err != nil {
			t.Fatalf("ReportEvidence %d: %v", i, err)
		}
	}
	nodes[0].srv.att.mu.Lock()
	n := len(nodes[0].srv.att.evSeenFIFO)
	nodes[0].srv.att.mu.Unlock()
	if n != 1 {
		t.Fatalf("node 1 queued the evidence %d times, want 1", n)
	}
}

func TestHeldEvidenceDeduplicatedAndBounded(t *testing.T) {
	keys := sigs.NodeKeys(testSeed, 4)
	never := func(int64) ([]byte, bool) { return nil, false }
	a := NewAccountant(1)
	ev := signedEvidence(4, 50, "k", []byte("r"))
	a.CheckRead(ev, never, keys, 1, 100)
	a.CheckRead(signedEvidence(4, 50, "k", []byte("r")), never, keys, 1, 101)
	if a.Held() != 1 || a.EvidenceRejected() != 1 {
		t.Fatalf("a held copy: held %d, rejected %d; want 1 and 1", a.Held(), a.EvidenceRejected())
	}
	// Fill up: heights 51.. hold the rest; the lowest height (50) goes first.
	for i := 1; i <= maxHeldEvidence; i++ {
		a.CheckRead(signedEvidence(4, 50+int64(i), "k", []byte("r")), never, keys, 1, 102)
	}
	if a.Held() != maxHeldEvidence || a.EvidenceDropped() != 1 {
		t.Fatalf("held %d, dropped %d; want %d and 1", a.Held(), a.EvidenceDropped(), maxHeldEvidence)
	}
	a.mu.Lock()
	_, low := a.held[50]
	a.mu.Unlock()
	if low {
		t.Fatal("the evidence of the lowest height was kept, a higher one dropped")
	}
	// The dropped copy may be held again (it is no longer held).
	a.CheckRead(ev, never, keys, 1, 103)
	if a.EvidenceDropped() != 2 {
		t.Fatalf("re-holding height 50 at the bound: dropped %d, want 2 (itself, the lowest)", a.EvidenceDropped())
	}
}

func TestRecheckHeldByHeight(t *testing.T) {
	keys := sigs.NodeKeys(testSeed, 4)
	good := []byte("certified root")
	certified := map[int64]bool{}
	cert := func(h int64) ([]byte, bool) {
		if certified[h] {
			return good, true
		}
		return nil, false
	}
	a := NewAccountant(1)
	a.CheckRead(signedEvidence(3, 20, "k", good), cert, keys, 1, 100)          // root matches, bad proof: bad read
	a.CheckRead(signedEvidence(4, 21, "k", []byte("wrong")), cert, keys, 1, 100) // wrong root: bad read
	a.CheckRead(signedEvidence(2, 5, "k", good), cert, keys, 1, 100)            // leaves the window
	if a.Held() != 3 {
		t.Fatalf("held %d, want 3", a.Held())
	}
	certified[21] = true
	a.RecheckHeld(cert, 10, 101)
	if h, ok := a.Height(4); !ok || h != 101 {
		t.Fatalf("node 4: evicted %v at %d, want at 101", ok, h)
	}
	if _, ok := a.Height(3); ok {
		t.Fatal("node 3 evicted before its height was certified")
	}
	if a.Held() != 1 || a.EvidenceDropped() != 1 {
		t.Fatalf("held %d, dropped %d; want 1 (height 20) and 1 (height 5, below the window)", a.Held(), a.EvidenceDropped())
	}
	certified[20] = true
	a.RecheckHeld(cert, 10, 102)
	if h, ok := a.Height(3); !ok || h != 102 || a.Held() != 0 {
		t.Fatalf("node 3: evicted %v at %d, held %d; want at 102, 0", ok, h, a.Held())
	}
	a.mu.Lock()
	nk := len(a.heldKeys)
	a.mu.Unlock()
	if nk != 0 {
		t.Fatalf("%d held keys left, want 0", nk)
	}
}
