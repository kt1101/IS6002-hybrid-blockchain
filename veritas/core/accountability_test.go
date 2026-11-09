package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/benchmark"
	"hybrid/veritas/sigs"
)

func TestAccountantEvictOnce(t *testing.T) {
	a := NewAccountant(1)
	if !a.Evict(4, 10, EvictDivergentRoot) {
		t.Fatal("first eviction of node 4 returned false")
	}
	if a.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1", a.Epoch())
	}
	if a.Evict(4, 12, EvictBadRead) {
		t.Fatal("second eviction of node 4 returned true")
	}
	if a.Epoch() != 1 {
		t.Fatalf("epoch after a repeated eviction = %d, want 1", a.Epoch())
	}
	if h, ok := a.Height(4); !ok || h != 10 {
		t.Fatalf("Height(4) = %d, %v; want the first eviction height 10", h, ok)
	}
	// Every attestation of node 4 processed after its eviction is skipped,
	// also the late ones of heights before the eviction.
	for _, h := range []int64{5, 10, 11} {
		if !a.Skip(4, h) {
			t.Fatalf("Skip(4, %d) = false after the eviction at 10", h)
		}
	}
	if a.Skip(3, 100) || a.SelfEvicted() {
		t.Fatal("node 3 / self wrongly evicted")
	}
	a.Evict(2, 20, EvictBadRead)
	if got := a.Nodes(); len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Fatalf("Nodes() = %v, want [2 4]", got)
	}
	if !NewAccountant(4).Evict(4, 1, EvictDivergentRoot) {
		t.Fatal("self eviction")
	}
}

func evictedSet(s *Server) []int32 { return s.att.acct.Nodes() }

func sameNodes(a, b []int32) bool { return fmt.Sprint(a) == fmt.Sprint(b) }

func TestCorruptNodeEvictedEverywhereSameHeight(t *testing.T) {
	nodes := attestCluster(t, 4, 1, map[int]FaultPolicy{4: suffixFault{}}, nil)
	setN(t, nodes[0].srv, 20)
	eventually(t, srvTestTimeout, "every node evicted node 4", func() bool {
		for _, n := range nodes {
			if !sameNodes(evictedSet(n.srv), []int32{4}) {
				return false
			}
		}
		return true
	})
	h0, _ := nodes[0].srv.att.acct.Height(4)
	for i, n := range nodes {
		if h, _ := n.srv.att.acct.Height(4); h != h0 {
			t.Fatalf("node %d evicted node 4 at height %d, node 1 at %d", i+1, h, h0)
		}
		if b := n.srv.Breakdown(); b.Evictions != 1 {
			t.Fatalf("node %d: Breakdown.Evictions = %d, want 1", i+1, b.Evictions)
		}
	}
	if _, err := getValue(nodes[3].srv, "k0"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Get on the evicted node: %v, want PermissionDenied", err)
	}
	if _, err := certifiedRead(nodes[3].srv, "k0"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetVerified on the evicted node: %v, want PermissionDenied", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	ev, err := nodes[0].srv.Evicted(ctx, &pbv.EvictedRequest{})
	if err != nil || ev.GetEpoch() != 1 || !sameNodes(ev.GetNodes(), []int32{4}) {
		t.Fatalf("Evicted on node 1 = %v, %v; want epoch 1, nodes [4]", ev, err)
	}
	r, err := certifiedRead(nodes[0].srv, "k1")
	if err != nil || r.GetEvictedEpoch() != 1 {
		t.Fatalf("certified read from node 1: %v, epoch %d; want epoch 1", err, r.GetEvictedEpoch())
	}
}

func TestHonestClusterEvictsNobody(t *testing.T) {
	nodes := attestCluster(t, 4, 1, nil, nil)
	setN(t, nodes[0].srv, 10)
	eventually(t, srvTestTimeout, "heights certified on every node", func() bool {
		for _, n := range nodes {
			if n.srv.Breakdown().CertifiedHeights < 5 {
				return false
			}
		}
		return true
	})
	for i, n := range nodes {
		if b := n.srv.Breakdown(); b.Evictions != 0 {
			t.Fatalf("honest node %d evicted %d node(s): %v", i+1, b.Evictions, evictedSet(n.srv))
		}
	}
}

func TestEvictedEmptyWithoutAttestations(t *testing.T) {
	nodes := attestCluster(t, 4, 0, nil, nil)
	ev, err := nodes[0].srv.Evicted(context.Background(), &pbv.EvictedRequest{})
	if err != nil || ev.GetEpoch() != 0 || len(ev.GetNodes()) != 0 {
		t.Fatalf("Evicted without attestations = %v, %v", ev, err)
	}
}

func TestF5F4QuorumF1OneSideEvicted(t *testing.T) {
	nodes := attestCluster(t, 4, 1, map[int]FaultPolicy{3: suffixFault{}, 4: suffixFault{}}, func(i int, c *ServerConfig) {
		c.AttestQuorum = 2
	})
	setN(t, nodes[0].srv, 20)
	eventually(t, srvTestTimeout, "every node evicted one side", func() bool {
		for _, n := range nodes {
			if len(evictedSet(n.srv)) != 2 {
				return false
			}
		}
		return true
	})
	want := evictedSet(nodes[0].srv)
	if !sameNodes(want, []int32{3, 4}) && !sameNodes(want, []int32{1, 2}) {
		t.Fatalf("evicted %v, want [3 4] or [1 2]", want)
	}
	for i, n := range nodes {
		if got := evictedSet(n.srv); !sameNodes(got, want) {
			t.Fatalf("node %d evicted %v, node 1 %v: eviction is not deterministic", i+1, got, want)
		}
	}
}

func TestF5F4Quorum2f1NoCertNoEviction(t *testing.T) {
	nodes := attestCluster(t, 4, 1, map[int]FaultPolicy{3: suffixFault{}, 4: suffixFault{}}, func(i int, c *ServerConfig) {
		c.AttestQuorum = 3
		c.CertifiedWait = 200 * time.Millisecond
	})
	setN(t, nodes[0].srv, 20)
	time.Sleep(300 * time.Millisecond) // let the attestations of the 20 blocks be applied
	for i, n := range nodes {
		if b := n.srv.Breakdown(); b.Evictions != 0 {
			t.Fatalf("node %d evicted %v with 2 vs 2 at quorum 3", i+1, evictedSet(n.srv))
		}
	}
	if _, err := certifiedRead(nodes[0].srv, "k0"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("certified read at quorum 3 with 2 vs 2: %v, want DeadlineExceeded", err)
	}
}

// ---- phase 3, Task 6: signed reads and bad_read evidence ----

func signedRead(t *testing.T, s *Server, key string) *pbv.GetVerifiedResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	r, err := s.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: key, Certified: true, Signed: true})
	if err != nil {
		t.Fatalf("signed certified read of %q: %v", key, err)
	}
	return r
}

func evidenceOf(key string, r *pbv.GetVerifiedResponse) *pbv.Evidence {
	return &pbv.Evidence{Culprit: r.GetNodeId(), Height: r.GetHeight(), Read: sigs.SignedReadOf(key, r)}
}

func report(t *testing.T, s *Server, ev *pbv.Evidence) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	_, err := s.ReportEvidence(ctx, ev)
	return err
}

func TestSignedReadIsSignedByTheNode(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 3)
	r := signedRead(t, nodes[1].srv, "k1")
	if r.GetNodeId() != 2 || !sigs.VerifyRead(sigs.Public(sigs.KeyFor(testSeed, sigs.NodeID(2))), sigs.SignedReadOf("k1", r)) {
		t.Fatalf("node 2's signed read does not verify: node_id %d", r.GetNodeId())
	}
	lie := signedRead(t, nodes[3].srv, "k1")
	if lie.GetValue() != "v1"+benchmark.CorruptMarker {
		t.Fatalf("readlie node returned %q", lie.GetValue())
	}
	if !sigs.VerifyRead(sigs.Public(sigs.KeyFor(testSeed, sigs.NodeID(4))), sigs.SignedReadOf("k1", lie)) {
		t.Fatal("the lying node's signature must cover the value it returned")
	}
}

func TestBadReadEvidenceEvictsEverywhere(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 5)
	ev := evidenceOf("k1", signedRead(t, nodes[3].srv, "k1"))
	if err := report(t, nodes[0].srv, ev); err != nil {
		t.Fatalf("ReportEvidence of a valid bad read: %v", err)
	}
	eventually(t, srvTestTimeout, "every node evicted node 4 for bad_read", func() bool {
		for _, n := range nodes {
			if !sameNodes(evictedSet(n.srv), []int32{4}) || n.srv.att.acct.Reason(4) != EvictBadRead {
				return false
			}
		}
		return true
	})
	h0, _ := nodes[0].srv.att.acct.Height(4)
	for i, n := range nodes {
		if h, _ := n.srv.att.acct.Height(4); h != h0 {
			t.Fatalf("node %d evicted node 4 at height %d, node 1 at %d", i+1, h, h0)
		}
	}
	if _, err := getValue(nodes[3].srv, "k0"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Get on the evicted readlie node: %v, want PermissionDenied", err)
	}
}

func TestReportEvidenceRejectsForgery(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 3)
	ev := evidenceOf("k1", signedRead(t, nodes[3].srv, "k1"))
	ev.Read.Sig = append([]byte(nil), ev.Read.Sig...)
	ev.Read.Sig[0] ^= 1
	if err := report(t, nodes[0].srv, ev); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ReportEvidence with a bad signature: %v, want InvalidArgument", err)
	}
	ev2 := evidenceOf("k1", signedRead(t, nodes[3].srv, "k1"))
	ev2.Culprit = 2 // node 4's signature, blamed on node 2
	if err := report(t, nodes[0].srv, ev2); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ReportEvidence blaming another node: %v, want InvalidArgument", err)
	}
}

func TestForgedEvidenceEvictsNobody(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 3)
	ev := evidenceOf("k1", signedRead(t, nodes[3].srv, "k1"))
	ev.Read.Sig = make([]byte, 64) // a node puts forged evidence on its block
	nodes[0].srv.att.addEvidence(ev)
	eventually(t, srvTestTimeout, "every node rejected the forged evidence", func() bool {
		for _, n := range nodes {
			if n.srv.Breakdown().EvidenceRejected != 1 {
				return false
			}
		}
		return true
	})
	for i, n := range nodes {
		if b := n.srv.Breakdown(); b.Evictions != 0 {
			t.Fatalf("node %d evicted %v on forged evidence", i+1, evictedSet(n.srv))
		}
	}
}

func TestEvidenceOfCorrectReadEvictsNobody(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 3)
	ev := evidenceOf("k1", signedRead(t, nodes[1].srv, "k1")) // honest node 2, correct value
	if err := report(t, nodes[0].srv, ev); err != nil {
		t.Fatalf("ReportEvidence of a correctly signed read: %v (the signature is valid)", err)
	}
	eventually(t, srvTestTimeout, "every node judged the correct read", func() bool {
		for _, n := range nodes {
			if n.srv.Breakdown().EvidenceRejected != 1 {
				return false
			}
		}
		return true
	})
	for i, n := range nodes {
		if b := n.srv.Breakdown(); b.Evictions != 0 {
			t.Fatalf("node %d evicted %v on evidence of a correct read", i+1, evictedSet(n.srv))
		}
	}
}

func TestDuplicateEvidenceEvictsOnce(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 3)
	ev := evidenceOf("k1", signedRead(t, nodes[3].srv, "k1"))
	for i := 0; i < 50; i++ {
		if err := report(t, nodes[i%3].srv, ev); err != nil {
			t.Fatalf("ReportEvidence %d: %v", i, err)
		}
	}
	eventually(t, srvTestTimeout, "every node evicted node 4", func() bool {
		for _, n := range nodes {
			if !sameNodes(evictedSet(n.srv), []int32{4}) {
				return false
			}
		}
		return true
	})
	time.Sleep(200 * time.Millisecond) // the remaining copies reach the apply loops
	for i, n := range nodes {
		if b := n.srv.Breakdown(); b.Evictions != 1 || n.srv.att.acct.Epoch() != 1 {
			t.Fatalf("node %d: %d evictions, epoch %d; want 1 and 1", i+1, b.Evictions, n.srv.att.acct.Epoch())
		}
	}
}

// badReadEvidence is real evidence of node 4 lying about k1 (a readlie
// cluster), with the certified root of its height.
func badReadEvidence(t *testing.T) (*pbv.Evidence, []byte) {
	t.Helper()
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 3)
	r := signedRead(t, nodes[3].srv, "k1")
	return evidenceOf("k1", r), r.GetRootDigest()
}

func TestEvidenceHeldUntilCertified(t *testing.T) {
	ev, root := badReadEvidence(t)
	keys := sigs.NodeKeys(testSeed, 4)
	certified := false
	cert := func(h int64) ([]byte, bool) {
		if !certified || h != ev.GetHeight() {
			return nil, false
		}
		return root, true
	}
	a := NewAccountant(1)
	a.CheckRead(ev, cert, keys, 1, 100)
	if a.Epoch() != 0 || a.Held() != 1 {
		t.Fatalf("evidence of an uncertified height: epoch %d, held %d; want 0 and 1", a.Epoch(), a.Held())
	}
	a.RecheckHeld(cert, keys, 1, 101)
	if a.Held() != 1 || a.Epoch() != 0 {
		t.Fatalf("still uncertified: held %d, epoch %d", a.Held(), a.Epoch())
	}
	certified = true
	a.RecheckHeld(cert, keys, 1, 102)
	if h, ok := a.Height(4); !ok || h != 102 || a.Reason(4) != EvictBadRead || a.Held() != 0 {
		t.Fatalf("after certification: evicted %v at %d (%s), held %d; want node 4 at 102 (bad_read), 0",
			ok, h, a.Reason(4), a.Held())
	}
}

func TestEvidenceOutsideWindowDropped(t *testing.T) {
	ev, root := badReadEvidence(t)
	cert := func(h int64) ([]byte, bool) { return root, true }
	a := NewAccountant(1)
	a.CheckRead(ev, cert, sigs.NodeKeys(testSeed, 4), ev.GetHeight()+1, 100)
	if a.EvidenceDropped() != 1 || a.Epoch() != 0 {
		t.Fatalf("evidence below the window: dropped %d, epoch %d; want 1 and 0", a.EvidenceDropped(), a.Epoch())
	}
}

func TestReadLieEvasiveHonestOnSignedReads(t *testing.T) {
	nodes := attestCluster(t, 4, 1, map[int]FaultPolicy{4: mustParseFault(t, FaultReadLieEvasive, time.Now(), 0)}, func(i int, c *ServerConfig) {
		c.CertifiedWait = 2 * time.Second
	})
	setN(t, nodes[0].srv, 3)
	if r := signedRead(t, nodes[3].srv, "k1"); r.GetValue() != "v1" || !verifyResp(r, "k1") {
		t.Fatalf("evasive node on a signed read: %q (verifies %v); want the honest v1", r.GetValue(), verifyResp(r, "k1"))
	}
	r, err := certifiedRead(nodes[3].srv, "k1")
	if err != nil || r.GetValue() != "v1"+benchmark.CorruptMarker {
		t.Fatalf("evasive node on an unsigned read: %v %v; want the corrupt value", r.GetValue(), err)
	}
}
