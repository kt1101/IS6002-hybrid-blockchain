package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"hybrid/consensus/memnet"
	pbv "hybrid/proto/veritas"
	"hybrid/veritas/sigs"
)

// attestCluster starts n servers on one memnet hub with --attest-every
// every; faults[i] (may be nil) is node i+1's FaultPolicy.
func attestCluster(t *testing.T, n, every int, faults map[int]FaultPolicy, tweak func(i int, c *ServerConfig)) []*clusterNode {
	t.Helper()
	hub := memnet.NewHub(n)
	nodes := make([]*clusterNode, n)
	for i := 0; i < n; i++ {
		st := NewMapStore()
		l := newTestLedger(t)
		ex := NewExecutor(st, l, VersionOrdered, faults[i+1])
		c := testCfg(VersionOrdered, CommitSync)
		c.NodeID = i + 1
		c.Signature = fmt.Sprintf("node%d", i+1)
		if every > 0 {
			c.AttestEvery = every
			c.N = n
			c.NodeKey = sigs.KeyFor(testSeed, sigs.NodeID(i+1))
			c.NodeKeys = sigs.NodeKeys(testSeed, n)
			c.AttestIdle = 20 * time.Millisecond
		}
		if tweak != nil {
			tweak(i, &c)
		}
		srv := NewServer(c, hub.Node(i), ex, l)
		t.Cleanup(func() { closeWithin(t, srv) })
		nodes[i] = &clusterNode{srv: srv, ex: ex, store: st}
	}
	return nodes
}

func setN(t *testing.T, s *Server, k int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	for i := 0; i < k; i++ {
		if _, err := s.Set(ctx, setReq(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 0)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
}

func TestHonestClusterCertifies(t *testing.T) {
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
		if _, ok := n.srv.Diverged(); ok {
			t.Fatalf("honest node %d diverged", i+1)
		}
		if b := n.srv.Breakdown(); b.AttestSent == 0 || b.AttestInvalid != 0 {
			t.Fatalf("node %d: sent %d invalid %d", i+1, b.AttestSent, b.AttestInvalid)
		}
	}
}

func TestCorruptNodeDiverges(t *testing.T) {
	nodes := attestCluster(t, 4, 1, map[int]FaultPolicy{4: suffixFault{}}, nil)
	setN(t, nodes[0].srv, 20)
	eventually(t, srvTestTimeout, "node 4 diverged", func() bool {
		_, ok := nodes[3].srv.Diverged()
		return ok
	})
	// Phase 3: the diverged node also evicted itself (its own attestation
	// conflicts with the certified root), and PermissionDenied comes first.
	if c := status.Code(getValueErr(nodes[3].srv, "k0")); c != codes.DataLoss && c != codes.PermissionDenied {
		t.Fatalf("Get on the diverged node: %v, want DataLoss or PermissionDenied", c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	if _, err := nodes[3].srv.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: "k0"}); status.Code(err) != codes.DataLoss && status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetVerified on the diverged node: %v, want DataLoss or PermissionDenied", err)
	}
	if _, err := nodes[3].srv.Verify(ctx, &pbv.VerifyRequest{Key: "k0"}); err != nil {
		t.Fatalf("Verify on the diverged node (the auditor still asks): %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, ok := nodes[i].srv.Diverged(); ok {
			t.Fatalf("honest node %d diverged", i+1)
		}
	}
}

func TestAttestOffByDefault(t *testing.T) {
	var counts [2]int64
	var roots [2][]byte
	for run, every := range []int{0, 1} {
		nodes := attestCluster(t, 4, every, nil, nil)
		setN(t, nodes[0].srv, 5)
		time.Sleep(150 * time.Millisecond) // idle attestation blocks, if any
		counts[run] = waitSameBlocks(t, nodes)
		roots[run] = nodes[0].ex.RootDigest()
		if every == 0 && nodes[0].srv.Breakdown().AttestSent != 0 {
			t.Fatal("attestations sent with --attest-every=0")
		}
	}
	if counts[0] != 5 {
		t.Fatalf("attest off: %d blocks for 5 Sets, want 5 (no extra blocks)", counts[0])
	}
	if counts[1] <= 5 {
		t.Fatalf("attest on: %d blocks, want attestation-only blocks too", counts[1])
	}
	if counts[1] > 5+4*5 {
		t.Fatalf("attest on: %d blocks for 5 Sets on 4 nodes (attestation-only blocks attested again?)", counts[1])
	}
	if string(roots[0]) != string(roots[1]) {
		t.Fatal("attestation-only blocks changed the state root")
	}
}

func certifiedRead(s *Server, key string) (*pbv.GetVerifiedResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	return s.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: key, Certified: true})
}

// checkCertified: the proof verifies and Q=3 distinct valid attestations
// sign the response's (height, root).
func checkCertified(t *testing.T, r *pbv.GetVerifiedResponse, key string) {
	t.Helper()
	if !verifyResp(r, key) {
		t.Fatal("certified response: proof does not verify")
	}
	keys := sigs.NodeKeys(testSeed, 4)
	seen := map[int32]bool{}
	for _, a := range r.GetAttestations() {
		pub, ok := keys(a.GetNodeId())
		if ok && a.GetHeight() == r.GetHeight() && string(a.GetRoot()) == string(r.GetRootDigest()) &&
			sigs.VerifyAttest(pub, a.GetHeight(), a.GetRoot(), a.GetSig()) {
			seen[a.GetNodeId()] = true
		}
	}
	if len(seen) < 3 {
		t.Fatalf("certified response with %d valid attestations, want >= 3", len(seen))
	}
}

// TestCertifiedReadHonest: every node serves a certified read, also with
// every=3 on an idle cluster (the last data block is not at a multiple of
// 3: the idle tick gets its root attested).
func TestCertifiedReadHonest(t *testing.T) {
	for _, every := range []int{1, 3} {
		nodes := attestCluster(t, 4, every, nil, nil)
		setN(t, nodes[0].srv, 7)
		for i, n := range nodes {
			r, err := certifiedRead(n.srv, "k0")
			if err != nil {
				t.Fatalf("every=%d node %d: %v", every, i+1, err)
			}
			if r.GetValue() != "v0" || r.GetHeight() < 1 {
				t.Fatalf("every=%d node %d: value %q height %d", every, i+1, r.GetValue(), r.GetHeight())
			}
			checkCertified(t, r, "k0")
		}
	}
}

func TestCertifiedReadFromCorruptNodeRejected(t *testing.T) {
	nodes := attestCluster(t, 4, 1, map[int]FaultPolicy{4: suffixFault{}}, func(i int, c *ServerConfig) {
		c.CertifiedWait = 300 * time.Millisecond
	})
	setN(t, nodes[0].srv, 5)
	for k := 0; k < 5; k++ {
		r, err := certifiedRead(nodes[3].srv, fmt.Sprintf("k%d", k))
		if err == nil {
			t.Fatalf("certified read %d from the corrupt node returned %q", k, r.GetValue())
		}
		if c := status.Code(err); c != codes.DataLoss && c != codes.DeadlineExceeded && c != codes.PermissionDenied {
			t.Fatalf("certified read from the corrupt node: %v, want DataLoss, DeadlineExceeded or PermissionDenied (evicted, phase 3)", err)
		}
	}
	r, err := certifiedRead(nodes[0].srv, "k1")
	if err != nil || r.GetValue() != "v1" {
		t.Fatalf("certified read from an honest node: %v %v", r, err)
	}
}

func TestCertifiedReadWaitsForCertification(t *testing.T) {
	// Nodes 3 and 4 sign with keys nobody knows: only 2 valid attestations
	// per height, never a quorum of 3.
	nodes := attestCluster(t, 4, 1, nil, func(i int, c *ServerConfig) {
		c.CertifiedWait = 500 * time.Millisecond
		if i >= 2 {
			c.NodeKey = sigs.KeyFor("wrong-seed", sigs.NodeID(i+1))
		}
	})
	setN(t, nodes[0].srv, 3)
	t0 := time.Now()
	_, err := certifiedRead(nodes[0].srv, "k0")
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("certified read without a quorum: %v, want DeadlineExceeded", err)
	}
	if d := time.Since(t0); d < 400*time.Millisecond || d > 3*time.Second {
		t.Fatalf("waited %v, want about the 500ms CertifiedWait", d)
	}
	if nodes[0].srv.Breakdown().AttestInvalid == 0 {
		t.Fatal("no invalid attestations counted")
	}
	if _, err := nodes[0].srv.GetVerified(context.Background(), &pbv.GetVerifiedRequest{Key: "k0"}); err != nil {
		t.Fatalf("a verified (not certified) read still works: %v", err)
	}
}

func TestCertifiedReadNeedsAttestations(t *testing.T) {
	nodes := attestCluster(t, 1, 0, nil, nil)
	setN(t, nodes[0].srv, 1)
	if _, err := certifiedRead(nodes[0].srv, "k0"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("certified read with attestations off: %v, want FailedPrecondition", err)
	}
}

// getValueErr is the error of a Get (nil on success).
func getValueErr(s *Server, key string) error {
	_, err := getValue(s, key)
	return err
}
