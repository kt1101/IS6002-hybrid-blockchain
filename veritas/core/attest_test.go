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
	if _, err := getValue(nodes[3].srv, "k0"); status.Code(err) != codes.DataLoss {
		t.Fatalf("Get on the diverged node: %v, want DataLoss", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	if _, err := nodes[3].srv.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: "k0"}); status.Code(err) != codes.DataLoss {
		t.Fatalf("GetVerified on the diverged node: %v, want DataLoss", err)
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
