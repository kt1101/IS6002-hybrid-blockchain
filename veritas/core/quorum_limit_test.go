package core

import (
	"testing"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/sigs"
)

// TestQuorumFixedAfterEviction documents a limitation (spec 3.3): the
// Certifier's quorum stays 2f+1 of N after an eviction, so with N=4 and
// node 4 evicted every height needs all of nodes 1-3; one more slow or
// silent node stops certification (certified reads then time out).
func TestQuorumFixedAfterEviction(t *testing.T) {
	keys := sigs.NodeKeys(testSeed, 4)
	evicted := func(node int32, _ int64) bool { return node == 4 }
	c := NewCertifier(4, 0, keys, 0, evicted)
	if c.Quorum() != 3 {
		t.Fatalf("quorum %d, want 3", c.Quorum())
	}
	root := []byte("root")
	att := func(node int32) *pbv.RootAttestation {
		return &pbv.RootAttestation{Height: 7, Root: root, NodeId: node,
			Sig: sigs.SignAttest(sigs.KeyFor(testSeed, sigs.NodeID(int(node))), 7, root)}
	}
	for _, n := range []int32{1, 2, 4} {
		if res := c.Add(att(n)); res.Certified {
			t.Fatalf("certified after node %d with node 4 evicted and node 3 silent", n)
		}
	}
	if _, _, ok := c.Certified(7); ok {
		t.Fatal("height 7 certified by two live nodes")
	}
	if res := c.Add(att(3)); !res.Certified {
		t.Fatal("not certified once nodes 1-3 all attested")
	}
}
