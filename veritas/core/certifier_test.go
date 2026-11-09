package core

import (
	"bytes"
	"context"
	"testing"
	"time"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/sigs"
)

func att(seed string, node int, h int64, root string) *pbv.RootAttestation {
	return &pbv.RootAttestation{Height: h, Root: []byte(root), NodeId: int32(node),
		Sig: sigs.SignAttest(sigs.KeyFor(seed, sigs.NodeID(node)), h, []byte(root))}
}

func newTestCertifier(n, keep int) *Certifier {
	return NewCertifier(n, 0, sigs.NodeKeys(testSeed, n), keep, nil)
}

func TestCertifierQuorum(t *testing.T) {
	c := newTestCertifier(4, 16)
	if q := c.Quorum(); q != 3 {
		t.Fatalf("quorum of N=4 = %d, want 3", q)
	}
	if q := NewCertifier(7, 0, sigs.NodeKeys(testSeed, 7), 16, nil).Quorum(); q != 5 {
		t.Fatalf("quorum of N=7 = %d, want 5", q)
	}
	for i := 1; i <= 2; i++ {
		if c.Add(att(testSeed, i, 5, "A")).Certified {
			t.Fatalf("certified after %d attestations", i)
		}
	}
	if _, _, ok := c.Certified(5); ok {
		t.Fatal("Certified(5) before quorum")
	}
	res := c.Add(att(testSeed, 3, 5, "A"))
	ok, root := res.Certified, res.Root
	if !ok || string(root) != "A" {
		t.Fatalf("third attestation: %v %q, want certified A", ok, root)
	}
	r, atts, ok := c.Certified(5)
	if !ok || string(r) != "A" || len(atts) != 3 {
		t.Fatalf("Certified(5) = %q, %d atts, %v", r, len(atts), ok)
	}
	if c.Add(att(testSeed, 4, 5, "A")).Certified {
		t.Fatal("a late fourth attestation reported a new certification")
	}
}

func TestCertifierIgnoresDuplicateSigner(t *testing.T) {
	c := newTestCertifier(4, 16)
	for i := 0; i < 3; i++ {
		if c.Add(att(testSeed, 2, 5, "A")).Certified {
			t.Fatal("one signer three times certified a height")
		}
	}
	if c.Duplicates() != 2 {
		t.Fatalf("Duplicates = %d, want 2", c.Duplicates())
	}
}

func TestCertifierRejectsUnknownKey(t *testing.T) {
	c := newTestCertifier(4, 16)
	if c.Add(att(testSeed, 9, 5, "A")).Certified {
		t.Fatal("node 9 certified")
	}
	bad := att(testSeed, 1, 5, "A")
	bad.Sig[0] ^= 1
	c.Add(bad)
	forged := att("other-seed", 2, 5, "A") // right id, wrong key
	c.Add(forged)
	if c.Invalid() != 3 {
		t.Fatalf("Invalid = %d, want 3", c.Invalid())
	}
	c.Add(att(testSeed, 1, 5, "A"))
	c.Add(att(testSeed, 2, 5, "A"))
	if _, _, ok := c.Certified(5); ok {
		t.Fatal("invalid attestations counted toward the quorum")
	}
}

func TestCertifierConflictingRoots(t *testing.T) {
	c := newTestCertifier(4, 16)
	c.Add(att(testSeed, 1, 5, "A"))
	c.Add(att(testSeed, 2, 5, "A"))
	c.Add(att(testSeed, 3, 5, "B"))
	if c.Add(att(testSeed, 4, 5, "B")).Certified {
		t.Fatal("2 + 2 votes certified")
	}
	// Every node attested once: no quorum. A third vote for A from a node
	// that voted B does not count (one vote per node and height).
	if c.Add(att(testSeed, 3, 5, "A")).Certified {
		t.Fatal("a node's second vote counted")
	}
	c2 := newTestCertifier(4, 16)
	c2.Add(att(testSeed, 1, 5, "A"))
	c2.Add(att(testSeed, 2, 5, "A"))
	c2.Add(att(testSeed, 3, 5, "B"))
	if res := c2.Add(att(testSeed, 4, 5, "A")); !res.Certified || !bytes.Equal(res.Root, []byte("A")) {
		t.Fatalf("third vote for A: %v %q", res.Certified, res.Root)
	}
}

func TestCertifierEvictsOld(t *testing.T) {
	c := newTestCertifier(4, 4)
	for h := int64(1); h <= 10; h++ {
		for i := 1; i <= 3; i++ {
			c.Add(att(testSeed, i, h, "R"))
		}
	}
	if _, _, ok := c.Certified(1); ok {
		t.Fatal("height 1 still kept with keep=4")
	}
	if _, _, ok := c.Certified(10); !ok {
		t.Fatal("height 10 not certified")
	}
	if c.Add(att(testSeed, 4, 2, "R")).Certified {
		t.Fatal("an evicted height certified again")
	}
}

func TestCertifierWait(t *testing.T) {
	c := newTestCertifier(4, 16)
	done := make(chan error, 1)
	go func() {
		_, atts, err := c.Wait(context.Background(), 3)
		if err == nil && len(atts) != 3 {
			err = context.Canceled
		}
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	for i := 1; i <= 3; i++ {
		c.Add(att(testSeed, i, 3, "R"))
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after certification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := c.Wait(ctx, 4); err == nil {
		t.Fatal("Wait of an uncertified height returned nil")
	}
}

// ---- phase 3: divergent attestations, skipped (evicted) nodes ----

func addRes(t *testing.T, c *Certifier, node int, h int64, root string) AddResult {
	t.Helper()
	return c.Add(att(testSeed, node, h, root))
}

func eqNodes(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDivergentAfterCertification(t *testing.T) {
	c := newTestCertifier(4, 16)
	for i := 1; i <= 2; i++ {
		if r := addRes(t, c, i, 7, "R"); r.Certified || len(r.Divergent) != 0 {
			t.Fatalf("node %d: %+v", i, r)
		}
	}
	if r := addRes(t, c, 3, 7, "R"); !r.Certified || string(r.Root) != "R" || len(r.Divergent) != 0 {
		t.Fatalf("node 3 should certify R with no divergent node: %+v", r)
	}
	r := addRes(t, c, 4, 7, "R'")
	if r.Certified || !eqNodes(r.Divergent, []int32{4}) {
		t.Fatalf("node 4 signing R' after certification: %+v, want Divergent [4]", r)
	}
	if string(r.Root) != "R" {
		t.Fatalf("Root after certification = %q, want the certified R", r.Root)
	}
}

func TestDivergentBeforeCertification(t *testing.T) {
	c := newTestCertifier(4, 16)
	addRes(t, c, 4, 7, "R'")
	addRes(t, c, 1, 7, "R")
	addRes(t, c, 2, 7, "R")
	r := addRes(t, c, 3, 7, "R")
	if !r.Certified || string(r.Root) != "R" || !eqNodes(r.Divergent, []int32{4}) {
		t.Fatalf("certifying R after node 4 signed R': %+v, want Certified R, Divergent [4]", r)
	}
}

func TestQuorumF1WrongRootFirst(t *testing.T) {
	c := NewCertifier(4, 2, sigs.NodeKeys(testSeed, 4), 16, nil)
	if r := addRes(t, c, 3, 7, "R'"); r.Certified {
		t.Fatalf("one vote certified: %+v", r)
	}
	if r := addRes(t, c, 4, 7, "R'"); !r.Certified || string(r.Root) != "R'" || len(r.Divergent) != 0 {
		t.Fatalf("nodes 3+4 at quorum 2 should certify R': %+v", r)
	}
	if r := addRes(t, c, 1, 7, "R"); r.Certified || !eqNodes(r.Divergent, []int32{1}) {
		t.Fatalf("node 1 after R' was certified: %+v, want Divergent [1]", r)
	}
	if r := addRes(t, c, 2, 7, "R"); r.Certified || !eqNodes(r.Divergent, []int32{2}) {
		t.Fatalf("node 2: %+v, want Divergent [2] and no second certification", r)
	}
	if root, _, _ := c.Certified(7); string(root) != "R'" {
		t.Fatalf("Certified(7) = %q, want the first certified root R'", root)
	}
}

func TestEvictedNodeNotCounted(t *testing.T) {
	skip := func(node int32, h int64) bool { return node == 4 && h >= 5 }
	c := NewCertifier(4, 0, sigs.NodeKeys(testSeed, 4), 16, skip)
	for _, n := range []int{1, 2, 4} {
		if r := addRes(t, c, n, 5, "R"); r.Certified {
			t.Fatalf("h=5: certified after node %d although node 4 is skipped", n)
		}
	}
	if r := addRes(t, c, 4, 5, "R'"); len(r.Divergent) != 0 {
		t.Fatalf("a skipped node must not be reported divergent: %+v", r)
	}
	var last AddResult
	for _, n := range []int{1, 2, 4} {
		last = addRes(t, c, n, 4, "R")
	}
	if !last.Certified {
		t.Fatal("h=4: node 4 is not skipped below height 5, its vote must count")
	}
}

func TestNoDivergentOnHonestCluster(t *testing.T) {
	c := newTestCertifier(4, 16)
	for h := int64(1); h <= 5; h++ {
		for n := 1; n <= 4; n++ {
			if r := addRes(t, c, n, h, "R"); len(r.Divergent) != 0 {
				t.Fatalf("h=%d node %d: %+v", h, n, r)
			}
		}
	}
}
