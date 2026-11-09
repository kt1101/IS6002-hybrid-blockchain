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
	return NewCertifier(n, 0, sigs.NodeKeys(testSeed, n), keep)
}

func TestCertifierQuorum(t *testing.T) {
	c := newTestCertifier(4, 16)
	if q := c.Quorum(); q != 3 {
		t.Fatalf("quorum of N=4 = %d, want 3", q)
	}
	if q := NewCertifier(7, 0, sigs.NodeKeys(testSeed, 7), 16).Quorum(); q != 5 {
		t.Fatalf("quorum of N=7 = %d, want 5", q)
	}
	for i := 1; i <= 2; i++ {
		if ok, _ := c.Add(att(testSeed, i, 5, "A")); ok {
			t.Fatalf("certified after %d attestations", i)
		}
	}
	if _, _, ok := c.Certified(5); ok {
		t.Fatal("Certified(5) before quorum")
	}
	ok, root := c.Add(att(testSeed, 3, 5, "A"))
	if !ok || string(root) != "A" {
		t.Fatalf("third attestation: %v %q, want certified A", ok, root)
	}
	r, atts, ok := c.Certified(5)
	if !ok || string(r) != "A" || len(atts) != 3 {
		t.Fatalf("Certified(5) = %q, %d atts, %v", r, len(atts), ok)
	}
	if ok, _ := c.Add(att(testSeed, 4, 5, "A")); ok {
		t.Fatal("a late fourth attestation reported a new certification")
	}
}

func TestCertifierIgnoresDuplicateSigner(t *testing.T) {
	c := newTestCertifier(4, 16)
	for i := 0; i < 3; i++ {
		if ok, _ := c.Add(att(testSeed, 2, 5, "A")); ok {
			t.Fatal("one signer three times certified a height")
		}
	}
	if c.Duplicates() != 2 {
		t.Fatalf("Duplicates = %d, want 2", c.Duplicates())
	}
}

func TestCertifierRejectsUnknownKey(t *testing.T) {
	c := newTestCertifier(4, 16)
	if ok, _ := c.Add(att(testSeed, 9, 5, "A")); ok {
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
	if ok, _ := c.Add(att(testSeed, 4, 5, "B")); ok {
		t.Fatal("2 + 2 votes certified")
	}
	// Every node attested once: no quorum. A third vote for A from a node
	// that voted B does not count (one vote per node and height).
	if ok, _ := c.Add(att(testSeed, 3, 5, "A")); ok {
		t.Fatal("a node's second vote counted")
	}
	c2 := newTestCertifier(4, 16)
	c2.Add(att(testSeed, 1, 5, "A"))
	c2.Add(att(testSeed, 2, 5, "A"))
	c2.Add(att(testSeed, 3, 5, "B"))
	if ok, root := c2.Add(att(testSeed, 4, 5, "A")); !ok || !bytes.Equal(root, []byte("A")) {
		t.Fatalf("third vote for A: %v %q", ok, root)
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
	if ok, _ := c.Add(att(testSeed, 4, 2, "R")); ok {
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
