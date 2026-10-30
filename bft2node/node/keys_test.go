package node

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/relab/hotstuff"
)

func TestDeriveKeyIsDeterministicAndSigns(t *testing.T) {
	a, err := DeriveKey("seed", 3)
	if err != nil {
		t.Fatal(err)
	}
	b, err := DeriveKey("seed", 3)
	if err != nil {
		t.Fatal(err)
	}
	if a.D.Cmp(b.D) != 0 || a.X.Cmp(b.X) != 0 || a.Y.Cmp(b.Y) != 0 {
		t.Fatal("DeriveKey is not deterministic")
	}
	if !a.Curve.IsOnCurve(a.X, a.Y) {
		t.Fatal("public key is not on the curve")
	}
	for _, other := range []struct {
		seed string
		id   hotstuff.ID
	}{{"seed", 4}, {"other", 3}} {
		c, err := DeriveKey(other.seed, other.id)
		if err != nil {
			t.Fatal(err)
		}
		if c.D.Cmp(a.D) == 0 {
			t.Fatalf("key of %+v equals the key of (seed, 3)", other)
		}
	}
	h := sha256.Sum256([]byte("block"))
	r, s, err := ecdsa.Sign(rand.Reader, a, h[:])
	if err != nil {
		t.Fatal(err)
	}
	if !ecdsa.Verify(&b.PublicKey, h[:], r, s) {
		t.Fatal("signature by the derived key does not verify")
	}
}
