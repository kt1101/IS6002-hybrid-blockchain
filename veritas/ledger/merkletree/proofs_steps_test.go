package merkletree

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
)

func stepsFixture(t *testing.T) (*SparseMerkleTree, [][]byte, [][]byte) {
	t.Helper()
	smt := NewSparseMerkleTree(NewSimpleMap(), sha256.New())
	var keys, values [][]byte
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("key-%03d", i))
		v := []byte(fmt.Sprintf("value-%03d", i))
		if _, err := smt.Update(k, v); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return smt, keys, values
}

func TestVerifyProofStepsMatchesVerifyProof(t *testing.T) {
	smt, keys, values := stepsFixture(t)
	root := smt.Root()
	for i := 0; i < 5; i++ {
		proof, err := smt.Prove(keys[i])
		if err != nil {
			t.Fatal(err)
		}
		ok, steps := VerifyProofSteps(proof, root, keys[i], values[i], sha256.New())
		if want := VerifyProof(proof, root, keys[i], values[i], sha256.New()); ok != want {
			t.Fatalf("key %d: ok=%v, VerifyProof=%v", i, ok, want)
		}
		if !ok {
			t.Fatalf("key %d: proof did not verify", i)
		}
		if len(steps) != len(proof.SideNodes)+1 {
			t.Fatalf("key %d: len(steps)=%d, want %d", i, len(steps), len(proof.SideNodes)+1)
		}
		if !bytes.Equal(steps[len(steps)-1], root) {
			t.Fatalf("key %d: last step is not the root", i)
		}
		th := newTreeHasher(sha256.New())
		wantLeaf, _ := th.digestLeaf(th.path(keys[i]), th.digest(values[i]))
		if !bytes.Equal(steps[0], wantLeaf) {
			t.Fatalf("key %d: steps[0] is not the leaf hash", i)
		}
	}
}

func TestVerifyProofStepsTampered(t *testing.T) {
	smt, keys, values := stepsFixture(t)
	proof, err := smt.Prove(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(proof.SideNodes) == 0 {
		t.Fatal("expected side nodes")
	}
	proof.SideNodes[len(proof.SideNodes)-1][0] ^= 0xff
	ok, steps := VerifyProofSteps(proof, smt.Root(), keys[0], values[0], sha256.New())
	if ok {
		t.Fatal("tampered proof verified")
	}
	if len(steps) > 0 && bytes.Equal(steps[len(steps)-1], smt.Root()) {
		t.Fatal("tampered proof reached the root")
	}
}

func TestVerifyProofStepsWrongValue(t *testing.T) {
	smt, keys, _ := stepsFixture(t)
	proof, err := smt.Prove(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := VerifyProofSteps(proof, smt.Root(), keys[0], []byte("x"), sha256.New()); ok {
		t.Fatal("wrong value verified")
	}
}

func TestVerifyProofStepsNonMembership(t *testing.T) {
	smt, _, _ := stepsFixture(t)
	absent := []byte("absent-key")
	proof, err := smt.Prove(absent)
	if err != nil {
		t.Fatal(err)
	}
	ok, steps := VerifyProofSteps(proof, smt.Root(), absent, defaultValue, sha256.New())
	if !ok {
		t.Fatal("non-membership proof did not verify")
	}
	if len(steps) == 0 || !bytes.Equal(steps[len(steps)-1], smt.Root()) {
		t.Fatal("last step is not the root")
	}
}
