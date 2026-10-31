package ledger

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"hybrid/veritas/ledger/merkletree"
)

func TestStateDigestComputation(t *testing.T) {
	dir, err := ioutil.TempDir("", "prefix1")
	if err != nil {
		log.Fatal(err)
	}
	ledger1, err := NewLedger(dir, true)
	if err != nil {
		log.Fatal(err)
	}
	ledger1.Append([]byte("A"), []byte("A"))
	ledger1.Append([]byte("B"), []byte("B"))
	ledger1.Append([]byte("C"), []byte("C"))
	ledger1Digest := ledger1.GetRootDigest()
	ledger1.Close()

	// Create another instance to test whether previous appended states are loaded properly from the persistence and reach the identical root digest.
	ledger2, err := NewLedger(dir, true)
	if err != nil {
		log.Fatal(err)
	}
	ledger2Digest := ledger2.GetRootDigest()
	assert.Equal(t, ledger1Digest, ledger2Digest)

	ledger2.AppendBlk([]byte("block111"))
	ledger2.AppendBlk([]byte("block222"))
	ledger2.AppendBlk([]byte("block333"))
	ledger2AfterAppendingBlkDigest := ledger2.GetRootDigest()
	// Verify the block appending does not interfere the state digest computation.
	assert.Equal(t, ledger2AfterAppendingBlkDigest, ledger2Digest)
	ledger2.Close()

	// Verify the non-interference after reloading db.
	ledger3, err := NewLedger(dir, true)
	if err != nil {
		log.Fatal(err)
	}
	ledger3Digest := ledger3.GetRootDigest()
	assert.Equal(t, ledger3Digest, ledger2AfterAppendingBlkDigest)
	ledger3.Close()
	defer os.RemoveAll(dir)
}

// The ledger's (pruned) tree yields the same digests and verifiable proofs as
// an unpruned tree under repeated overwrites, also after a reload.
func TestLedgerPrunedMatchesUnpruned(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLedger(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	ref := merkletree.NewSparseMerkleTree(merkletree.NewSimpleMap(), sha256.New())
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("user%03d", i%37))
		v := []byte(fmt.Sprintf("val-%d", i%11)) // identical values across keys
		if err := l.Append(k, v); err != nil {
			t.Fatal(err)
		}
		if _, err := ref.Update(k, v); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(l.GetRootDigest(), ref.Root()) {
			t.Fatalf("append %d: digest %x, unpruned %x", i, l.GetRootDigest(), ref.Root())
		}
	}
	for i := 1963; i < 2000; i++ {
		k := []byte(fmt.Sprintf("user%03d", i%37))
		v := []byte(fmt.Sprintf("val-%d", i%11))
		p, err := l.ProveKey(k)
		if err != nil || !merkletree.VerifyProof(p, l.GetRootDigest(), k, v, sha256.New()) {
			t.Fatalf("proof for %s does not verify (err %v)", k, err)
		}
	}
	digest := l.GetRootDigest()
	l.Close()
	l2, err := NewLedger(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if !bytes.Equal(l2.GetRootDigest(), digest) {
		t.Fatalf("digest after reload %x, want %x", l2.GetRootDigest(), digest)
	}
}
