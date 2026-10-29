package core

import (
	"hybrid/veritas/ledger/merkletree"
)

// Prove returns the ledger's current Merkle root together with a proof for
// key, both taken under ONE hold of the executor lock, so the proof is always
// for the returned root (never for a root from a later block).
//
// It takes the EXCLUSIVE lock (e.mu.Lock), not RLock: the sparse Merkle
// tree's treeHasher shares a single sha256 hash.Hash, so two concurrent
// proofs would race on it (Apply already holds the exclusive lock; RootDigest
// only reads the root slice and never hashes, so RLock is enough there).
// Prove therefore serialises with Apply and with other Prove calls, and only
// ever observes state at a block boundary.
//
// The returned root and proof are deep copies and belong to the caller.
// The error is ledger.ProveKey's (e.g. "no merkle tree" when the ledger was
// opened without one).
func (e *Executor) Prove(key []byte) (root []byte, proof merkletree.SparseMerkleProof, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	p, err := e.ledger.ProveKey(key)
	if err != nil {
		return nil, merkletree.SparseMerkleProof{}, err
	}
	root = copyBytes(e.ledger.GetRootDigest())

	proof.NonMembershipLeafData = copyBytes(p.NonMembershipLeafData)
	if p.SideNodes != nil {
		proof.SideNodes = make([][]byte, len(p.SideNodes))
		for i, n := range p.SideNodes {
			proof.SideNodes[i] = copyBytes(n)
		}
	}
	return root, proof, nil
}

// copyBytes returns a copy of b, preserving nil.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
