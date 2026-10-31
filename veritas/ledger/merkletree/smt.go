// Package smt implements a Sparse Merkle tree.
package merkletree

import (
	"bytes"
	"errors"
	"hash"
)

const (
	right = 1
)

var defaultValue []byte

type keyAlreadyEmptyError struct{}

func (e *keyAlreadyEmptyError) Error() string {
	return "key already empty"
}

type SparseMerkleTree struct {
	th   treeHasher
	kvs  KVStore
	root []byte

	// Pruning (NewPrunedSparseMerkleTree only; nil refs = no pruning).
	//
	// Every KVStore entry is content-addressed (key = hash of its stored
	// bytes: valueHash = H(value), leafHash = H(leafData), nodeHash =
	// H(nodeData)), so two references to the same key always mean the same
	// bytes. refs[k] counts the references the CURRENT tree holds on entry
	// k: one per tree position whose node (inner node or leaf) hashes to k,
	// plus one per live leaf whose valueHash is k. An entry is deleted from
	// kvs exactly when its count drops to 0. See Update for the diff
	// argument that keeps the counts exact.
	refs map[string]uint32
	// recording makes set() remember every key it writes (only while a
	// pruned Update/Delete runs).
	recording bool
	written   [][]byte
	// pruneAnomalies counts decrements of an untracked key and failed
	// kvs.Delete calls. Neither can corrupt the live tree (the entry is just
	// left in the store); tests assert it stays 0.
	pruneAnomalies int
}

// ErrPrunedTree is returned by UpdateForRoot / DeleteForRoot on a pruned tree:
// writes against an arbitrary root would bypass the reference counts.
var ErrPrunedTree = errors.New("smt: UpdateForRoot/DeleteForRoot are not supported on a pruned tree; use Update/Delete")

func NewSparseMerkleTree(kvs KVStore, hasher hash.Hash) *SparseMerkleTree {
	smt := SparseMerkleTree{
		th:  *newTreeHasher(hasher),
		kvs: kvs,
	}

	smt.SetRoot(smt.th.placeholder())

	return &smt
}

// NewPrunedSparseMerkleTree returns an empty tree over kvs (which must be
// empty) that deletes the entries an Update or Delete orphans: the replaced
// leaf, its value (unless another live leaf has the same value) and the
// replaced path nodes. The store then holds exactly the entries reachable
// from the current root, i.e. O(live keys) instead of O(writes). Root digests
// and proofs are identical to an unpruned tree fed the same operations.
//
// Only the CURRENT root stays readable: GetForRoot/HasForRoot/ProveForRoot on
// an older root may fail with *InvalidKeyError. SetRoot must not be used, and
// UpdateForRoot/DeleteForRoot return ErrPrunedTree. Like the unpruned tree,
// it is not safe for concurrent use.
func NewPrunedSparseMerkleTree(kvs KVStore, hasher hash.Hash) *SparseMerkleTree {
	smt := NewSparseMerkleTree(kvs, hasher)
	smt.refs = make(map[string]uint32)
	return smt
}

func ImportSparseMerkleTree(kvs KVStore, hasher hash.Hash, root []byte) *SparseMerkleTree {
	smt := SparseMerkleTree{
		th:   *newTreeHasher(hasher),
		kvs:  kvs,
		root: root,
	}
	return &smt
}

func (smt *SparseMerkleTree) Root() []byte {
	return smt.root
}

func (smt *SparseMerkleTree) SetRoot(root []byte) {
	smt.root = root
}

func (smt *SparseMerkleTree) depth() int {
	return smt.th.pathSize() * 8
}

func (smt *SparseMerkleTree) Get(key []byte) ([]byte, error) {
	value, err := smt.GetForRoot(key, smt.Root())
	return value, err
}

func (smt *SparseMerkleTree) GetForRoot(key []byte, root []byte) ([]byte, error) {
	if bytes.Equal(root, smt.th.placeholder()) {
		// The tree is empty, return the default value.
		return defaultValue, nil
	}

	path := smt.th.path(key)
	currentHash := root
	for i := 0; i < smt.depth(); i++ {
		currentData, err := smt.kvs.Get(currentHash)
		if err != nil {
			return nil, err
		} else if smt.th.isLeaf(currentData) {
			// We've reached the end. Is this the actual leaf?
			p, valueHash := smt.th.parseLeaf(currentData)
			if !bytes.Equal(path, p) {
				// Nope. Therefore the key is actually empty.
				return defaultValue, nil
			}
			// Otherwise, yes. Return the value.
			value, err := smt.kvs.Get(valueHash)
			if err != nil {
				return nil, err
			}
			return value, nil
		}

		leftNode, rightNode := smt.th.parseNode(currentData)
		if hasBit(path, i) == right {
			currentHash = rightNode
		} else {
			currentHash = leftNode
		}

		if bytes.Equal(currentHash, smt.th.placeholder()) {
			// We've hit a placeholder value; this is the end.
			return defaultValue, nil
		}
	}
	currentData, err := smt.kvs.Get(currentHash)
	if err != nil {
		return nil, err
	}
	_, valueHash := smt.th.parseLeaf(currentData)
	value, err := smt.kvs.Get(valueHash)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (smt *SparseMerkleTree) Has(key []byte) (bool, error) {
	val, err := smt.Get(key)
	return !bytes.Equal(defaultValue, val), err
}

func (smt *SparseMerkleTree) HasForRoot(key, root []byte) (bool, error) {
	val, err := smt.GetForRoot(key, root)
	return !bytes.Equal(defaultValue, val), err
}

func (smt *SparseMerkleTree) Update(key []byte, value []byte) ([]byte, error) {
	if smt.refs != nil {
		return smt.updatePruned(key, value)
	}
	newRoot, err := smt.UpdateForRoot(key, value, smt.Root())
	if err == nil {
		smt.SetRoot(newRoot)
	}
	return newRoot, err
}

// updatePruned is Update on a pruned tree.
//
// Why the counts stay exact: an Update/Delete of key k replaces only the
// nodes on k's path. Let P_old be the inner nodes visited from the old root
// down k's path (sideNodesAndPathForRoot) and L_old the leaf that walk ended
// on. Every node off that path (the side-node subtrees) is kept unchanged;
// it may move to another depth (a split pushes the other key's leaf down, a
// delete pulls the sibling leaf up), but it still occupies exactly one
// position. The new path consists exactly of the entries the update writes
// (set()): the value, the new leaf, the split node and the new inner nodes,
// each a distinct position (the value is one reference from the new leaf).
// L_old leaves the tree only if it is k's own leaf (same path); a
// different key's leaf found by the walk stays (it becomes the sibling of
// the split node). Hence
//
//	refs_new = refs_old + written - P_old - [L_old, value(L_old) if L_old is k's leaf]
//
// We add first and subtract second, so an entry that is both written and
// replaced (e.g. writing the same value again gives the same leaf and path
// hashes) goes +1 then -1 and is never deleted. Entries are content
// addressed, so equal keys (identical values under different keys, or any
// coincidence between a value and a node encoding) are the same bytes and a
// shared counter is sound.
func (smt *SparseMerkleTree) updatePruned(key []byte, value []byte) ([]byte, error) {
	root := smt.root
	path := smt.th.path(key)
	sideNodes, pathNodes, oldLeafHash, oldLeafData, err := smt.sideNodesAndPathForRoot(path, root)
	if err != nil {
		return nil, err
	}

	smt.recording = true
	smt.written = smt.written[:0]
	defer func() {
		smt.recording = false
		smt.written = smt.written[:0]
	}()

	var newRoot []byte
	if bytes.Equal(value, defaultValue) {
		newRoot, err = smt.deleteWithSideNodes(path, sideNodes, oldLeafHash, oldLeafData)
		if _, ok := err.(*keyAlreadyEmptyError); ok {
			// Nothing was written and the tree is unchanged.
			return root, nil
		}
	} else {
		newRoot, err = smt.updateWithSideNodes(path, value, sideNodes, oldLeafHash, oldLeafData)
	}
	if err != nil {
		// The root is not moved. Entries already written stay in the store
		// uncounted: harmless garbage (a later write of the same key counts
		// it normally).
		return nil, err
	}

	for _, k := range smt.written {
		smt.refs[string(k)]++
	}
	for _, k := range pathNodes {
		smt.unref(k)
	}
	if !bytes.Equal(oldLeafHash, smt.th.placeholder()) && oldLeafData != nil {
		if oldPath, oldValueHash := smt.th.parseLeaf(oldLeafData); bytes.Equal(oldPath, path) {
			smt.unref(oldLeafHash)
			smt.unref(oldValueHash)
		}
	}
	smt.root = newRoot
	return newRoot, nil
}

// unref drops one reference on key and deletes the entry at zero.
func (smt *SparseMerkleTree) unref(key []byte) {
	k := string(key)
	n, ok := smt.refs[k]
	if !ok || n == 0 {
		smt.pruneAnomalies++
		return
	}
	if n > 1 {
		smt.refs[k] = n - 1
		return
	}
	delete(smt.refs, k)
	if err := smt.kvs.Delete(key); err != nil {
		smt.pruneAnomalies++
	}
}

// set writes one entry and, during a pruned update, records its key.
func (smt *SparseMerkleTree) set(key []byte, value []byte) error {
	if err := smt.kvs.Set(key, value); err != nil {
		return err
	}
	if smt.recording {
		smt.written = append(smt.written, key)
	}
	return nil
}

func (smt *SparseMerkleTree) Delete(key []byte) ([]byte, error) {
	return smt.Update(key, defaultValue)
}

func (smt *SparseMerkleTree) UpdateForRoot(key []byte, value []byte, root []byte) ([]byte, error) {
	if smt.refs != nil {
		return nil, ErrPrunedTree
	}
	path := smt.th.path(key)
	sideNodes, oldLeafHash, oldLeafData, err := smt.sideNodesForRoot(path, root)
	if err != nil {
		return nil, err
	}

	var newRoot []byte
	if bytes.Equal(value, defaultValue) {
		newRoot, err = smt.deleteWithSideNodes(path, sideNodes, oldLeafHash, oldLeafData)
		if _, ok := err.(*keyAlreadyEmptyError); ok {
			// This key is already empty; return the old root.
			return root, nil
		}
	} else {
		newRoot, err = smt.updateWithSideNodes(path, value, sideNodes, oldLeafHash, oldLeafData)
	}
	return newRoot, err
}

func (smt *SparseMerkleTree) DeleteForRoot(key, root []byte) ([]byte, error) {
	return smt.UpdateForRoot(key, defaultValue, root)
}

func (smt *SparseMerkleTree) deleteWithSideNodes(path []byte, sideNodes [][]byte, oldLeafHash []byte, oldLeafData []byte) ([]byte, error) {
	if bytes.Equal(oldLeafHash, smt.th.placeholder()) {
		// This key is already empty as it is a placeholder; return an error.
		return nil, &keyAlreadyEmptyError{}
	} else if actualPath, _ := smt.th.parseLeaf(oldLeafData); !bytes.Equal(path, actualPath) {
		return nil, &keyAlreadyEmptyError{}
	}

	var currentHash, currentData []byte
	nonPlaceholderReached := false
	for i := smt.depth() - 1; i >= 0; i-- {
		if sideNodes[i] == nil {
			continue
		}

		sideNode := make([]byte, smt.th.pathSize())
		copy(sideNode, sideNodes[i])

		if currentData == nil {
			sideNodeValue, err := smt.kvs.Get(sideNode)
			if err != nil {
				return nil, err
			}

			if smt.th.isLeaf(sideNodeValue) {
				currentHash = sideNode
				currentData = sideNode
				continue
			} else {
				currentData = smt.th.placeholder()
				nonPlaceholderReached = true
			}
		}

		if !nonPlaceholderReached && bytes.Equal(sideNode, smt.th.placeholder()) {
			continue
		} else if !nonPlaceholderReached {
			nonPlaceholderReached = true
		}

		if hasBit(path, i) == right {
			currentHash, currentData = smt.th.digestNode(sideNode, currentData)
		} else {
			currentHash, currentData = smt.th.digestNode(currentData, sideNode)
		}
		err := smt.set(currentHash, currentData)
		if err != nil {
			return nil, err
		}
		currentData = currentHash
	}

	if currentHash == nil {
		currentHash = smt.th.placeholder()
	}
	return currentHash, nil
}

func (smt *SparseMerkleTree) updateWithSideNodes(path []byte, value []byte, sideNodes [][]byte, oldLeafHash []byte, oldLeafData []byte) ([]byte, error) {
	valueHash := smt.th.digest(value)
	if err := smt.set(valueHash, value); err != nil {
		return nil, err
	}

	currentHash, currentData := smt.th.digestLeaf(path, valueHash)
	if err := smt.set(currentHash, currentData); err != nil {
		return nil, err
	}
	currentData = currentHash
	var commonPrefixCount int
	if bytes.Equal(oldLeafHash, smt.th.placeholder()) {
		commonPrefixCount = smt.depth()
	} else {
		actualPath, _ := smt.th.parseLeaf(oldLeafData)
		commonPrefixCount = countCommonPrefix(path, actualPath)
	}
	if commonPrefixCount != smt.depth() {
		if hasBit(path, commonPrefixCount) == right {
			currentHash, currentData = smt.th.digestNode(oldLeafHash, currentData)
		} else {
			currentHash, currentData = smt.th.digestNode(currentData, oldLeafHash)
		}

		err := smt.set(currentHash, currentData)
		if err != nil {
			return nil, err
		}

		currentData = currentHash
	}

	for i := smt.depth() - 1; i >= 0; i-- {
		sideNode := make([]byte, smt.th.pathSize())

		if sideNodes[i] == nil {
			if commonPrefixCount != smt.depth() && commonPrefixCount > i {
				copy(sideNode, smt.th.placeholder())
			} else {
				continue
			}
		} else {
			copy(sideNode, sideNodes[i])
		}

		if hasBit(path, i) == right {
			currentHash, currentData = smt.th.digestNode(sideNode, currentData)
		} else {
			currentHash, currentData = smt.th.digestNode(currentData, sideNode)
		}
		err := smt.set(currentHash, currentData)
		if err != nil {
			return nil, err
		}
		currentData = currentHash
	}

	return currentHash, nil
}

func (smt *SparseMerkleTree) sideNodesForRoot(path []byte, root []byte) ([][]byte, []byte, []byte, error) {
	sideNodes, _, leafHash, leafData, err := smt.sideNodesAndPathForRoot(path, root)
	return sideNodes, leafHash, leafData, err
}

// sideNodesAndPathForRoot is the original sideNodesForRoot that additionally
// returns pathNodes: the hashes of the inner nodes visited from root down
// path (root first; empty when root is a placeholder or a leaf). The other
// return values are unchanged.
func (smt *SparseMerkleTree) sideNodesAndPathForRoot(path []byte, root []byte) ([][]byte, [][]byte, []byte, []byte, error) {
	sideNodes := make([][]byte, smt.depth())

	if bytes.Equal(root, smt.th.placeholder()) {
		// If the root is a placeholder, there are no sidenodes to return.
		// Let the "actual path" be the input path.
		return sideNodes, nil, smt.th.placeholder(), nil, nil
	}

	currentData, err := smt.kvs.Get(root)
	if err != nil {
		return nil, nil, nil, nil, err
	} else if smt.th.isLeaf(currentData) {
		// If the root is a leaf, there are also no sidenodes to return.
		return sideNodes, nil, root, currentData, nil
	}
	pathNodes := [][]byte{root}

	var nodeHash []byte
	for i := 0; i < smt.depth(); i++ {
		leftNode, rightNode := smt.th.parseNode(currentData)

		if hasBit(path, i) == right {
			sideNodes[i] = leftNode
			nodeHash = rightNode
		} else {
			sideNodes[i] = rightNode
			nodeHash = leftNode
		}

		if bytes.Equal(nodeHash, smt.th.placeholder()) {
			// If the node is a placeholder, we've reached the end.
			return sideNodes, pathNodes, nodeHash, nil, nil
		}

		currentData, err = smt.kvs.Get(nodeHash)
		if err != nil {
			return nil, nil, nil, nil, err
		} else if smt.th.isLeaf(currentData) {
			break
		}
		pathNodes = append(pathNodes, nodeHash)
	}

	return sideNodes, pathNodes, nodeHash, currentData, err
}

func (smt *SparseMerkleTree) Prove(key []byte) (SparseMerkleProof, error) {
	proof, err := smt.ProveForRoot(key, smt.Root())
	return proof, err
}

func (smt *SparseMerkleTree) ProveForRoot(key []byte, root []byte) (SparseMerkleProof, error) {
	path := smt.th.path(key)
	sideNodes, leafHash, leafData, err := smt.sideNodesForRoot(path, root)
	if err != nil {
		return SparseMerkleProof{}, err
	}

	var nonEmptySideNodes [][]byte
	for _, v := range sideNodes {
		if v != nil {
			nonEmptySideNodes = append(nonEmptySideNodes, v)
		}
	}

	var nonMembershipLeafData []byte
	if !bytes.Equal(leafHash, smt.th.placeholder()) {
		nonMembershipLeafData = leafData
	}

	proof := SparseMerkleProof{
		SideNodes:             nonEmptySideNodes,
		NonMembershipLeafData: nonMembershipLeafData,
	}

	return proof, err
}

func (smt *SparseMerkleTree) ProveCompact(key []byte) (SparseCompactMerkleProof, error) {
	proof, err := smt.ProveCompactForRoot(key, smt.Root())
	return proof, err
}

func (smt *SparseMerkleTree) ProveCompactForRoot(key []byte, root []byte) (SparseCompactMerkleProof, error) {
	proof, err := smt.ProveForRoot(key, root)
	if err != nil {
		return SparseCompactMerkleProof{}, err
	}
	compactedProof, err := CompactProof(proof, smt.th.hasher)
	return compactedProof, err
}
