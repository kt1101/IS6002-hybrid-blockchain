// Package verify checks a key's Merkle proof against a node's ledger root,
// returns the leaf-to-root path for drawing, and compares the same key on
// every replica.
package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"hybrid/demo/cluster"
	"hybrid/veritas/ledger/merkletree"
)

// treeDepth is the number of levels of the sparse Merkle tree (bits of sha256).
const treeDepth = 256

// Replica verdicts.
const (
	VerdictMajority    = "valid · majority"
	VerdictMinority    = "valid · minority root"
	VerdictInvalid     = "invalid"
	VerdictUnreachable = "unreachable"
	VerdictNotFound    = "not found"
)

// Step is one hash on the leaf-to-root path.
type Step struct {
	Level       int    `json:"level"`        // tree level (root's children = 1)
	Hash        string `json:"hash"`         // hex hash computed at this step
	Sibling     string `json:"sibling"`      // hex side node combined at this step; "" for the leaf step
	SiblingSide string `json:"sibling_side"` // "left" or "right"; "" for the leaf step
	Empty       bool   `json:"empty"`        // the leaf step, standing for the collapsed empty levels below it
	Span        int    `json:"span"`         // levels collapsed when Empty
}

// Replica is one node's answer for the key.
type Replica struct {
	Node    string `json:"node"`
	Value   string `json:"value"`
	Root    string `json:"root"`
	Verdict string `json:"verdict"`
}

// Result is the outcome of verifying a key on one node.
type Result struct {
	Valid    bool      `json:"valid"`
	Found    bool      `json:"found"`
	Node     string    `json:"node"`
	Key      string    `json:"key"`
	Value    string    `json:"value"`
	Version  int64     `json:"version"`
	Root     string    `json:"root"`
	LeafHash string    `json:"leaf_hash"`
	Steps    []Step    `json:"steps"`
	Replicas []Replica `json:"replicas"`
}

// answer is what one node replied for the key.
type answer struct {
	node     string
	err      error // Get or Prove failed: the node did not answer
	kv       cluster.KV
	found    bool
	proof    cluster.Proof
	valid    bool     // proof verifies against the node's own root
	hashes   [][]byte // VerifyProofSteps output (found keys only)
	leafData []byte
}

// Run verifies key on node and compares it with every replica. It returns an
// error (wrapping cluster.ErrNodeDown when the node is down) if node itself
// cannot answer; other replicas failing only shows up in their verdicts.
func Run(ctx context.Context, c cluster.Cluster, node, key string) (Result, error) {
	var answers []answer
	var asked *answer
	for _, n := range c.Nodes() {
		a := ask(ctx, c, n.Name, key)
		answers = append(answers, a)
		if n.Name == node {
			asked = &answers[len(answers)-1]
		}
	}
	if asked == nil {
		return Result{}, fmt.Errorf("verify: unknown node %q", node)
	}
	if asked.err != nil {
		return Result{}, fmt.Errorf("verify %s on %s: %w", key, node, asked.err)
	}

	maj := majorityRoot(answers)
	res := Result{
		Valid:    asked.found && asked.valid,
		Found:    asked.found,
		Node:     node,
		Key:      key,
		Value:    asked.kv.Value,
		Version:  asked.kv.Version,
		Root:     hex.EncodeToString(asked.proof.Root),
		Steps:    []Step{},
		Replicas: make([]Replica, 0, len(answers)),
	}
	if asked.found {
		res.Steps = buildSteps(asked.proof, asked.hashes, key)
		if len(asked.hashes) > 0 {
			res.LeafHash = hex.EncodeToString(asked.hashes[0])
		}
	}
	for _, a := range answers {
		res.Replicas = append(res.Replicas, replicaOf(a, maj))
	}
	return res, nil
}

// ask fetches and verifies key on one node.
func ask(ctx context.Context, c cluster.Cluster, node, key string) answer {
	a := answer{node: node}
	kv, found, err := c.Get(ctx, node, key)
	if err != nil {
		a.err = err
		return a
	}
	p, err := c.Prove(ctx, node, key)
	if err != nil {
		a.err = err
		return a
	}
	a.kv, a.found, a.proof = kv, found, p
	if found {
		a.leafData = leafValue(kv)
		a.valid, a.hashes = merkletree.VerifyProofSteps(
			merkletree.SparseMerkleProof{SideNodes: p.SideNodes, NonMembershipLeafData: p.NonMembershipLeafData},
			p.Root, []byte(key), a.leafData, sha256.New())
	}
	return a
}

// leafValue is the value the ledger stores under a key: value-version.
func leafValue(kv cluster.KV) []byte {
	return []byte(kv.Value + "-" + strconv.FormatInt(kv.Version, 10))
}

// majorityRoot returns the root shared by more than half of the nodes that
// answered, or nil if there is none.
func majorityRoot(answers []answer) []byte {
	counts := map[string]int{}
	answered := 0
	for _, a := range answers {
		if a.err != nil {
			continue
		}
		answered++
		counts[string(a.proof.Root)]++
	}
	for root, n := range counts {
		if 2*n > answered {
			return []byte(root)
		}
	}
	return nil
}

func replicaOf(a answer, majority []byte) Replica {
	r := Replica{Node: a.node}
	switch {
	case a.err != nil:
		r.Verdict = VerdictUnreachable
		return r
	case !a.found:
		r.Root = hex.EncodeToString(a.proof.Root)
		r.Verdict = VerdictNotFound
		return r
	}
	r.Value = a.kv.Value
	r.Root = hex.EncodeToString(a.proof.Root)
	switch {
	case !a.valid:
		r.Verdict = VerdictInvalid
	case bytes.Equal(a.proof.Root, majority):
		r.Verdict = VerdictMajority
	default:
		r.Verdict = VerdictMinority
	}
	return r
}

// buildSteps turns the hashes of VerifyProofSteps into drawable steps, leaf
// first. Side node i sits at tree level i+1 (the root's children are level
// 1); the leaf step stands for the empty levels len(SideNodes)+1..treeDepth
// collapsed into one.
func buildSteps(p cluster.Proof, hashes [][]byte, key string) []Step {
	steps := []Step{}
	n := len(p.SideNodes)
	if len(hashes) != n+1 {
		return steps
	}
	path := sha256.Sum256([]byte(key))
	span := treeDepth - n
	leafLevel := n + 1
	if span <= 0 {
		span, leafLevel = 0, treeDepth
	}
	steps = append(steps, Step{Level: leafLevel, Hash: hex.EncodeToString(hashes[0]), Empty: span > 0, Span: span})
	for i := n - 1; i >= 0; i-- {
		side := "right"
		if hasBit(path[:], i) {
			side = "left"
		}
		steps = append(steps, Step{
			Level:       i + 1,
			Hash:        hex.EncodeToString(hashes[n-i]),
			Sibling:     hex.EncodeToString(p.SideNodes[i]),
			SiblingSide: side,
		})
	}
	return steps
}

// hasBit mirrors merkletree's unexported hasBit, which tests bit position of
// data counting from the low bit of each byte.
func hasBit(data []byte, position int) bool {
	return int(data[position/8])&(1<<(uint(position)%8)) > 0
}
