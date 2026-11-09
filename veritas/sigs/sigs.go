// Package sigs holds the Ed25519 signatures of phase 2: client signatures on
// Sets (--verify-sigs) and node signatures on state roots (root
// attestations, see veritas/core certifier).
//
// Keys are derived deterministically from a shared seed and an identity
// (KeyFor), as bft2node/node/keys.go derives the HotStuff keys, so no key
// has to be distributed: every node can compute every public key. This is
// enough to measure the cost of real signatures; it is not key management.
package sigs

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"

	pbv "hybrid/proto/veritas"
)

// DefaultSeed is the key seed of the experiments (run_one.sh KEY_SEED).
const DefaultSeed = "hbdb-phase2"

// KeyFor derives the Ed25519 private key of identity id from seed.
func KeyFor(seed, id string) ed25519.PrivateKey {
	h := sha256.Sum256([]byte("veritas-key\x00" + seed + "\x00" + id))
	return ed25519.NewKeyFromSeed(h[:])
}

// Public returns the public half of k.
func Public(k ed25519.PrivateKey) ed25519.PublicKey {
	return k.Public().(ed25519.PublicKey)
}

// SetBytes is the canonical byte string a client signs for a Set:
// key 0x00 value 0x00 version (int64, 8 bytes big-endian).
func SetBytes(key, value string, version int64) []byte {
	b := make([]byte, 0, len(key)+len(value)+10)
	b = append(b, key...)
	b = append(b, 0)
	b = append(b, value...)
	b = append(b, 0)
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], uint64(version))
	return append(b, v[:]...)
}

// SignSet signs SetBytes(key, value, version) with k.
func SignSet(k ed25519.PrivateKey, key, value string, version int64) []byte {
	return ed25519.Sign(k, SetBytes(key, value, version))
}

// VerifySet reports whether sig is pub's signature on SetBytes(key, value, version).
func VerifySet(pub ed25519.PublicKey, key, value string, version int64, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, SetBytes(key, value, version), sig)
}

// SigMode selects where a node checks the client signatures of Sets
// (veritas --verify-sigs).
type SigMode int

const (
	// SigNone checks nothing (phase 1).
	SigNone SigMode = iota
	// SigIngress checks at the node that receives the Set (Server.Set /
	// BatchSet): a bad signature is codes.Unauthenticated.
	SigIngress
	// SigAll also checks on every replica before applying a block (the
	// BFT model: the proposer is not trusted): a SharedLog with a bad
	// signature is aborted.
	SigAll
)

// ParseSigMode parses none|ingress|all ("" is none).
func ParseSigMode(s string) (SigMode, error) {
	switch s {
	case "", "none":
		return SigNone, nil
	case "ingress":
		return SigIngress, nil
	case "all":
		return SigAll, nil
	}
	return SigNone, fmt.Errorf("sigs: unknown signature mode %q (want none, ingress or all)", s)
}

func (m SigMode) String() string {
	switch m {
	case SigNone:
		return "none"
	case SigIngress:
		return "ingress"
	case SigAll:
		return "all"
	}
	return fmt.Sprintf("SigMode(%d)", int(m))
}

// ClientID is the identity of client k (driver k of ycsbbench).
func ClientID(k int) string { return "client" + strconv.Itoa(k) }

// ClientKeys returns the public-key lookup of the clients of seed: an id
// "client<k>" (k a non-negative decimal integer) has the key
// Public(KeyFor(seed, id)); any other id is unknown. Keys are cached.
func ClientKeys(seed string) func(id string) (ed25519.PublicKey, bool) {
	var cache sync.Map
	return func(id string) (ed25519.PublicKey, bool) {
		if v, ok := cache.Load(id); ok {
			return v.(ed25519.PublicKey), true
		}
		num := strings.TrimPrefix(id, "client")
		if num == id || num == "" {
			return nil, false
		}
		for _, c := range num {
			if c < '0' || c > '9' {
				return nil, false
			}
		}
		pub := Public(KeyFor(seed, id))
		cache.Store(id, pub)
		return pub, true
	}
}

// NodeID is the key identity of node i (veritas --node-id i, compose node<i>).
func NodeID(i int) string { return "node" + strconv.Itoa(i) }

// AttestBytes is what a node signs to attest its state root at height:
// "veritas-attest" 0x00 height (int64, 8 bytes big-endian) root.
func AttestBytes(height int64, root []byte) []byte {
	b := make([]byte, 0, 15+8+len(root))
	b = append(b, "veritas-attest\x00"...)
	var h [8]byte
	binary.BigEndian.PutUint64(h[:], uint64(height))
	b = append(b, h[:]...)
	return append(b, root...)
}

// SignAttest signs AttestBytes(height, root) with k.
func SignAttest(k ed25519.PrivateKey, height int64, root []byte) []byte {
	return ed25519.Sign(k, AttestBytes(height, root))
}

// VerifyAttest reports whether sig is pub's attestation of (height, root).
func VerifyAttest(pub ed25519.PublicKey, height int64, root, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, AttestBytes(height, root), sig)
}

// NodeKeys returns the public keys of nodes 1..n of seed (any other id is
// unknown).
func NodeKeys(seed string, n int) func(nodeID int32) (ed25519.PublicKey, bool) {
	pubs := make([]ed25519.PublicKey, n+1)
	for i := 1; i <= n; i++ {
		pubs[i] = Public(KeyFor(seed, NodeID(i)))
	}
	return func(id int32) (ed25519.PublicKey, bool) {
		if id < 1 || int(id) > n {
			return nil, false
		}
		return pubs[id], true
	}
}

// ReadBytes is what a node signs on a read response (phase 3, signed
// reads): "veritas-read" 0x00 key 0x00 value 0x00 version (8 bytes BE)
// height (8 bytes BE) root sha256(side nodes concatenated ||
// non-membership leaf data). The proof is hashed so the signed bytes stay
// short.
func ReadBytes(key, value string, version, height int64, root []byte, sideNodes [][]byte, nonMember []byte) []byte {
	h := sha256.New()
	for _, n := range sideNodes {
		h.Write(n)
	}
	h.Write(nonMember)
	b := make([]byte, 0, 13+len(key)+1+len(value)+1+16+len(root)+sha256.Size)
	b = append(b, "veritas-read\x00"...)
	b = append(b, key...)
	b = append(b, 0)
	b = append(b, value...)
	b = append(b, 0)
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(version))
	b = append(b, n[:]...)
	binary.BigEndian.PutUint64(n[:], uint64(height))
	b = append(b, n[:]...)
	b = append(b, root...)
	return h.Sum(b)
}

func readBytesOf(r *pbv.SignedRead) []byte {
	return ReadBytes(r.GetKey(), r.GetValue(), r.GetVersion(), r.GetHeight(), r.GetRoot(),
		r.GetSideNodes(), r.GetNonMembershipLeafData())
}

// SignRead signs ReadBytes of r with k (r.Sig is ignored).
func SignRead(k ed25519.PrivateKey, r *pbv.SignedRead) []byte {
	return ed25519.Sign(k, readBytesOf(r))
}

// VerifyRead reports whether r.Sig is pub's signature on r.
func VerifyRead(pub ed25519.PublicKey, r *pbv.SignedRead) bool {
	if r == nil || len(pub) != ed25519.PublicKeySize || len(r.GetSig()) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, readBytesOf(r), r.GetSig())
}

// SignedReadOf is the SignedRead of a signed GetVerified response for key
// (the evidence a client sends when the response does not verify).
func SignedReadOf(key string, resp *pbv.GetVerifiedResponse) *pbv.SignedRead {
	return &pbv.SignedRead{
		Key: key, Value: resp.GetValue(), Version: resp.GetVersion(), Height: resp.GetHeight(),
		Root: resp.GetRootDigest(), SideNodes: resp.GetSideNodes(),
		NonMembershipLeafData: resp.GetNonMembershipLeafData(), NodeId: resp.GetNodeId(), Sig: resp.GetNodeSig(),
	}
}
