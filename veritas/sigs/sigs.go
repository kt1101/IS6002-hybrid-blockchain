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
