package node

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/relab/hotstuff"
)

// DeriveKey returns replica id's ECDSA P-256 key, derived deterministically
// from seed: every replica computes every other replica's public key from
// (seed, id) alone, so no key files have to be generated or distributed.
// This is a testbed convenience: anyone who knows the seed can sign for any
// replica (the experiments run on a private Docker network).
//
// The scalar is SHA-256("hbdb-bft2-key", seed, id, counter) for the first
// counter that gives a valid P-256 scalar (crypto/ecdh checks the range).
// ecdsa.GenerateKey is not used: it does not promise a deterministic result
// for a deterministic reader.
func DeriveKey(seed string, id hotstuff.ID) (*ecdsa.PrivateKey, error) {
	for ctr := uint32(0); ctr < 64; ctr++ {
		h := sha256.New()
		h.Write([]byte("hbdb-bft2-key\x00"))
		h.Write([]byte(seed))
		var buf [8]byte
		binary.BigEndian.PutUint32(buf[0:4], uint32(id))
		binary.BigEndian.PutUint32(buf[4:8], ctr)
		h.Write(buf[:])
		d := h.Sum(nil)
		k, err := ecdh.P256().NewPrivateKey(d)
		if err != nil {
			continue // out of range: next counter
		}
		pub := k.PublicKey().Bytes() // uncompressed: 0x04 || X || Y
		if len(pub) != 65 || pub[0] != 4 {
			return nil, fmt.Errorf("bft2-node: unexpected P-256 public key encoding (%d bytes)", len(pub))
		}
		return &ecdsa.PrivateKey{
			PublicKey: ecdsa.PublicKey{
				Curve: elliptic.P256(),
				X:     new(big.Int).SetBytes(pub[1:33]),
				Y:     new(big.Int).SetBytes(pub[33:65]),
			},
			D: new(big.Int).SetBytes(d),
		}, nil
	}
	return nil, fmt.Errorf("bft2-node: no valid key for replica %d", id)
}
