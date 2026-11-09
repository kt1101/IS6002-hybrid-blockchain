package core

import (
	"context"
	"crypto/ed25519"
	"sync"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/sigs"
)

// DefaultAttestKeep is how many recent heights a Certifier keeps.
const DefaultAttestKeep = 1024

// Certifier collects root attestations per height (phase 2, B2). A height
// is certified once Quorum distinct nodes signed the same root: Quorum =
// 2f+1 with f = floor((n-1)/3) unless given. Each node counts once per
// height (its first valid attestation): later ones from it are duplicates.
// An attestation with a bad signature or from a node without a key is
// invalid. Only the attestations of the last keep heights are kept.
//
// The server feeds it from the apply loop, in block order, so every node
// reaches the same certifications at the same block (it is deterministic).
// Certified and Wait may be called from any goroutine.
type Certifier struct {
	n, quorum, keep int
	keys            func(nodeID int32) (ed25519.PublicKey, bool)

	mu         sync.Mutex
	heights    map[int64]*heightVotes
	maxHeight  int64
	invalid    int64
	duplicates int64
	waiters    map[int64][]chan struct{}
}

type heightVotes struct {
	voted     map[int32]bool
	byRoot    map[string][]*pbv.RootAttestation
	certified bool
	root      []byte
	atts      []*pbv.RootAttestation // the quorum, in arrival order
}

// NewCertifier: n nodes, quorum (0 = 2*((n-1)/3)+1), node keys, keep
// heights (<= 0 = DefaultAttestKeep).
func NewCertifier(n, quorum int, keys func(nodeID int32) (ed25519.PublicKey, bool), keep int) *Certifier {
	if quorum <= 0 {
		quorum = 2*((n-1)/3) + 1
	}
	if keep <= 0 {
		keep = DefaultAttestKeep
	}
	return &Certifier{n: n, quorum: quorum, keep: keep, keys: keys,
		heights: make(map[int64]*heightVotes), waiters: make(map[int64][]chan struct{})}
}

// Quorum is the number of matching attestations that certify a height.
func (c *Certifier) Quorum() int { return c.quorum }

// Add records a and reports whether it certified its height just now (with
// the certified root).
func (c *Certifier) Add(a *pbv.RootAttestation) (certified bool, root []byte) {
	pub, ok := c.keys(a.GetNodeId())
	if !ok || !sigs.VerifyAttest(pub, a.GetHeight(), a.GetRoot(), a.GetSig()) {
		c.mu.Lock()
		c.invalid++
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	h := a.GetHeight()
	if h <= c.maxHeight-int64(c.keep) {
		return false, nil // evicted
	}
	hv := c.heights[h]
	if hv == nil {
		hv = &heightVotes{voted: make(map[int32]bool), byRoot: make(map[string][]*pbv.RootAttestation)}
		c.heights[h] = hv
		if h > c.maxHeight {
			c.maxHeight = h
			c.evict()
		}
	}
	if hv.voted[a.GetNodeId()] {
		c.duplicates++
		return false, nil
	}
	hv.voted[a.GetNodeId()] = true
	if hv.certified {
		return false, nil
	}
	k := string(a.GetRoot())
	hv.byRoot[k] = append(hv.byRoot[k], a)
	if len(hv.byRoot[k]) < c.quorum {
		return false, nil
	}
	hv.certified = true
	hv.root = append([]byte(nil), a.GetRoot()...)
	hv.atts = append([]*pbv.RootAttestation(nil), hv.byRoot[k]...)
	for _, ch := range c.waiters[h] {
		close(ch)
	}
	delete(c.waiters, h)
	return true, append([]byte(nil), hv.root...)
}

// evict drops the heights at or below maxHeight-keep. Called with mu held.
func (c *Certifier) evict() {
	for h := range c.heights {
		if h <= c.maxHeight-int64(c.keep) {
			delete(c.heights, h)
		}
	}
}

// Certified returns the certified root of h and its quorum of attestations.
func (c *Certifier) Certified(h int64) ([]byte, []*pbv.RootAttestation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	hv := c.heights[h]
	if hv == nil || !hv.certified {
		return nil, nil, false
	}
	return append([]byte(nil), hv.root...), append([]*pbv.RootAttestation(nil), hv.atts...), true
}

// Wait returns Certified(h) once h is certified, or ctx.Err().
func (c *Certifier) Wait(ctx context.Context, h int64) ([]byte, []*pbv.RootAttestation, error) {
	c.mu.Lock()
	if hv := c.heights[h]; hv != nil && hv.certified {
		c.mu.Unlock()
		r, a, _ := c.Certified(h)
		return r, a, nil
	}
	ch := make(chan struct{})
	c.waiters[h] = append(c.waiters[h], ch)
	c.mu.Unlock()
	select {
	case <-ch:
		r, a, ok := c.Certified(h)
		if !ok {
			return nil, nil, context.DeadlineExceeded // evicted meanwhile
		}
		return r, a, nil
	case <-ctx.Done():
		c.mu.Lock()
		ws := c.waiters[h]
		for i, w := range ws {
			if w == ch {
				c.waiters[h] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(c.waiters[h]) == 0 {
			delete(c.waiters, h)
		}
		c.mu.Unlock()
		return nil, nil, ctx.Err()
	}
}

// Invalid is the number of attestations dropped for a bad signature or an
// unknown node; Duplicates the number of further attestations of a node
// for a height it already attested.
func (c *Certifier) Invalid() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.invalid
}

func (c *Certifier) Duplicates() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.duplicates
}
