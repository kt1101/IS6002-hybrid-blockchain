package core

import (
	"bytes"
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
//
// Phase 3: Add also reports the divergent attestations of a height (AddResult),
// and skip(node, height) == true drops a node's attestation (an evicted node:
// not counted, never reported divergent).
type Certifier struct {
	n, quorum, keep int
	keys            func(nodeID int32) (ed25519.PublicKey, bool)
	skip            func(nodeID int32, height int64) bool

	mu         sync.Mutex
	heights    map[int64]*heightVotes
	maxHeight  int64
	invalid    int64
	duplicates int64
	skipped    int64
	waiters    map[int64][]chan struct{}
}

type heightVotes struct {
	voted     map[int32]bool
	order     []*pbv.RootAttestation // every counted attestation, in arrival order
	byRoot    map[string][]*pbv.RootAttestation
	certified bool
	root      []byte
	atts      []*pbv.RootAttestation // the quorum, in arrival order
}

// NewCertifier: n nodes, quorum (0 = 2*((n-1)/3)+1), node keys, keep
// heights (<= 0 = DefaultAttestKeep), skip (nil = count every node).
func NewCertifier(n, quorum int, keys func(nodeID int32) (ed25519.PublicKey, bool), keep int,
	skip func(nodeID int32, height int64) bool) *Certifier {
	if quorum <= 0 {
		quorum = 2*((n-1)/3) + 1
	}
	if keep <= 0 {
		keep = DefaultAttestKeep
	}
	return &Certifier{n: n, quorum: quorum, keep: keep, keys: keys, skip: skip,
		heights: make(map[int64]*heightVotes), waiters: make(map[int64][]chan struct{})}
}

// Quorum is the number of matching attestations that certify a height.
func (c *Certifier) Quorum() int { return c.quorum }

// AddResult is what one Add found.
//
//   - Certified: this attestation certified its height just now.
//   - Root: the certified root of the height whenever it is certified (now or
//     before), else nil.
//   - Divergent: the nodes whose counted attestation of this height is
//     for another root than the certified one, in arrival order: when the
//     height was just certified, every such node that attested before; once
//     it is certified, the node of this attestation if its root differs.
//     Honest nodes reach the same root at the same height, so a divergent
//     node is faulty unless the certified root itself is a faulty one
//     (more than quorum-1 colluding nodes).
type AddResult struct {
	Certified bool
	Root      []byte
	Divergent []int32
}

// Add records a and reports what it changed (AddResult).
func (c *Certifier) Add(a *pbv.RootAttestation) AddResult {
	pub, ok := c.keys(a.GetNodeId())
	if !ok || !sigs.VerifyAttest(pub, a.GetHeight(), a.GetRoot(), a.GetSig()) {
		c.mu.Lock()
		c.invalid++
		c.mu.Unlock()
		return AddResult{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	h := a.GetHeight()
	if h <= c.maxHeight-int64(c.keep) {
		return AddResult{} // evicted
	}
	if c.skip != nil && c.skip(a.GetNodeId(), h) {
		c.skipped++
		return AddResult{}
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
		return AddResult{}
	}
	hv.voted[a.GetNodeId()] = true
	hv.order = append(hv.order, a)
	if hv.certified {
		res := AddResult{Root: append([]byte(nil), hv.root...)}
		if !bytes.Equal(a.GetRoot(), hv.root) {
			res.Divergent = []int32{a.GetNodeId()}
		}
		return res
	}
	k := string(a.GetRoot())
	hv.byRoot[k] = append(hv.byRoot[k], a)
	if len(hv.byRoot[k]) < c.quorum {
		return AddResult{}
	}
	hv.certified = true
	hv.root = append([]byte(nil), a.GetRoot()...)
	hv.atts = append([]*pbv.RootAttestation(nil), hv.byRoot[k]...)
	for _, ch := range c.waiters[h] {
		close(ch)
	}
	delete(c.waiters, h)
	res := AddResult{Certified: true, Root: append([]byte(nil), hv.root...)}
	for _, o := range hv.order {
		if !bytes.Equal(o.GetRoot(), hv.root) {
			res.Divergent = append(res.Divergent, o.GetNodeId())
		}
	}
	return res
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

// MinHeight is the lowest height the Certifier still keeps (older ones were
// evicted from its window).
func (c *Certifier) MinHeight() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.maxHeight - int64(c.keep) + 1; m > 1 {
		return m
	}
	return 1
}

// Skipped is the number of attestations dropped by skip (evicted nodes).
func (c *Certifier) Skipped() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.skipped
}
