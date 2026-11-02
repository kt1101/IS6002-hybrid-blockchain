package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"

	"hybrid/demo/cluster"
)

const testTimeout = 10 * time.Second

func newSim(t *testing.T) *cluster.Sim {
	t.Helper()
	c, err := cluster.NewSim(cluster.SimConfig{N: 4})
	if err != nil {
		t.Fatalf("NewSim: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func setAndWait(t *testing.T, c cluster.Cluster, key, val string, nodes ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := c.Set(ctx, "node1", key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	deadline := time.Now().Add(testTimeout)
	for _, n := range nodes {
		for {
			if _, ok, err := c.Get(ctx, n, key); err == nil && ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never applied %s", n, key)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func verdicts(r Result) map[string]string {
	m := map[string]string{}
	for _, rep := range r.Replicas {
		m[rep.Node] = rep.Verdict
	}
	return m
}

func TestVerifyValid(t *testing.T) {
	c := newSim(t)
	setAndWait(t, c, "user43", "bob", "node1", "node2", "node3", "node4")
	setAndWait(t, c, "user42", "alice", "node1", "node2", "node3", "node4")

	r, err := Run(context.Background(), c, "node2", "user42")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !r.Valid || !r.Found {
		t.Fatalf("Valid=%v Found=%v, want both true", r.Valid, r.Found)
	}
	if r.Value != "alice" || r.Version <= 0 || r.Node != "node2" || r.Key != "user42" {
		t.Fatalf("unexpected result %+v", r)
	}
	if len(r.Steps) < 2 {
		t.Fatalf("%d steps, want a leaf and at least one side node", len(r.Steps))
	}
	if got := r.Steps[len(r.Steps)-1].Hash; got != r.Root {
		t.Fatalf("last step %s != root %s", got, r.Root)
	}
	if r.Steps[0].Hash != r.LeafHash || !r.Steps[0].Empty {
		t.Fatalf("first step %+v, want empty leaf step with hash %s", r.Steps[0], r.LeafHash)
	}
	if first := r.Steps[0]; first.Span != 256-(len(r.Steps)-1) {
		t.Fatalf("empty span %d, want %d", first.Span, 256-(len(r.Steps)-1))
	}
	checkChain(t, r)
	for i, s := range r.Steps[1:] {
		if s.SiblingSide != "left" && s.SiblingSide != "right" {
			t.Fatalf("step %d side %q", i+1, s.SiblingSide)
		}
		if want := len(r.Steps) - i - 1; s.Level != want {
			t.Fatalf("step %d level %d, want %d", i+1, s.Level, want)
		}
	}
	if len(r.Replicas) != 4 {
		t.Fatalf("%d replicas, want 4", len(r.Replicas))
	}
	for _, rep := range r.Replicas {
		if rep.Verdict != "valid · majority" || rep.Root != r.Root || rep.Value != "alice" {
			t.Fatalf("replica %+v, want valid · majority on root %s", rep, r.Root)
		}
	}
}

func TestVerifyCorruptReplica(t *testing.T) {
	c := newSim(t)
	if err := c.Inject(context.Background(), "node3", cluster.FaultCorrupt); err != nil {
		t.Fatalf("Inject: %v", err)
	}
	setAndWait(t, c, "user42", "alice", "node1", "node2", "node3", "node4")

	r, err := Run(context.Background(), c, "node1", "user42")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	v := verdicts(r)
	want := map[string]string{
		"node1": "valid · majority", "node2": "valid · majority",
		"node3": "valid · minority root", "node4": "valid · majority",
	}
	for n, w := range want {
		if v[n] != w {
			t.Errorf("%s verdict %q, want %q", n, v[n], w)
		}
	}
}

func TestVerifyMissingKey(t *testing.T) {
	c := newSim(t)
	setAndWait(t, c, "other", "x", "node1")

	r, err := Run(context.Background(), c, "node1", "ghost")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.Found || r.Valid {
		t.Fatalf("Found=%v Valid=%v, want false", r.Found, r.Valid)
	}
	if v := verdicts(r)["node1"]; v != "not found" {
		t.Fatalf("node1 verdict %q, want not found", v)
	}
}

func TestVerifyDownNode(t *testing.T) {
	c := newSim(t)
	setAndWait(t, c, "user42", "alice", "node1", "node2", "node3", "node4")
	if err := c.Inject(context.Background(), "node4", cluster.FaultCrash); err != nil {
		t.Fatalf("Inject: %v", err)
	}

	r, err := Run(context.Background(), c, "node1", "user42")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	v := verdicts(r)
	if v["node4"] != "unreachable" {
		t.Errorf("node4 verdict %q, want unreachable", v["node4"])
	}
	for _, n := range []string{"node1", "node2", "node3"} {
		if v[n] != "valid · majority" {
			t.Errorf("%s verdict %q, want valid · majority", n, v[n])
		}
	}

	if _, err := Run(context.Background(), c, "node4", "user42"); !errors.Is(err, cluster.ErrNodeDown) {
		t.Fatalf("Run on down node = %v, want ErrNodeDown", err)
	}
}

// checkChain recomputes the path from the leaf up, independently of the
// package under test: leaf = H(0x00 || H(key) || H(value-version)), and each
// side step is H(0x01 || left || right) with the sibling on the reported side.
func checkChain(t *testing.T, r Result) {
	t.Helper()
	h := func(parts ...[]byte) []byte {
		sum := sha256.Sum256(bytes.Join(parts, nil))
		return sum[:]
	}
	keyPath := h([]byte(r.Key))
	valueHash := h([]byte(r.Value + "-" + strconv.FormatInt(r.Version, 10)))
	cur := h([]byte{0}, keyPath, valueHash)
	if got := hex.EncodeToString(cur); got != r.LeafHash || got != r.Steps[0].Hash {
		t.Fatalf("leaf hash %s, want %s", r.LeafHash, got)
	}
	for k, s := range r.Steps[1:] {
		sib, err := hex.DecodeString(s.Sibling)
		if err != nil || len(sib) != sha256.Size {
			t.Fatalf("step %d sibling %q: %v", k+1, s.Sibling, err)
		}
		switch s.SiblingSide {
		case "left":
			cur = h([]byte{1}, sib, cur)
		case "right":
			cur = h([]byte{1}, cur, sib)
		default:
			t.Fatalf("step %d side %q", k+1, s.SiblingSide)
		}
		if got := hex.EncodeToString(cur); got != s.Hash {
			t.Fatalf("step %d hash %s, recomputed %s (side %s)", k+1, s.Hash, got, s.SiblingSide)
		}
	}
	if got := hex.EncodeToString(cur); got != r.Root {
		t.Fatalf("recomputed root %s != Root %s", got, r.Root)
	}
}
