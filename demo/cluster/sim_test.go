package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"hybrid/veritas/benchmark"
)

func newTestSim(t *testing.T) *Sim {
	t.Helper()
	c, err := NewSim(SimConfig{})
	if err != nil {
		t.Fatalf("NewSim: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustSet(t *testing.T, c Cluster, node, key, val string) {
	t.Helper()
	if err := c.Set(ctxT(t, testTimeout), node, key, val); err != nil {
		t.Fatalf("Set(%s, %s=%s): %v", node, key, val, err)
	}
}

func getVal(c Cluster, node, key string) (string, bool) {
	kv, ok, err := c.Get(context.Background(), node, key)
	if err != nil || !ok {
		return "", false
	}
	return kv.Value, true
}

// rootsAt collects BlockEvents until every node has reported height h and
// returns each node's root at h.
func rootsAt(t *testing.T, c Cluster, h int64) map[string][]byte {
	t.Helper()
	roots := map[string][]byte{}
	n := len(c.Nodes())
	timeout := time.After(testTimeout)
	for len(roots) < n {
		select {
		case ev := <-c.Events():
			if ev.Height == h {
				roots[ev.Node] = ev.Root
			}
		case <-timeout:
			t.Fatalf("only %d/%d nodes reported height %d", len(roots), n, h)
		}
	}
	return roots
}

func TestSimInfo(t *testing.T) {
	c := newTestSim(t)
	info := c.Info()
	if info.Mode != "sim" || info.Backend != "memnet" || info.N != 4 || info.F != 1 || !info.CanReset {
		t.Fatalf("Info = %+v", info)
	}
	if len(info.Faults) != 4 || len(info.Healable) != 3 {
		t.Fatalf("Info faults = %v healable = %v", info.Faults, info.Healable)
	}
	nodes := c.Nodes()
	if len(nodes) != 4 {
		t.Fatalf("Nodes = %+v", nodes)
	}
	for i, n := range nodes {
		if n.Name != fmt.Sprintf("node%d", i+1) || !n.Up || len(n.Faults) != 0 {
			t.Fatalf("node %d = %+v", i, n)
		}
	}
}

func TestSimSetReplicates(t *testing.T) {
	c := newTestSim(t)
	mustSet(t, c, "node1", "k", "v1")
	for _, n := range c.Nodes() {
		name := n.Name
		eventually(t, testTimeout, name+" has k=v1", func() bool {
			v, ok := getVal(c, name, "k")
			return ok && v == "v1"
		})
		kv, _, _ := c.Get(context.Background(), name, "k")
		if kv.Version <= 0 {
			t.Fatalf("%s: version %d, want > 0", name, kv.Version)
		}
	}
	roots := rootsAt(t, c, 1)
	want := roots["node1"]
	if len(want) == 0 {
		t.Fatal("node1 reported an empty root")
	}
	for name, r := range roots {
		if !bytes.Equal(r, want) {
			t.Fatalf("%s root %x != node1 root %x", name, r, want)
		}
	}

	p, err := c.Prove(context.Background(), "node2", "k")
	if err != nil {
		t.Fatalf("Prove: %v", err)
	}
	if !bytes.Equal(p.Root, want) {
		t.Fatalf("proof root %x != block root %x", p.Root, want)
	}
	if _, ok, err := c.Get(context.Background(), "node1", "missing"); ok || err != nil {
		t.Fatalf("Get(missing) ok=%v err=%v, want false, nil", ok, err)
	}
}

func TestSimCorruptDiverges(t *testing.T) {
	c := newTestSim(t)
	if err := c.Inject(context.Background(), "node3", FaultCorrupt); err != nil {
		t.Fatalf("Inject corrupt: %v", err)
	}
	mustSet(t, c, "node1", "k", "v")
	eventually(t, testTimeout, "node3 applied k", func() bool {
		_, ok := getVal(c, "node3", "k")
		return ok
	})
	if v, _ := getVal(c, "node3", "k"); !strings.HasSuffix(v, benchmark.CorruptMarker) {
		t.Fatalf("node3 value %q lacks corrupt marker", v)
	}
	if v, _ := getVal(c, "node1", "k"); v != "v" {
		t.Fatalf("node1 value %q, want v", v)
	}
	roots := rootsAt(t, c, 1)
	if bytes.Equal(roots["node3"], roots["node1"]) {
		t.Fatal("node3 root equals node1 root after corruption")
	}
	if !bytes.Equal(roots["node2"], roots["node1"]) {
		t.Fatal("honest roots differ")
	}
	if err := c.Heal(context.Background(), "node3"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Heal corrupt node = %v, want ErrUnsupported", err)
	}
	if f := c.Nodes()[2].Faults; len(f) != 1 || f[0] != FaultCorrupt {
		t.Fatalf("node3 faults = %v, want [corrupt]", f)
	}
}

func TestSimCrashHeal(t *testing.T) {
	c := newTestSim(t)
	if err := c.Inject(context.Background(), "node4", FaultCrash); err != nil {
		t.Fatalf("Inject crash: %v", err)
	}
	if n := c.Nodes()[3]; n.Up || len(n.Faults) != 1 || n.Faults[0] != FaultCrash {
		t.Fatalf("node4 = %+v, want down with [crash]", n)
	}
	if _, _, err := c.Get(context.Background(), "node4", "k"); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("Get on crashed node = %v, want ErrNodeDown", err)
	}
	if err := c.Set(context.Background(), "node4", "k", "x"); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("Set on crashed node = %v, want ErrNodeDown", err)
	}
	if _, err := c.Prove(context.Background(), "node4", "k"); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("Prove on crashed node = %v, want ErrNodeDown", err)
	}
	for i := 1; i <= 5; i++ {
		mustSet(t, c, "node1", "k", fmt.Sprint("v", i))
	}
	if err := c.Heal(context.Background(), "node4"); err != nil {
		t.Fatalf("Heal: %v", err)
	}
	eventually(t, testTimeout, "node4 catches up", func() bool {
		v, ok := getVal(c, "node4", "k")
		return ok && v == "v5"
	})
	if n := c.Nodes()[3]; !n.Up || len(n.Faults) != 0 {
		t.Fatalf("node4 after heal = %+v", n)
	}
}

func TestSimSilentTimesOut(t *testing.T) {
	c := newTestSim(t)
	if err := c.Inject(context.Background(), "node2", FaultSilent); err != nil {
		t.Fatalf("Inject silent: %v", err)
	}
	if err := c.Set(ctxT(t, time.Second), "node2", "k", "lost"); err == nil {
		t.Fatal("Set via silent node succeeded")
	}
	mustSet(t, c, "node1", "k", "ok")
	eventually(t, testTimeout, "silent node2 still receives blocks", func() bool {
		v, ok := getVal(c, "node2", "k")
		return ok && v == "ok"
	})
}

func TestSimSlow(t *testing.T) {
	c, err := NewSim(SimConfig{SlowDelay: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewSim: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Inject(context.Background(), "node1", FaultSlow); err != nil {
		t.Fatalf("Inject slow: %v", err)
	}
	start := time.Now()
	mustSet(t, c, "node1", "k", "v")
	if d := time.Since(start); d < 200*time.Millisecond {
		t.Fatalf("Set via slow node took %v, want >= 2x SlowDelay", d)
	}
	if err := c.Heal(context.Background(), "node1"); err != nil {
		t.Fatalf("Heal slow: %v", err)
	}
}

func TestSimReset(t *testing.T) {
	c := newTestSim(t)
	mustSet(t, c, "node1", "k", "v")
	if err := c.Inject(context.Background(), "node2", FaultCorrupt); err != nil {
		t.Fatal(err)
	}
	if err := c.Inject(context.Background(), "node3", FaultCrash); err != nil {
		t.Fatal(err)
	}
	if err := c.Reset(context.Background()); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	for _, n := range c.Nodes() {
		if !n.Up || len(n.Faults) != 0 {
			t.Fatalf("after Reset %+v", n)
		}
		if _, ok, err := c.Get(context.Background(), n.Name, "k"); ok || err != nil {
			t.Fatalf("after Reset %s Get k ok=%v err=%v, want empty store", n.Name, ok, err)
		}
	}
	mustSet(t, c, "node2", "k", "fresh")
	eventually(t, testTimeout, "node2 has fresh honest value", func() bool {
		v, ok := getVal(c, "node2", "k")
		return ok && v == "fresh"
	})
}

func TestSimBadInput(t *testing.T) {
	c := newTestSim(t)
	if err := c.Set(context.Background(), "node9", "k", "v"); err == nil {
		t.Fatal("Set on unknown node succeeded")
	}
	if err := c.Inject(context.Background(), "node1", Fault("meteor")); err == nil {
		t.Fatal("Inject of unknown fault succeeded")
	}
}
