package hub

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"hybrid/demo/cluster"
)

func newSim(t *testing.T) cluster.Cluster {
	t.Helper()
	c, err := cluster.NewSim(cluster.SimConfig{N: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func runHub(t *testing.T, h *Hub) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); h.Run(ctx) }()
	t.Cleanup(func() { cancel(); wg.Wait() })
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHubSnapshotFromSim(t *testing.T) {
	c := newSim(t)
	h := New(c, time.Now)
	runHub(t, h)
	h.SetTrackedKey("k")

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := c.Set(ctx, "node1", "k", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * time.Second)

	s := h.Snapshot()
	if len(s.Nodes) != 4 {
		t.Fatalf("nodes = %d, want 4", len(s.Nodes))
	}
	h0 := s.Nodes[0].Height
	if h0 == 0 {
		t.Fatalf("node1 height is 0: %+v", s.Nodes)
	}
	for _, n := range s.Nodes {
		if n.Status != "up" || n.Height != h0 || n.Group != "A" || n.Root == "" || n.Value != "v2" {
			t.Errorf("unexpected node view %+v (want up, height %d, group A, value v2)", n, h0)
		}
	}
	if s.Consistency.Verdict != "CONSISTENT" || s.Consistency.Agree != 4 || s.Consistency.Up != 4 {
		t.Errorf("consistency = %+v", s.Consistency)
	}
	if len(s.Blocks) == 0 {
		t.Error("no blocks in snapshot")
	}
	if s.TrackedKey != "k" || s.Info.N != 4 {
		t.Errorf("tracked key / info = %q / %+v", s.TrackedKey, s.Info)
	}
}

func TestHubJudgesCorruptNodeDivergent(t *testing.T) {
	c := newSim(t)
	h := New(c, time.Now)
	runHub(t, h)
	ctx := context.Background()
	if err := c.Inject(ctx, "node3", cluster.FaultCorrupt); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "node1", "k", "v"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "DIVERGENT verdict and node3 fault", 5*time.Second, func() bool {
		s := h.Snapshot() // faults are refreshed on the 1s tick
		return s.Consistency.Verdict == "DIVERGENT" && len(s.Nodes[2].Faults) == 1
	})
	s := h.Snapshot()
	if len(s.Consistency.Divergent) != 1 || s.Consistency.Divergent[0] != "node3" {
		t.Fatalf("divergent = %v, want [node3]", s.Consistency.Divergent)
	}
	for _, n := range s.Nodes {
		want := "up"
		if n.Name == "node3" {
			want = "divergent"
		}
		if n.Status != want {
			t.Errorf("%s status = %s, want %s", n.Name, n.Status, want)
		}
		if n.Name == "node3" && (len(n.Faults) != 1 || n.Faults[0] != "corrupt") {
			t.Errorf("node3 faults = %v", n.Faults)
		}
	}
}

func TestHubCrashedNodeIsDownNotDivergent(t *testing.T) {
	c := newSim(t)
	h := New(c, time.Now)
	runHub(t, h)
	ctx := context.Background()
	if err := c.Inject(ctx, "node4", cluster.FaultCrash); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "node1", "k", "v"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "node4 down", 5*time.Second, func() bool {
		s := h.Snapshot()
		return s.Nodes[3].Status == "down" && s.Consistency.Verdict == "CONSISTENT" && s.Consistency.Height > 0
	})
	s := h.Snapshot()
	if s.Nodes[3].Group != "" {
		t.Errorf("down node group = %q, want empty", s.Nodes[3].Group)
	}
	if len(s.Consistency.Down) != 1 || s.Consistency.Down[0] != "node4" || len(s.Consistency.Divergent) != 0 {
		t.Errorf("consistency = %+v", s.Consistency)
	}
}

func TestHubSlowSubscriberDropped(t *testing.T) {
	c := newSim(t)
	h := New(c, time.Now)
	runHub(t, h)
	_, unsub := h.Subscribe() // never read
	defer unsub()

	for i := 0; i < 200; i++ { // well past the subscriber's 64-slot buffer
		h.Logf("info", "line %d", i)
	}
	if err := c.Set(context.Background(), "node1", "k", "v"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "snapshot to advance", 3*time.Second, func() bool {
		s := h.Snapshot()
		return len(s.Blocks) > 0 && s.Nodes[0].Height > 0
	})
	if n := len(h.Snapshot().Log); n < 200 {
		t.Errorf("log has %d lines, want at least 200 (hub must keep logging)", n)
	}
}

func TestHubSubscribeReceivesUpdates(t *testing.T) {
	c := newSim(t)
	h := New(c, time.Now)
	runHub(t, h)
	ch, unsub := h.Subscribe()
	h.Logf("warn", "hello %d", 1)
	h.Mark("crash node2")
	h.SetLoad(LoadState{Running: true, Clients: 4, Workload: "write"})

	seen := map[string]bool{}
	timeout := time.After(3 * time.Second)
	for !(seen["log"] && seen["load"] && seen["nodes"] && seen["consistency"] && seen["metrics"]) {
		select {
		case u := <-ch:
			seen[u.Kind] = true
			if u.Kind == "log" {
				if l, ok := u.Data.(LogLine); !ok || l.Level != "warn" || l.Text != "hello 1" {
					t.Errorf("log update = %#v", u.Data)
				}
			}
		case <-timeout:
			t.Fatalf("missing update kinds, saw %v", seen)
		}
	}
	unsub()
	unsub() // idempotent
}

func TestHubResetClearsState(t *testing.T) {
	c := newSim(t)
	h := New(c, time.Now)
	runHub(t, h)
	ctx := context.Background()
	h.SetTrackedKey("k")
	h.SetStep(3)
	h.SetLoad(LoadState{Running: true, Clients: 2})
	h.RecordOp(time.Millisecond, nil)
	h.Mark("crash")
	for i := 0; i < 2; i++ {
		if err := c.Set(ctx, "node1", "k", "v"); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "blocks", 3*time.Second, func() bool { return len(h.Snapshot().Blocks) > 0 })

	if err := c.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	h.Reset()

	s := h.Snapshot()
	if len(s.Blocks) != 0 {
		t.Errorf("blocks after reset: %+v", s.Blocks)
	}
	for _, n := range s.Nodes {
		if n.Height != 0 || n.Root != "" || n.Group != "" {
			t.Errorf("node not cleared: %+v", n)
		}
	}
	if s.TrackedKey != "" || s.Step != 0 || s.Load.Running || len(s.Metrics.Markers) != 0 || s.Metrics.Errors != 0 {
		t.Errorf("state not cleared: key=%q step=%d load=%+v metrics=%+v", s.TrackedKey, s.Step, s.Load, s.Metrics)
	}
	if len(s.Log) != 1 || s.Log[0].Text != "Cluster reset" {
		t.Errorf("log = %+v, want one 'Cluster reset' line", s.Log)
	}

	if err := c.Set(ctx, "node1", "k2", "v"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "CONSISTENT after reset", 5*time.Second, func() bool {
		s := h.Snapshot()
		return s.Consistency.Verdict == "CONSISTENT" && s.Consistency.Height > 0 && s.Consistency.Agree == 4
	})
	for _, n := range h.Snapshot().Nodes {
		if n.Height > 3 {
			t.Errorf("%s height %d: stale pre-reset events leaked in", n.Name, n.Height)
		}
	}
}
