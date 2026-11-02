package core

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"hybrid/consensus/memnet"
)

// appliedLog is a mutex-guarded recorder for one node's OnApplied events.
type appliedLog struct {
	mu  sync.Mutex
	evs []AppliedEvent
}

func (l *appliedLog) add(ev AppliedEvent) {
	l.mu.Lock()
	l.evs = append(l.evs, ev)
	l.mu.Unlock()
}

func (l *appliedLog) snapshot() []AppliedEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]AppliedEvent(nil), l.evs...)
}

// newObservedCluster is newCluster with a separate OnApplied recorder per
// node (newCluster copies one cfg, and an event does not name its node).
func newObservedCluster(t *testing.T, cfg ServerConfig, n int) ([]*clusterNode, []*appliedLog) {
	t.Helper()
	hub := memnet.NewHub(n)
	nodes := make([]*clusterNode, n)
	logs := make([]*appliedLog, n)
	for i := range nodes {
		st := NewMapStore()
		l := newTestLedger(t)
		ex := NewExecutor(st, l, cfg.Mode, NoFault{})
		logs[i] = &appliedLog{}
		c := cfg
		c.NodeID = i + 1
		c.Signature = fmt.Sprintf("node%d", i+1)
		c.OnApplied = logs[i].add
		srv := NewServer(c, hub.Node(i), ex, l)
		t.Cleanup(func() { closeWithin(t, srv) })
		nodes[i] = &clusterNode{srv: srv, ex: ex, store: st}
	}
	return nodes, logs
}

func TestOnAppliedReportsEveryBlock(t *testing.T) {
	const sets = 5
	nodes, logs := newObservedCluster(t, testCfg(VersionClient, CommitSync), 3)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	for i := 0; i < sets; i++ {
		if _, err := nodes[0].srv.Set(ctx, setReq(fmt.Sprintf("k%d", i), "v", 1)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	for i, lg := range logs {
		eventually(t, srvTestTimeout, fmt.Sprintf("node%d to report %d blocks", i+1, sets), func() bool {
			return len(lg.snapshot()) >= sets
		})
	}
	for i, lg := range logs {
		evs := lg.snapshot()
		var txs int
		for j, ev := range evs {
			if want := int64(j + 1); ev.Height != want {
				t.Errorf("node%d event %d: Height = %d, want %d", i+1, j, ev.Height, want)
			}
			if ev.Proposer != "node1" {
				t.Errorf("node%d event %d: Proposer = %q, want node1", i+1, j, ev.Proposer)
			}
			if ev.Aborted != 0 {
				t.Errorf("node%d event %d: Aborted = %d, want 0", i+1, j, ev.Aborted)
			}
			txs += ev.Txs
		}
		if txs != sets {
			t.Errorf("node%d: sum of Txs = %d, want %d", i+1, txs, sets)
		}
		if last := evs[len(evs)-1]; !bytes.Equal(last.Root, nodes[i].ex.RootDigest()) {
			t.Errorf("node%d: last event Root %x != RootDigest %x", i+1, last.Root, nodes[i].ex.RootDigest())
		}
	}
}

func TestOnAppliedNilIsNoop(t *testing.T) {
	_, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	if _, err := nodes[0].srv.Set(ctx, setReq("k", "v", 1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
}
