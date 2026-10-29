package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"hybrid/consensus/memnet"
	pbv "hybrid/proto/veritas"
)

// srvTestTimeout bounds every call that could hang, so a regression fails
// fast instead of running into go test's 10-minute timeout.
const srvTestTimeout = 5 * time.Second

type clusterNode struct {
	srv   *Server
	ex    *Executor
	store KVStore
}

// testCfg: block size 1 (every Set is its own block) and a short flush, so
// tests do not wait for the 1 s default timer.
func testCfg(mode VersionMode, commit CommitMode) ServerConfig {
	return ServerConfig{BlockSize: 1, Mode: mode, Commit: commit, FlushInterval: 20 * time.Millisecond}
}

func mapStores(n int) []KVStore {
	out := make([]KVStore, n)
	for i := range out {
		out[i] = NewMapStore()
	}
	return out
}

// newCluster starts one Server per store on a single memnet hub. Node i has
// NodeID i+1 and Signature "node<i+1>", and its own ledger in t.TempDir().
// Servers are closed in Cleanup before their ledgers.
func newCluster(t *testing.T, cfg ServerConfig, stores ...KVStore) (*memnet.Hub, []*clusterNode) {
	t.Helper()
	hub := memnet.NewHub(len(stores))
	nodes := make([]*clusterNode, len(stores))
	for i, st := range stores {
		l := newTestLedger(t) // registers l.Close before the server's Close
		ex := NewExecutor(st, l, cfg.Mode, NoFault{})
		c := cfg
		c.NodeID = i + 1
		c.Signature = fmt.Sprintf("node%d", i+1)
		srv := NewServer(c, hub.Node(i), ex, l)
		t.Cleanup(func() { closeWithin(t, srv) })
		nodes[i] = &clusterNode{srv: srv, ex: ex, store: st}
	}
	return hub, nodes
}

func closeWithin(t *testing.T, s *Server) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		s.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(srvTestTimeout):
		t.Errorf("Server.Close did not return within %v", srvTestTimeout)
	}
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func getValue(s *Server, key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	resp, err := s.Get(ctx, &pbv.GetRequest{Key: key})
	if err != nil {
		return "", err
	}
	return resp.GetValue(), nil
}

func setReq(key, val string, ver int64) *pbv.SetRequest {
	return &pbv.SetRequest{Key: key, Value: val, Version: ver}
}

// waitSameBlocks waits until every node has applied the same number of
// blocks (> 0) for two polls in a row. Call it only once no more blocks
// will be submitted.
func waitSameBlocks(t *testing.T, nodes []*clusterNode) int64 {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	prev := int64(-1)
	for {
		b0 := nodes[0].srv.Breakdown().Blocks
		same := b0 > 0
		for _, n := range nodes[1:] {
			if n.srv.Breakdown().Blocks != b0 {
				same = false
			}
		}
		if same && b0 == prev {
			return b0
		}
		if same {
			prev = b0
		} else {
			prev = -1
		}
		if time.Now().After(deadline) {
			for i, n := range nodes {
				t.Logf("node%d blocks=%d", i+1, n.srv.Breakdown().Blocks)
			}
			t.Fatalf("nodes did not converge on the same block count")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---- required tests (brief) ----

func TestSyncSetVisibleOnAllNodesAfterReturn(t *testing.T) {
	_, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()

	if _, err := nodes[0].srv.Set(ctx, setReq("k", "v", 1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// On the submitting node the write is applied before Set returns.
	if v, err := getValue(nodes[0].srv, "k"); err != nil || v != "v" {
		t.Fatalf("node1 Get = %q, %v; want \"v\"", v, err)
	}
	// The other nodes received the same block in the same broadcast step and
	// apply it on their own deliver loops, so they see it shortly after.
	for i, n := range nodes[1:] {
		n := n
		eventually(t, 2*time.Second, fmt.Sprintf("node%d to see k=v", i+2), func() bool {
			v, err := getValue(n.srv, "k")
			return err == nil && v == "v"
		})
	}
}

func TestAsyncSetReturnsBeforeCommit(t *testing.T) {
	hub, nodes := newCluster(t, testCfg(VersionClient, CommitAsync), mapStores(3)...)
	for i := range nodes {
		hub.Drop(i, true) // no node ever applies anything
	}
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()

	start := time.Now()
	if _, err := nodes[0].srv.Set(ctx, setReq("k", "v", 1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if el := time.Since(start); el > 100*time.Millisecond {
		t.Fatalf("async Set took %v, want <= 100ms", el)
	}
	// The block was flushed and submitted, but never applied.
	eventually(t, 2*time.Second, "the block to be flushed", func() bool {
		return nodes[0].srv.Breakdown().BatchBlocks == 1
	})
	for i, n := range nodes {
		if _, err := getValue(n.srv, "k"); status.Code(err) != codes.NotFound {
			t.Fatalf("node%d Get err = %v, want NotFound (nothing committed)", i+1, err)
		}
		if b := n.srv.Breakdown().Blocks; b != 0 {
			t.Fatalf("node%d applied %d blocks, want 0", i+1, b)
		}
	}
}

// Review Focus #2: a sync Set whose SharedLog is aborted must return
// ErrAborted, not hang.
func TestSyncSetReturnsErrAbortedOnStaleVersion(t *testing.T) {
	_, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()

	if _, err := nodes[0].srv.Set(ctx, setReq("k", "new", 10)); err != nil {
		t.Fatalf("Set v10: %v", err)
	}

	for _, tc := range []struct {
		node int
		ver  int64
	}{{1, 5}, {2, 10}} { // older version; equal version (also stale)
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		_, err := nodes[tc.node].srv.Set(sctx, setReq("k", "stale", tc.ver))
		el := time.Since(start)
		scancel()
		if !errors.Is(err, ErrAborted) {
			t.Fatalf("node%d Set v%d: err = %v, want ErrAborted", tc.node+1, tc.ver, err)
		}
		if status.Code(err) != codes.Aborted {
			t.Fatalf("gRPC code = %v, want Aborted", status.Code(err))
		}
		if el >= 2*time.Second {
			t.Fatalf("aborted Set took %v", el)
		}
	}
	for i, n := range nodes {
		n := n
		eventually(t, 2*time.Second, fmt.Sprintf("node%d k=new", i+1), func() bool {
			v, err := getValue(n.srv, "k")
			return err == nil && v == "new"
		})
		if c := n.srv.waiters.count(); c != 0 {
			t.Fatalf("node%d has %d waiters left", i+1, c)
		}
	}
}

func TestBatcherFlushesOnTimer(t *testing.T) {
	type flushed struct {
		at  time.Time
		blk *pbv.Block
	}
	ch := make(chan flushed, 4)
	start := time.Now()
	b := NewBatcher(100, 0, func(blk *pbv.Block) error { // 0 = default 1 s
		ch <- flushed{time.Now(), blk}
		return nil
	})
	defer b.Close()
	b.Add(&pbv.SharedLog{Seq: 7, Sets: []*pbv.SetRequest{setReq("k", "v", 7)}})

	select {
	case f := <-ch:
		if el := f.at.Sub(start); el < 900*time.Millisecond || el > 2*time.Second {
			t.Fatalf("flushed after %v, want ~1s", el)
		}
		if len(f.blk.GetTxs()) != 1 || f.blk.Txs[0].GetSeq() != 7 {
			t.Fatalf("block = %v, want one SharedLog with seq 7", f.blk)
		}
		if f.blk.GetType() != pbv.MessageType_Approve {
			t.Fatalf("block type = %v, want Approve", f.blk.GetType())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("block not flushed by the timer within 3s")
	}
}

func TestVerifyRootMatchesAcrossNodes(t *testing.T) {
	for _, mode := range []VersionMode{VersionClient, VersionOrdered} {
		mode := mode
		t.Run(map[VersionMode]string{VersionClient: "client", VersionOrdered: "ordered"}[mode], func(t *testing.T) {
			cfg := testCfg(mode, CommitSync)
			cfg.BlockSize = 5
			_, nodes := newCluster(t, cfg, mapStores(3)...)
			ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
			defer cancel()

			var wg sync.WaitGroup
			errs := make(chan error, 50)
			for i := 0; i < 50; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					req := setReq(fmt.Sprintf("k%d", i%10), fmt.Sprintf("v%d", i), int64(i+1))
					_, err := nodes[i%3].srv.Set(ctx, req)
					// Concurrent client versions may legitimately abort.
					if err != nil && !(mode == VersionClient && errors.Is(err, ErrAborted)) {
						errs <- fmt.Errorf("Set %d on node%d: %w", i, i%3+1, err)
					}
				}(i)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			if t.Failed() {
				t.FailNow()
			}

			waitSameBlocks(t, nodes)
			var first *pbv.VerifyResponse
			for i, n := range nodes {
				resp, err := n.srv.Verify(ctx, &pbv.VerifyRequest{Key: "k0"})
				if err != nil {
					t.Fatalf("node%d Verify: %v", i+1, err)
				}
				if len(resp.GetRootDigest()) == 0 || len(resp.GetSideNodes()) == 0 {
					t.Fatalf("node%d: empty root or proof: %v", i+1, resp)
				}
				if !bytes.Equal(resp.GetRootDigest(), n.ex.RootDigest()) {
					t.Fatalf("node%d: Verify root != executor root", i+1)
				}
				if first == nil {
					first = resp
					continue
				}
				if !bytes.Equal(resp.GetRootDigest(), first.GetRootDigest()) {
					t.Fatalf("node%d root %x != node1 root %x", i+1, resp.GetRootDigest(), first.GetRootDigest())
				}
				if !proto.Equal(resp, first) {
					t.Fatalf("node%d proof differs from node1's", i+1)
				}
			}
		})
	}
}

// ---- additional tests ----

func TestBatcherSizeFlushAndCloseFlushesRemainder(t *testing.T) {
	ch := make(chan *pbv.Block, 4)
	b := NewBatcher(3, time.Hour, func(blk *pbv.Block) error {
		ch <- blk
		return nil
	})
	recvBlk := func() *pbv.Block {
		t.Helper()
		select {
		case blk := <-ch:
			return blk
		case <-time.After(srvTestTimeout):
			t.Fatal("no block")
			return nil
		}
	}
	for seq := int64(1); seq <= 5; seq++ {
		b.Add(&pbv.SharedLog{Seq: seq})
	}
	blk := recvBlk()
	if got := len(blk.GetTxs()); got != 3 {
		t.Fatalf("size flush: %d txs, want 3", got)
	}
	for i, sl := range blk.GetTxs() {
		if sl.GetSeq() != int64(i+1) {
			t.Fatalf("tx %d has seq %d, want %d (Add order)", i, sl.GetSeq(), i+1)
		}
	}
	done := make(chan struct{})
	go func() { b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(srvTestTimeout):
		t.Fatal("Close hung")
	}
	if blk := recvBlk(); len(blk.GetTxs()) != 2 || blk.Txs[0].GetSeq() != 4 {
		t.Fatalf("close flush = %v, want seqs 4,5", blk)
	}
	b.Add(&pbv.SharedLog{Seq: 6}) // dropped, must not block
	b.Close()                     // idempotent
	select {
	case blk := <-ch:
		t.Fatalf("unexpected block after Close: %v", blk)
	default:
	}
}

// A sync Set whose ctx ends (or whose block never arrives) must return and
// leave no waiter behind.
func TestSyncSetCtxCancelRemovesWaiter(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		hub, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
		hub.Drop(0, true) // node1 never sees its own block
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := nodes[0].srv.Set(ctx, setReq("k", "v", 1))
		if !errors.Is(err, context.DeadlineExceeded) || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("err = %v (code %v), want context.DeadlineExceeded", err, status.Code(err))
		}
		if el := time.Since(start); el > time.Second {
			t.Fatalf("Set returned after %v", el)
		}
		if c := nodes[0].srv.waiters.count(); c != 0 {
			t.Fatalf("%d waiters left", c)
		}
		// The block was not lost: the other nodes applied it.
		eventually(t, 2*time.Second, "node2 to apply", func() bool {
			v, err := getValue(nodes[1].srv, "k")
			return err == nil && v == "v"
		})
	})
	t.Run("cancel", func(t *testing.T) {
		hub, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
		hub.Drop(0, true)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		res := make(chan error, 1)
		go func() {
			_, err := nodes[0].srv.Set(ctx, setReq("k", "v", 1))
			res <- err
		}()
		select {
		case err := <-res:
			if !errors.Is(err, context.Canceled) || status.Code(err) != codes.Canceled {
				t.Fatalf("err = %v (code %v), want context.Canceled", err, status.Code(err))
			}
		case <-time.After(srvTestTimeout):
			t.Fatal("Set did not honour ctx cancel")
		}
		if c := nodes[0].srv.waiters.count(); c != 0 {
			t.Fatalf("%d waiters left", c)
		}
	})
	t.Run("sync timeout (lost block)", func(t *testing.T) {
		cfg := testCfg(VersionClient, CommitSync)
		cfg.SyncTimeout = 150 * time.Millisecond
		hub, nodes := newCluster(t, cfg, mapStores(3)...)
		hub.Drop(0, true)
		ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
		defer cancel()
		_, err := nodes[0].srv.Set(ctx, setReq("k", "v", 1))
		if !errors.Is(err, ErrCommitTimeout) || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("err = %v (code %v), want ErrCommitTimeout/DeadlineExceeded", err, status.Code(err))
		}
		if c := nodes[0].srv.waiters.count(); c != 0 {
			t.Fatalf("%d waiters left", c)
		}
	})
}

func TestSetAfterStopFailsFast(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		hub, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
		hub.Drop(0, true)
		// A sync Set in flight when Close is called must be released.
		inflight := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
			defer cancel()
			_, err := nodes[0].srv.Set(ctx, setReq("a", "v", 1))
			inflight <- err
		}()
		eventually(t, 2*time.Second, "the in-flight waiter", func() bool {
			return nodes[0].srv.waiters.count() == 1
		})
		closeWithin(t, nodes[0].srv)
		select {
		case err := <-inflight:
			if !errors.Is(err, ErrStopped) {
				t.Fatalf("in-flight Set err = %v, want ErrStopped", err)
			}
		case <-time.After(time.Second):
			t.Fatal("in-flight Set not released by Close")
		}

		ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
		defer cancel()
		start := time.Now()
		_, err := nodes[0].srv.Set(ctx, setReq("b", "v", 2))
		if !errors.Is(err, ErrStopped) || status.Code(err) != codes.Unavailable {
			t.Fatalf("Set after Close: err = %v (code %v), want ErrStopped/Unavailable", err, status.Code(err))
		}
		if el := time.Since(start); el > 100*time.Millisecond {
			t.Fatalf("Set after Close took %v", el)
		}
		if _, err := getValue(nodes[0].srv, "a"); !errors.Is(err, ErrStopped) {
			t.Fatalf("Get after Close: err = %v, want ErrStopped", err)
		}
		if c := nodes[0].srv.waiters.count(); c != 0 {
			t.Fatalf("%d waiters left", c)
		}
	})
	t.Run("apply error", func(t *testing.T) {
		stores := []KVStore{failStore{NewMapStore()}, NewMapStore(), NewMapStore()}
		_, nodes := newCluster(t, testCfg(VersionClient, CommitSync), stores...)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := nodes[0].srv.Set(ctx, setReq("k", "v", 1))
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("Set on failing node: err = %v, want ErrStopped", err)
		}
		start := time.Now()
		if _, err := nodes[0].srv.Set(ctx, setReq("k", "v", 2)); !errors.Is(err, ErrStopped) {
			t.Fatalf("second Set: err = %v, want ErrStopped", err)
		}
		if el := time.Since(start); el > 100*time.Millisecond {
			t.Fatalf("Set on stopped node took %v", el)
		}
		if c := nodes[0].srv.waiters.count(); c != 0 {
			t.Fatalf("%d waiters left", c)
		}
		// The stopped node must not wedge the rest of the cluster.
		if _, err := nodes[1].srv.Set(ctx, setReq("k2", "v2", 3)); err != nil {
			t.Fatalf("Set on healthy node2: %v", err)
		}
	})
}

func TestWaiterSetResolveCancelFailAll(t *testing.T) {
	ws := newWaiterSet()
	errX := errors.New("x")
	w1, _ := ws.add(7)
	w2, _ := ws.add(8)
	ready := func(w *waiter) (error, bool) {
		select {
		case err := <-w.ch:
			return err, true
		default:
			return nil, false
		}
	}

	ws.resolve(7, errX)
	if err, ok := ready(w1); !ok || err != errX {
		t.Fatalf("w1 = %v, %v; want errX", err, ok)
	}
	if _, ok := ready(w2); ok {
		t.Fatal("w2 resolved by seq 7")
	}
	ws.resolve(7, nil)  // already resolved (duplicate delivery): no-op
	ws.resolve(99, nil) // no waiter: no-op
	ws.remove(7, w1)    // already removed: no-op
	if c := ws.count(); c != 1 {
		t.Fatalf("count = %d, want 1", c)
	}
	if _, ok := ws.cancel(8, w2); ok {
		t.Fatal("w2 had a result")
	}
	if c := ws.count(); c != 0 {
		t.Fatalf("count = %d, want 0", c)
	}

	w3, _ := ws.add(9)
	errY := errors.New("y")
	ws.failAll(errY)
	if err, ok := ready(w3); !ok || err != errY {
		t.Fatalf("w3 = %v, %v; want errY", err, ok)
	}
	if _, err := ws.add(10); err != errY {
		t.Fatalf("add after failAll: %v, want errY", err)
	}
}

// Two concurrent sync Sets with the SAME key and SAME client version on one
// node land in one block (size 2, no timer flush): the first SharedLog
// commits, the second is stale (equal version) and aborts. Each Set must
// get its own result: exactly one nil and one ErrAborted, and the nil one
// is the value that was stored.
func TestSyncSetsSameVersionGetDistinctResults(t *testing.T) {
	cfg := testCfg(VersionClient, CommitSync)
	cfg.BlockSize = 2
	cfg.FlushInterval = time.Hour // only the size flush can fire
	_, nodes := newCluster(t, cfg, mapStores(3)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()

	type result struct {
		val string
		err error
	}
	res := make(chan result, 2)
	for _, val := range []string{"a", "b"} {
		go func(val string) {
			_, err := nodes[0].srv.Set(ctx, setReq("k", val, 42))
			res <- result{val, err}
		}(val)
	}
	var committed []string
	aborted := 0
	for i := 0; i < 2; i++ {
		r := <-res
		switch {
		case r.err == nil:
			committed = append(committed, r.val)
		case errors.Is(r.err, ErrAborted):
			aborted++
		default:
			t.Fatalf("Set %s: %v", r.val, r.err)
		}
	}
	if len(committed) != 1 || aborted != 1 {
		t.Fatalf("committed %v, aborted %d; want exactly one of each", committed, aborted)
	}
	if v, err := getValue(nodes[0].srv, "k"); err != nil || v != committed[0] {
		t.Fatalf("Get k = %q, %v; want the committed value %q", v, err, committed[0])
	}
	if b := nodes[0].srv.Breakdown().Blocks; b != 1 {
		t.Fatalf("applied %d blocks, want 1 (both Sets in one block)", b)
	}
	if c := nodes[0].srv.waiters.count(); c != 0 {
		t.Fatalf("%d waiters left", c)
	}
}

func TestBreakdownMeasuresOwnBlocks(t *testing.T) {
	_, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	for v := int64(1); v <= 3; v++ {
		if _, err := nodes[0].srv.Set(ctx, setReq(fmt.Sprintf("k%d", v), "v", v)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	b := nodes[0].srv.Breakdown()
	if b.BatchBlocks != 3 || b.ConsensusBlocks != 3 || b.QueueBlocks != 3 || b.Blocks != 3 {
		t.Fatalf("node1 breakdown = %+v, want 3 batched/consensus/queued/applied blocks", b)
	}
	if b.Execute <= 0 || b.Ledger <= 0 || b.Batch < 0 || b.Consensus < 0 || b.Queue < 0 {
		t.Fatalf("node1 breakdown durations = %+v", b)
	}
	waitSameBlocks(t, nodes)
	b2 := nodes[1].srv.Breakdown()
	if b2.BatchBlocks != 0 || b2.ConsensusBlocks != 0 || b2.QueueBlocks != 3 || b2.Blocks != 3 ||
		b2.Execute <= 0 || b2.Ledger <= 0 {
		t.Fatalf("node2 breakdown = %+v, want only 3 queued/applied blocks", b2)
	}

	path := filepath.Join(t.TempDir(), "breakdown-node1.json")
	if err := nodes[0].srv.WriteBreakdownJSON(path); err != nil {
		t.Fatalf("WriteBreakdownJSON: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0644 {
		t.Fatalf("breakdown file mode %v, want 0644", perm)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("bad JSON %s: %v", data, err)
	}
	for _, k := range []string{"node", "batch_ns", "consensus_ns", "queue_ns", "execute_ns", "ledger_ns",
		"blocks", "batch_blocks", "consensus_blocks", "queue_blocks"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("JSON %s lacks %q", data, k)
		}
	}
	if got["node"] != float64(1) || got["blocks"] != float64(3) {
		t.Fatalf("JSON %s: want node 1, blocks 3", data)
	}
}

// SharedLog.Seq is NodeID<<40 | counter in both version modes.
func TestSeqIsNodeScoped(t *testing.T) {
	for _, mode := range []VersionMode{VersionClient, VersionOrdered} {
		_, nodes := newCluster(t, testCfg(mode, CommitSync), mapStores(3)...)
		s := nodes[2].srv // NodeID 3
		if got, want := s.nextSeq(), int64(3)<<40|1; got != want {
			t.Fatalf("mode %d: seq = %#x, want %#x", mode, got, want)
		}
		if got, want := s.nextSeq(), int64(3)<<40|2; got != want {
			t.Fatalf("mode %d: seq = %#x, want %#x", mode, got, want)
		}
	}
}

func TestNewServerRejectsBadConfig(t *testing.T) {
	ex, _ := newTestExecutor(t, VersionClient, nil)
	good := ServerConfig{NodeID: 1, Signature: "node1", Mode: VersionClient, Commit: CommitSync}
	for _, tc := range []struct {
		name string
		edit func(*ServerConfig)
	}{
		{"empty signature", func(c *ServerConfig) { c.Signature = "" }},
		{"negative node id", func(c *ServerConfig) { c.NodeID = -1 }},
		{"node id too large", func(c *ServerConfig) { c.NodeID = 1 << 23 }},
		{"unknown commit mode", func(c *ServerConfig) { c.Commit = 7 }},
		{"mode mismatch", func(c *ServerConfig) { c.Mode = VersionOrdered }},
	} {
		cfg := good
		tc.edit(&cfg)
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: NewServer did not panic", tc.name)
				}
			}()
			NewServer(cfg, memnet.NewHub(1).Node(0), ex, nil)
		}()
	}
}

// A block delivered twice (e.g. a retried Submit) is applied twice, which is
// deterministic; waking its already-resolved waiters is a no-op.
func TestDuplicateDeliveryIsHarmless(t *testing.T) {
	hub, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(3)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	if _, err := nodes[0].srv.Set(ctx, setReq("k", "v1", 1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	raw, err := proto.Marshal(&pbv.Block{
		Txs:       []*pbv.SharedLog{{Seq: int64(1)<<40 | 1, Sets: []*pbv.SetRequest{setReq("k", "v1", 1)}}},
		Type:      pbv.MessageType_Approve,
		Signature: "node1",
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := hub.Node(1).Submit(ctx, raw); err != nil {
			t.Fatalf("replay Submit: %v", err)
		}
	}
	if _, err := nodes[0].srv.Set(ctx, setReq("k2", "v2", 2)); err != nil {
		t.Fatalf("Set after replay: %v", err)
	}
	if n := waitSameBlocks(t, nodes); n != 4 {
		t.Fatalf("applied %d blocks, want 4", n)
	}
	for i, n := range nodes {
		if err := n.srv.stoppedErr(); err != nil {
			t.Fatalf("node%d stopped: %v", i+1, err)
		}
		if !bytes.Equal(n.ex.RootDigest(), nodes[0].ex.RootDigest()) {
			t.Fatalf("node%d root differs", i+1)
		}
		if v, err := getValue(n.srv, "k"); err != nil || v != "v1" {
			t.Fatalf("node%d k = %q, %v", i+1, v, err)
		}
	}
}
