package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// testTimeouts sets raft's timers for the tests: raft's defaults (1 s
// heartbeat and election, a 500 ms lease below the heartbeat as
// ValidateConfig requires), so that a slow -race run does not cause
// spurious elections (which may lose or duplicate accepted blocks), and
// quieter logs.
func testTimeouts(c *hraft.Config) {
	c.HeartbeatTimeout = time.Second
	c.ElectionTimeout = time.Second
	c.LeaderLeaseTimeout = 500 * time.Millisecond
	c.CommitTimeout = 50 * time.Millisecond
	c.LogLevel = "WARN"
}

// freeAddr returns a localhost TCP address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	a := l.Addr().String()
	l.Close()
	return a
}

// startCluster starts n nodes over connected in-memory transports, each with
// a localhost forward address and in-memory raft stores (DataDir ""; see
// raft_bolt_test.go for why bolt is not used here). node1 (index 0)
// bootstraps the full peer set. All nodes are closed at the end of the test.
func startCluster(t *testing.T, n int) []*Raft {
	t.Helper()
	return startClusterStores(t, n, false)
}

// startClusterStores is startCluster; with bolt each node keeps its raft
// log in a bolt store (and its snapshots in a file store) in a temp dir.
func startClusterStores(t *testing.T, n int, bolt bool) []*Raft {
	t.Helper()
	addrs := make([]hraft.ServerAddress, n)
	trans := make([]*hraft.InmemTransport, n)
	for i := range trans {
		addrs[i], trans[i] = hraft.NewInmemTransport("")
	}
	for i := range trans {
		for j := range trans {
			if i != j {
				trans[i].Connect(addrs[j], trans[j])
			}
		}
	}
	peers := make([]Peer, n)
	for i := range peers {
		peers[i] = Peer{ID: fmt.Sprintf("node%d", i+1), RaftAddr: string(addrs[i]), ForwardAddr: freeAddr(t)}
	}
	nodes := make([]*Raft, n)
	for i := range nodes {
		dir := ""
		if bolt {
			dir = t.TempDir()
		}
		r, err := New(Config{
			ID:         peers[i].ID,
			DataDir:    dir,
			Peers:      peers,
			Bootstrap:  i == 0,
			Transport:  trans[i],
			raftConfig: testTimeouts,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", peers[i].ID, err)
		}
		nodes[i] = r
	}
	t.Cleanup(func() {
		for _, r := range nodes {
			r.Close()
		}
	})
	return nodes
}

// waitLeader returns the index of the node that is leader, among the nodes
// not in skip, waiting up to 10 s.
func waitLeader(t *testing.T, nodes []*Raft, skip int) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, r := range nodes {
			if i != skip && r.raft.State() == hraft.Leader {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within 10s")
	return -1
}

// recvUntil reads r's Deliver until it got a block equal to want, returning
// every block read (want included). It fails the test after timeout.
func recvUntil(t *testing.T, r *Raft, want []byte, timeout time.Duration) [][]byte {
	t.Helper()
	var got [][]byte
	tm := time.NewTimer(timeout)
	defer tm.Stop()
	for {
		select {
		case b, ok := <-r.Deliver():
			if !ok {
				t.Fatalf("%s: Deliver closed before %q arrived (got %q)", r.self.ID, want, got)
			}
			got = append(got, b)
			if bytes.Equal(b, want) {
				return got
			}
		case <-tm.C:
			t.Fatalf("%s: %q not delivered within %v (got %q)", r.self.ID, want, timeout, got)
		}
	}
}

// recvN reads exactly n blocks from r's Deliver within timeout.
func recvN(t *testing.T, r *Raft, n int, timeout time.Duration) [][]byte {
	t.Helper()
	got := make([][]byte, 0, n)
	tm := time.NewTimer(timeout)
	defer tm.Stop()
	for len(got) < n {
		select {
		case b, ok := <-r.Deliver():
			if !ok {
				t.Fatalf("%s: Deliver closed after %d of %d blocks", r.self.ID, len(got), n)
			}
			got = append(got, b)
		case <-tm.C:
			t.Fatalf("%s: only %d of %d blocks within %v", r.self.ID, len(got), n, timeout)
		}
	}
	return got
}

func TestRaftSameOrderAllNodes(t *testing.T) {
	nodes := startCluster(t, 3)
	const perNode = 10
	// Every node submits concurrently, so leader and followers (forwarded)
	// proposals interleave.
	var wg sync.WaitGroup
	errs := make(chan error, len(nodes)*perNode)
	sent := make(map[string]bool)
	for i, r := range nodes {
		for k := 0; k < perNode; k++ {
			sent[fmt.Sprintf("blk-%d-%d", i, k)] = true
		}
		wg.Add(1)
		go func(i int, r *Raft) {
			defer wg.Done()
			buf := make([]byte, 0, 32)
			for k := 0; k < perNode; k++ {
				buf = append(buf[:0], fmt.Sprintf("blk-%d-%d", i, k)...)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := r.Submit(ctx, buf)
				cancel()
				if err != nil {
					errs <- fmt.Errorf("node%d Submit %d: %v", i+1, k, err)
				}
				// Submit must not retain buf: scribble over it.
				for j := range buf {
					buf[j] = 'X'
				}
			}
		}(i, r)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	total := len(nodes) * perNode
	seqs := make([][][]byte, len(nodes))
	for i, r := range nodes {
		seqs[i] = recvN(t, r, total, 10*time.Second)
	}
	seen := make(map[string]bool)
	for _, b := range seqs[0] {
		if !sent[string(b)] {
			t.Errorf("node1 delivered %q, which nobody submitted", b)
		}
		if seen[string(b)] {
			t.Errorf("node1 delivered %q twice", b)
		}
		seen[string(b)] = true
	}
	if len(seen) != total {
		t.Errorf("node1 delivered %d distinct blocks, want %d", len(seen), total)
	}
	for i := 1; i < len(nodes); i++ {
		for k := range seqs[0] {
			if !bytes.Equal(seqs[i][k], seqs[0][k]) {
				t.Fatalf("node%d block %d = %q, node1 has %q", i+1, k, seqs[i][k], seqs[0][k])
			}
		}
	}
}

func TestRaftSubmitOnFollowerIsForwarded(t *testing.T) {
	nodes := startCluster(t, 3)
	li := waitLeader(t, nodes, -1)
	fi := (li + 1) % len(nodes)
	if nodes[fi].raft.State() == hraft.Leader {
		t.Fatalf("node%d unexpectedly leader too", fi+1)
	}
	before := atomic.LoadInt64(&nodes[li].forwardsServed)

	want := []byte("from-follower")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := nodes[fi].Submit(ctx, want); err != nil {
		t.Fatalf("Submit on follower node%d: %v", fi+1, err)
	}
	for _, r := range nodes {
		got := recvUntil(t, r, want, 10*time.Second)
		if len(got) != 1 {
			t.Errorf("%s: delivered %q before the forwarded block", r.self.ID, got[:len(got)-1])
		}
	}
	if n := atomic.LoadInt64(&nodes[li].forwardsServed) - before; n < 1 {
		// Leadership may have moved between waitLeader and Submit; then
		// another node served the forward. Count over all nodes.
		var all int64
		for _, r := range nodes {
			all += atomic.LoadInt64(&r.forwardsServed)
		}
		if all < 1 {
			t.Errorf("no node served a forwarded submit")
		}
	}
}

func TestRaftSurvivesLeaderShutdown(t *testing.T) {
	nodes := startCluster(t, 3)
	li := waitLeader(t, nodes, -1)

	first := []byte("before-shutdown")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := nodes[li].Submit(ctx, first); err != nil {
		t.Fatalf("Submit on leader: %v", err)
	}
	for _, r := range nodes {
		recvUntil(t, r, first, 10*time.Second)
	}

	if err := nodes[li].Close(); err != nil {
		t.Logf("closing leader node%d: %v", li+1, err)
	}
	start := time.Now()
	deadline := start.Add(10 * time.Second)
	survivor := nodes[(li+1)%len(nodes)]
	after := []byte("after-shutdown")
	var err error
	for time.Now().Before(deadline) {
		sctx, scancel := context.WithDeadline(context.Background(), deadline)
		err = survivor.Submit(sctx, after)
		scancel()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Submit on %s after leader shutdown: still failing after 10s: %v", survivor.self.ID, err)
	}
	t.Logf("Submit accepted %v after the leader shut down", time.Since(start))

	var seqs [][][]byte
	for i, r := range nodes {
		if i == li {
			continue
		}
		seqs = append(seqs, recvUntil(t, r, after, 10*time.Second))
	}
	// Same order on both survivors, up to the first copy of the block (a
	// failed Submit attempt may still have been accepted: duplicates later).
	if len(seqs[0]) != len(seqs[1]) {
		t.Fatalf("survivors disagree: %q vs %q", seqs[0], seqs[1])
	}
	for k := range seqs[0] {
		if !bytes.Equal(seqs[0][k], seqs[1][k]) {
			t.Fatalf("survivors disagree at %d: %q vs %q", k, seqs[0], seqs[1])
		}
	}
}

func TestRaftCloseIsIdempotentAndSubmitAfterCloseFails(t *testing.T) {
	nodes := startCluster(t, 3)
	waitLeader(t, nodes, -1)

	// Submits racing with Close must all return (with or without error).
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for _, r := range nodes {
		wg.Add(1)
		go func(r *Raft) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				r.Submit(ctx, []byte("racing"))
				cancel()
			}
		}(r)
	}
	// Drain Deliver so FSM Apply never blocks on a full channel.
	for _, r := range nodes {
		go func(r *Raft) {
			for range r.Deliver() {
			}
		}(r)
	}
	time.Sleep(200 * time.Millisecond)
	for _, r := range nodes {
		if err := r.Close(); err != nil {
			t.Logf("%s Close: %v", r.self.ID, err)
		}
	}
	done := make(chan struct{})
	go func() {
		close(stop)
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("Submit concurrent with Close did not return")
	}

	for _, r := range nodes {
		r.Close() // second Close: no panic
		err := r.Submit(context.Background(), []byte("late"))
		if !errors.Is(err, ErrClosed) {
			t.Errorf("%s: Submit after Close = %v, want ErrClosed", r.self.ID, err)
		}
		select {
		case _, ok := <-r.Deliver():
			for ok {
				_, ok = <-r.Deliver()
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: Deliver not closed after Close", r.self.ID)
		}
	}
}

// TestRaftCloseWithFullDeliver: nobody reads Deliver, so every node's FSM
// ends up blocked on the full channel; Close must still return promptly
// (closeCh releases the blocked FSM Apply) and close Deliver.
func TestRaftCloseWithFullDeliver(t *testing.T) {
	nodes := startCluster(t, 3)
	li := waitLeader(t, nodes, -1)
	const extra = 20
	for k := 0; k < DeliverBuffer+extra; k++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := nodes[li].Submit(ctx, []byte(fmt.Sprintf("full-%d", k)))
		cancel()
		if err != nil {
			t.Fatalf("Submit %d on leader node%d: %v", k, li+1, err)
		}
	}
	// Wait until every node's Deliver is full (its FSM is then blocked).
	deadline := time.Now().Add(10 * time.Second)
	for _, r := range nodes {
		for len(r.Deliver()) < DeliverBuffer {
			if time.Now().After(deadline) {
				t.Fatalf("%s: Deliver holds %d blocks after 10s, want %d", r.self.ID, len(r.Deliver()), DeliverBuffer)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, r := range nodes {
		done := make(chan error, 1)
		go func(r *Raft) { done <- r.Close() }(r)
		select {
		case err := <-done:
			if err != nil {
				t.Logf("%s Close: %v", r.self.ID, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: Close with a full Deliver did not return within 5s", r.self.ID)
		}
		n := 0
		for range r.Deliver() { // returns once Deliver is closed
			n++
		}
		if n != DeliverBuffer {
			t.Errorf("%s: read %d buffered blocks after Close, want %d", r.self.ID, n, DeliverBuffer)
		}
	}
}

// TestRaftSubmitHonoursCtxWithoutQuorum: with the leader and one follower
// closed, the last node can neither reach a leader nor elect one; Submit
// must give up at its ctx deadline instead of waiting for a leader.
func TestRaftSubmitHonoursCtxWithoutQuorum(t *testing.T) {
	nodes := startCluster(t, 3)
	li := waitLeader(t, nodes, -1)
	si := (li + 1) % len(nodes) // a follower: it never leads again alone
	for i, r := range nodes {
		if i != si {
			r.Close()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	err := nodes[si].Submit(ctx, []byte("no-quorum"))
	took := time.Since(start)
	if err == nil {
		t.Fatalf("Submit without quorum on node%d succeeded", si+1)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Submit without quorum = %v, want a context.DeadlineExceeded error", err)
	}
	if took > 2*time.Second {
		t.Errorf("Submit without quorum took %v, want about 1s (its ctx)", took)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	a := Peer{ID: "a", RaftAddr: "ra", ForwardAddr: "127.0.0.1:1"}
	b := Peer{ID: "b", RaftAddr: "rb", ForwardAddr: "127.0.0.1:2"}
	dupID := b
	dupID.ID = "a"
	dupAddr := b
	dupAddr.RaftAddr = "ra"
	noFwd := b
	noFwd.ForwardAddr = ""
	cases := []struct {
		name string
		cfg  Config
	}{
		{"empty id", Config{ID: "", Peers: []Peer{a, b}}},
		{"id not a peer", Config{ID: "c", Peers: []Peer{a, b}}},
		{"duplicate id", Config{ID: "a", Peers: []Peer{a, dupID}}},
		{"duplicate raft addr", Config{ID: "a", Peers: []Peer{a, dupAddr}}},
		{"missing forward addr", Config{ID: "a", Peers: []Peer{a, noFwd}}},
	}
	for _, c := range cases {
		if r, err := New(c.cfg); err == nil {
			r.Close()
			t.Errorf("%s: New succeeded, want an error", c.name)
		}
	}
}
