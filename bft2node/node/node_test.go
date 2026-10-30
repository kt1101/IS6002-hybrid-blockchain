package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/logging"
)

var quietOnce sync.Once

// testCluster starts n replicas (IDs 1..n) on 127.0.0.1 ports, in this
// process, with short view timeouts (generous enough for -race). Every node
// is stopped at the end of the test.
func testCluster(t *testing.T, n int) []*Node {
	t.Helper()
	quietOnce.Do(func() { logging.SetLogLevel("error") })
	lis := make([]net.Listener, n)
	peers := make([]Peer, n)
	for i := range lis {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lis[i] = l
		peers[i] = Peer{ID: hotstuff.ID(i + 1), Addr: l.Addr().String()}
	}
	nodes := make([]*Node, n)
	for i := range nodes {
		nd, err := New(Config{
			ID:             hotstuff.ID(i + 1),
			Peers:          peers,
			Listener:       lis[i],
			KeySeed:        "test-seed",
			ViewTimeout:    500 * time.Millisecond,
			MinViewTimeout: 200 * time.Millisecond,
			MaxViewTimeout: time.Second,
			IdleWait:       5 * time.Millisecond,
			ConnectTimeout: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("New(%d): %v", i+1, err)
		}
		nodes[i] = nd
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.Stop()
		}
	})
	for i, nd := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := nd.Start(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Start(%d): %v", i+1, err)
		}
	}
	return nodes
}

// collector reads a node's commit log like the /deliver stream does and
// keeps every command it got, checking that the seqs are contiguous from 0.
type collector struct {
	mu  sync.Mutex
	got [][]byte
	err error
}

func collect(nd *Node) *collector {
	c := &collector{}
	go func() {
		for {
			first, items, err := nd.Log().Peek(context.Background(), 64)
			if err != nil {
				return // stopped
			}
			c.mu.Lock()
			if first != uint64(len(c.got)) && c.err == nil {
				c.err = fmt.Errorf("seq %d after %d commands", first, len(c.got))
			}
			c.got = append(c.got, items...)
			c.mu.Unlock()
			nd.Log().Advance(len(items))
		}
	}()
	return c
}

func (c *collector) snapshot() ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.got...), c.err
}

// waitCount waits until every collector holds at least want commands.
func waitCount(t *testing.T, cs []*collector, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		done := true
		var counts []int
		for _, c := range cs {
			got, err := c.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			counts = append(counts, len(got))
			if len(got) < want {
				done = false
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %v: delivered counts %v, want >= %d each", timeout, counts, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// payload is a unique command: a readable prefix and random bytes, which
// include zero bytes and invalid UTF-8 (exact bytes must survive).
func payload(node, k, extra int) []byte {
	b := make([]byte, extra)
	if _, err := rand.Read(b); err != nil {
		panic(err) // not t.Fatal: called from submitter goroutines
	}
	return append([]byte(fmt.Sprintf("node%d-cmd%d|", node, k)), b...)
}

// sameSequence fails unless every sequence equals the first one.
func sameSequence(t *testing.T, seqs [][][]byte) {
	t.Helper()
	for i := 1; i < len(seqs); i++ {
		if len(seqs[i]) != len(seqs[0]) {
			t.Fatalf("node %d delivered %d commands, node 1 delivered %d", i+1, len(seqs[i]), len(seqs[0]))
		}
		for j := range seqs[0] {
			if !bytes.Equal(seqs[i][j], seqs[0][j]) {
				t.Fatalf("position %d differs between node 1 and node %d", j, i+1)
			}
		}
	}
}

// exactlyOnce fails unless seq holds every command of want exactly once
// (byte for byte) and nothing else.
func exactlyOnce(t *testing.T, seq [][]byte, want [][]byte) {
	t.Helper()
	count := make(map[string]int)
	for _, b := range seq {
		count[string(b)]++
	}
	for _, w := range want {
		if count[string(w)] != 1 {
			t.Fatalf("command %.16q delivered %d times, want once", w, count[string(w)])
		}
	}
	if len(seq) != len(want) {
		t.Fatalf("delivered %d commands, want %d", len(seq), len(want))
	}
}

// TestBFT2SameOrderAllNodes: 4 replicas, every one submits commands
// concurrently (one of them 512 KiB); every replica, the proposers included,
// delivers every command exactly once with its exact bytes, all in the same
// order, and no filler block ever reaches the log.
func TestBFT2SameOrderAllNodes(t *testing.T) {
	const n, perNode = 4, 15
	nodes := testCluster(t, n)
	cs := make([]*collector, n)
	for i, nd := range nodes {
		cs[i] = collect(nd)
	}

	var mu sync.Mutex
	var all [][]byte
	var wg sync.WaitGroup
	for i, nd := range nodes {
		wg.Add(1)
		go func(i int, nd *Node) {
			defer wg.Done()
			for k := 0; k < perNode; k++ {
				extra := 1 + k*37
				if i == 0 && k == perNode-1 {
					extra = 512 << 10
				}
				p := payload(i+1, k, extra)
				mu.Lock()
				all = append(all, append([]byte(nil), p...))
				mu.Unlock()
				if err := nd.Submit(p); err != nil {
					t.Errorf("node %d Submit %d: %v", i+1, k, err)
					return
				}
				p[0] ^= 0xff // Submit must not retain the caller's buffer
				time.Sleep(time.Duration(k%3) * time.Millisecond)
			}
		}(i, nd)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	waitCount(t, cs, n*perNode, 60*time.Second)
	time.Sleep(300 * time.Millisecond) // nothing more (no duplicates, no fillers) may arrive
	seqs := make([][][]byte, n)
	for i, c := range cs {
		seqs[i], _ = c.snapshot()
	}
	sameSequence(t, seqs)
	exactlyOnce(t, seqs[0], all)
}

// TestBFT2SurvivesOneCrash: with f=1 of 4 replicas crashed (stopped: its
// server and connections close), the other 3 keep committing: commands
// submitted on the survivors after the crash are delivered on all three,
// exactly once and in the same order, and the crashed node's deliveries are
// a prefix of theirs.
func TestBFT2SurvivesOneCrash(t *testing.T) {
	const n = 4
	nodes := testCluster(t, n)
	cs := make([]*collector, n)
	for i, nd := range nodes {
		cs[i] = collect(nd)
	}

	var before [][]byte
	for i, nd := range nodes {
		for k := 0; k < 4; k++ {
			p := payload(i+1, k, 64)
			before = append(before, p)
			if err := nd.Submit(p); err != nil {
				t.Fatalf("node %d Submit: %v", i+1, err)
			}
		}
	}
	waitCount(t, cs, len(before), 60*time.Second)

	crashed := nodes[n-1]
	crashed.Stop()
	if err := crashed.Submit([]byte("after stop")); err == nil {
		t.Fatal("Submit on a stopped node returned nil")
	}
	prefix, _ := cs[n-1].snapshot()

	survivors := nodes[:n-1]
	var after [][]byte
	for k := 0; k < 8; k++ {
		for i, nd := range survivors {
			p := payload(i+1, 100+k, 64)
			after = append(after, p)
			if err := nd.Submit(p); err != nil {
				t.Fatalf("node %d Submit after the crash: %v", i+1, err)
			}
		}
	}
	waitCount(t, cs[:n-1], len(before)+len(after), 90*time.Second)
	time.Sleep(300 * time.Millisecond)

	seqs := make([][][]byte, n-1)
	for i := range survivors {
		seqs[i], _ = cs[i].snapshot()
	}
	sameSequence(t, seqs)
	exactlyOnce(t, seqs[0], append(append([][]byte(nil), before...), after...))
	if len(prefix) > len(seqs[0]) {
		t.Fatalf("crashed node delivered %d commands, survivors %d", len(prefix), len(seqs[0]))
	}
	for j := range prefix {
		if !bytes.Equal(prefix[j], seqs[0][j]) {
			t.Fatalf("crashed node's delivery %d differs from the survivors'", j)
		}
	}
}

func TestNewValidatesConfig(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	a := lis.Addr().String()
	for name, cfg := range map[string]Config{
		"no peers":      {ID: 1, Listener: lis},
		"one replica":   {ID: 1, Peers: []Peer{{1, a}}, Listener: lis},
		"id not in set": {ID: 3, Peers: []Peer{{1, a}, {2, a}}, Listener: lis},
		"ids not 1..n":  {ID: 1, Peers: []Peer{{1, a}, {3, a}}, Listener: lis},
		"duplicate id":  {ID: 1, Peers: []Peer{{1, a}, {1, a}}, Listener: lis},
		"empty address": {ID: 1, Peers: []Peer{{1, a}, {2, ""}}, Listener: lis},
		"id zero":       {ID: 0, Peers: []Peer{{0, a}, {1, a}}, Listener: lis},
	} {
		if nd, err := New(cfg); err == nil {
			nd.Stop()
			t.Errorf("%s: New returned no error", name)
		}
	}
}

// Submit checks the size and queues before Start (the local API serves
// from New on); Stop makes it fail.
func TestSubmitBeforeStartAndAfterStop(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nd, err := New(Config{ID: 1, Peers: []Peer{{1, lis.Addr().String()}, {2, "127.0.0.1:1"}}, Listener: lis})
	if err != nil {
		t.Fatal(err)
	}
	defer nd.Stop()
	if err := nd.Submit(nil); err != ErrEmpty {
		t.Fatalf("Submit(empty) = %v, want ErrEmpty", err)
	}
	if err := nd.Submit(make([]byte, DefaultMaxCommandBytes+1)); err != ErrTooLarge {
		t.Fatalf("Submit(too large) = %v, want ErrTooLarge", err)
	}
	if err := nd.Submit(make([]byte, DefaultMaxCommandBytes)); err != nil {
		t.Fatalf("Submit(max size) before Start = %v, want nil", err)
	}
	// Start waits for the (absent) peer until Stop ends it.
	errc := make(chan error, 1)
	go func() { errc <- nd.Start(context.Background()) }()
	time.Sleep(50 * time.Millisecond)
	nd.Stop()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("Start returned nil although the peer never came up")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not end Start's wait for peers")
	}
	if err := nd.Submit([]byte("x")); err != ErrStopped {
		t.Fatalf("Submit after Stop = %v, want ErrStopped", err)
	}
	nd.Stop() // idempotent
}
