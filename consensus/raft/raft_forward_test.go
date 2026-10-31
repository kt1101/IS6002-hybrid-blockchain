package raft

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blackHole listens on localhost and accepts connections but never answers,
// like a crashed leader whose packets are dropped. It returns its address.
func blackHole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
	})
	return ln.Addr().String()
}

// N5: a forward to a leader that stops answering is cancelled as soon as
// raft reports another leader, not after forwardTimeout (15 s).
func TestForwardCancelledWhenLeaderChanges(t *testing.T) {
	p := Peer{ID: "node1", RaftAddr: "raft-node1", ForwardAddr: blackHole(t)}
	var cur atomic.Value
	cur.Store(p.RaftAddr)
	time.AfterFunc(300*time.Millisecond, func() { cur.Store("raft-node2") })
	t0 := time.Now()
	err := forwardTo(context.Background(), newForwardClient(), p, []byte("blk"), func() string { return cur.Load().(string) })
	el := time.Since(t0)
	if err == nil {
		t.Fatal("forward to a black hole succeeded")
	}
	if !strings.Contains(err.Error(), "leader changed during the forward") {
		t.Fatalf("err=%v, want a leader-change error", err)
	}
	if el < 250*time.Millisecond || el > 3*time.Second {
		t.Fatalf("forward returned after %v, want ~0.3 s (the leader change), not forwardTimeout", el)
	}
}

// An election (no leader known) also cancels the forward.
func TestForwardCancelledWhenLeaderUnknown(t *testing.T) {
	p := Peer{ID: "node1", RaftAddr: "raft-node1", ForwardAddr: blackHole(t)}
	var cur atomic.Value
	cur.Store(p.RaftAddr)
	time.AfterFunc(200*time.Millisecond, func() { cur.Store("") })
	t0 := time.Now()
	if err := forwardTo(context.Background(), newForwardClient(), p, []byte("blk"), func() string { return cur.Load().(string) }); err == nil {
		t.Fatal("forward to a black hole succeeded")
	}
	if el := time.Since(t0); el > 3*time.Second {
		t.Fatalf("forward returned after %v", el)
	}
}

// With an unchanged leader the forward still ends at the caller's deadline.
func TestForwardHonoursCtxOnBlackHole(t *testing.T) {
	p := Peer{ID: "node1", RaftAddr: "raft-node1", ForwardAddr: blackHole(t)}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	err := forwardTo(ctx, newForwardClient(), p, []byte("blk"), func() string { return p.RaftAddr })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want DeadlineExceeded", err)
	}
	if el := time.Since(t0); el > 3*time.Second {
		t.Fatalf("forward returned after %v", el)
	}
}

// A healthy leader answers and the watcher does not interfere.
func TestForwardToHealthyLeader(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != submitPath {
			http.NotFound(w, r)
			return
		}
		time.Sleep(100 * time.Millisecond) // a few polls of leader()
		w.WriteHeader(http.StatusOK)
	})}
	go srv.Serve(ln)
	defer srv.Close()
	p := Peer{ID: "node1", RaftAddr: "raft-node1", ForwardAddr: ln.Addr().String()}
	if err := forwardTo(context.Background(), newForwardClient(), p, []byte("blk"), func() string { return p.RaftAddr }); err != nil {
		t.Fatalf("forward to a healthy leader: %v", err)
	}
}

func TestForwardClientTimeouts(t *testing.T) {
	c := newForwardClient()
	if c.Timeout != forwardTimeout {
		t.Fatalf("client timeout %v, want forwardTimeout %v", c.Timeout, forwardTimeout)
	}
	if tr, ok := c.Transport.(*http.Transport); !ok || tr.DialContext == nil {
		t.Fatalf("forward client has no dialer with a timeout: %#v", c.Transport)
	}
	if forwardDialTimeout > 2*time.Second {
		t.Fatalf("forwardDialTimeout=%v, want about 1 s", forwardDialTimeout)
	}
}
