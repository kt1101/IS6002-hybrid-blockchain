package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbv "hybrid/proto/veritas"
)

// droppedBlock is the wire form of a block proposed by signer whose single
// SharedLog has the given seq (what the backend hands to AbortBlock).
func droppedBlock(t *testing.T, signer string, seq int64) []byte {
	t.Helper()
	raw, err := proto.Marshal(&pbv.Block{
		Type:      pbv.MessageType_Approve,
		Signature: signer,
		Txs:       []*pbv.SharedLog{{Seq: seq, Sets: []*pbv.SetRequest{setReq("k", "v", 1)}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestAbortBlockFailsSyncWaitersWithErrAborted(t *testing.T) {
	hub, nodes := newCluster(t, testCfg(VersionClient, CommitSync), mapStores(1)...)
	hub.Drop(0, true) // the block is never delivered
	s := nodes[0].srv

	res := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
		defer cancel()
		_, err := s.Set(ctx, setReq("k", "v", 1))
		res <- err
	}()
	eventually(t, srvTestTimeout, "the Set's waiter", func() bool { return s.waiters.count() == 1 })

	// node1's first Set has seq NodeID<<40 | 1.
	start := time.Now()
	s.AbortBlock(droppedBlock(t, "node1", int64(1)<<40|1))
	select {
	case err := <-res:
		if !errors.Is(err, ErrAborted) || status.Code(err) != codes.Aborted {
			t.Fatalf("err = %v (code %v), want ErrAborted/Aborted", err, status.Code(err))
		}
		if el := time.Since(start); el > time.Second {
			t.Fatalf("Set returned %v after AbortBlock", el)
		}
	case <-time.After(srvTestTimeout):
		t.Fatal("Set did not return after AbortBlock")
	}
	if c := s.waiters.count(); c != 0 {
		t.Fatalf("%d waiters left", c)
	}
	if d := s.Breakdown().Dropped; d != 1 {
		t.Fatalf("Breakdown.Dropped = %d, want 1", d)
	}
	// Already resolved: a second AbortBlock of the same block is harmless.
	s.AbortBlock(droppedBlock(t, "node1", int64(1)<<40|1))
}

func TestAbortBlockIgnoresOtherProposersAndUnknownSeqs(t *testing.T) {
	cfg := testCfg(VersionClient, CommitSync)
	cfg.SyncTimeout = 2 * time.Second
	hub, nodes := newCluster(t, cfg, mapStores(1)...)
	hub.Drop(0, true)
	s := nodes[0].srv

	res := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
		defer cancel()
		_, err := s.Set(ctx, setReq("k", "v", 1))
		res <- err
	}()
	eventually(t, srvTestTimeout, "the Set's waiter", func() bool { return s.waiters.count() == 1 })

	s.AbortBlock(droppedBlock(t, "node2", int64(1)<<40|1)) // other proposer, same seq
	s.AbortBlock(droppedBlock(t, "node1", int64(1)<<40|99))
	s.AbortBlock([]byte{0xff, 0xff, 0xff}) // undecodable
	empty, _ := proto.Marshal(&pbv.Block{Signature: "node1"})
	s.AbortBlock(empty)

	if c := s.waiters.count(); c != 1 {
		t.Fatalf("waiters = %d after foreign/unknown AbortBlocks, want 1", c)
	}
	// The waiter still ends by its own SyncTimeout, not by an abort.
	select {
	case err := <-res:
		if !errors.Is(err, ErrCommitTimeout) {
			t.Fatalf("err = %v, want ErrCommitTimeout", err)
		}
	case <-time.After(srvTestTimeout):
		t.Fatal("Set did not time out")
	}
	// Only the unknown own seq counts as an own dropped block.
	if d := s.Breakdown().Dropped; d != 1 {
		t.Fatalf("Breakdown.Dropped = %d, want 1 (only node1's block)", d)
	}
}

func TestAbortBlockAsyncIsNoopForWaiters(t *testing.T) {
	hub, nodes := newCluster(t, testCfg(VersionClient, CommitAsync), mapStores(1)...)
	hub.Drop(0, true)
	s := nodes[0].srv
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	if _, err := s.Set(ctx, setReq("k", "v", 1)); err != nil {
		t.Fatalf("async Set: %v", err)
	}
	s.AbortBlock(droppedBlock(t, "node1", int64(1)<<40|1))
	if c := s.waiters.count(); c != 0 {
		t.Fatalf("async mode has %d waiters", c)
	}
	if d := s.Breakdown().Dropped; d != 1 {
		t.Fatalf("Breakdown.Dropped = %d, want 1", d)
	}
}
