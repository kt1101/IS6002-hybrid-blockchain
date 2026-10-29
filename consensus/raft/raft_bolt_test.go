//go:build !race
// +build !race

// The bolt-backed test is excluded from -race builds: -race turns on the
// checkptr instrumentation, and github.com/boltdb/bolt v1.3.1 (the store
// behind raft-boltdb, pinned by go.mod) fails it with "fatal error: checkptr:
// converted pointer straddles multiple allocations" (bucket.go:626) as soon
// as a store is opened. That is a fatal runtime error that kills the whole
// test binary. The other tests use in-memory raft stores; this one keeps the
// production store path (bolt log store + file snapshot store) covered in
// plain `go test` runs. Production binaries are not built with -race.

package raft

import (
	"context"
	"testing"
	"time"
)

func TestRaftBoltStoresSmoke(t *testing.T) {
	nodes := startClusterStores(t, 3, true)
	li := waitLeader(t, nodes, -1)
	fi := (li + 1) % len(nodes)
	for _, src := range []int{li, fi} { // leader path, then forwarded
		want := []byte("bolt-" + nodes[src].self.ID)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := nodes[src].Submit(ctx, want)
		cancel()
		if err != nil {
			t.Fatalf("Submit on %s: %v", nodes[src].self.ID, err)
		}
		for _, r := range nodes {
			recvUntil(t, r, want, 10*time.Second)
		}
	}
}
