package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	raftcons "hybrid/consensus/raft"
	"hybrid/veritas/benchmark"
	"hybrid/veritas/core"
)

func TestParseRaftPeers(t *testing.T) {
	got, err := parseRaftPeers("node1=172.28.0.11:7100/172.28.0.11:7101, node2=172.28.0.12:7100/172.28.0.12:7101,")
	if err != nil {
		t.Fatalf("parseRaftPeers: %v", err)
	}
	want := []raftcons.Peer{
		{ID: "node1", RaftAddr: "172.28.0.11:7100", ForwardAddr: "172.28.0.11:7101"},
		{ID: "node2", RaftAddr: "172.28.0.12:7100", ForwardAddr: "172.28.0.12:7101"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseRaftPeers = %+v, want %+v", got, want)
	}

	for _, bad := range []string{
		"",
		",",
		"node1",
		"=a:1/b:2",
		"node1=a:1",
		"node1=a:1/",
		"node1=/b:2",
		"node1=a:1/b:2/c:3",
		"node1=a:1/b:2,node2",
	} {
		if p, err := parseRaftPeers(bad); err == nil {
			t.Errorf("parseRaftPeers(%q) = %+v, want an error", bad, p)
		}
	}
}

func TestBuildFault(t *testing.T) {
	now := time.Now()
	for _, backend := range []string{"kafka", "raft", "tendermint", "bft2"} {
		for _, spec := range []string{"none", "corrupt"} {
			if _, err := buildFault(spec, backend, now, time.Minute); err != nil {
				t.Errorf("buildFault(%s, %s): %v", spec, backend, err)
			}
		}
	}
	if _, err := buildFault("silent", "kafka", now, time.Minute); err != nil {
		t.Errorf("buildFault(silent, kafka): %v", err)
	}
	for _, backend := range []string{"raft", "tendermint", "bft2"} {
		_, err := buildFault("silent", backend, now, time.Minute)
		if err == nil || !strings.Contains(err.Error(), "--consensus=kafka") {
			t.Errorf("buildFault(silent, %s) = %v, want a --consensus=kafka error", backend, err)
		}
	}
	for _, bad := range []string{"", "bogus"} {
		if _, err := buildFault(bad, "kafka", now, 0); err == nil {
			t.Errorf("buildFault(%q) accepted", bad)
		}
	}
	if _, err := buildFault("corrupt", "kafka", now, -time.Second); err == nil {
		t.Errorf("negative --fault-after accepted")
	}
}

// SIGUSR1 activates an armed fault at once (the harness's injection) and the
// handler then writes the spec to the active file (the harness's
// confirmation); a stale active file is removed when the handler is set up.
func TestFaultSignalActivates(t *testing.T) {
	fp, err := buildFault("corrupt", "tendermint", time.Now(), 1000*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := fp.OnApply("k", "v"); got != "v" {
		t.Fatalf("before the signal: %q", got)
	}
	active := filepath.Join(t.TempDir(), "fault-active")
	if err := ioutil.WriteFile(active, []byte("stale\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := watchFaultSignal(fp, "corrupt", active); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(active); !os.IsNotExist(err) {
		t.Fatalf("stale active file not removed: %v", err)
	}
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		b, err := ioutil.ReadFile(active)
		if err == nil && string(b) == "corrupt\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("active file %s not written 5 s after SIGUSR1: %q, %v", active, b, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fp.(core.Activator).Active() {
		t.Fatalf("active file written but the fault is not active")
	}
	if got := fp.OnApply("k", "v"); got != "v"+benchmark.CorruptMarker {
		t.Fatalf("after the signal: %q", got)
	}
}

func TestWriteFileAtomic(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	if err := writeFileAtomic(p, "silent\n"); err != nil {
		t.Fatal(err)
	}
	if b, err := ioutil.ReadFile(p); err != nil || string(b) != "silent\n" {
		t.Fatalf("read back %q, %v", b, err)
	}
	if _, err := os.Stat(p + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}
	if err := writeFileAtomic(filepath.Join(t.TempDir(), "missing-dir", "f"), "x"); err == nil {
		t.Fatalf("write into a missing directory succeeded")
	}
}

func TestSnapshotPath(t *testing.T) {
	if got := snapshotPath("/results/breakdown-node3.json", 2); got != "/results/breakdown-node3.snap2.json" {
		t.Fatalf("snapshotPath=%q", got)
	}
	if got := snapshotPath("/tmp/b", 1); got != "/tmp/b.snap1.json" {
		t.Fatalf("snapshotPath without .json=%q", got)
	}
}

func TestSnapshotSignalWritesNumberedSnapshots(t *testing.T) {
	paths := make(chan string, 4)
	watchSnapshotSignal(func(p string) error { paths <- p; return nil }, "/results/breakdown-node1.json")
	for k := 1; k <= 2; k++ {
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR2); err != nil {
			t.Fatal(err)
		}
		select {
		case p := <-paths:
			if want := snapshotPath("/results/breakdown-node1.json", k); p != want {
				t.Fatalf("snapshot %d written to %q, want %q", k, p, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("snapshot %d not written 5 s after SIGUSR2", k)
		}
	}
}
