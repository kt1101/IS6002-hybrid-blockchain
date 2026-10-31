package main

import (
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

// SIGUSR1 activates an armed fault at once (the harness's injection).
func TestFaultSignalActivates(t *testing.T) {
	fp, err := buildFault("corrupt", "tendermint", time.Now(), 1000*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := fp.OnApply("k", "v"); got != "v" {
		t.Fatalf("before the signal: %q", got)
	}
	watchFaultSignal(fp, "corrupt")
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !fp.(core.Activator).Active() {
		if time.Now().After(deadline) {
			t.Fatalf("fault not active 5 s after SIGUSR1")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := fp.OnApply("k", "v"); got != "v"+benchmark.CorruptMarker {
		t.Fatalf("after the signal: %q", got)
	}
}
