package main

import (
	"reflect"
	"testing"

	raftcons "hybrid/consensus/raft"
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
