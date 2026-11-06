package main

import (
	"reflect"
	"testing"

	"hybrid/bft2node/node"
)

func TestParsePeers(t *testing.T) {
	got, err := parsePeers("1=172.28.0.11:7200, 2=172.28.0.12:7200,")
	if err != nil {
		t.Fatalf("parsePeers: %v", err)
	}
	want := []node.Peer{{ID: 1, Addr: "172.28.0.11:7200"}, {ID: 2, Addr: "172.28.0.12:7200"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parsePeers = %+v, want %+v", got, want)
	}
	for _, bad := range []string{"", ",", "1", "=a:1", "1=", "0=a:1", "x=a:1", "1=a", "1=a:1,2"} {
		if p, err := parsePeers(bad); err == nil {
			t.Errorf("parsePeers(%q) = %+v, want an error", bad, p)
		}
	}
}

func TestWithForwardAddrs(t *testing.T) {
	ps := []node.Peer{{ID: 1, Addr: "172.28.0.11:7200"}, {ID: 2, Addr: "172.28.0.12:7200"}}
	got, err := withForwardAddrs(ps, 7202)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ForwardAddr != "172.28.0.11:7202" || got[1].ForwardAddr != "172.28.0.12:7202" {
		t.Fatalf("ForwardAddr = %q, %q", got[0].ForwardAddr, got[1].ForwardAddr)
	}
	if ps[0].ForwardAddr != "" {
		t.Fatal("input modified")
	}
}
