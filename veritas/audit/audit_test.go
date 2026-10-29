package audit

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	pbv "hybrid/proto/veritas"
)

func roots(vals ...string) map[string][]byte {
	m := make(map[string][]byte, len(vals))
	for i, v := range vals {
		m[NodeName(i+1)] = []byte(v)
	}
	return m
}

func TestDivergentFindsMinority(t *testing.T) {
	got := Divergent(roots("aa", "aa", "bb", "aa"))
	want := []string{"node3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Divergent = %v, want %v", got, want)
	}
}

func TestDivergentEmptyWhenAllEqual(t *testing.T) {
	got := Divergent(roots("aa", "aa", "aa", "aa"))
	if len(got) != 0 {
		t.Fatalf("Divergent = %v, want empty", got)
	}
}

func TestDivergentEmptyInput(t *testing.T) {
	if got := Divergent(nil); len(got) != 0 {
		t.Fatalf("Divergent(nil) = %v, want empty", got)
	}
}

func TestDivergentSortedAscending(t *testing.T) {
	m := map[string][]byte{}
	for i := 1; i <= 12; i++ {
		m[NodeName(i)] = []byte("good")
	}
	m["node10"] = []byte("bad")
	m["node2"] = []byte("bad")
	m["node11"] = []byte("bad")
	got := Divergent(m)
	want := []string{"node2", "node10", "node11"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Divergent = %v, want %v", got, want)
	}
}

// With no strict majority there is no reference digest: every node is divergent.
func TestDivergentNoStrictMajorityReportsAll(t *testing.T) {
	cases := map[string]map[string][]byte{
		"2-2":       roots("aa", "aa", "bb", "bb"),
		"all-diff":  roots("aa", "bb", "cc", "dd"),
		"2-1-1":     roots("aa", "aa", "bb", "cc"),
		"1-1 (n=2)": roots("aa", "bb"),
	}
	for name, m := range cases {
		got := Divergent(m)
		if len(got) != len(m) {
			t.Errorf("%s: Divergent = %v, want all %d nodes", name, got, len(m))
		}
		if HasMajority(m) {
			t.Errorf("%s: HasMajority = true, want false", name)
		}
	}
}

func TestHasMajority(t *testing.T) {
	if !HasMajority(roots("aa", "aa", "bb")) {
		t.Error("2 of 3 should be a majority")
	}
	if !HasMajority(roots("aa")) {
		t.Error("a single node is its own majority")
	}
	if HasMajority(nil) {
		t.Error("no nodes: no majority")
	}
}

func TestDivergentTreatsNilAndEmptyAlike(t *testing.T) {
	m := map[string][]byte{"node1": nil, "node2": {}, "node3": []byte("x")}
	got := Divergent(m)
	if !reflect.DeepEqual(got, []string{"node3"}) {
		t.Fatalf("Divergent = %v, want [node3]", got)
	}
}

func TestParseAddrs(t *testing.T) {
	got, err := ParseAddrs(" a:1, b:2 ,c:3")
	if err != nil || !reflect.DeepEqual(got, []string{"a:1", "b:2", "c:3"}) {
		t.Fatalf("ParseAddrs = %v, %v", got, err)
	}
	for _, bad := range []string{"", "  ", "a:1,,b:2", "a:1,"} {
		if _, err := ParseAddrs(bad); err == nil {
			t.Errorf("ParseAddrs(%q): want error", bad)
		}
	}
}

// fakeNode is a Veritas node whose Verify returns a fixed root or error.
type fakeNode struct {
	pbv.UnimplementedNodeServer
	root []byte
	err  error
}

func (f *fakeNode) Verify(ctx context.Context, req *pbv.VerifyRequest) (*pbv.VerifyResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &pbv.VerifyResponse{RootDigest: f.root}, nil
}

// startFake serves n on a free local port and returns its address.
func startFake(t *testing.T, n *fakeNode) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	pbv.RegisterNodeServer(s, n)
	go s.Serve(lis)
	return lis.Addr().String(), s.Stop
}

func TestCollectRootsReturnsDigestPerNode(t *testing.T) {
	var addrs []string
	for _, r := range []string{"r1", "r1", "r2"} {
		a, stop := startFake(t, &fakeNode{root: []byte(r)})
		defer stop()
		addrs = append(addrs, a)
	}
	got, err := CollectRoots(context.Background(), addrs)
	if err != nil {
		t.Fatalf("CollectRoots: %v", err)
	}
	want := map[string][]byte{"node1": []byte("r1"), "node2": []byte("r1"), "node3": []byte("r2")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CollectRoots = %v, want %v", got, want)
	}
	if d := Divergent(got); !reflect.DeepEqual(d, []string{"node3"}) {
		t.Fatalf("Divergent = %v, want [node3]", d)
	}
}

func TestCollectRootsNamesUnreachableNode(t *testing.T) {
	a1, stop1 := startFake(t, &fakeNode{root: []byte("r")})
	defer stop1()
	// A port that was just closed: nothing listens on it.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dead := lis.Addr().String()
	lis.Close()

	old := PerNodeTimeout
	PerNodeTimeout = 3 * time.Second
	defer func() { PerNodeTimeout = old }()

	got, err := CollectRoots(context.Background(), []string{a1, dead})
	if err == nil {
		t.Fatalf("CollectRoots = %v, want an error", got)
	}
	if got != nil {
		t.Errorf("map = %v, want nil on error", got)
	}
	if !strings.Contains(err.Error(), "node2") || strings.Contains(err.Error(), "node1") {
		t.Errorf("error %q should name node2 only", err)
	}
}

func TestCollectRootsNamesRPCError(t *testing.T) {
	a1, stop1 := startFake(t, &fakeNode{root: []byte("r")})
	defer stop1()
	a2, stop2 := startFake(t, &fakeNode{err: errors.New("boom")})
	defer stop2()
	_, err := CollectRoots(context.Background(), []string{a1, a2})
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "node2") || !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %q should name node2 and carry the RPC error", err)
	}
}

func TestCollectRootsNoAddrs(t *testing.T) {
	if _, err := CollectRoots(context.Background(), nil); err == nil {
		t.Fatal("want an error for an empty address list")
	}
}

func TestCollectRootsRejectsEmptyDigest(t *testing.T) {
	a1, stop1 := startFake(t, &fakeNode{root: []byte("r")})
	defer stop1()
	a2, stop2 := startFake(t, &fakeNode{}) // Verify succeeds with an empty RootDigest
	defer stop2()
	got, err := CollectRoots(context.Background(), []string{a1, a2})
	if err == nil {
		t.Fatalf("CollectRoots = %v, want an error", got)
	}
	if !strings.Contains(err.Error(), "node2") || !strings.Contains(err.Error(), "empty root digest") {
		t.Errorf("error %q should name node2 and say empty root digest", err)
	}
	if strings.Contains(err.Error(), "node1") {
		t.Errorf("error %q should not name node1", err)
	}
}
