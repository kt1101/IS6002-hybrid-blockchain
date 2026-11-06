package driver

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/ledger"
	"hybrid/veritas/sigs"
)

// fakeNode serves GetVerified from a real ledger (one key), optionally
// lying about the value, and records Sets.
type fakeNode struct {
	pbv.UnimplementedNodeServer
	l      *ledger.LogLedger
	value  string // what the ledger holds
	answer string // what GetVerified returns as value
	sets   []*pbv.SetRequest
}

func (f *fakeNode) GetVerified(ctx context.Context, req *pbv.GetVerifiedRequest) (*pbv.GetVerifiedResponse, error) {
	p, err := f.l.ProveKey([]byte(req.GetKey()))
	if err != nil {
		return nil, err
	}
	return &pbv.GetVerifiedResponse{Value: f.answer, Version: 7, Height: 1, RootDigest: f.l.GetRootDigest(),
		SideNodes: p.SideNodes, NonMembershipLeafData: p.NonMembershipLeafData}, nil
}

func (f *fakeNode) Set(ctx context.Context, req *pbv.SetRequest) (*pbv.SetResponse, error) {
	f.sets = append(f.sets, req)
	return &pbv.SetResponse{}, nil
}

func startFake(t *testing.T, f *fakeNode, opts Options) *Driver {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pbv.RegisterNodeServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	opts.Dialer = func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	d, err := OpenWith("bufnet", "", "sig", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func newFake(t *testing.T, value, answer string) *fakeNode {
	t.Helper()
	l, err := ledger.NewLedger(t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Append([]byte("k"), []byte(value+"-7")); err != nil {
		t.Fatal(err)
	}
	return &fakeNode{l: l, value: value, answer: answer}
}

func TestDriverGetVerifiedAcceptsValidProof(t *testing.T) {
	var seen int
	d := startFake(t, newFake(t, "v", "v"), Options{OnVerify: func(int, int64) { seen++ }})
	got, err := d.GetVerified(context.Background(), "k")
	if err != nil || got != "v" {
		t.Fatalf("GetVerified = %q, %v; want v", got, err)
	}
	if seen != 1 {
		t.Fatalf("OnVerify called %d times, want 1", seen)
	}
}

func TestDriverGetVerifiedRejectsTamperedValue(t *testing.T) {
	d := startFake(t, newFake(t, "v", "evil"), Options{})
	got, err := d.GetVerified(context.Background(), "k")
	if !errors.Is(err, ErrProofInvalid) {
		t.Fatalf("GetVerified = %q, %v; want ErrProofInvalid", got, err)
	}
	if got != "" {
		t.Fatalf("value %q returned with an invalid proof", got)
	}
}

func TestDriverGetUsesVerifiedReadMode(t *testing.T) {
	d := startFake(t, newFake(t, "v", "evil"), Options{ReadMode: ReadVerified})
	if _, err := d.Get(context.Background(), "k"); !errors.Is(err, ErrProofInvalid) {
		t.Fatalf("Get in verified mode: %v, want ErrProofInvalid", err)
	}
}

func TestDriverSignsSets(t *testing.T) {
	f := newFake(t, "v", "v")
	id := sigs.ClientID(3)
	d := startFake(t, f, Options{ClientID: id, Key: sigs.KeyFor("s", id)})
	if err := d.Set(context.Background(), "key", "val"); err != nil {
		t.Fatal(err)
	}
	r := f.sets[0]
	if r.GetClientId() != id || !sigs.VerifySet(sigs.Public(sigs.KeyFor("s", id)), "key", "val", r.GetVersion(), r.GetSig()) {
		t.Fatalf("Set not signed by %s: %+v", id, r)
	}
}

func TestParseReadMode(t *testing.T) {
	for s, want := range map[string]ReadMode{"": ReadPlain, "plain": ReadPlain, "verified": ReadVerified, "certified": ReadCertified} {
		if got, err := ParseReadMode(s); err != nil || got != want {
			t.Fatalf("ParseReadMode(%q) = %v, %v", s, got, err)
		}
	}
	if _, err := ParseReadMode("x"); err == nil {
		t.Fatal("ParseReadMode accepted x")
	}
}
