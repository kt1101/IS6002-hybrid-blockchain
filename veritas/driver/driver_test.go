package driver

import (
	"context"
	"errors"
	"net"
	"strings"
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
	// phase 3
	evidence []*pbv.Evidence
	signed   []bool // GetVerifiedRequest.signed of every call
	// signers: the attestations of a certified read: node id and the node
	// whose key signs it.
	signers []struct{ id, key int }
}

func (f *fakeNode) GetVerified(ctx context.Context, req *pbv.GetVerifiedRequest) (*pbv.GetVerifiedResponse, error) {
	f.signed = append(f.signed, req.GetSigned())
	p, err := f.l.ProveKey([]byte(req.GetKey()))
	if err != nil {
		return nil, err
	}
	root := f.l.GetRootDigest()
	r := &pbv.GetVerifiedResponse{Value: f.answer, Version: 7, Height: 1, RootDigest: root,
		SideNodes: p.SideNodes, NonMembershipLeafData: p.NonMembershipLeafData}
	if req.GetCertified() {
		for _, n := range f.signers {
			r.Attestations = append(r.Attestations, &pbv.RootAttestation{Height: 1, Root: root, NodeId: int32(n.id),
				Sig: sigs.SignAttest(sigs.KeyFor("s", sigs.NodeID(n.key)), 1, root)})
		}
	}
	return r, nil
}

func (f *fakeNode) Evicted(ctx context.Context, req *pbv.EvictedRequest) (*pbv.EvictedResponse, error) {
	return &pbv.EvictedResponse{Epoch: 2, Nodes: []int32{3, 4}}, nil
}

func (f *fakeNode) ReportEvidence(ctx context.Context, ev *pbv.Evidence) (*pbv.EvidenceAck, error) {
	f.evidence = append(f.evidence, ev)
	return &pbv.EvidenceAck{}, nil
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
	for s, want := range map[string]ReadMode{"": ReadPlain, "plain": ReadPlain, "verified": ReadVerified, "certified": ReadCertified, "quorum": ReadQuorum} {
		if got, err := ParseReadMode(s); err != nil || got != want {
			t.Fatalf("ParseReadMode(%q) = %v, %v", s, got, err)
		}
	}
	if _, err := ParseReadMode("x"); err == nil {
		t.Fatal("ParseReadMode accepted x")
	}
}

func certOpts() Options {
	return Options{ReadMode: ReadCertified, NodeKeys: sigs.NodeKeys("s", 4), Quorum: 3}
}

func TestDriverCertifiedReadAcceptsQuorum(t *testing.T) {
	f := newFake(t, "v", "v")
	f.signers = []struct{ id, key int }{{1, 1}, {2, 2}, {4, 4}}
	d := startFake(t, f, certOpts())
	if got, err := d.Get(context.Background(), "k"); err != nil || got != "v" {
		t.Fatalf("certified Get = %q, %v", got, err)
	}
}

func TestDriverRejectsForgedAttestations(t *testing.T) {
	for name, signers := range map[string][]struct{ id, key int }{
		"one key, three ids": {{1, 1}, {2, 1}, {3, 1}},
		"one node thrice":    {{2, 2}, {2, 2}, {2, 2}},
		"two of three":       {{1, 1}, {2, 2}},
		"unknown node":       {{1, 1}, {2, 2}, {9, 9}},
	} {
		f := newFake(t, "v", "v")
		f.signers = signers
		d := startFake(t, f, certOpts())
		if got, err := d.Get(context.Background(), "k"); !errors.Is(err, ErrCertInvalid) || got != "" {
			t.Errorf("%s: Get = %q, %v; want ErrCertInvalid", name, got, err)
		}
	}
}

// ---- phase 3 ----

func TestDriverGetCertifiedRespReturnsResponseOnBadProof(t *testing.T) {
	f := newFake(t, "v", "evil")
	f.signers = []struct{ id, key int }{{1, 1}, {2, 2}, {3, 3}}
	d := startFake(t, f, certOpts())
	r, err := d.GetCertifiedResp(context.Background(), "k", true)
	if !errors.Is(err, ErrProofInvalid) || r == nil || r.GetValue() != "evil" {
		t.Fatalf("GetCertifiedResp on a lying node = %v, %v; want the response and ErrProofInvalid", r, err)
	}
	if len(f.signed) != 1 || !f.signed[0] {
		t.Fatalf("signed flag not sent: %v", f.signed)
	}
	f2 := newFake(t, "v", "v")
	f2.signers = f.signers
	d2 := startFake(t, f2, certOpts())
	if r, err := d2.GetCertifiedResp(context.Background(), "k", false); err != nil || r.GetValue() != "v" {
		t.Fatalf("GetCertifiedResp on an honest node = %v, %v", r, err)
	}
}

func TestDriverEvictedAndReportEvidence(t *testing.T) {
	f := newFake(t, "v", "v")
	d := startFake(t, f, Options{})
	epoch, nodes, err := d.Evicted(context.Background())
	if err != nil || epoch != 2 || len(nodes) != 2 || nodes[0] != 3 || nodes[1] != 4 {
		t.Fatalf("Evicted = %d %v %v", epoch, nodes, err)
	}
	ev := &pbv.Evidence{Culprit: 4, Height: 9, Read: &pbv.SignedRead{Key: "k", NodeId: 4, Height: 9}}
	if err := d.ReportEvidence(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if len(f.evidence) != 1 || f.evidence[0].GetCulprit() != 4 {
		t.Fatalf("evidence not delivered: %v", f.evidence)
	}
}

func TestDriverQuorumModeGetIsPlain(t *testing.T) {
	// The quorum comparison lives in ycsbbench's reader; the driver's Get in
	// quorum mode is the plain Get.
	f := newFake(t, "v", "v")
	d := startFake(t, f, Options{ReadMode: ReadQuorum})
	if _, err := d.Get(context.Background(), "k"); err == nil || !strings.Contains(err.Error(), "Unimplemented") {
		t.Fatalf("quorum-mode Get should call the plain Get (unimplemented by the fake): %v", err)
	}
	if len(f.signed) != 0 {
		t.Fatal("quorum-mode Get called GetVerified")
	}
}
