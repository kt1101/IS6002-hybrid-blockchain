package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/benchmark"
	"hybrid/veritas/driver"
)

// fakeNode is a nodeAPI whose answers the test sets.
type fakeNode struct {
	mu sync.Mutex

	val    string // plain Get value
	getErr error

	// certified reads: certResps[i] answers call i (the last one repeats).
	certResps []certAnswer

	epoch   int64
	evicted []int32

	gets, certs, evictedCalls, reports int
	signedCalls                        int
	lastEvidence                       *pbv.Evidence
	reportErr                          error
}

type certAnswer struct {
	resp *pbv.GetVerifiedResponse
	err  error
}

func (f *fakeNode) Get(ctx context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	return f.val, f.getErr
}

func (f *fakeNode) GetCertifiedResp(ctx context.Context, key string, signed bool) (*pbv.GetVerifiedResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.certs
	f.certs++
	if signed {
		f.signedCalls++
	}
	if len(f.certResps) == 0 {
		return &pbv.GetVerifiedResponse{Value: f.val}, nil
	}
	if i >= len(f.certResps) {
		i = len(f.certResps) - 1
	}
	a := f.certResps[i]
	return a.resp, a.err
}

func (f *fakeNode) Evicted(ctx context.Context) (int64, []int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictedCalls++
	return f.epoch, f.evicted, nil
}

func (f *fakeNode) ReportEvidence(ctx context.Context, ev *pbv.Evidence) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports++
	f.lastEvidence = ev
	return f.reportErr
}

func (f *fakeNode) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets + f.certs
}

func fakes(n int) ([]*fakeNode, []nodeAPI) {
	fs := make([]*fakeNode, n)
	api := make([]nodeAPI, n)
	for i := range fs {
		fs[i] = &fakeNode{val: fmt.Sprintf("v%d", i)}
		api[i] = fs[i]
	}
	return fs, api
}

func proofErr() error { return fmt.Errorf("%w: key %q at height 7", driver.ErrProofInvalid, "k") }

// badResp is a response of node id whose proof does not verify; signed ones
// carry a signature.
func badResp(id int32, signed bool) *pbv.GetVerifiedResponse {
	r := &pbv.GetVerifiedResponse{Value: "lie", Height: 7, RootDigest: []byte("root")}
	if signed {
		r.NodeId = id
		r.NodeSig = []byte("sig")
	}
	return r
}

func TestReaderFailoverOnPermissionDenied(t *testing.T) {
	fs, api := fakes(4)
	fs[0].getErr = status.Error(codes.PermissionDenied, "node 1 was evicted at height 3")
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadPlain, Failover: true, SignReads: signOff, F: 1}, st)
	v, err := r.Read(context.Background(), "k")
	if err != nil || v != "v1" {
		t.Fatalf("Read = %q, %v; want v1 from node 1", v, err)
	}
	if st.Failovers.Load() != 1 {
		t.Fatalf("Failovers = %d, want 1", st.Failovers.Load())
	}
	if _, err := r.Read(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if fs[0].gets != 1 || fs[1].gets != 2 {
		t.Fatalf("gets = %d on node 0, %d on node 1; want 1 and 2", fs[0].gets, fs[1].gets)
	}
}

func TestReaderNoFailoverOnUnavailable(t *testing.T) {
	fs, api := fakes(4)
	fs[0].getErr = status.Error(codes.Unavailable, "connection refused")
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadPlain, Failover: true, SignReads: signOff, F: 1}, st)
	for i := 0; i < 2; i++ {
		if _, err := r.Read(context.Background(), "k"); status.Code(err) != codes.Unavailable {
			t.Fatalf("Read err = %v, want Unavailable", err)
		}
	}
	if fs[0].gets != 2 || fs[1].calls() != 0 || st.Failovers.Load() != 0 {
		t.Fatalf("gets node0=%d node1=%d failovers=%d; want 2, 0, 0", fs[0].gets, fs[1].calls(), st.Failovers.Load())
	}
}

func TestReaderNoFailoverWithoutFlag(t *testing.T) {
	fs, api := fakes(4)
	fs[0].getErr = status.Error(codes.DataLoss, "diverged")
	r := newReader(api, 0, readerConfig{Mode: driver.ReadPlain, SignReads: signOff, F: 1}, &readStats{})
	if _, err := r.Read(context.Background(), "k"); status.Code(err) != codes.DataLoss {
		t.Fatalf("Read err = %v, want DataLoss", err)
	}
	if fs[1].calls() != 0 {
		t.Fatal("read went to node 1 without --failover")
	}
}

func TestReaderSignOnFailureReportsEvidence(t *testing.T) {
	fs, api := fakes(4)
	fs[0].certResps = []certAnswer{
		{badResp(1, false), proofErr()},
		{badResp(1, true), proofErr()},
	}
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadCertified, Failover: true, SignReads: signOnFailure, F: 1}, st)
	v, err := r.Read(context.Background(), "k")
	if err != nil || v != "v1" {
		t.Fatalf("Read = %q, %v; want v1 from node 1", v, err)
	}
	if fs[1].reports != 1 || st.EvidenceSent.Load() != 1 {
		t.Fatalf("reports on node 1 = %d, EvidenceSent = %d; want 1, 1", fs[1].reports, st.EvidenceSent.Load())
	}
	ev := fs[1].lastEvidence
	if ev.GetCulprit() != 1 || ev.GetHeight() != 7 || ev.GetRead().GetValue() != "lie" || string(ev.GetRead().GetSig()) != "sig" {
		t.Fatalf("evidence = %v", ev)
	}
	if st.Rejected.Load() != 1 || fs[0].signedCalls != 1 {
		t.Fatalf("Rejected = %d, signed calls = %d; want 1, 1", st.Rejected.Load(), fs[0].signedCalls)
	}
	r.Read(context.Background(), "k")
	if fs[0].certs != 2 {
		t.Fatalf("node 0 read %d times, want 2 (avoided after the evidence)", fs[0].certs)
	}
}

func TestReaderEvasiveNodeSuspected(t *testing.T) {
	fs, api := fakes(4)
	good := &pbv.GetVerifiedResponse{Value: "v0", NodeId: 1, NodeSig: []byte("sig")}
	fs[0].certResps = []certAnswer{
		{badResp(1, false), proofErr()},
		{good, nil},
	}
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadCertified, Failover: true, SignReads: signOnFailure, F: 1}, st)
	v, err := r.Read(context.Background(), "k")
	if err != nil || v != "v0" {
		t.Fatalf("Read = %q, %v; want the verified signed v0", v, err)
	}
	for _, f := range fs {
		if f.reports != 0 {
			t.Fatal("evidence sent for a correct signed read")
		}
	}
	r.Read(context.Background(), "k")
	if fs[0].certs != 2 || fs[1].certs != 1 {
		t.Fatalf("certified reads node0=%d node1=%d; want 2, 1 (node 0 suspected)", fs[0].certs, fs[1].certs)
	}
}

func TestReaderSignAlwaysReportsAtOnce(t *testing.T) {
	fs, api := fakes(4)
	fs[0].certResps = []certAnswer{{badResp(1, true), proofErr()}}
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadCertified, Failover: true, SignReads: signAlways, F: 1}, st)
	if _, err := r.Read(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if fs[0].certs != 1 || fs[1].reports != 1 || st.EvidenceSent.Load() != 1 {
		t.Fatalf("node0 reads=%d reports=%d sent=%d; want 1, 1, 1", fs[0].certs, fs[1].reports, st.EvidenceSent.Load())
	}
}

func TestReaderCertTimeout(t *testing.T) {
	fs, api := fakes(4)
	fs[0].certResps = []certAnswer{{nil, status.Error(codes.DeadlineExceeded, "not certified within 5s")}}
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadCertified, Failover: true, SignReads: signOff, F: 1}, st)
	if _, err := r.Read(context.Background(), "k"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if st.CertTimeouts.Load() != 1 || st.Failovers.Load() != 0 {
		t.Fatalf("CertTimeouts = %d, Failovers = %d; want 1, 0", st.CertTimeouts.Load(), st.Failovers.Load())
	}
}

func TestReaderQuorumMismatch(t *testing.T) {
	fs, api := fakes(4)
	fs[0].val, fs[1].val, fs[2].val = "a", "b", "b"
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadQuorum, SignReads: signOff, F: 1}, st)
	v, err := r.Read(context.Background(), "k")
	if err != nil || v != "b" {
		t.Fatalf("Read = %q, %v; want b from nodes 1 and 2", v, err)
	}
	if st.QuorumErrors.Load() < 1 {
		t.Fatal("QuorumErrors = 0")
	}
	fs[2].val = "c"
	if _, err := r.Read(context.Background(), "k"); !errors.Is(err, ErrQuorumMismatch) {
		t.Fatalf("err = %v, want ErrQuorumMismatch after two differing sets", err)
	}
	if fs[3].gets != 0 {
		t.Fatal("quorum read asked more than two sets")
	}
}

func TestReaderQuorumAgrees(t *testing.T) {
	fs, api := fakes(4)
	for _, f := range fs {
		f.val = "x"
	}
	st := &readStats{}
	r := newReader(api, 2, readerConfig{Mode: driver.ReadQuorum, SignReads: signOff, F: 1}, st)
	if v, err := r.Read(context.Background(), "k"); err != nil || v != "x" {
		t.Fatalf("Read = %q, %v", v, err)
	}
	if fs[2].gets != 1 || fs[3].gets != 1 || fs[0].gets+fs[1].gets != 0 || st.QuorumErrors.Load() != 0 {
		t.Fatal("quorum read did not ask exactly nodes 2 and 3")
	}
}

func TestReaderAllNodesExcluded(t *testing.T) {
	fs, api := fakes(4)
	fs[0].certResps = []certAnswer{{&pbv.GetVerifiedResponse{Value: "v0", EvictedEpoch: 4}, nil}}
	fs[0].epoch, fs[0].evicted = 4, []int32{1, 2, 3, 4}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadCertified, Failover: true, SignReads: signOff, F: 1}, &readStats{})
	if _, err := r.Read(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	before := 0
	for _, f := range fs {
		before += f.calls()
	}
	if _, err := r.Read(context.Background(), "k"); !errors.Is(err, ErrNoNode) {
		t.Fatalf("err = %v, want ErrNoNode", err)
	}
	after := 0
	for _, f := range fs {
		after += f.calls()
	}
	if after != before {
		t.Fatalf("%d more node calls with every node excluded", after-before)
	}
}

func TestReaderRefreshesEvictedOnEpoch(t *testing.T) {
	fs, api := fakes(4)
	fs[0].certResps = []certAnswer{{&pbv.GetVerifiedResponse{Value: "v0", EvictedEpoch: 1}, nil}}
	fs[0].epoch, fs[0].evicted = 1, []int32{3} // node id 3 = index 2
	fs[1].certResps = []certAnswer{{nil, status.Error(codes.DataLoss, "diverged")}}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadCertified, Failover: true, SignReads: signOff, F: 1}, &readStats{})
	for i := 0; i < 3; i++ {
		if _, err := r.Read(context.Background(), "k"); err != nil {
			t.Fatal(err)
		}
	}
	if fs[0].evictedCalls != 1 {
		t.Fatalf("Evicted called %d times, want 1", fs[0].evictedCalls)
	}
	// Leaving node 0 (DataLoss on node 1 does not matter: it is not asked).
	fs[0].certResps = []certAnswer{{nil, status.Error(codes.PermissionDenied, "evicted")}}
	v, err := r.Read(context.Background(), "k")
	if err != nil || v != "v3" {
		t.Fatalf("Read = %q, %v; want v3 (node 1 DataLoss, node 2 evicted)", v, err)
	}
	if fs[2].calls() != 0 {
		t.Fatal("evicted node 2 was read")
	}
}

func TestReaderWrongBeforeFailover(t *testing.T) {
	fs, api := fakes(4)
	fs[0].val = "x" + benchmark.CorruptMarker
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadPlain, Failover: true, SignReads: signOff, F: 1}, st)
	for i := 0; i < 3; i++ {
		r.Read(context.Background(), "k")
	}
	if st.WrongBeforeFailover.Load() != 0 {
		t.Fatal("counted before the failover")
	}
	fs[0].getErr = status.Error(codes.DataLoss, "diverged")
	if v, err := r.Read(context.Background(), "k"); err != nil || v != "v1" {
		t.Fatalf("Read = %q, %v", v, err)
	}
	if st.WrongBeforeFailover.Load() != 3 {
		t.Fatalf("WrongBeforeFailover = %d, want 3", st.WrongBeforeFailover.Load())
	}
}

func TestReaderConcurrent(t *testing.T) {
	fs, api := fakes(4)
	fs[0].getErr = status.Error(codes.DataLoss, "diverged")
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadPlain, Failover: true, SignReads: signOff, F: 1}, st)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := r.Read(context.Background(), "k"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if st.Failovers.Load() != 1 {
		t.Fatalf("Failovers = %d, want 1", st.Failovers.Load())
	}
}

func TestOpsCSVPhase3Columns(t *testing.T) {
	if !strings.HasSuffix(benchmark.OpsCSVHeader, ",rejected_reads,quorum_errors,failovers,wrong_before_failover,evidence_sent,cert_timeouts,first_failover_unix_ms,evidence_failed") {
		t.Fatalf("OpsCSVHeader = %q", benchmark.OpsCSVHeader)
	}
}

func TestReaderEvidenceRefusedCounted(t *testing.T) {
	fs, api := fakes(4)
	fs[0].certResps = []certAnswer{
		{badResp(1, false), proofErr()},
		{badResp(1, true), proofErr()},
	}
	fs[1].reportErr = status.Error(codes.ResourceExhausted, "full")
	st := &readStats{}
	r := newReader(api, 0, readerConfig{Mode: driver.ReadCertified, Failover: true, SignReads: signOnFailure, F: 1}, st)
	if _, err := r.Read(context.Background(), "k"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if fs[1].reports != 1 || st.EvidenceSent.Load() != 0 || st.EvidenceFailed.Load() != 1 {
		t.Fatalf("reports %d, sent %d, failed %d; want 1, 0, 1", fs[1].reports, st.EvidenceSent.Load(), st.EvidenceFailed.Load())
	}
}
