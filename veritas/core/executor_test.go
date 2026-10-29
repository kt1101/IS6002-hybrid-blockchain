package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas"
	"hybrid/veritas/ledger"
)

// ---- helpers ----

func newTestLedger(t *testing.T) *ledger.LogLedger {
	t.Helper()
	dir := t.TempDir() // registered first, so its removal runs after Close
	l, err := ledger.NewLedger(dir, true)
	if err != nil {
		t.Fatalf("NewLedger: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func newTestExecutor(t *testing.T, mode VersionMode, fp FaultPolicy) (*Executor, KVStore) {
	t.Helper()
	s := NewMapStore()
	return NewExecutor(s, newTestLedger(t), mode, fp), s
}

func mkSet(key, val string, ver int64) *pbv.SetRequest {
	return &pbv.SetRequest{Key: key, Value: val, Version: ver}
}

func mkLog(seq int64, sets ...*pbv.SetRequest) *pbv.SharedLog {
	return &pbv.SharedLog{Seq: seq, Sets: sets}
}

func mkBlock(txs ...*pbv.SharedLog) *pbv.Block {
	return &pbv.Block{Txs: txs}
}

func rawOf(height int64) []byte {
	return []byte(fmt.Sprintf("raw-block-%d", height))
}

func putEntry(t *testing.T, s KVStore, key, val string, ver int64) {
	t.Helper()
	e, err := veritas.Encode(val, ver)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set(context.Background(), key, e); err != nil {
		t.Fatal(err)
	}
}

func getEntry(t *testing.T, s KVStore, key string) *veritas.Value {
	t.Helper()
	raw, ok, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	if !ok {
		t.Fatalf("Get %q: missing", key)
	}
	v, err := veritas.Decode(raw)
	if err != nil {
		t.Fatalf("Decode %q: %v", key, err)
	}
	return v
}

func mustApply(t *testing.T, e *Executor, height int64, blk *pbv.Block) (committed, aborted []int64) {
	t.Helper()
	c, a, err := e.Apply(context.Background(), height, rawOf(height), blk)
	if err != nil {
		t.Fatalf("Apply height %d: %v", height, err)
	}
	return c, a
}

func eqSeqs(got, want []int64) bool {
	if len(got) == 0 && len(want) == 0 {
		return true
	}
	return reflect.DeepEqual(got, want)
}

// ---- required tests ----

func TestApplyWritesValueAndVersion(t *testing.T) {
	e, s := newTestExecutor(t, VersionClient, NoFault{})
	before := e.RootDigest()

	c, a := mustApply(t, e, 1, mkBlock(mkLog(7, mkSet("k", "v", 7))))

	if got := getEntry(t, s, "k"); *got != (veritas.Value{Val: "v", Version: 7}) {
		t.Fatalf("store[k] = %+v, want {v 7}", *got)
	}
	if !eqSeqs(c, []int64{7}) || !eqSeqs(a, nil) {
		t.Fatalf("committed=%v aborted=%v, want [7] []", c, a)
	}
	if bytes.Equal(before, e.RootDigest()) {
		t.Fatalf("root digest unchanged after apply")
	}
}

func TestApplySkipsStaleClientVersion(t *testing.T) {
	e, s := newTestExecutor(t, VersionClient, NoFault{})
	putEntry(t, s, "k", "old", 9)
	before := e.RootDigest()

	c, a := mustApply(t, e, 1, mkBlock(mkLog(7, mkSet("k", "v", 7))))

	if !eqSeqs(a, []int64{7}) || !eqSeqs(c, nil) {
		t.Fatalf("committed=%v aborted=%v, want [] [7]", c, a)
	}
	if got := getEntry(t, s, "k"); *got != (veritas.Value{Val: "old", Version: 9}) {
		t.Fatalf("store[k] = %+v, want unchanged {old 9}", *got)
	}
	if !bytes.Equal(before, e.RootDigest()) {
		t.Fatalf("root digest changed although the only tx was aborted")
	}
}

func TestValidateRejectsBlockWithStaleWrite(t *testing.T) {
	e, s := newTestExecutor(t, VersionClient, NoFault{})
	putEntry(t, s, "k", "old", 9)
	ctx := context.Background()

	stale := mkBlock(mkLog(10, mkSet("fresh", "x", 10)), mkLog(7, mkSet("k", "v", 7)))
	if e.Validate(ctx, stale) {
		t.Fatalf("Validate = true for block with stale write")
	}
	ok := mkBlock(mkLog(10, mkSet("fresh", "x", 10)), mkLog(11, mkSet("k", "v", 11)))
	if !e.Validate(ctx, ok) {
		t.Fatalf("Validate = false for block with only fresh writes")
	}
	// read-only
	if _, found, _ := s.Get(ctx, "fresh"); found {
		t.Fatalf("Validate wrote to the store")
	}
	if got := getEntry(t, s, "k"); got.Version != 9 {
		t.Fatalf("Validate modified store[k]: %+v", *got)
	}
}

func TestOrderedModeVersionIsHeightIndex(t *testing.T) {
	e, s := newTestExecutor(t, VersionOrdered, NoFault{})

	c, a := mustApply(t, e, 3, mkBlock(
		mkLog(100, mkSet("a", "va", 1)),
		mkLog(101, mkSet("b", "vb", 1)),
		mkLog(102, mkSet("c", "vc", 1)),
	))

	if got := getEntry(t, s, "c"); *got != (veritas.Value{Val: "vc", Version: 3<<20 | 2}) {
		t.Fatalf("store[c] = %+v, want {vc %d}", *got, int64(3<<20|2))
	}
	if got := getEntry(t, s, "a"); got.Version != 3<<20|0 {
		t.Fatalf("store[a].Version = %d, want %d", got.Version, int64(3<<20))
	}
	if !eqSeqs(c, []int64{100, 101, 102}) || !eqSeqs(a, nil) {
		t.Fatalf("committed=%v aborted=%v", c, a)
	}
}

func TestSameKeyTwiceInBlockLastWins(t *testing.T) {
	for _, mode := range []VersionMode{VersionClient, VersionOrdered} {
		mode := mode
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			e, s := newTestExecutor(t, mode, NoFault{})
			c, a := mustApply(t, e, 1, mkBlock(
				mkLog(5, mkSet("k", "first", 5)),
				mkLog(6, mkSet("k", "second", 6)),
			))
			if got := getEntry(t, s, "k"); got.Val != "second" {
				t.Fatalf("store[k].Val = %q, want second", got.Val)
			}
			if !eqSeqs(c, []int64{5, 6}) || !eqSeqs(a, nil) {
				t.Fatalf("committed=%v aborted=%v, want [5 6] []", c, a)
			}

			// Ledger must also reflect the last write only: compare with a
			// ledger that saw just the second write.
			ref := newTestLedger(t)
			ver := int64(6)
			if mode == VersionOrdered {
				ver = 1<<20 | 1
			}
			if err := ref.Append([]byte("k"), []byte(fmt.Sprintf("second-%v", ver))); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(ref.GetRootDigest(), e.RootDigest()) {
				t.Fatalf("ledger root does not match last-write-wins state")
			}
		})
	}
}

func TestTwoExecutorsSameBlocksSameRoot(t *testing.T) {
	blocks := []*pbv.Block{
		mkBlock(mkLog(1, mkSet("a", "a1", 1)), mkLog(2, mkSet("b", "b1", 2))),
		mkBlock(mkLog(3, mkSet("a", "a2", 3), mkSet("c", "c1", 3)), mkLog(0, mkSet("b", "stale", 0))),
		mkBlock(mkLog(4, mkSet("b", "b2", 4)), mkLog(5, mkSet("a", "a3", 5))),
	}
	for _, mode := range []VersionMode{VersionClient, VersionOrdered} {
		mode := mode
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			e1, s1 := newTestExecutor(t, mode, NoFault{})
			e2, s2 := newTestExecutor(t, mode, NoFault{})
			empty := e1.RootDigest()
			for i, b := range blocks {
				h := int64(i + 1)
				c1, a1 := mustApply(t, e1, h, b)
				c2, a2 := mustApply(t, e2, h, b)
				if !eqSeqs(c1, c2) || !eqSeqs(a1, a2) {
					t.Fatalf("height %d: outcomes differ: %v/%v vs %v/%v", h, c1, a1, c2, a2)
				}
			}
			r1, r2 := e1.RootDigest(), e2.RootDigest()
			if len(r1) == 0 || bytes.Equal(r1, empty) {
				t.Fatalf("root digest empty/unchanged")
			}
			if !bytes.Equal(r1, r2) {
				t.Fatalf("roots differ: %x vs %x", r1, r2)
			}
			for _, k := range []string{"a", "b", "c"} {
				if *getEntry(t, s1, k) != *getEntry(t, s2, k) {
					t.Fatalf("store[%s] differs", k)
				}
			}
		})
	}
}

// ---- extras pinning documented edge decisions ----

// Staleness in VersionClient is checked against the store as already
// updated by earlier SharedLogs of the same block (sequential semantics).
func TestStaleCheckSeesEarlierSharedLogInSameBlock(t *testing.T) {
	e, s := newTestExecutor(t, VersionClient, NoFault{})
	c, a := mustApply(t, e, 1, mkBlock(
		mkLog(8, mkSet("k", "eight", 8)),
		mkLog(7, mkSet("k", "seven", 7)),
	))
	if !eqSeqs(c, []int64{8}) || !eqSeqs(a, []int64{7}) {
		t.Fatalf("committed=%v aborted=%v, want [8] [7]", c, a)
	}
	if got := getEntry(t, s, "k"); *got != (veritas.Value{Val: "eight", Version: 8}) {
		t.Fatalf("store[k] = %+v, want {eight 8}", *got)
	}
}

// Validate only looks at the current store, not intra-block effects.
func TestValidateIgnoresIntraBlockWrites(t *testing.T) {
	e, _ := newTestExecutor(t, VersionClient, NoFault{})
	blk := mkBlock(mkLog(8, mkSet("k", "eight", 8)), mkLog(7, mkSet("k", "seven", 7)))
	if !e.Validate(context.Background(), blk) {
		t.Fatalf("Validate = false, want true (checks current store only)")
	}
}

// Equal stored version counts as stale (original rule: stored >= tx).
func TestApplyEqualVersionIsStale(t *testing.T) {
	e, _ := newTestExecutor(t, VersionClient, NoFault{})
	_, _ = mustApply(t, e, 1, mkBlock(mkLog(7, mkSet("k", "v", 7))))
	c, a := mustApply(t, e, 2, mkBlock(mkLog(7, mkSet("k", "w", 7))))
	if !eqSeqs(c, nil) || !eqSeqs(a, []int64{7}) {
		t.Fatalf("committed=%v aborted=%v, want [] [7]", c, a)
	}
}

// A SharedLog with one stale Set is skipped entirely.
func TestStaleSharedLogIsSkippedWhole(t *testing.T) {
	e, s := newTestExecutor(t, VersionClient, NoFault{})
	putEntry(t, s, "k", "old", 9)
	c, a := mustApply(t, e, 1, mkBlock(mkLog(5, mkSet("fresh", "x", 5), mkSet("k", "v", 5))))
	if !eqSeqs(c, nil) || !eqSeqs(a, []int64{5}) {
		t.Fatalf("committed=%v aborted=%v, want [] [5]", c, a)
	}
	if _, found, _ := s.Get(context.Background(), "fresh"); found {
		t.Fatalf("fresh Set of an aborted SharedLog was applied")
	}
}

// Ordered mode never aborts, whatever the stored/client versions.
func TestOrderedModeNeverAborts(t *testing.T) {
	e, s := newTestExecutor(t, VersionOrdered, NoFault{})
	putEntry(t, s, "k", "old", 1<<60)
	c, a := mustApply(t, e, 2, mkBlock(mkLog(1, mkSet("k", "v", 0))))
	if !eqSeqs(c, []int64{1}) || !eqSeqs(a, nil) {
		t.Fatalf("committed=%v aborted=%v, want [1] []", c, a)
	}
	if got := getEntry(t, s, "k"); *got != (veritas.Value{Val: "v", Version: 2 << 20}) {
		t.Fatalf("store[k] = %+v", *got)
	}
}

// Ledger entries use the original format value+"-"+fmt %v(version).
func TestLedgerEntryFormatMatchesOriginal(t *testing.T) {
	e, _ := newTestExecutor(t, VersionClient, NoFault{})
	mustApply(t, e, 1, mkBlock(mkLog(42, mkSet("k", "v", 42))))
	ref := newTestLedger(t)
	if err := ref.Append([]byte("k"), []byte("v-42")); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ref.GetRootDigest(), e.RootDigest()) {
		t.Fatalf("ledger entry format differs from value-version")
	}
}

type suffixFault struct{}

func (suffixFault) OnApply(key, val string) string { return val + "-CORRUPT" }

// The FaultPolicy output reaches both the store and the ledger.
func TestFaultPolicyAffectsStoreAndLedger(t *testing.T) {
	honest, _ := newTestExecutor(t, VersionClient, NoFault{})
	bad, bs := newTestExecutor(t, VersionClient, suffixFault{})
	blk := mkBlock(mkLog(1, mkSet("k", "v", 1)))
	mustApply(t, honest, 1, blk)
	mustApply(t, bad, 1, blk)
	if got := getEntry(t, bs, "k"); got.Val != "v-CORRUPT" {
		t.Fatalf("store[k].Val = %q, want v-CORRUPT", got.Val)
	}
	if bytes.Equal(honest.RootDigest(), bad.RootDigest()) {
		t.Fatalf("corrupt replica has the same root as honest replica")
	}
}

type failStore struct{ KVStore }

var errBoom = errors.New("boom")

func (failStore) Set(ctx context.Context, key, val string) error { return errBoom }

// Store errors are returned, not swallowed.
func TestApplyReturnsStoreError(t *testing.T) {
	e := NewExecutor(failStore{NewMapStore()}, newTestLedger(t), VersionClient, NoFault{})
	_, _, err := e.Apply(context.Background(), 1, rawOf(1), mkBlock(mkLog(1, mkSet("k", "v", 1))))
	if !errors.Is(err, errBoom) {
		t.Fatalf("Apply err = %v, want wrapping %v", err, errBoom)
	}
}

func TestMapStoreMissingKey(t *testing.T) {
	v, ok, err := NewMapStore().Get(context.Background(), "nope")
	if err != nil || ok || v != "" {
		t.Fatalf("Get missing = (%q, %v, %v), want (\"\", false, nil)", v, ok, err)
	}
}

// ---- fix round 1 ----

type failGetStore struct{ KVStore }

func (failGetStore) Get(ctx context.Context, key string) (string, bool, error) {
	return "", false, errBoom
}

// ValidateErr surfaces store Get errors instead of turning them into an
// Abort vote; Validate still returns false.
func TestValidateErrSurfacesStoreGetError(t *testing.T) {
	e := NewExecutor(failGetStore{NewMapStore()}, newTestLedger(t), VersionClient, NoFault{})
	blk := mkBlock(mkLog(1, mkSet("k", "v", 1)))
	ok, err := e.ValidateErr(context.Background(), blk)
	if ok || !errors.Is(err, errBoom) {
		t.Fatalf("ValidateErr = (%v, %v), want (false, %v)", ok, err, errBoom)
	}
	if e.Validate(context.Background(), blk) {
		t.Fatalf("Validate = true on store error")
	}
}

// In ordered mode validation never reads the store and always passes.
func TestValidateOrderedModeAlwaysTrue(t *testing.T) {
	e := NewExecutor(failGetStore{NewMapStore()}, newTestLedger(t), VersionOrdered, NoFault{})
	if ok, err := e.ValidateErr(context.Background(), mkBlock(mkLog(1, mkSet("k", "v", 0)))); !ok || err != nil {
		t.Fatalf("ValidateErr = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestOrderedModeRejectsHeightOutOfRange(t *testing.T) {
	e, s := newTestExecutor(t, VersionOrdered, NoFault{})
	before := e.RootDigest()
	for _, h := range []int64{0, -1, maxOrderedHeight} {
		if _, _, err := e.Apply(context.Background(), h, rawOf(h), mkBlock(mkLog(1, mkSet("k", "v", 1)))); err == nil {
			t.Fatalf("Apply height %d: err = nil, want range error", h)
		}
	}
	if _, found, _ := s.Get(context.Background(), "k"); found {
		t.Fatalf("store written despite rejected height")
	}
	if !bytes.Equal(before, e.RootDigest()) {
		t.Fatalf("ledger changed despite rejected height")
	}
	// The largest legal height works.
	mustApply(t, e, maxOrderedHeight-1, mkBlock(mkLog(1, mkSet("k", "v", 1))))
}

func TestOrderedModeRejectsTooManySharedLogs(t *testing.T) {
	e, s := newTestExecutor(t, VersionOrdered, NoFault{})
	sl := mkLog(1, mkSet("k", "v", 1))
	txs := make([]*pbv.SharedLog, maxOrderedTxs+1)
	for i := range txs {
		txs[i] = sl
	}
	if _, _, err := e.Apply(context.Background(), 1, rawOf(1), &pbv.Block{Txs: txs}); err == nil {
		t.Fatalf("Apply with %d shared logs: err = nil", len(txs))
	}
	if _, found, _ := s.Get(context.Background(), "k"); found {
		t.Fatalf("store written despite rejected block")
	}
}

func TestApplyReturnsStoreGetError(t *testing.T) {
	e := NewExecutor(failGetStore{NewMapStore()}, newTestLedger(t), VersionClient, NoFault{})
	_, _, err := e.Apply(context.Background(), 1, rawOf(1), mkBlock(mkLog(1, mkSet("k", "v", 1))))
	if !errors.Is(err, errBoom) {
		t.Fatalf("Apply err = %v, want wrapping %v", err, errBoom)
	}
}

func TestApplyReturnsDecodeError(t *testing.T) {
	e, s := newTestExecutor(t, VersionClient, NoFault{})
	if err := s.Set(context.Background(), "k", "not-json"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Apply(context.Background(), 1, rawOf(1), mkBlock(mkLog(1, mkSet("k", "v", 1)))); err == nil {
		t.Fatalf("Apply err = nil, want decode error")
	}
}

// RootDigest (and Validate) may run concurrently with Apply; meaningful
// under -race.
func TestRootDigestConcurrentWithApply(t *testing.T) {
	e, _ := newTestExecutor(t, VersionClient, NoFault{})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = e.RootDigest()
			_ = e.Validate(context.Background(), mkBlock(mkLog(1, mkSet("k0", "x", 1))))
		}
	}()
	for h := int64(1); h <= 50; h++ {
		mustApply(t, e, h, mkBlock(
			mkLog(h, mkSet(fmt.Sprintf("k%d", h%5), fmt.Sprintf("v%d", h), h)),
		))
	}
	close(stop)
	<-done
}

// The ledger timing hook accumulates the time of ledger.Append/AppendBlk
// during Apply and TakeLedgerTime returns and resets it.
func TestApplyAccumulatesLedgerTime(t *testing.T) {
	e, _ := newTestExecutor(t, VersionClient, NoFault{})
	if d := e.TakeLedgerTime(); d != 0 {
		t.Fatalf("initial ledger time = %v, want 0", d)
	}
	mustApply(t, e, 1, mkBlock(
		mkLog(1, mkSet("a", "x", 1)),
		mkLog(2, mkSet("b", "y", 2)),
	))
	if d := e.TakeLedgerTime(); d <= 0 {
		t.Fatalf("ledger time after Apply = %v, want > 0", d)
	}
	if d := e.TakeLedgerTime(); d != 0 {
		t.Fatalf("ledger time after Take = %v, want 0 (reset)", d)
	}
}
