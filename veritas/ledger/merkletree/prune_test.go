package merkletree

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"testing"
)

// expectedRefs walks the current tree and returns the reference counts a
// pruned tree must hold: one per tree position (inner node or leaf) and one
// per live leaf on its value. It fails if any reachable entry is missing.
func expectedRefs(t testing.TB, smt *SparseMerkleTree) map[string]uint32 {
	t.Helper()
	out := map[string]uint32{}
	ph := smt.th.placeholder()
	var walk func(h []byte, depth int)
	walk = func(h []byte, depth int) {
		if bytes.Equal(h, ph) {
			return
		}
		if depth > smt.depth() {
			t.Fatalf("tree deeper than %d", smt.depth())
		}
		data, err := smt.kvs.Get(h)
		if err != nil {
			t.Fatalf("live node %x at depth %d missing: %v", h, depth, err)
		}
		out[string(h)]++
		if smt.th.isLeaf(data) {
			_, vh := smt.th.parseLeaf(data)
			if _, err := smt.kvs.Get(vh); err != nil {
				t.Fatalf("live value %x missing: %v", vh, err)
			}
			out[string(vh)]++
			return
		}
		l, r := smt.th.parseNode(data)
		walk(l, depth+1)
		walk(r, depth+1)
	}
	walk(smt.root, 0)
	return out
}

// checkPruned asserts that the store holds exactly the entries reachable from
// the current root and that the reference counts match the tree.
func checkPruned(t testing.TB, smt *SparseMerkleTree) {
	t.Helper()
	want := expectedRefs(t, smt)
	if smt.pruneAnomalies != 0 {
		t.Fatalf("pruneAnomalies = %d", smt.pruneAnomalies)
	}
	if len(smt.refs) != len(want) {
		t.Fatalf("refs has %d keys, tree references %d", len(smt.refs), len(want))
	}
	for k, n := range want {
		if smt.refs[k] != n {
			t.Fatalf("refs[%x] = %d, want %d", k, smt.refs[k], n)
		}
	}
	m := smt.kvs.(*SimpleMap).m
	if len(m) != len(want) {
		t.Fatalf("store has %d entries, tree reaches %d", len(m), len(want))
	}
	for k := range m {
		if _, ok := want[k]; !ok {
			t.Fatalf("store holds unreachable entry %x", k)
		}
	}
}

func proofsEqual(a, b SparseMerkleProof) bool {
	if len(a.SideNodes) != len(b.SideNodes) || !bytes.Equal(a.NonMembershipLeafData, b.NonMembershipLeafData) {
		return false
	}
	for i := range a.SideNodes {
		if !bytes.Equal(a.SideNodes[i], b.SideNodes[i]) {
			return false
		}
	}
	return true
}

func storeLen(smt *SparseMerkleTree) int { return len(smt.kvs.(*SimpleMap).m) }

func storeBytes(smt *SparseMerkleTree) int {
	n := 0
	for k, v := range smt.kvs.(*SimpleMap).m {
		n += len(k) + len(v)
	}
	return n
}

// (a) + (b): a random sequence of 20k updates/deletes over 500 keys, with
// repeated values (rewrite of the same value) and identical values shared by
// many keys. After every step the pruned root equals the unpruned reference
// root, proofs are identical and verify, and Get returns the live value;
// periodically the store is checked to hold exactly the reachable entries.
// The pruned store stays O(live keys) while the reference grows per write.
func TestPrunedMatchesUnpruned(t *testing.T) {
	const nKeys = 500
	const steps = 20000
	rng := rand.New(rand.NewSource(42))
	ref := NewSparseMerkleTree(NewSimpleMap(), sha256.New())
	pr := NewPrunedSparseMerkleTree(NewSimpleMap(), sha256.New())

	keys := make([][]byte, nKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("user%06d", i))
	}
	pool := make([][]byte, 8)
	for i := range pool {
		pool[i] = []byte(fmt.Sprintf("shared-value-%d", i))
	}
	live := map[string][]byte{}
	var deletes, sameValue, shared int

	for step := 0; step < steps; step++ {
		k := keys[rng.Intn(nKeys)]
		var v []byte
		switch r := rng.Intn(100); {
		case r < 10:
			v = nil // delete (of a live or an absent key)
		case r < 20:
			if old, ok := live[string(k)]; ok {
				v = append([]byte(nil), old...) // same value again
				sameValue++
			} else {
				v = pool[0]
			}
		case r < 50:
			v = pool[rng.Intn(len(pool))] // identical value under many keys
			shared++
		default:
			v = make([]byte, 16+rng.Intn(48))
			rng.Read(v)
		}

		var r1, r2 []byte
		var err1, err2 error
		if v == nil {
			r1, err1 = ref.Delete(k)
			r2, err2 = pr.Delete(k)
			delete(live, string(k))
			deletes++
		} else {
			r1, err1 = ref.Update(k, v)
			r2, err2 = pr.Update(k, v)
			live[string(k)] = v
		}
		if err1 != nil || err2 != nil {
			t.Fatalf("step %d: ref err %v, pruned err %v", step, err1, err2)
		}
		if !bytes.Equal(r1, r2) || !bytes.Equal(pr.Root(), r2) || !bytes.Equal(ref.Root(), r1) {
			t.Fatalf("step %d: root mismatch: ref %x pruned %x", step, r1, r2)
		}

		pk := keys[rng.Intn(nKeys)]
		want := live[string(pk)]
		proof, err := pr.Prove(pk)
		if err != nil {
			t.Fatalf("step %d: Prove: %v", step, err)
		}
		refProof, err := ref.Prove(pk)
		if err != nil {
			t.Fatalf("step %d: ref Prove: %v", step, err)
		}
		if !proofsEqual(proof, refProof) {
			t.Fatalf("step %d: proof for %s differs from reference", step, pk)
		}
		if !VerifyProof(proof, pr.Root(), pk, want, sha256.New()) {
			t.Fatalf("step %d: proof for %s does not verify", step, pk)
		}
		got, err := pr.Get(pk)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("step %d: Get(%s) = %q, %v; want %q", step, pk, got, err, want)
		}

		// Bounded: inner nodes of the trie ~1.44 per leaf, plus leaf and value.
		if n := storeLen(pr); n > 5*len(live)+32 {
			t.Fatalf("step %d: pruned store %d entries for %d live keys", step, n, len(live))
		}
		if step < 300 || step%97 == 0 || step == steps-1 {
			checkPruned(t, pr)
		}
	}

	nLive := len(live)
	prN, refN := storeLen(pr), storeLen(ref)
	t.Logf("%d steps (%d deletes, %d same-value rewrites, %d shared values), %d live keys: pruned store %d entries (%.2f/key), unpruned %d entries",
		steps, deletes, sameValue, shared, nLive, prN, float64(prN)/float64(nLive), refN)
	if refN < 10*prN {
		t.Fatalf("unpruned store (%d) should have grown far beyond the pruned one (%d)", refN, prN)
	}

	// Deleting every key empties the pruned store completely.
	for k := range live {
		if _, err := pr.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
		if _, err := ref.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(pr.Root(), ref.Root()) {
			t.Fatalf("root mismatch while emptying")
		}
	}
	if !bytes.Equal(pr.Root(), pr.th.placeholder()) {
		t.Fatalf("root after deleting all keys = %x", pr.Root())
	}
	checkPruned(t, pr)
	if n := storeLen(pr); n != 0 {
		t.Fatalf("store holds %d entries after deleting every key", n)
	}
}

// Small hand-checked cases: rewriting the same value keeps every entry;
// a value shared by two keys survives the update of one of them.
func TestPrunedSharedAndSameValue(t *testing.T) {
	pr := NewPrunedSparseMerkleTree(NewSimpleMap(), sha256.New())
	h := func(b []byte) []byte {
		s := sha256.Sum256(b)
		return s[:]
	}
	must := func(_ []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(pr.Update([]byte("a"), []byte("v")))
	must(pr.Update([]byte("b"), []byte("v")))
	checkPruned(t, pr)
	if pr.refs[string(h([]byte("v")))] != 2 {
		t.Fatalf("shared value refcount = %d, want 2", pr.refs[string(h([]byte("v")))])
	}
	root := pr.Root()
	n := storeLen(pr)
	must(pr.Update([]byte("a"), []byte("v"))) // same value: no change at all
	checkPruned(t, pr)
	if !bytes.Equal(root, pr.Root()) || storeLen(pr) != n {
		t.Fatalf("rewriting the same value changed root or store")
	}
	must(pr.Update([]byte("a"), []byte("w")))
	checkPruned(t, pr)
	if _, err := pr.kvs.Get(h([]byte("v"))); err != nil {
		t.Fatalf("value still used by b was deleted: %v", err)
	}
	must(pr.Update([]byte("b"), []byte("w")))
	checkPruned(t, pr)
	if _, err := pr.kvs.Get(h([]byte("v"))); err == nil {
		t.Fatalf("orphaned value v was not deleted")
	}
	must(pr.Delete([]byte("nope"))) // absent key: no-op
	checkPruned(t, pr)
	if got, _ := pr.Get([]byte("b")); !bytes.Equal(got, []byte("w")) {
		t.Fatalf("Get(b) = %q", got)
	}
	if _, err := pr.UpdateForRoot([]byte("a"), []byte("x"), pr.Root()); !errors.Is(err, ErrPrunedTree) {
		t.Fatalf("UpdateForRoot on pruned tree: err = %v, want ErrPrunedTree", err)
	}
}

// The pruned tree also works on BadgerStore (KVStore.Delete).
func TestPrunedBadgerStore(t *testing.T) {
	store, err := NewBadgerStore(filepath.Join(t.TempDir(), "bs"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ref := NewSparseMerkleTree(NewSimpleMap(), sha256.New())
	pr := NewPrunedSparseMerkleTree(store, sha256.New())
	h := func(b []byte) []byte {
		s := sha256.Sum256(b)
		return s[:]
	}
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("k%d", i%7))
		v := []byte(fmt.Sprintf("v%d", i))
		if _, err := ref.Update(k, v); err != nil {
			t.Fatal(err)
		}
		if _, err := pr.Update(k, v); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ref.Root(), pr.Root()) {
			t.Fatalf("root mismatch at %d", i)
		}
	}
	if _, err := store.Get(h([]byte("v0"))); err == nil {
		t.Fatalf("orphaned value v0 still in badger")
	}
	var ike *InvalidKeyError
	if _, err := store.Get(h([]byte("v0"))); !errors.As(err, &ike) {
		t.Fatalf("want InvalidKeyError, got %v", err)
	}
	proof, err := pr.Prove([]byte("k6"))
	if err != nil || !VerifyProof(proof, pr.Root(), []byte("k6"), []byte("v48"), sha256.New()) {
		t.Fatalf("proof for k6 does not verify (err %v)", err)
	}
	if pr.pruneAnomalies != 0 {
		t.Fatalf("pruneAnomalies = %d", pr.pruneAnomalies)
	}
	if err := store.Delete([]byte("missing")); err != nil {
		t.Fatalf("Delete of a missing key: %v", err)
	}
}

func fillTree(tr *SparseMerkleTree, rng *rand.Rand, nKeys, rounds, valueSize int) {
	for r := 0; r < rounds; r++ {
		for i := 0; i < nKeys; i++ {
			v := make([]byte, valueSize)
			rng.Read(v)
			if _, err := tr.Update([]byte(fmt.Sprintf("user%06d", i)), v); err != nil {
				panic(err)
			}
		}
	}
}

// Reports bytes retained per live key (store payload and Go heap) for the
// pruned and the unpruned tree with YCSB-like 1000-byte values, 10 writes per
// key. The pruned payload must stay near one value + a few nodes per key.
func TestPrunedBytesRetainedPerKey(t *testing.T) {
	const nKeys, rounds, valueSize = 1000, 10, 1000
	payload := map[bool]float64{}
	for _, pruned := range []bool{false, true} {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		var tr *SparseMerkleTree
		if pruned {
			tr = NewPrunedSparseMerkleTree(NewSimpleMap(), sha256.New())
		} else {
			tr = NewSparseMerkleTree(NewSimpleMap(), sha256.New())
		}
		fillTree(tr, rand.New(rand.NewSource(1)), nKeys, rounds, valueSize)
		runtime.GC()
		runtime.ReadMemStats(&after)
		heap := float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / nKeys
		payload[pruned] = float64(storeBytes(tr)) / nKeys
		t.Logf("pruned=%v: %d keys x %d writes, %d B values: store %d entries, payload %.0f B/key, heap %.0f B/key",
			pruned, nKeys, rounds, valueSize, storeLen(tr), payload[pruned], heap)
		runtime.KeepAlive(tr)
	}
	if payload[true] > valueSize+600 {
		t.Fatalf("pruned payload %.0f B/key, want <= %d", payload[true], valueSize+600)
	}
	if payload[false] < float64(rounds)/2*payload[true] {
		t.Fatalf("unpruned payload %.0f B/key should be ~%dx the pruned %.0f", payload[false], rounds, payload[true])
	}
}

// BenchmarkPrunedUpdate: update cost and retained store bytes per live key
// (10k keys, 1000-byte values, as in the experiments).
func BenchmarkPrunedUpdate(b *testing.B) {
	const nKeys, valueSize = 10000, 1000
	tr := NewPrunedSparseMerkleTree(NewSimpleMap(), sha256.New())
	rng := rand.New(rand.NewSource(1))
	fillTree(tr, rng, nKeys, 1, valueSize)
	v := make([]byte, valueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rng.Read(v)
		val := append([]byte(nil), v...)
		if _, err := tr.Update([]byte(fmt.Sprintf("user%06d", rng.Intn(nKeys))), val); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(storeBytes(tr))/nKeys, "storeB/livekey")
	b.ReportMetric(float64(storeLen(tr))/nKeys, "entries/livekey")
}
