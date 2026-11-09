package core

import (
	"reflect"
	"strings"
	"testing"
	"time"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/audit"
	"hybrid/veritas/benchmark"
)

// pbvBlockAt is a block with the height consensus delivered it at.
type pbvBlockAt struct {
	height int64
	blk    *pbv.Block
}

func mustParseFault(t *testing.T, spec string, start time.Time, after time.Duration) FaultPolicy {
	t.Helper()
	fp, err := ParseFault(spec, start, after)
	if err != nil {
		t.Fatalf("ParseFault(%q, start, %v): %v", spec, after, err)
	}
	return fp
}

func TestNoFaultIsIdentity(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	for name, fp := range map[string]FaultPolicy{
		"NoFault{}":                          NoFault{},
		"none, delay already passed":         mustParseFault(t, "none", past, 0),
		"none, delay of an hour still ahead": mustParseFault(t, "none", time.Now(), time.Hour),
	} {
		if !fp.OnVote() {
			t.Errorf("%s: OnVote() = false, want true", name)
		}
		for _, v := range []string{"", "v", "value-1", "x" + benchmark.CorruptMarker} {
			if got := fp.OnApply("k", v); got != v {
				t.Errorf("%s: OnApply(k, %q) = %q, want it unchanged", name, v, got)
			}
		}
	}
	// "none" is NoFault itself, so the executor and the kafka backend see no
	// policy object at all.
	if fp := mustParseFault(t, "none", past, 0); !reflect.DeepEqual(fp, NoFault{}) {
		t.Errorf("ParseFault(none) = %#v, want NoFault{}", fp)
	}
}

func TestCorruptOnlyAfterDelay(t *testing.T) {
	now := time.Now()

	// Before start+after: untouched.
	before := mustParseFault(t, "corrupt", now, time.Hour)
	if got := before.OnApply("k", "v"); got != "v" {
		t.Fatalf("before the delay: OnApply = %q, want %q", got, "v")
	}
	// After start+after: the marker suffix.
	after := mustParseFault(t, "corrupt", now.Add(-2*time.Hour), time.Hour)
	if got, want := after.OnApply("k", "v"), "v"+benchmark.CorruptMarker; got != want {
		t.Fatalf("after the delay: OnApply = %q, want %q", got, want)
	}
	if benchmark.CorruptMarker != "-CORRUPT" {
		t.Fatalf("CorruptMarker = %q, want -CORRUPT", benchmark.CorruptMarker)
	}
	// A corrupt replica keeps voting (it is wrong, not silent).
	if !before.OnVote() || !after.OnVote() {
		t.Fatalf("corrupt policy stopped voting")
	}

	// Crossing the mark while the process runs.
	fp := mustParseFault(t, "corrupt", time.Now(), 50*time.Millisecond)
	if got := fp.OnApply("k", "v"); got != "v" {
		t.Fatalf("just after start: OnApply = %q, want %q", got, "v")
	}
	time.Sleep(120 * time.Millisecond)
	if got := fp.OnApply("k", "v"); !strings.HasSuffix(got, benchmark.CorruptMarker) {
		t.Fatalf("after the mark: OnApply = %q, want the %s suffix", got, benchmark.CorruptMarker)
	}

	// Activate (the harness's SIGUSR1) switches the fault on early.
	early := mustParseFault(t, "corrupt", time.Now(), time.Hour)
	a, ok := early.(Activator)
	if !ok {
		t.Fatalf("ParseFault(corrupt) = %T, which does not implement Activator", early)
	}
	if a.Active() {
		t.Fatalf("Active() before the delay and before Activate")
	}
	a.Activate()
	a.Activate() // idempotent
	if !a.Active() {
		t.Fatalf("Active() = false after Activate")
	}
	if got := early.OnApply("k", "v"); got != "v"+benchmark.CorruptMarker {
		t.Fatalf("after Activate: OnApply = %q", got)
	}
}

func TestSilentStopsVoting(t *testing.T) {
	now := time.Now()
	before := mustParseFault(t, "silent", now, time.Hour)
	if !before.OnVote() {
		t.Fatalf("before the delay: OnVote() = false, want true")
	}
	after := mustParseFault(t, "silent", now.Add(-2*time.Hour), time.Hour)
	if after.OnVote() {
		t.Fatalf("after the delay: OnVote() = true, want false")
	}
	// A silent replica applies honestly.
	for _, fp := range []FaultPolicy{before, after} {
		if got := fp.OnApply("k", "v"); got != "v" {
			t.Fatalf("silent policy changed the value: %q", got)
		}
	}
	// Activate silences it early.
	early := mustParseFault(t, "silent", now, time.Hour)
	early.(Activator).Activate()
	if early.OnVote() {
		t.Fatalf("after Activate: OnVote() = true, want false")
	}
}

func TestParseFaultRejectsBadInput(t *testing.T) {
	for _, tc := range []struct {
		spec  string
		after time.Duration
	}{
		{"", 0},
		{"bogus", 0},
		{"Corrupt", 0},
		{" silent", 0},
		{"corrupt", -time.Second},
		{"none", -time.Second},
	} {
		if fp, err := ParseFault(tc.spec, time.Now(), tc.after); err == nil {
			t.Errorf("ParseFault(%q, now, %v) = %#v, want an error", tc.spec, tc.after, fp)
		}
	}
}

// Three replicas apply the same blocks; the one with a corrupt policy ends
// with a different root digest and the auditor's majority rule names it.
func TestCorruptNodeDivergesRoot(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	policies := map[string]FaultPolicy{
		"node1": mustParseFault(t, "none", past, 0),
		"node2": NoFault{},
		"node3": mustParseFault(t, "corrupt", past, 0),
	}
	blocks := []*pbvBlockAt{
		{1, mkBlock(mkLog(1, mkSet("k1", "v1", 0)), mkLog(2, mkSet("k2", "v2", 0)))},
		{2, mkBlock(mkLog(3, mkSet("k1", "v1b", 0)))},
	}
	roots := map[string][]byte{}
	stores := map[string]KVStore{}
	for name, fp := range policies {
		ex, s := newTestExecutor(t, VersionOrdered, fp)
		for _, b := range blocks {
			mustApply(t, ex, b.height, b.blk)
		}
		roots[name] = ex.RootDigest()
		stores[name] = s
	}
	if string(roots["node1"]) != string(roots["node2"]) {
		t.Fatalf("honest replicas disagree: %x vs %x", roots["node1"], roots["node2"])
	}
	if got := audit.Divergent(roots); !reflect.DeepEqual(got, []string{"node3"}) {
		t.Fatalf("audit.Divergent = %v, want [node3]", got)
	}
	// The corrupt replica serves wrong values (what ycsbbench counts as
	// wrong_gets); the honest ones do not.
	if got := getEntry(t, stores["node3"], "k1").Val; got != "v1b"+benchmark.CorruptMarker {
		t.Fatalf("node3 k1 = %q, want v1b%s", got, benchmark.CorruptMarker)
	}
	if got := getEntry(t, stores["node1"], "k1").Val; got != "v1b" {
		t.Fatalf("node1 k1 = %q, want v1b", got)
	}
}

// ---- F6: read-path faults (phase 3) ----

func readFaultOf(t *testing.T, fp FaultPolicy) ReadFault {
	t.Helper()
	rf, ok := fp.(ReadFault)
	if !ok {
		t.Fatalf("%T does not implement ReadFault", fp)
	}
	return rf
}

func TestParseFaultReadLie(t *testing.T) {
	fp := mustParseFault(t, FaultReadLie, time.Now(), time.Hour)
	rf := readFaultOf(t, fp)
	if got := rf.OnRead("k", "v", false); got != "v" {
		t.Fatalf("before activation: OnRead = %q, want %q", got, "v")
	}
	fp.(Activator).Activate()
	if got := rf.OnRead("k", "v", false); got != "v"+benchmark.CorruptMarker {
		t.Fatalf("after activation: OnRead = %q, want %q", got, "v"+benchmark.CorruptMarker)
	}
	if got := rf.OnRead("k", "v", true); got != "v"+benchmark.CorruptMarker {
		t.Fatalf("after activation, signed: OnRead = %q, want %q", got, "v"+benchmark.CorruptMarker)
	}
	if got := fp.OnApply("k", "v"); got != "v" {
		t.Fatalf("readlie must keep the state honest: OnApply = %q", got)
	}
	if !fp.OnVote() {
		t.Fatal("readlie must keep voting")
	}
}

func TestReadLieEvasiveHonestWhenSigned(t *testing.T) {
	fp := mustParseFault(t, FaultReadLieEvasive, time.Now(), time.Hour)
	fp.(Activator).Activate()
	rf := readFaultOf(t, fp)
	if got := rf.OnRead("k", "v", true); got != "v" {
		t.Fatalf("signed read: OnRead = %q, want %q", got, "v")
	}
	if got := rf.OnRead("k", "v", false); got != "v"+benchmark.CorruptMarker {
		t.Fatalf("unsigned read: OnRead = %q, want %q", got, "v"+benchmark.CorruptMarker)
	}
	if got := fp.OnApply("k", "v"); got != "v" {
		t.Fatalf("OnApply = %q, want %q", got, "v")
	}
}

func TestReadFaultsOnlyForReadKinds(t *testing.T) {
	for _, spec := range []string{FaultSilent, FaultCorrupt} {
		fp := mustParseFault(t, spec, time.Now(), 0)
		if rf, ok := fp.(ReadFault); ok {
			if got := rf.OnRead("k", "v", false); got != "v" {
				t.Fatalf("%s: OnRead = %q, want it unchanged", spec, got)
			}
		}
	}
	if _, ok := FaultPolicy(NoFault{}).(ReadFault); ok {
		if got := FaultPolicy(NoFault{}).(ReadFault).OnRead("k", "v", false); got != "v" {
			t.Fatalf("NoFault: OnRead = %q", got)
		}
	}
}

// readLieCluster: 4 nodes with attestations, node 4 lying on reads from the start.
func readLieCluster(t *testing.T) []*clusterNode {
	t.Helper()
	return attestCluster(t, 4, 1, map[int]FaultPolicy{4: mustParseFault(t, FaultReadLie, time.Now(), 0)}, func(i int, c *ServerConfig) {
		c.CertifiedWait = 2 * time.Second
	})
}

func TestReadLieKeepsRootsEqual(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 20)
	eventually(t, srvTestTimeout, "every node applied the 20 Sets with equal roots", func() bool {
		r0 := nodes[0].ex.RootDigest()
		for _, n := range nodes {
			if v, err := getValue(n.srv, "k19"); err != nil || !strings.HasPrefix(v, "v19") {
				return false
			}
			if string(n.ex.RootDigest()) != string(r0) {
				return false
			}
		}
		return true
	})
	for i, n := range nodes {
		if _, ok := n.srv.Diverged(); ok {
			t.Fatalf("node %d diverged: readlie must not change the state", i+1)
		}
	}
	got, err := getValue(nodes[3].srv, "k0")
	if err != nil || got != "v0"+benchmark.CorruptMarker {
		t.Fatalf("Get on the lying node = %q, %v; want %q", got, err, "v0"+benchmark.CorruptMarker)
	}
	if got, err := getValue(nodes[0].srv, "k0"); err != nil || got != "v0" {
		t.Fatalf("Get on an honest node = %q, %v; want v0", got, err)
	}
}

func TestCertifiedReadFromReadLieRejectedByClient(t *testing.T) {
	nodes := readLieCluster(t)
	setN(t, nodes[0].srv, 5)
	r, err := certifiedRead(nodes[3].srv, "k1")
	if err != nil {
		t.Fatalf("certified read from the lying node: %v (its state is honest, the read must be served)", err)
	}
	if r.GetValue() != "v1"+benchmark.CorruptMarker {
		t.Fatalf("lying node returned %q, want the corrupt value", r.GetValue())
	}
	if verifyResp(r, "k1") {
		t.Fatal("the proof verifies for the lied value: the client could not detect F6")
	}
	// The proof, root and version are those of the real value.
	honest := *r
	honest.Value = "v1"
	if !verifyResp(&honest, "k1") {
		t.Fatal("proof of the lying node does not verify for the real value")
	}
	ok, err := certifiedRead(nodes[0].srv, "k1")
	if err != nil || ok.GetValue() != "v1" || !verifyResp(ok, "k1") {
		t.Fatalf("certified read from an honest node: %v %v", ok, err)
	}
}
