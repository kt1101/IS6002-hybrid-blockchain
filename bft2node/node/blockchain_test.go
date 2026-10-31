package node

import (
	"testing"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/logging"
)

// fakeConsensus reports a fixed committed block (all PruneToHeight needs).
type fakeConsensus struct{ committed *hotstuff.Block }

func (f *fakeConsensus) StopVoting(hotstuff.View)        {}
func (f *fakeConsensus) Propose(hotstuff.SyncInfo)       {}
func (f *fakeConsensus) CommittedBlock() *hotstuff.Block { return f.committed }
func (f *fakeConsensus) ChainLength() int                { return 3 }

func TestBlockChainEvictsOldViewsAndReportsForks(t *testing.T) {
	// Budget 0: evict everything allowed, i.e. below height-3.
	chain := newBlockChain(3, 0)
	cs := &fakeConsensus{}
	chain.consensus = cs
	chain.logger = logging.New("test")

	var qc hotstuff.QuorumCert
	parent := hotstuff.GetGenesis()
	byView := map[hotstuff.View]*hotstuff.Block{}
	for v := hotstuff.View(1); v <= 10; v++ {
		b := hotstuff.NewBlock(parent.Hash(), qc, hotstuff.Command("cmd"), v, 1)
		chain.Store(b)
		byView[v] = b
		parent = b
	}
	// A block of view 4 that is not on the chain (a fork).
	fork := hotstuff.NewBlock(byView[2].Hash(), qc, "forked", 4, 2)
	chain.Store(fork)
	chain.Store(byView[4]) // view 4 at height 4 is the chain's block again

	cs.committed = byView[8]
	forked := chain.PruneToHeight(8)
	if len(forked) != 0 {
		t.Fatalf("forked = %v, want none (blockAtHeight[4] is the chain block)", forked)
	}
	for v := hotstuff.View(1); v <= 10; v++ {
		_, ok := chain.LocalGet(byView[v].Hash())
		if want := v >= 5; ok != want {
			t.Errorf("view %d: present = %v, want %v (minKeep 3 below height 8)", v, ok, want)
		}
	}
	if _, ok := chain.LocalGet(fork.Hash()); ok {
		t.Error("the forked block of view 4 was not evicted")
	}
	if _, ok := chain.LocalGet(hotstuff.GetGenesis().Hash()); !ok {
		t.Error("genesis was evicted")
	}

	// A block on another branch at a view above the last prune height is
	// reported as forked by the next prune.
	other := hotstuff.NewBlock(byView[8].Hash(), qc, "other", 11, 3)
	chain.Store(other)
	next := hotstuff.NewBlock(byView[10].Hash(), qc, "next", 12, 4)
	chain.Store(next)
	cs.committed = next
	// next's parent chain: 10 -> 9 -> 8 ... (11 is the fork)
	forked = chain.PruneToHeight(12)
	if len(forked) != 1 || forked[0].Hash() != other.Hash() {
		t.Fatalf("forked = %v, want [the view-11 block]", forked)
	}
	// Horizon 12-3 = 9: view 8 goes, views 9 and up stay.
	if _, ok := chain.LocalGet(byView[8].Hash()); ok {
		t.Error("view 8 was not evicted (horizon 9)")
	}
	if _, ok := chain.LocalGet(byView[9].Hash()); !ok {
		t.Error("view 9 was evicted (horizon 9)")
	}
}

// The byte budget: eviction removes the oldest views only while the stored
// blocks cost more than retainBytes, so with room to spare nothing goes, and
// fillers (tiny) cost little.
func TestBlockChainEvictsByBytes(t *testing.T) {
	const cmdLen = 10000
	budget := int64(4 * (cmdLen + blockOverhead)) // room for 4 big blocks
	chain := newBlockChain(2, budget)
	cs := &fakeConsensus{}
	chain.consensus = cs
	chain.logger = logging.New("test")

	var qc hotstuff.QuorumCert
	parent := hotstuff.GetGenesis()
	byView := map[hotstuff.View]*hotstuff.Block{}
	for v := hotstuff.View(1); v <= 20; v++ {
		cmd := hotstuff.Command("")
		if v%5 == 0 {
			cmd = hotstuff.Command(make([]byte, cmdLen)) // views 5, 10, 15, 20
		}
		b := hotstuff.NewBlock(parent.Hash(), qc, cmd, v, 1)
		chain.Store(b)
		byView[v] = b
		parent = b
	}
	cs.committed = byView[20]
	chain.PruneToHeight(20)
	if chain.bytes > budget {
		t.Fatalf("stored %d bytes after prune, budget %d", chain.bytes, budget)
	}
	// 4 big blocks + 16 fillers exceed the budget by the fillers: the oldest
	// views go until it fits, so view 5 (big) is gone and views 10..20 stay.
	for v := hotstuff.View(1); v <= 20; v++ {
		_, ok := chain.LocalGet(byView[v].Hash())
		if v >= 10 && !ok {
			t.Errorf("view %d evicted, want kept (it fits the budget)", v)
		}
		if v <= 5 && ok {
			t.Errorf("view %d kept, want evicted (over the budget)", v)
		}
	}

	// A generous budget evicts nothing, even far below the height.
	roomy := newBlockChain(2, 1<<30)
	roomy.consensus = cs
	roomy.logger = chain.logger
	for v := hotstuff.View(1); v <= 20; v++ {
		roomy.Store(byView[v])
	}
	roomy.PruneToHeight(20)
	for v := hotstuff.View(1); v <= 20; v++ {
		if _, ok := roomy.LocalGet(byView[v].Hash()); !ok {
			t.Errorf("roomy: view %d evicted", v)
		}
	}
}
