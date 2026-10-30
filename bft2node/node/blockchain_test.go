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
	chain := newBlockChain(3)
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
			t.Errorf("view %d: present = %v, want %v (retain 3 below height 8)", v, ok, want)
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
