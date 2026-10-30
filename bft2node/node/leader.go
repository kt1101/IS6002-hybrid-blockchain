package node

import (
	"math/rand"
	"sync"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/modules"
)

// shuffledRoundRobin is the leader rotation: every replica leads exactly
// once per epoch of n consecutive views (as round robin), but the order of
// the leaders within epoch e is a permutation drawn from math/rand seeded
// with seed+e, the same on every replica (it depends on the view only).
//
// Why not the library's round robin (leader(v) = v mod n + 1): the votes
// for the block of view v go to leader(v+1). With a fixed order, the
// replica just before a crashed one ALWAYS hands its votes to the crashed
// leader, so none of its blocks ever gets a QC: they are forked and, with
// the re-queue, re-proposed in the same doomed slot forever (TestBFT2
// SurvivesOneCrash: replica 3's commands never committed once replica 4
// was stopped). Shuffling the order per epoch gives every live replica
// slots followed by a live leader, while keeping each replica's share of
// views (and so throughput in the fault-free case) the same.
type shuffledRoundRobin struct {
	n    int
	seed int64

	mu    sync.Mutex
	epoch hotstuff.View // epoch of perm (cache)
	perm  []int
}

var _ modules.LeaderRotation = (*shuffledRoundRobin)(nil)

func newShuffledRoundRobin(n int, seed int64) *shuffledRoundRobin {
	return &shuffledRoundRobin{n: n, seed: seed}
}

// GetLeader returns the leader of view.
func (s *shuffledRoundRobin) GetLeader(view hotstuff.View) hotstuff.ID {
	n := hotstuff.View(s.n)
	epoch, pos := view/n, view%n
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.perm == nil || s.epoch != epoch {
		s.perm = rand.New(rand.NewSource(s.seed + int64(epoch))).Perm(s.n)
		s.epoch = epoch
	}
	return hotstuff.ID(s.perm[pos] + 1)
}
