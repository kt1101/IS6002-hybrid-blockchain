package node

import (
	"testing"

	"github.com/relab/hotstuff"
)

func TestShuffledRoundRobinIsAPermutationPerEpochAndDeterministic(t *testing.T) {
	const n = 4
	a, b := newShuffledRoundRobin(n, 7), newShuffledRoundRobin(n, 7)
	for epoch := hotstuff.View(0); epoch < 50; epoch++ {
		seen := map[hotstuff.ID]bool{}
		for pos := hotstuff.View(0); pos < n; pos++ {
			v := epoch*n + pos
			id := a.GetLeader(v)
			if id < 1 || id > n || seen[id] {
				t.Fatalf("epoch %d: leader %d of view %d is out of range or repeated", epoch, id, v)
			}
			seen[id] = true
		}
	}
	// Same seed: same schedule, also when queried out of order.
	// View is unsigned: stop before v-7 would wrap around.
	for v := hotstuff.View(300); v >= 7; v -= 7 {
		if a.GetLeader(v) != b.GetLeader(v) {
			t.Fatalf("view %d: two rotations with the same seed disagree", v)
		}
	}
}

// The property the crash test needs: with any one replica dead, every live
// replica regularly leads a view whose NEXT leader is alive (so its block
// can get a QC). With fixed round robin the dead replica's predecessor never
// does.
func TestShuffledRoundRobinGivesEveryLiveReplicaUsableSlots(t *testing.T) {
	const n = 4
	// The schedule deployments use (bft2-node's default --key-seed).
	r := newShuffledRoundRobin(n, leaderSeed(DefaultKeySeed))
	for dead := hotstuff.ID(1); dead <= n; dead++ {
		usable := map[hotstuff.ID]int{}
		for v := hotstuff.View(1); v <= 400; v++ {
			l := r.GetLeader(v)
			if l != dead && r.GetLeader(v+1) != dead {
				usable[l]++
			}
		}
		for id := hotstuff.ID(1); id <= n; id++ {
			if id != dead && usable[id] < 20 {
				t.Errorf("dead %d: replica %d has only %d usable slots in 400 views", dead, id, usable[id])
			}
		}
	}
}
