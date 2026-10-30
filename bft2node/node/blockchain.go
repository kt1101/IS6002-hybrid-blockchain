package node

// Adapted from github.com/relab/hotstuff v0.5.0 blockchain/blockchain.go
// (MIT license). The library's blockChain never deletes from its blocks map,
// so every block, i.e. every Veritas block, stays in memory for the life of
// the process (hundreds of MB per run inside a 768m container). This copy is
// identical except that PruneToHeight also evicts the blocks of views older
// than retain views below the pruned height, and Get releases its fetch
// context when the fetch returns.

import (
	"context"
	"sync"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
	"github.com/relab/hotstuff/synchronizer"
)

type blockChain struct {
	configuration modules.Configuration
	consensus     modules.Consensus
	eventLoop     *eventloop.EventLoop
	logger        logging.Logger

	retain hotstuff.View

	mut           sync.Mutex
	pruneHeight   hotstuff.View
	blocks        map[hotstuff.Hash]*hotstuff.Block
	blockAtHeight map[hotstuff.View]*hotstuff.Block
	pendingFetch  map[hotstuff.Hash]context.CancelFunc // allows a pending fetch operation to be canceled

	byView    map[hotstuff.View][]hotstuff.Hash // hashes in blocks, by view (for eviction)
	evictedTo hotstuff.View                     // blocks of views < evictedTo are gone (genesis excepted)
}

var (
	_ modules.BlockChain = (*blockChain)(nil)
	_ modules.Module     = (*blockChain)(nil)
)

// onceFunc returns f wrapped so that only its first call runs.
func onceFunc(f func()) func() {
	var once sync.Once
	return func() { once.Do(f) }
}

// newBlockChain returns a blockchain that keeps the blocks of the last
// retain views below the last pruned (committed) height, plus everything
// above it. retain must be >= 1.
func newBlockChain(retain hotstuff.View) *blockChain {
	if retain < 1 {
		retain = 1
	}
	bc := &blockChain{
		retain:        retain,
		blocks:        make(map[hotstuff.Hash]*hotstuff.Block),
		blockAtHeight: make(map[hotstuff.View]*hotstuff.Block),
		pendingFetch:  make(map[hotstuff.Hash]context.CancelFunc),
		byView:        make(map[hotstuff.View][]hotstuff.Hash),
		evictedTo:     1,
	}
	bc.Store(hotstuff.GetGenesis())
	return bc
}

func (chain *blockChain) InitModule(mods *modules.Core) {
	mods.Get(
		&chain.configuration,
		&chain.consensus,
		&chain.eventLoop,
		&chain.logger,
	)
}

// index records hash under its view for eviction. Called with mut held.
// Genesis (view 0) is never evicted; a block older than the eviction
// horizon (fetched late) is filed under the horizon so the next pass evicts it.
func (chain *blockChain) index(block *hotstuff.Block) {
	v := block.View()
	if v == 0 {
		return
	}
	if v < chain.evictedTo {
		v = chain.evictedTo
	}
	chain.byView[v] = append(chain.byView[v], block.Hash())
}

// Store stores a block in the blockchain
func (chain *blockChain) Store(block *hotstuff.Block) {
	chain.mut.Lock()
	defer chain.mut.Unlock()

	if _, ok := chain.blocks[block.Hash()]; !ok {
		chain.index(block)
	}
	chain.blocks[block.Hash()] = block
	chain.blockAtHeight[block.View()] = block

	// cancel any pending fetch operations
	if cancel, ok := chain.pendingFetch[block.Hash()]; ok {
		cancel()
	}
}

// LocalGet retrieves a block given its hash. It will only try the local cache.
func (chain *blockChain) LocalGet(hash hotstuff.Hash) (*hotstuff.Block, bool) {
	chain.mut.Lock()
	defer chain.mut.Unlock()

	block, ok := chain.blocks[hash]
	if !ok {
		return nil, false
	}

	return block, true
}

// Get retrieves a block given its hash. Get will try to find the block locally.
// If it is not available locally, it will try to fetch the block.
func (chain *blockChain) Get(hash hotstuff.Hash) (block *hotstuff.Block, ok bool) {
	// need to declare vars early, or else we won't be able to use goto
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	chain.mut.Lock()
	block, ok = chain.blocks[hash]
	if ok {
		goto done
	}

	ctx, cancel = synchronizer.TimeoutContext(chain.eventLoop.Context(), chain.eventLoop)
	// Second change: cancel is also called once the fetch returns (the
	// library never unregisters the context's event handlers unless the
	// block arrives through Store). It unregisters handlers by slot, so it
	// must run only once: Store may call it too.
	cancel = onceFunc(cancel)
	chain.pendingFetch[hash] = cancel

	chain.mut.Unlock()
	chain.logger.Debugf("Attempting to fetch block: %.8s", hash)
	block, ok = chain.configuration.Fetch(ctx, hash)
	chain.mut.Lock()

	delete(chain.pendingFetch, hash)
	cancel()
	if !ok {
		// check again in case the block arrived while we we fetching
		block, ok = chain.blocks[hash]
		goto done
	}

	chain.logger.Debugf("Successfully fetched block: %.8s", hash)

	if _, ok := chain.blocks[hash]; !ok {
		chain.index(block)
	}
	chain.blocks[hash] = block
	chain.blockAtHeight[block.View()] = block

done:
	chain.mut.Unlock()

	if !ok {
		return nil, false
	}

	return block, true
}

// Extends checks if the given block extends the branch of the target block.
func (chain *blockChain) Extends(block, target *hotstuff.Block) bool {
	current := block
	ok := true
	for ok && current.View() > target.View() {
		current, ok = chain.Get(current.Parent())
	}
	return ok && current.Hash() == target.Hash()
}

func (chain *blockChain) PruneToHeight(height hotstuff.View) (forkedBlocks []*hotstuff.Block) {
	chain.mut.Lock()
	defer chain.mut.Unlock()

	committedHeight := chain.consensus.CommittedBlock().View()
	committedViews := make(map[hotstuff.View]bool)
	committedViews[committedHeight] = true
	for h := committedHeight; h >= chain.pruneHeight; {
		block, ok := chain.blockAtHeight[h]
		if !ok {
			break
		}
		parent, ok := chain.blocks[block.Parent()]
		if !ok || parent.View() < chain.pruneHeight {
			break
		}
		h = parent.View()
		committedViews[h] = true
	}

	for h := height; h > chain.pruneHeight; h-- {
		if !committedViews[h] {
			block, ok := chain.blockAtHeight[h]
			if ok {
				chain.logger.Debugf("PruneToHeight: found forked block: %v", block)
				forkedBlocks = append(forkedBlocks, block)
			}
		}
		delete(chain.blockAtHeight, h)
	}
	chain.pruneHeight = height

	// The only change from the library: evict old blocks. Everything at or
	// above height-retain stays (the commit walk above only looks at views
	// >= the previous prune height; votes, QCs and fetches look at recent views).
	if height > chain.retain {
		horizon := height - chain.retain
		for v := chain.evictedTo; v < horizon; v++ {
			for _, h := range chain.byView[v] {
				delete(chain.blocks, h)
			}
			delete(chain.byView, v)
		}
		if horizon > chain.evictedTo {
			chain.evictedTo = horizon
		}
	}
	return forkedBlocks
}
