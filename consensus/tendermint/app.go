package tendermint

import (
	"sync"

	abcitypes "github.com/tendermint/tendermint/abci/types"
)

// DeliverBuffer is the capacity of the Deliver channel. When it is full,
// DeliverTx waits for room, which holds Tendermint's ABCI connection and so
// stalls this node's block execution (backpressure).
const DeliverBuffer = 256

var _ abcitypes.Application = (*App)(nil)

// App is the ABCI application of the Tendermint backend. One ABCI tx is one
// marshalled Veritas block; the app does not interpret it. DeliverTx pushes a
// copy of every tx to the Deliver channel, in Tendermint's DeliverTx order
// (block by block, tx by tx), which is the same on every node.
//
// The app keeps no state of its own (the core keeps its state in redis and
// the ledger), so Commit returns an empty app hash and Info reports height
// 0. Every node returns the same (empty) app hash after every block, so the
// AppHash in the block headers agrees on all validators.
type App struct {
	abcitypes.BaseApplication

	deliver chan []byte
	closeCh chan struct{}

	mu        sync.RWMutex // write-held only to close deliver
	closed    bool
	closeOnce sync.Once
}

// NewApp returns an App with an empty Deliver channel of DeliverBuffer.
func NewApp() *App {
	return &App{
		deliver: make(chan []byte, DeliverBuffer),
		closeCh: make(chan struct{}),
	}
}

// Info reports no stored state (height 0, empty app hash).
func (*App) Info(abcitypes.RequestInfo) abcitypes.ResponseInfo {
	return abcitypes.ResponseInfo{Data: "veritas core (consensus/tendermint)"}
}

// GasPerTx is the gas every tx wants (CheckTx GasWanted). With the genesis
// consensus_params.block.max_gas of experiments/cluster/tm_config.py (200
// for this backend) it caps the txs per Tendermint block at max_gas: the
// mempool reaps a proposal until the gas total would exceed max_gas. The cap
// keeps a block below the 256 async requests Tendermint v0.35.0's socket
// ABCI client can queue (execBlockOnProxyApp sends every DeliverTx of a
// block with DeliverTxAsync; the 257th fails with "buffer is full" and
// consensus halts), whatever the size of the Veritas blocks.
const GasPerTx = 1

// CheckTx accepts every tx (the core validates blocks when it applies them)
// and asks for GasPerTx gas.
func (*App) CheckTx(abcitypes.RequestCheckTx) abcitypes.ResponseCheckTx {
	return abcitypes.ResponseCheckTx{Code: abcitypes.CodeTypeOK, GasWanted: GasPerTx}
}

// DeliverTx pushes a copy of req.Tx (one Veritas block) to Deliver, waiting
// for room, and returns Code 0. After Close it drops the tx: it never sends
// on the closed channel, and a DeliverTx blocked on a full Deliver returns
// once Close is called.
func (a *App) DeliverTx(req abcitypes.RequestDeliverTx) abcitypes.ResponseDeliverTx {
	data := append([]byte(nil), req.Tx...)
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		return abcitypes.ResponseDeliverTx{Code: abcitypes.CodeTypeOK}
	}
	select {
	case a.deliver <- data:
	case <-a.closeCh:
	}
	return abcitypes.ResponseDeliverTx{Code: abcitypes.CodeTypeOK}
}

// Commit returns an empty app hash (the app has no state), the same on
// every node, and RetainHeight 0 (Tendermint keeps every block).
func (*App) Commit() abcitypes.ResponseCommit {
	return abcitypes.ResponseCommit{}
}

// Deliver returns the channel of delivered blocks, closed by Close.
func (a *App) Deliver() <-chan []byte { return a.deliver }

// Close closes Deliver exactly once; later calls do nothing. A DeliverTx
// waiting for room returns first (closeCh), so Close never blocks on a full
// Deliver.
func (a *App) Close() {
	a.closeOnce.Do(func() {
		close(a.closeCh)
		a.mu.Lock()
		a.closed = true
		close(a.deliver)
		a.mu.Unlock()
	})
}
