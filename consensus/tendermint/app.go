package tendermint

import (
	"fmt"
	"log"
	"sync"

	abcitypes "github.com/tendermint/tendermint/abci/types"
)

// DeliverBuffer is the capacity of the Deliver channel.
const DeliverBuffer = 256

// DeliverQueueBytes bounds the blocks DeliverTx has taken but the core has
// not read from Deliver yet (the internal queue behind the channel).
// DeliverTx never waits: the ABCI socket server serialises every connection
// (consensus, mempool, query) behind one lock, so a DeliverTx waiting for
// a slow core would also hold CheckTx and make broadcast_tx_sync time out
// (the tendermint F2 liveness loss of phase 1). Going over the bound is a
// fatal error, not backpressure: the app logs "tendermint: deliver queue
// overflow", records it (Err) and closes Deliver, so the node stops instead
// of hanging or silently dropping a block.
const DeliverQueueBytes = 128 << 20

var _ abcitypes.Application = (*App)(nil)

// App is the ABCI application of the Tendermint backend. One ABCI tx is one
// marshalled Veritas block; the app does not interpret it. DeliverTx queues
// a copy of every tx; a pump goroutine passes them to the Deliver channel
// in Tendermint's DeliverTx order (block by block, tx by tx), which is the
// same on every node.
//
// The app keeps no state of its own (the core keeps its state in redis and
// the ledger), so Commit returns an empty app hash and Info reports height
// 0. Every node returns the same (empty) app hash after every block, so the
// AppHash in the block headers agrees on all validators.
type App struct {
	abcitypes.BaseApplication

	deliver  chan []byte
	maxBytes int64
	logf     func(format string, args ...interface{})

	mu     sync.Mutex // guards queue, qbytes, closed, err; never held while sending
	queue  [][]byte
	qbytes int64
	closed bool
	err    error

	wake      chan struct{} // 1-buffered: the queue got a block
	closeCh   chan struct{}
	pumpDone  chan struct{}
	closeOnce sync.Once
}

// NewApp returns an App with an empty Deliver channel of DeliverBuffer and
// an internal queue bounded by DeliverQueueBytes.
func NewApp() *App { return newApp(DeliverQueueBytes) }

func newApp(maxBytes int64) *App {
	a := &App{
		deliver:  make(chan []byte, DeliverBuffer),
		maxBytes: maxBytes,
		logf:     log.Printf,
		wake:     make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
		pumpDone: make(chan struct{}),
	}
	go a.pump()
	return a
}

// pump moves queued blocks to Deliver, in order, until Close.
func (a *App) pump() {
	defer close(a.pumpDone)
	for {
		a.mu.Lock()
		var b []byte
		if len(a.queue) > 0 {
			b = a.queue[0]
		}
		a.mu.Unlock()
		if b == nil {
			select {
			case <-a.wake:
				continue
			case <-a.closeCh:
				return
			}
		}
		select {
		case a.deliver <- b:
			a.mu.Lock()
			a.queue[0] = nil
			a.queue = a.queue[1:]
			a.qbytes -= int64(len(b))
			a.mu.Unlock()
		case <-a.closeCh:
			return
		}
	}
}

// Info reports no stored state (height 0, empty app hash).
func (*App) Info(abcitypes.RequestInfo) abcitypes.ResponseInfo {
	return abcitypes.ResponseInfo{Data: "veritas core (consensus/tendermint)"}
}

// CheckTx accepts every tx (the core validates blocks when it applies them).
// It asks for no gas: a gas cap per block (GasWanted 1 + genesis max_gas)
// stalled the chain at load start, so the tx count per Tendermint block is
// bounded by the harness instead (sync commit, CLIENTS and LOAD_THREADS <=
// 256: experiments/lib.sh hbdb_check_tendermint_limits).
func (*App) CheckTx(abcitypes.RequestCheckTx) abcitypes.ResponseCheckTx {
	return abcitypes.ResponseCheckTx{Code: abcitypes.CodeTypeOK}
}

// DeliverTx queues a copy of req.Tx (one Veritas block) for Deliver and
// returns Code 0 without waiting. After Close (or a failure) it drops the
// tx. A tx that would take the queue over its byte bound fails the app
// (see DeliverQueueBytes): Err is set and Deliver is closed.
func (a *App) DeliverTx(req abcitypes.RequestDeliverTx) abcitypes.ResponseDeliverTx {
	data := append([]byte(nil), req.Tx...)
	if data == nil {
		data = []byte{}
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return abcitypes.ResponseDeliverTx{Code: abcitypes.CodeTypeOK}
	}
	if a.qbytes > 0 && a.qbytes+int64(len(data)) > a.maxBytes {
		a.err = fmt.Errorf("tendermint: deliver queue overflow: %d bytes queued + %d > %d (the core does not keep up)",
			a.qbytes, len(data), a.maxBytes)
		err := a.err
		a.mu.Unlock()
		a.logf("%v; stopping this node's Deliver", err)
		a.Close()
		return abcitypes.ResponseDeliverTx{Code: abcitypes.CodeTypeOK}
	}
	a.queue = append(a.queue, data)
	a.qbytes += int64(len(data))
	a.mu.Unlock()
	select {
	case a.wake <- struct{}{}:
	default:
	}
	return abcitypes.ResponseDeliverTx{Code: abcitypes.CodeTypeOK}
}

// Err is the failure of the app (deliver queue overflow), or nil.
func (a *App) Err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// Commit returns an empty app hash (the app has no state), the same on
// every node, and RetainHeight 0 (Tendermint keeps every block).
func (*App) Commit() abcitypes.ResponseCommit {
	return abcitypes.ResponseCommit{}
}

// Deliver returns the channel of delivered blocks, closed by Close.
func (a *App) Deliver() <-chan []byte { return a.deliver }

// Close closes Deliver exactly once; later calls do nothing. It never
// blocks on a full Deliver: the pump stops, the queued blocks that still fit
// in Deliver are moved there (readable before the channel reports closed),
// the rest are dropped.
func (a *App) Close() {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		a.mu.Unlock()
		close(a.closeCh)
		<-a.pumpDone
		a.mu.Lock()
		for len(a.queue) > 0 {
			select {
			case a.deliver <- a.queue[0]:
				a.queue = a.queue[1:]
				continue
			default:
			}
			break
		}
		a.queue, a.qbytes = nil, 0
		a.mu.Unlock()
		close(a.deliver)
	})
}
