// Package tendermint is the Tendermint consensus backend (Tendermint
// v0.35.0 over a socket ABCI connection, as veritastm/).
//
// Every node runs a tendermint process next to the veritas server. The
// server listens on Config.ABCISocket with App (the ABCI application);
// tendermint connects to it (--proxy-app) and is reached over its RPC on
// Config.RPCAddr. One ABCI tx is one marshalled Veritas block.
//
// Submit calls BroadcastTxSync and returns once CheckTx passed on this node
// (ACCEPTANCE, not commit). A transport error, an RPC error (mempool full,
// tx too large, tx already in the cache, ...) or a non-zero CheckTx code is
// returned as an error. The core waits for its block on Deliver.
//
// App.DeliverTx pushes a copy of every tx to Deliver in Tendermint's
// DeliverTx order, which is the same on every node, the proposer included.
// The core runs with --version-mode=ordered: its height is the count of
// delivered blocks, so it only needs that common order.
package tendermint

import (
	"context"
	"errors"
	"fmt"
	"sync"

	abciserver "github.com/tendermint/tendermint/abci/server"
	abcitypes "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/service"
	rpchttp "github.com/tendermint/tendermint/rpc/client/http"

	"hybrid/consensus"
)

var _ consensus.Consensus = (*Tendermint)(nil)

// ErrClosed is returned by Submit once Close has been called.
var ErrClosed = errors.New("tendermint consensus: closed")

// Config configures one node's Tendermint backend.
type Config struct {
	// ABCISocket is the address the ABCI socket server listens on, e.g.
	// tcp://0.0.0.0:26658 (tendermint's --proxy-app dials it).
	ABCISocket string
	// RPCAddr is tendermint's RPC address, e.g. tcp://127.0.0.1:26657.
	RPCAddr string
}

// Tendermint is one node's Tendermint backend. It implements
// consensus.Consensus.
type Tendermint struct {
	app *App
	srv service.Service // ABCI socket server
	rpc *rpchttp.HTTP

	closeCh   chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// New starts the ABCI socket server on cfg.ABCISocket and builds the RPC
// client for cfg.RPCAddr. It does not wait for tendermint: until tendermint
// runs, Submit fails (connection refused) and Deliver stays empty.
func New(cfg Config) (*Tendermint, error) {
	if cfg.ABCISocket == "" {
		return nil, errors.New("tendermint consensus: ABCISocket is required")
	}
	if cfg.RPCAddr == "" {
		return nil, errors.New("tendermint consensus: RPCAddr is required")
	}
	cli, err := rpchttp.New(cfg.RPCAddr)
	if err != nil {
		return nil, fmt.Errorf("tendermint consensus: rpc client %s: %w", cfg.RPCAddr, err)
	}
	app := NewApp()
	srv := abciserver.NewSocketServer(cfg.ABCISocket, app)
	if err := srv.Start(); err != nil {
		app.Close()
		return nil, fmt.Errorf("tendermint consensus: abci server %s: %w", cfg.ABCISocket, err)
	}
	return &Tendermint{
		app:     app,
		srv:     srv,
		rpc:     cli,
		closeCh: make(chan struct{}),
	}, nil
}

// Submit hands blk to tendermint's mempool with BroadcastTxSync and returns
// nil once CheckTx accepted it (see the package comment). It never retains
// blk (the RPC client encodes it before returning). It is bounded by ctx and
// by Close; after Close it returns ErrClosed.
func (t *Tendermint) Submit(ctx context.Context, blk []byte) error {
	if t.isClosed() {
		return ErrClosed
	}
	ctx, cancel := t.withClose(ctx)
	defer cancel()
	res, err := t.rpc.BroadcastTxSync(ctx, blk)
	if err != nil {
		if t.isClosed() {
			return fmt.Errorf("%w (broadcast: %v)", ErrClosed, err)
		}
		return fmt.Errorf("tendermint consensus: broadcast_tx_sync: %w", err)
	}
	if res.Code != abcitypes.CodeTypeOK || res.MempoolError != "" {
		return fmt.Errorf("tendermint consensus: tx rejected: code %d, log %q, mempool error %q",
			res.Code, res.Log, res.MempoolError)
	}
	return nil
}

// Deliver returns the channel of delivered blocks, closed by Close.
func (t *Tendermint) Deliver() <-chan []byte { return t.app.Deliver() }

// Close fails pending and later Submits, stops the ABCI server (tendermint
// loses its app connection) and closes Deliver. It is idempotent; later
// calls return the first call's result.
func (t *Tendermint) Close() error {
	t.closeOnce.Do(func() {
		close(t.closeCh)
		// Close the app first: a DeliverTx blocked on a full Deliver returns
		// (closeCh), so nothing holds the server's app lock any more.
		t.app.Close()
		if err := t.srv.Stop(); err != nil && !errors.Is(err, service.ErrAlreadyStopped) {
			t.closeErr = fmt.Errorf("tendermint consensus: stop abci server: %w", err)
		}
	})
	return t.closeErr
}

func (t *Tendermint) isClosed() bool {
	select {
	case <-t.closeCh:
		return true
	default:
		return false
	}
}

// withClose derives a context that is also cancelled by Close.
func (t *Tendermint) withClose(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-t.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
