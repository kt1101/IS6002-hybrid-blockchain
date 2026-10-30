package tendermint

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	abciclient "github.com/tendermint/tendermint/abci/client"
	abcitypes "github.com/tendermint/tendermint/abci/types"
)

// fakeRPC is a JSON-RPC endpoint standing in for tendermint's RPC: it
// records every broadcast_tx_sync tx and answers with reply.
type fakeRPC struct {
	mu    sync.Mutex
	txs   [][]byte
	reply func(tx []byte) (result, rpcErr string) // raw JSON of "result" or "error"
	hang  chan struct{}                           // if set, wait on it before answering
}

func (f *fakeRPC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Tx string `json:"tx"`
		} `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Method != "broadcast_tx_sync" {
		http.Error(w, "unexpected method "+req.Method, http.StatusBadRequest)
		return
	}
	tx, err := base64.StdEncoding.DecodeString(req.Params.Tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.txs = append(f.txs, tx)
	reply, hang := f.reply, f.hang
	f.mu.Unlock()
	if hang != nil {
		select {
		case <-hang:
		case <-r.Context().Done():
			return
		}
	}
	result, rpcErr := `{"code":0,"log":"","codespace":"","mempool_error":""}`, ""
	if reply != nil {
		result, rpcErr = reply(tx)
	}
	w.Header().Set("Content-Type", "application/json")
	if rpcErr != "" {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"error":` + rpcErr + `}`))
		return
	}
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
}

func (f *fakeRPC) got() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.txs...)
}

// newTestBackend starts a backend whose ABCI server listens on a free
// loopback port and whose RPC is f. It returns the backend and the ABCI
// address.
func newTestBackend(t *testing.T, f *fakeRPC) (*Tendermint, string) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	addr := freeAddr(t)
	tm, err := New(Config{ABCISocket: "tcp://" + addr, RPCAddr: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tm.Close() })
	return tm, addr
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestNewRequiresAddresses(t *testing.T) {
	if _, err := New(Config{RPCAddr: "tcp://127.0.0.1:26657"}); err == nil {
		t.Fatal("New without ABCISocket: want an error")
	}
	if _, err := New(Config{ABCISocket: "tcp://127.0.0.1:0"}); err == nil {
		t.Fatal("New without RPCAddr: want an error")
	}
}

// Submit sends the exact bytes, returns on acceptance, and does not retain
// the caller's buffer.
func TestSubmitBroadcastsExactBytes(t *testing.T) {
	f := &fakeRPC{}
	tm, _ := newTestBackend(t, f)
	blk := []byte("veritas block \x00\x01\xff")
	want := append([]byte(nil), blk...)
	if err := tm.Submit(context.Background(), blk); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	copy(blk, "XXXXXXXX")
	got := f.got()
	if len(got) != 1 || !bytes.Equal(got[0], want) {
		t.Fatalf("broadcast txs = %q, want [%q]", got, want)
	}
	// Acceptance only: nothing is delivered until tendermint runs DeliverTx.
	select {
	case b := <-tm.Deliver():
		t.Fatalf("Submit delivered %q by itself", b)
	default:
	}
}

func TestSubmitReturnsCheckTxAndMempoolRejections(t *testing.T) {
	cases := map[string]func([]byte) (string, string){
		"checktx code": func([]byte) (string, string) {
			return `{"code":3,"log":"bad tx","codespace":"","mempool_error":""}`, ""
		},
		"mempool error": func([]byte) (string, string) {
			return `{"code":0,"log":"","codespace":"","mempool_error":"post-check failed"}`, ""
		},
		"rpc error (mempool full)": func([]byte) (string, string) {
			return "", `{"code":-32603,"message":"Internal error","data":"mempool is full: number of txs 100000 (max: 100000)"}`
		},
		"rpc error (tx too large)": func([]byte) (string, string) {
			return "", `{"code":-32603,"message":"Internal error","data":"tx too large. Max size is 4194304, but got 5000000"}`
		},
	}
	for name, reply := range cases {
		t.Run(name, func(t *testing.T) {
			tm, _ := newTestBackend(t, &fakeRPC{reply: reply})
			if err := tm.Submit(context.Background(), []byte("blk")); err == nil {
				t.Fatal("Submit = nil, want an error")
			}
		})
	}
}

func TestSubmitFailsWhenRPCIsDown(t *testing.T) {
	tm, err := New(Config{ABCISocket: "tcp://" + freeAddr(t), RPCAddr: "tcp://" + freeAddr(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tm.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tm.Submit(ctx, []byte("blk")); err == nil {
		t.Fatal("Submit with no tendermint RPC = nil, want an error")
	}
}

func TestSubmitHonoursCtx(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang)
	tm, _ := newTestBackend(t, &fakeRPC{hang: hang})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := tm.Submit(ctx, []byte("blk"))
	if err == nil {
		t.Fatal("Submit on a hanging RPC = nil, want an error")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Submit took %v, want about the ctx timeout", d)
	}
}

// A Submit in flight when Close is called returns an error (not block); one
// after Close returns ErrClosed. Neither panics.
func TestSubmitConcurrentWithAndAfterClose(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang)
	tm, _ := newTestBackend(t, &fakeRPC{hang: hang})
	errc := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { errc <- tm.Submit(context.Background(), []byte("blk")) }()
	}
	time.Sleep(50 * time.Millisecond)
	if err := tm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i := 0; i < 8; i++ {
		select {
		case err := <-errc:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("in-flight Submit after Close = %v, want ErrClosed", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("in-flight Submit still blocked after Close")
		}
	}
	if err := tm.Submit(context.Background(), []byte("blk")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
}

func TestCloseIsIdempotentAndClosesDeliver(t *testing.T) {
	tm, _ := newTestBackend(t, &fakeRPC{})
	if tm.Deliver() != tm.Deliver() {
		t.Fatal("Deliver returns different channels")
	}
	if err := tm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tm.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case _, ok := <-tm.Deliver():
		if ok {
			t.Fatal("Deliver yielded a block after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Deliver not closed after Close")
	}
}

// End to end over the real ABCI socket protocol: an ABCI socket client (what
// tendermint uses) sends DeliverTx requests; the backend delivers the exact
// bytes in order and answers Code 0; Commit returns the empty app hash.
func TestABCISocketDeliversInOrder(t *testing.T) {
	tm, addr := newTestBackend(t, &fakeRPC{})
	cli := abciclient.NewSocketClient("tcp://"+addr, true)
	if err := cli.Start(); err != nil {
		t.Fatalf("abci client start: %v", err)
	}
	defer func() { _ = cli.Stop() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := [][]byte{[]byte("b1"), bytes.Repeat([]byte{7}, 200<<10), []byte("b3")}
	for i, tx := range want {
		res, err := cli.DeliverTxSync(ctx, abcitypes.RequestDeliverTx{Tx: tx})
		if err != nil {
			t.Fatalf("DeliverTxSync %d: %v", i, err)
		}
		if res.Code != abcitypes.CodeTypeOK {
			t.Fatalf("DeliverTx %d: code %d", i, res.Code)
		}
	}
	for i, w := range want {
		if got := recv(t, tm.Deliver()); !bytes.Equal(got, w) {
			t.Fatalf("block %d: got %d bytes, want %d", i, len(got), len(w))
		}
	}
	chk, err := cli.CheckTxSync(ctx, abcitypes.RequestCheckTx{Tx: []byte("anything")})
	if err != nil || chk.Code != abcitypes.CodeTypeOK {
		t.Fatalf("CheckTxSync = %+v, %v; want code 0", chk, err)
	}
	com, err := cli.CommitSync(ctx)
	if err != nil || len(com.Data) != 0 {
		t.Fatalf("CommitSync = %+v, %v; want empty app hash", com, err)
	}
}
