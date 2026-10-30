package tendermint

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	abcitypes "github.com/tendermint/tendermint/abci/types"
)

// recv reads one block from Deliver or fails after a second.
func recv(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case b, ok := <-ch:
		if !ok {
			t.Fatal("Deliver closed early")
		}
		return b
	case <-time.After(time.Second):
		t.Fatal("no block on Deliver within 1s")
	}
	return nil
}

func TestDeliverTxForwardsBlockBytes(t *testing.T) {
	app := NewApp()
	defer app.Close()
	want := [][]byte{[]byte("block-1"), []byte("block-2"), bytes.Repeat([]byte{0xab}, 100<<10)}
	for i, tx := range want {
		res := app.DeliverTx(abcitypes.RequestDeliverTx{Tx: tx})
		if res.Code != abcitypes.CodeTypeOK {
			t.Fatalf("DeliverTx %d: code %d, want 0", i, res.Code)
		}
	}
	for i, w := range want {
		if got := recv(t, app.Deliver()); !bytes.Equal(got, w) {
			t.Fatalf("block %d: got %d bytes, want %d bytes", i, len(got), len(w))
		}
	}
	select {
	case b := <-app.Deliver():
		t.Fatalf("unexpected 4th block %q", b)
	default:
	}
}

// Every tx wants exactly one unit of gas, so genesis block.max_gas caps the
// txs per Tendermint block (below the socket ABCI client's 256-request queue).
func TestCheckTxWantsOneGas(t *testing.T) {
	app := NewApp()
	defer app.Close()
	for _, tx := range [][]byte{nil, []byte("x"), bytes.Repeat([]byte{1}, 1<<20)} {
		if res := app.CheckTx(abcitypes.RequestCheckTx{Tx: tx}); res.GasWanted != 1 {
			t.Fatalf("CheckTx(%d bytes).GasWanted = %d, want 1", len(tx), res.GasWanted)
		}
	}
}

func TestCheckTxAcceptsAll(t *testing.T) {
	app := NewApp()
	defer app.Close()
	for _, tx := range [][]byte{nil, {}, []byte("x"), bytes.Repeat([]byte{1}, 1<<20)} {
		if res := app.CheckTx(abcitypes.RequestCheckTx{Tx: tx}); res.Code != abcitypes.CodeTypeOK {
			t.Fatalf("CheckTx(%d bytes): code %d, want 0", len(tx), res.Code)
		}
		if res := app.CheckTx(abcitypes.RequestCheckTx{Tx: tx, Type: abcitypes.CheckTxType_Recheck}); res.Code != abcitypes.CodeTypeOK {
			t.Fatalf("recheck CheckTx(%d bytes): code %d, want 0", len(tx), res.Code)
		}
	}
	// CheckTx never delivers.
	select {
	case b := <-app.Deliver():
		t.Fatalf("CheckTx delivered %q", b)
	default:
	}
}

// The ABCI server reuses its request buffers: Deliver must get a copy.
func TestDeliverTxCopiesTx(t *testing.T) {
	app := NewApp()
	defer app.Close()
	tx := []byte("original")
	app.DeliverTx(abcitypes.RequestDeliverTx{Tx: tx})
	copy(tx, "XXXXXXXX")
	if got := recv(t, app.Deliver()); string(got) != "original" {
		t.Fatalf("delivered %q, want %q", got, "original")
	}
}

// Every node must return the same app hash; the app has no state, so it is
// empty, and Info reports no stored height (no replay check against it).
func TestCommitAndInfoAreStateless(t *testing.T) {
	app := NewApp()
	defer app.Close()
	app.DeliverTx(abcitypes.RequestDeliverTx{Tx: []byte("b")})
	if c := app.Commit(); len(c.Data) != 0 || c.RetainHeight != 0 {
		t.Fatalf("Commit = %+v, want empty app hash and RetainHeight 0", c)
	}
	if i := app.Info(abcitypes.RequestInfo{}); i.LastBlockHeight != 0 || len(i.LastBlockAppHash) != 0 {
		t.Fatalf("Info = %+v, want height 0 and empty app hash", i)
	}
}

func TestAppCloseIsIdempotentAndDropsLaterTxs(t *testing.T) {
	app := NewApp()
	app.DeliverTx(abcitypes.RequestDeliverTx{Tx: []byte("before")})
	app.Close()
	app.Close()
	// After Close DeliverTx must neither panic (send on closed channel) nor block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < DeliverBuffer+10; i++ {
			if res := app.DeliverTx(abcitypes.RequestDeliverTx{Tx: []byte("after")}); res.Code != abcitypes.CodeTypeOK {
				t.Errorf("DeliverTx after Close: code %d", res.Code)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DeliverTx after Close blocked")
	}
	// The block delivered before Close is still readable, then the channel is closed.
	if b, ok := <-app.Deliver(); !ok || string(b) != "before" {
		t.Fatalf("first read = %q, %v; want \"before\", true", b, ok)
	}
	if b, ok := <-app.Deliver(); ok {
		t.Fatalf("Deliver still open after Close, got %q", b)
	}
}

// DeliverTx runs on Tendermint's ABCI connection goroutine and waits while
// Deliver is full; Close must still finish and release it.
func TestAppCloseWithFullDeliver(t *testing.T) {
	app := NewApp()
	for i := 0; i < DeliverBuffer; i++ {
		app.DeliverTx(abcitypes.RequestDeliverTx{Tx: []byte(fmt.Sprint(i))})
	}
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		app.DeliverTx(abcitypes.RequestDeliverTx{Tx: []byte("overflow")})
	}()
	select {
	case <-blocked:
		t.Fatal("DeliverTx on a full Deliver returned without waiting")
	case <-time.After(50 * time.Millisecond):
	}
	closed := make(chan struct{})
	go func() {
		app.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked behind a DeliverTx waiting on a full Deliver")
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("DeliverTx still blocked after Close")
	}
	n := 0
	for range app.Deliver() {
		n++
	}
	if n != DeliverBuffer {
		t.Fatalf("drained %d blocks after Close, want %d (the overflow tx is dropped)", n, DeliverBuffer)
	}
}
