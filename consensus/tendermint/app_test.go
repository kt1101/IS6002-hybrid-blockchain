package tendermint

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
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

// DeliverTx never waits, even with Deliver full (the ABCI socket server holds
// one lock over every connection, so a waiting DeliverTx would hold CheckTx
// too); Close returns at once, the blocks that fit in Deliver stay readable
// and the rest are dropped.
func TestAppCloseWithFullDeliver(t *testing.T) {
	app := NewApp()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < DeliverBuffer+50; i++ {
			app.DeliverTx(abcitypes.RequestDeliverTx{Tx: []byte(fmt.Sprint(i))})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DeliverTx blocked on a full Deliver")
	}
	waitFor(t, func() bool { return len(app.Deliver()) == DeliverBuffer })
	closed := make(chan struct{})
	go func() {
		app.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked")
	}
	n := 0
	for b := range app.Deliver() {
		if string(b) != fmt.Sprint(n) {
			t.Fatalf("block %d = %q", n, b)
		}
		n++
	}
	if n != DeliverBuffer {
		t.Fatalf("drained %d blocks after Close, want %d", n, DeliverBuffer)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDeliverTxDoesNotBlockWhenConsumerStalls(t *testing.T) {
	app := NewApp()
	defer app.Close()
	tx := bytes.Repeat([]byte{7}, 1024)
	stop := make(chan struct{})
	slow := make(chan time.Duration, 1)
	go func() {
		var worst time.Duration
		for {
			select {
			case <-stop:
				slow <- worst
				return
			default:
			}
			t0 := time.Now()
			app.CheckTx(abcitypes.RequestCheckTx{Tx: tx})
			if d := time.Since(t0); d > worst {
				worst = d
			}
			time.Sleep(time.Millisecond)
		}
	}()
	t0 := time.Now()
	for i := 0; i < 10000; i++ {
		app.DeliverTx(abcitypes.RequestDeliverTx{Tx: tx})
	}
	took := time.Since(t0)
	close(stop)
	if took > 2*time.Second {
		t.Fatalf("10000 DeliverTx with a stalled consumer took %v, want < 2s", took)
	}
	if w := <-slow; w > 100*time.Millisecond {
		t.Fatalf("CheckTx took up to %v meanwhile, want < 100ms", w)
	}
	if err := app.Err(); err != nil {
		t.Fatalf("Err = %v, want nil (10 MB < DeliverQueueBytes)", err)
	}
}

func TestDeliverOrderPreserved(t *testing.T) {
	app := NewApp()
	defer app.Close()
	for i := 0; i < 1000; i++ {
		app.DeliverTx(abcitypes.RequestDeliverTx{Tx: []byte(fmt.Sprint(i))})
	}
	for i := 0; i < 1000; i++ {
		if got := recv(t, app.Deliver()); string(got) != fmt.Sprint(i) {
			t.Fatalf("block %d = %q", i, got)
		}
	}
}

func TestDeliverQueueOverflowStops(t *testing.T) {
	app := newApp(1 << 20)
	var mu sync.Mutex
	var logged []string
	app.logf = func(format string, args ...interface{}) {
		mu.Lock()
		logged = append(logged, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	tx := bytes.Repeat([]byte{1}, 64<<10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < DeliverBuffer+64; i++ {
			app.DeliverTx(abcitypes.RequestDeliverTx{Tx: tx})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("DeliverTx blocked")
	}
	if app.Err() == nil {
		t.Fatal("Err = nil after overflowing the deliver queue")
	}
	n := 0
	timeout := time.After(5 * time.Second)
	for open := true; open; {
		select {
		case _, ok := <-app.Deliver():
			if !ok {
				open = false
			} else {
				n++
			}
		case <-timeout:
			t.Fatalf("Deliver not closed after the overflow (%d read)", n)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, l := range logged {
		if strings.Contains(l, "tendermint: deliver queue overflow") {
			found = true
		}
	}
	if !found {
		t.Fatalf("log %q has no overflow line", logged)
	}
	app.Close() // still idempotent after a failure
}
