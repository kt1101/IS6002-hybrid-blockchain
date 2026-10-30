package bft2

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAPI stands in for bft2-node's local API: /submit records the bodies
// (or answers with status, or hangs until released), /deliver streams the
// frames pushed on frames (raw bytes, so tests can send bad sequences).
type fakeAPI struct {
	mu        sync.Mutex
	got       [][]byte
	status    int           // /submit reply code (0 = 200)
	hang      chan struct{} // if set, /submit waits on it (or the request ctx)
	refuse    int           // /deliver answers 503 this many times first
	delivers  int
	frames    chan []byte
	stop      chan struct{}
	stopOnce  sync.Once
	streaming chan struct{} // closed when a /deliver stream started
	strOnce   sync.Once
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{frames: make(chan []byte, 1024), stop: make(chan struct{}), streaming: make(chan struct{})}
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case submitPath:
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.got = append(f.got, body)
		status, hang := f.status, f.hang
		f.mu.Unlock()
		if hang != nil {
			select {
			case <-hang:
			case <-r.Context().Done():
				return
			}
		}
		if status != 0 && status != http.StatusOK {
			http.Error(w, "refused by fake", status)
			return
		}
		w.WriteHeader(http.StatusOK)
	case deliverPath:
		f.mu.Lock()
		f.delivers++
		refuse := f.delivers <= f.refuse
		f.mu.Unlock()
		if refuse {
			http.Error(w, "not yet", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		f.strOnce.Do(func() { close(f.streaming) })
		for {
			select {
			case b := <-f.frames:
				if _, err := w.Write(b); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			case <-f.stop:
				return
			case <-r.Context().Done():
				return
			}
		}
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeAPI) submitted() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.got...)
}

func (f *fakeAPI) set(fn func(f *fakeAPI)) {
	f.mu.Lock()
	fn(f)
	f.mu.Unlock()
}

// endStreams makes every /deliver handler return (so httptest.Server.Close
// does not wait for them).
func (f *fakeAPI) endStreams() { f.stopOnce.Do(func() { close(f.stop) }) }

// start serves f on a loopback port and returns a backend connected to it.
func start(t *testing.T, f *fakeAPI) (*BFT2, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(f)
	b, err := New(Config{APIAddr: strings.TrimPrefix(srv.URL, "http://"), ConnectTimeout: 5 * time.Second})
	if err != nil {
		srv.Close()
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		b.Close()
		f.endStreams()
		srv.Close()
	})
	return b, srv
}

func recv(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case b, ok := <-ch:
		if !ok {
			t.Fatal("Deliver closed")
		}
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("no block delivered within 5s")
	}
	return nil
}

func TestSubmitSendsExactBytesAndDoesNotRetainThem(t *testing.T) {
	f := newFakeAPI()
	b, _ := start(t, f)
	blk := []byte{0, 1, 2, 0xff, 0, 'v'}
	want := append([]byte(nil), blk...)
	if err := b.Submit(context.Background(), blk); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	blk[0] = 0x55 // the caller may reuse its buffer after Submit
	got := f.submitted()
	if len(got) != 1 || !bytes.Equal(got[0], want) {
		t.Fatalf("sidecar got %q, want %q", got, want)
	}
}

func TestSubmitRefusedIsAnError(t *testing.T) {
	f := newFakeAPI()
	b, _ := start(t, f)
	for _, code := range []int{http.StatusServiceUnavailable, http.StatusRequestEntityTooLarge, http.StatusBadRequest, http.StatusInternalServerError} {
		f.set(func(f *fakeAPI) { f.status = code })
		err := b.Submit(context.Background(), []byte("blk"))
		if err == nil {
			t.Fatalf("Submit with reply %d returned nil", code)
		}
		if errors.Is(err, ErrClosed) {
			t.Fatalf("Submit with reply %d returned ErrClosed", code)
		}
	}
}

func TestSubmitUnreachableIsAnError(t *testing.T) {
	f := newFakeAPI()
	b, srv := start(t, f)
	f.endStreams()
	srv.Close()
	if err := b.Submit(context.Background(), []byte("blk")); err == nil {
		t.Fatal("Submit to a closed sidecar returned nil")
	}
}

func TestSubmitIsBoundedByCtx(t *testing.T) {
	f := newFakeAPI()
	hang := make(chan struct{})
	defer close(hang)
	f.set(func(f *fakeAPI) { f.hang = hang })
	b, _ := start(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := b.Submit(ctx, []byte("blk")); err == nil {
		t.Fatal("Submit returned nil although the sidecar never answered")
	}
	if d := time.Since(begin); d > 3*time.Second {
		t.Fatalf("Submit took %v with a 100ms ctx", d)
	}
}

func TestCloseUnblocksPendingSubmitAndLaterSubmitsFail(t *testing.T) {
	f := newFakeAPI()
	hang := make(chan struct{})
	defer close(hang)
	f.set(func(f *fakeAPI) { f.hang = hang })
	b, _ := start(t, f)
	errc := make(chan error, 1)
	go func() { errc <- b.Submit(context.Background(), []byte("blk")) }()
	time.Sleep(50 * time.Millisecond)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("pending Submit after Close = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not unblock a pending Submit")
	}
	if err := b.Submit(context.Background(), []byte("blk")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
}

func TestDeliverExactBytesInOrder(t *testing.T) {
	f := newFakeAPI()
	b, _ := start(t, f)
	want := [][]byte{[]byte("one"), {0, 0, 0xff}, bytes.Repeat([]byte("x"), 100000), []byte("four")}
	var stream []byte
	for i, w := range want {
		stream = appendFrame(stream, uint64(i), w)
	}
	// Split mid-frame: the reader must reassemble.
	f.frames <- stream[:7]
	f.frames <- stream[7:]
	for i, w := range want {
		if got := recv(t, b.Deliver()); !bytes.Equal(got, w) {
			t.Fatalf("block %d: got %d bytes, want %d", i, len(got), len(w))
		}
	}
	if b.Deliver() != b.Deliver() {
		t.Fatal("Deliver returns different channels")
	}
}

func TestStreamGapOrEndClosesDeliver(t *testing.T) {
	for name, frames := range map[string][]byte{
		"gap":         appendFrame(appendFrame(nil, 0, []byte("a")), 2, []byte("c")),
		"first not 0": appendFrame(nil, 1, []byte("b")),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeAPI()
			b, _ := start(t, f)
			f.frames <- frames
			deadline := time.After(5 * time.Second)
			for {
				select {
				case _, ok := <-b.Deliver():
					if !ok {
						return
					}
				case <-deadline:
					t.Fatal("Deliver not closed after a bad sequence")
				}
			}
		})
	}
	// The sidecar ending the stream (it stopped) closes Deliver too.
	f := newFakeAPI()
	b, _ := start(t, f)
	<-f.streaming
	f.endStreams()
	select {
	case _, ok := <-b.Deliver():
		if ok {
			t.Fatal("unexpected block")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Deliver not closed after the stream ended")
	}
}

func TestCloseWhileDeliveryBlockedAndIdempotent(t *testing.T) {
	f := newFakeAPI()
	b, _ := start(t, f)
	var stream []byte
	for i := 0; i < DeliverBuffer+50; i++ {
		stream = appendFrame(stream, uint64(i), []byte("blk"))
	}
	f.frames <- stream
	time.Sleep(200 * time.Millisecond) // the reader fills Deliver and blocks
	done := make(chan error, 1)
	go func() { done <- b.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked while delivery was blocked")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	n := 0
	for range b.Deliver() { // must end: Deliver is closed
		n++
	}
	if n > DeliverBuffer {
		t.Fatalf("drained %d blocks, more than the buffer %d", n, DeliverBuffer)
	}
}

func TestNewWaitsForTheSidecarThenGivesUp(t *testing.T) {
	// Refused twice, then streaming: New retries.
	f := newFakeAPI()
	f.refuse = 2
	b, _ := start(t, f)
	f.frames <- appendFrame(nil, 0, []byte("x"))
	if got := recv(t, b.Deliver()); string(got) != "x" {
		t.Fatalf("got %q", got)
	}

	// Nothing listening: New fails after ConnectTimeout.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	begin := time.Now()
	if b, err := New(Config{APIAddr: addr, ConnectTimeout: 500 * time.Millisecond}); err == nil {
		b.Close()
		t.Fatal("New with no sidecar returned no error")
	}
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("New took %v with ConnectTimeout 500ms", d)
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("New without APIAddr returned no error")
	}
}
