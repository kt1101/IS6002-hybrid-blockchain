package api

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"hybrid/bft2node/node"
	"hybrid/bft2node/wire"
)

type fakeReplica struct {
	mu  sync.Mutex
	got [][]byte
	err error
	log *node.CommitLog
	max int
}

func (f *fakeReplica) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *fakeReplica) submitted() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.got...)
}

func (f *fakeReplica) Submit(cmd []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if len(cmd) == 0 {
		return node.ErrEmpty
	}
	f.got = append(f.got, append([]byte(nil), cmd...))
	return nil
}

func (f *fakeReplica) Log() *node.CommitLog { return f.log }
func (f *fakeReplica) MaxCommandBytes() int { return f.max }

func newFake() (*fakeReplica, *httptest.Server) {
	f := &fakeReplica{log: node.NewCommitLog(1 << 20), max: 1000}
	return f, httptest.NewServer(New(f))
}

func post(t *testing.T, url string, body []byte) int {
	t.Helper()
	resp, err := http.Post(url+wire.SubmitPath, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func TestSubmitStatusCodes(t *testing.T) {
	f, srv := newFake()
	defer srv.Close()
	defer f.log.Close()
	blk := []byte{0, 1, 2, 0xff, 'x'}
	if code := post(t, srv.URL, blk); code != http.StatusOK {
		t.Fatalf("submit = %d, want 200", code)
	}
	if got := f.submitted(); len(got) != 1 || !bytes.Equal(got[0], blk) {
		t.Fatalf("replica got %q, want exactly %q", got, blk)
	}
	if code := post(t, srv.URL, nil); code != http.StatusBadRequest {
		t.Fatalf("empty submit = %d, want 400", code)
	}
	if code := post(t, srv.URL, make([]byte, 1001)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized submit = %d, want 413", code)
	}
	f.setErr(node.ErrQueueFull)
	if code := post(t, srv.URL, blk); code != http.StatusServiceUnavailable {
		t.Fatalf("queue-full submit = %d, want 503", code)
	}
	f.setErr(node.ErrStopped)
	if code := post(t, srv.URL, blk); code != http.StatusServiceUnavailable {
		t.Fatalf("stopped submit = %d, want 503", code)
	}
	resp, err := http.Get(srv.URL + wire.SubmitPath)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /submit = %d, want 405", resp.StatusCode)
	}
}

func TestDeliverStreamsCommitLogInOrderToOneSubscriber(t *testing.T) {
	f, srv := newFake()
	defer srv.Close()
	defer f.log.Close()
	f.log.Add([]byte("first"))
	f.log.Add([]byte{0, 0, 0})

	resp, err := http.Get(srv.URL + wire.DeliverPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /deliver = %d, want 200", resp.StatusCode)
	}
	r := bufio.NewReader(resp.Body)

	// A second subscriber is refused while the first is connected.
	resp2, err := http.Get(srv.URL + wire.DeliverPath)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("second GET /deliver = %d, want 409", resp2.StatusCode)
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		f.log.Add([]byte("later"))
	}()
	want := [][]byte{[]byte("first"), {0, 0, 0}, []byte("later")}
	for i, w := range want {
		seq, p, err := wire.ReadFrame(r, wire.MaxFrame)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if seq != uint64(i) || !bytes.Equal(p, w) {
			t.Fatalf("frame %d = (%d, %q), want (%d, %q)", i, seq, p, i, w)
		}
	}
	// Sent frames leave the log (Advance after the flush): the next
	// command is the only one sent next, with seq 3.
	f.log.Add([]byte("probe"))
	if seq, p, err := wire.ReadFrame(r, wire.MaxFrame); err != nil || seq != 3 || string(p) != "probe" {
		t.Fatalf("probe frame = (%d, %q, %v), want (3, probe)", seq, p, err)
	}

	// Closing the log (node stopping) ends the stream.
	f.log.Close()
	if _, _, err := wire.ReadFrame(r, wire.MaxFrame); err == nil {
		t.Fatal("stream continued after the commit log closed")
	}
}
