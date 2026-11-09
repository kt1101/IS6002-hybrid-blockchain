package node

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/relab/hotstuff"

	"hybrid/bft2node/wire"
)

// forwarder sends this replica's submitted commands to one peer's forward
// endpoint, in order, from one goroutine (a bounded queue: when the peer is
// slow or down and the queue is full, commands are not forwarded to it and
// a warning is logged; they stay pending here and at the other peers).
type forwarder struct {
	self   hotstuff.ID
	peer   Peer
	url    string
	client *http.Client
	ch     chan []byte
	done   chan struct{}

	mu      sync.Mutex
	closed  bool
	lastLog time.Time
}

func newForwarder(self hotstuff.ID, p Peer) *forwarder {
	f := &forwarder{
		self:   self,
		peer:   p,
		url:    "http://" + p.ForwardAddr + wire.ForwardPath,
		client: &http.Client{Timeout: forwardTimeout},
		ch:     make(chan []byte, forwardQueueLen),
		done:   make(chan struct{}),
	}
	go f.run()
	return f
}

func (f *forwarder) send(cmd []byte) {
	b := append([]byte(nil), cmd...)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	select {
	case f.ch <- b:
	default:
		f.warn("forward queue to replica %d full, command not forwarded", f.peer.ID)
	}
}

// warn logs at most once a second. Called with f.mu held or from run.
func (f *forwarder) warn(format string, args ...any) {
	if time.Since(f.lastLog) < time.Second {
		return
	}
	f.lastLog = time.Now()
	log.Printf("bft2-node %d: "+format, append([]any{f.self}, args...)...)
}

func (f *forwarder) run() {
	defer close(f.done)
	for cmd := range f.ch {
		if err := f.post(cmd); err != nil {
			f.mu.Lock()
			f.warn("forward to replica %d (%s): %v", f.peer.ID, f.peer.ForwardAddr, err)
			f.mu.Unlock()
		}
	}
}

func (f *forwarder) post(cmd []byte) error {
	resp, err := f.client.Post(f.url, "application/octet-stream", bytes.NewReader(cmd))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// close stops forwarding (pending commands are dropped) and waits for the
// goroutine, which ends after at most one in-flight POST (forwardTimeout).
func (f *forwarder) close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	close(f.ch)
	f.mu.Unlock()
	for range f.ch {
	}
	<-f.done
}
