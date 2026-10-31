// Package bft2 is the second BFT consensus backend: HotStuff
// (github.com/relab/hotstuff v0.5.0, chained HotStuff) run by a sidecar,
// bft2-node (the separate Go module bft2node/, which needs Go >= 1.23.4;
// this module stays on its own go.mod and toolchain).
//
// Every node runs bft2-node next to the veritas server; the replicas talk to
// each other over the library's gorums/gRPC transport (port 7200). This
// package is a thin client of the sidecar's LOCAL API (127.0.0.1:7201,
// protocol in wire.go, a copy of bft2node/wire):
//
//   - Submit POSTs the block to /submit and returns nil once the sidecar
//     queued it for proposal (ACCEPTANCE, not commit); a refusal (queue
//     full, too large, stopping) or an unreachable sidecar is an error. A
//     replica proposes its own queued blocks, one per view, when it is
//     leader (a per-epoch shuffled round robin), so an accepted block may
//     still be lost if this node crashes, or, rarely, delivered twice (a
//     forked block is re-proposed).
//   - Deliver carries the committed blocks of the GET /deliver stream: one
//     HotStuff command is one Veritas block, exact bytes, in commit order,
//     the same on every replica, the proposer included (empty filler blocks
//     are dropped by the sidecar on every replica alike). The core runs with
//     --version-mode=ordered: its height is the count of delivered blocks.
//
// The stream is not resumed: if it breaks or skips a sequence number,
// Deliver is closed (backend failure) and the node stops applying blocks.
package bft2

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"hybrid/consensus"
)

var _ consensus.Consensus = (*BFT2)(nil)

// ErrClosed is returned by Submit once Close has been called.
var ErrClosed = errors.New("bft2 consensus: closed")

const (
	// DeliverBuffer is the capacity of the Deliver channel. When it is full
	// the stream reader waits, TCP backpressure reaches the sidecar, whose
	// commit log then fills and holds its replica's event loop.
	DeliverBuffer = 256
	// DefaultConnectTimeout bounds New's wait for the sidecar's stream.
	DefaultConnectTimeout = 60 * time.Second
	// maxBlock bounds a delivered frame (the sidecar refuses commands over
	// 4 MiB by default).
	maxBlock = 64 << 20
	// connectRetry is the pause between New's attempts.
	connectRetry = 200 * time.Millisecond
)

// Config configures one node's bft2 backend.
type Config struct {
	// APIAddr is the sidecar's local API, host:port (e.g. 127.0.0.1:7201).
	APIAddr string
	// ConnectTimeout bounds how long New retries opening the delivery
	// stream (default DefaultConnectTimeout).
	ConnectTimeout time.Duration
}

// BFT2 is one node's bft2 backend. It implements consensus.Consensus.
type BFT2 struct {
	base    string // http://APIAddr
	client  *http.Client
	deliver chan []byte

	closeCh    chan struct{}
	closeOnce  sync.Once
	stopStream context.CancelFunc
	readerDone chan struct{}
}

// New opens the delivery stream of the sidecar at cfg.APIAddr, retrying
// until cfg.ConnectTimeout (the sidecar may still be starting), and starts
// reading it.
func New(cfg Config) (*BFT2, error) {
	if cfg.APIAddr == "" {
		return nil, errors.New("bft2 consensus: APIAddr is required")
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = DefaultConnectTimeout
	}
	b := &BFT2{
		base: "http://" + cfg.APIAddr,
		client: &http.Client{Transport: &http.Transport{
			Proxy:               nil, // loopback only; ignore HTTP_PROXY
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		}},
		deliver:    make(chan []byte, DeliverBuffer),
		closeCh:    make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	streamCtx, stop := context.WithCancel(context.Background())
	b.stopStream = stop
	body, err := b.openStream(streamCtx, cfg.ConnectTimeout)
	if err != nil {
		stop()
		return nil, err
	}
	go b.read(body)
	return b, nil
}

// openStream GETs /deliver until it answers 200 or timeout passes.
func (b *BFT2) openStream(ctx context.Context, timeout time.Duration) (io.ReadCloser, error) {
	deadline := time.Now().Add(timeout)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.base+deliverPath, nil)
		if err != nil {
			return nil, fmt.Errorf("bft2 consensus: %w", err)
		}
		resp, err := b.client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			return resp.Body, nil
		}
		if err == nil {
			msg, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			err = fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("bft2 consensus: delivery stream %s%s not available after %v: %v", b.base, deliverPath, timeout, err)
		}
		time.Sleep(connectRetry)
	}
}

// read decodes the stream into Deliver until it ends, breaks, skips a seq
// or Close is called; then it closes Deliver (its only closer).
func (b *BFT2) read(body io.ReadCloser) {
	defer close(b.readerDone)
	defer close(b.deliver)
	defer body.Close()
	r := bufio.NewReaderSize(body, 256<<10)
	var next uint64
	for {
		seq, blk, err := readFrame(r, maxBlock)
		if err != nil {
			if !b.isClosed() {
				log.Printf("bft2 consensus: delivery stream ended after %d blocks: %v (closing Deliver)", next, err)
			}
			return
		}
		if seq != next {
			log.Printf("bft2 consensus: delivery stream skipped from seq %d to %d (closing Deliver)", next, seq)
			return
		}
		next++
		select {
		case b.deliver <- blk:
		case <-b.closeCh:
			return
		}
	}
}

// Submit sends a copy of blk to the sidecar and returns nil once it was
// accepted (queued for proposal). It is bounded by ctx and by Close; after
// Close it returns ErrClosed.
func (b *BFT2) Submit(ctx context.Context, blk []byte) error {
	if b.isClosed() {
		return ErrClosed
	}
	ctx, cancel := b.withClose(ctx)
	defer cancel()
	// Copy: the transport may still read the body after Do returns, and
	// the caller may reuse blk as soon as Submit returns.
	body := append([]byte(nil), blk...)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.base+submitPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("bft2 consensus: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := b.client.Do(req)
	if err != nil {
		if b.isClosed() {
			return fmt.Errorf("%w (submit: %v)", ErrClosed, err)
		}
		return fmt.Errorf("bft2 consensus: submit: %w", err)
	}
	defer resp.Body.Close()
	msg, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bft2 consensus: block refused: status %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// Deliver returns the channel of committed blocks, closed by Close or when
// the delivery stream fails.
func (b *BFT2) Deliver() <-chan []byte { return b.deliver }

// Close fails pending and later Submits, ends the delivery stream and
// closes Deliver (also while the reader is blocked on a full Deliver). It
// is idempotent.
func (b *BFT2) Close() error {
	b.closeOnce.Do(func() {
		close(b.closeCh)
		b.stopStream()
		<-b.readerDone
		b.client.CloseIdleConnections()
	})
	return nil
}

func (b *BFT2) isClosed() bool {
	select {
	case <-b.closeCh:
		return true
	default:
		return false
	}
}

// withClose derives a context that is also cancelled by Close.
func (b *BFT2) withClose(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-b.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
