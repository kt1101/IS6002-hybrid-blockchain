// Package api serves the local API of bft2-node (see package wire for the
// protocol) to the veritas server on the same container.
package api

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"

	"hybrid/bft2node/node"
	"hybrid/bft2node/wire"
)

// Replica is what the API needs from a replica (node.Node implements it).
type Replica interface {
	Submit(cmd []byte) error
	Log() *node.CommitLog
	MaxCommandBytes() int
}

// Forwarder is what the forward endpoint needs (node.Node implements it).
type Forwarder interface {
	Forward(cmd []byte) error
	MaxCommandBytes() int
}

// ForwardHandler serves POST /forward (wire.ForwardPath) for the other
// replicas (bft2-node --forward-port): the body is queued with f.Forward.
func ForwardHandler(f Forwarder) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(wire.ForwardPath, func(w http.ResponseWriter, r *http.Request) {
		queueBody(w, r, f.MaxCommandBytes(), f.Forward)
	})
	return mux
}

// streamBatch is the most frames written between two flushes.
const streamBatch = 64

// Handler serves POST /submit and GET /deliver.
type Handler struct {
	r   Replica
	mux *http.ServeMux

	mu         sync.Mutex
	subscribed bool
}

// New returns the handler for r.
func New(r Replica) *Handler {
	h := &Handler{r: r, mux: http.NewServeMux()}
	h.mux.HandleFunc(wire.SubmitPath, h.submit)
	h.mux.HandleFunc(wire.DeliverPath, h.deliver)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// submit: 200 once the replica queued the command (acceptance).
func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	queueBody(w, r, h.r.MaxCommandBytes(), h.r.Submit)
}

// queueBody reads a POSTed command and hands it to queue, mapping its
// errors to the wire status codes.
func queueBody(w http.ResponseWriter, r *http.Request, max int, queue func([]byte) error) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(max)+1))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) > max {
		http.Error(w, node.ErrTooLarge.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	if err := queue(body); err != nil {
		code := http.StatusInternalServerError
		switch {
		case errors.Is(err, node.ErrEmpty):
			code = http.StatusBadRequest
		case errors.Is(err, node.ErrTooLarge):
			code = http.StatusRequestEntityTooLarge
		case errors.Is(err, node.ErrQueueFull), errors.Is(err, node.ErrStopped):
			code = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), code)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// deliver streams the commit log to one subscriber at a time. A command
// leaves the log only after its frame was written and flushed, so a
// subscriber that disconnects loses at most what was in flight.
func (h *Handler) deliver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	h.mu.Lock()
	if h.subscribed {
		h.mu.Unlock()
		http.Error(w, "another subscriber is connected", http.StatusConflict)
		return
	}
	h.subscribed = true
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.subscribed = false
		h.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	bw := bufio.NewWriterSize(w, 256<<10)
	l := h.r.Log()
	var hdr []byte
	for {
		first, items, err := l.Peek(r.Context(), streamBatch)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, node.ErrStopped) {
				log.Printf("bft2-node: deliver stream: %v", err)
			}
			return
		}
		for i, it := range items {
			hdr = wire.AppendHeader(hdr[:0], first+uint64(i), len(it))
			if _, err := bw.Write(hdr); err != nil {
				log.Printf("bft2-node: deliver stream write: %v", err)
				return
			}
			if _, err := bw.Write(it); err != nil {
				log.Printf("bft2-node: deliver stream write: %v", err)
				return
			}
		}
		if err := bw.Flush(); err != nil {
			log.Printf("bft2-node: deliver stream write: %v", err)
			return
		}
		flusher.Flush()
		l.Advance(len(items))
	}
}
