// Package observe exposes node-side HTTP/SSE endpoints a live demo uses to
// watch a Veritas node: block-applied events, key lookup with version and
// Merkle proofs.
package observe

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"hybrid/veritas/core"
	"hybrid/veritas/ledger/merkletree"
)

const (
	subscriberBuffer = 256
	pingInterval     = 15 * time.Second
)

// Event is the JSON form of one applied block.
type Event struct {
	Node     string `json:"node"`
	Height   int64  `json:"height"`
	Proposer string `json:"proposer"`
	Txs      int    `json:"txs"`
	Aborted  int    `json:"aborted"`
	Root     string `json:"root"`  // hex
	AtMs     int64  `json:"at_ms"` // unix ms, time of Publish
}

// Broadcaster fans applied-block events out to subscribers without ever
// blocking the publisher.
type Broadcaster struct {
	node    string
	dropped uint64

	mu   sync.Mutex
	subs map[chan Event]struct{}
}

// NewBroadcaster returns a Broadcaster tagging events with the node name.
func NewBroadcaster(node string) *Broadcaster {
	return &Broadcaster{node: node, subs: make(map[chan Event]struct{})}
}

// Publish delivers ev to every subscriber. It never blocks: a subscriber whose
// buffer is full misses the event and Dropped is incremented.
func (b *Broadcaster) Publish(ev core.AppliedEvent) {
	out := Event{
		Node:     b.node,
		Height:   ev.Height,
		Proposer: ev.Proposer,
		Txs:      ev.Txs,
		Aborted:  ev.Aborted,
		Root:     hex.EncodeToString(ev.Root),
		AtMs:     time.Now().UnixNano() / int64(time.Millisecond),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- out:
		default:
			atomic.AddUint64(&b.dropped, 1)
		}
	}
}

// Subscribe returns a channel of events and a cancel func that unsubscribes
// and closes the channel. cancel is idempotent.
func (b *Broadcaster) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, subscriberBuffer)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, ch)
			b.mu.Unlock()
			close(ch)
		})
	}
}

// Dropped returns the number of events dropped because a subscriber was full.
func (b *Broadcaster) Dropped() uint64 { return atomic.LoadUint64(&b.dropped) }

func (b *Broadcaster) subscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

// KVFunc looks up a key, returning its value and version.
type KVFunc func(ctx context.Context, key string) (value string, version int64, ok bool, err error)

// ProveFunc returns the current root and a Merkle proof for key.
type ProveFunc func(key []byte) (root []byte, proof merkletree.SparseMerkleProof, err error)

// Handler serves GET /events (SSE), /kv?key=, /prove?key= and /healthz.
func Handler(b *Broadcaster, kv KVFunc, prove ProveFunc) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		serveEvents(b, w, r)
	})
	mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key"})
			return
		}
		if kv == nil {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "kv not available"})
			return
		}
		value, version, ok, err := kv(r.Context(), key)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
			return
		}
		writeJSON(w, http.StatusOK, struct {
			Value   string `json:"value"`
			Version int64  `json:"version"`
		}{value, version})
	})
	mux.HandleFunc("/prove", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key"})
			return
		}
		if prove == nil {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "prove not available"})
			return
		}
		root, proof, err := prove([]byte(key))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		side := make([]string, len(proof.SideNodes))
		for i, n := range proof.SideNodes {
			side[i] = hex.EncodeToString(n)
		}
		writeJSON(w, http.StatusOK, struct {
			Root                  string   `json:"root"`
			SideNodes             []string `json:"side_nodes"`
			NonMembershipLeafData string   `json:"non_membership_leaf_data"`
		}{hex.EncodeToString(root), side, hex.EncodeToString(proof.NonMembershipLeafData)})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func serveEvents(b *Broadcaster, w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch, cancel := b.Subscribe()
	defer cancel()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // headers out only once subscribed

	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := w.Write([]byte("event: block\ndata: " + string(data) + "\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case <-ping.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
