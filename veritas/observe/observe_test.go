package observe

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hybrid/veritas/core"
	"hybrid/veritas/ledger/merkletree"
)

func TestPublishNeverBlocks(t *testing.T) {
	b := NewBroadcaster("n1")
	_, cancel := b.Subscribe()
	defer cancel()

	start := time.Now()
	for i := 0; i < 10000; i++ {
		b.Publish(core.AppliedEvent{Height: int64(i), Root: []byte{1}})
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("10000 publishes took %v, want < 1s", d)
	}
	if got := b.Dropped(); got < 10000-256 {
		t.Fatalf("Dropped() = %d, want >= %d", got, 10000-256)
	}
}

func TestSubscribeReceivesEvents(t *testing.T) {
	b := NewBroadcaster("n1")
	ch, cancel := b.Subscribe()
	defer cancel()

	before := time.Now().UnixNano() / int64(time.Millisecond)
	b.Publish(core.AppliedEvent{Height: 7, Proposer: "p", Txs: 3, Aborted: 1, Root: []byte{0xde, 0xad}})

	select {
	case ev := <-ch:
		if ev.Node != "n1" || ev.Height != 7 || ev.Proposer != "p" || ev.Txs != 3 || ev.Aborted != 1 {
			t.Fatalf("unexpected event %+v", ev)
		}
		if ev.Root != "dead" {
			t.Fatalf("root = %q, want dead", ev.Root)
		}
		if ev.AtMs < before {
			t.Fatalf("AtMs = %d, want >= %d", ev.AtMs, before)
		}
	case <-time.After(time.Second):
		t.Fatal("no event received")
	}

	cancel()
	b.Publish(core.AppliedEvent{Height: 8})
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed with no further events after cancel")
	}
}

func TestEventsSSE(t *testing.T) {
	b := NewBroadcaster("n1")
	srv := httptest.NewServer(Handler(b, nil, nil))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}
	// Headers are flushed after the handler subscribed, so publishing now
	// cannot be missed.
	b.Publish(core.AppliedEvent{Height: 42, Proposer: "p", Txs: 2, Root: []byte{0xab}})

	lines := make(chan string, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	next := func() string {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatal("stream ended early")
			}
			return l
		case <-time.After(2 * time.Second):
			t.Fatal("timeout reading SSE line")
		}
		return ""
	}
	if l := next(); l != "event: block" {
		t.Fatalf("first line = %q", l)
	}
	l := next()
	if !strings.HasPrefix(l, "data: ") {
		t.Fatalf("second line = %q", l)
	}
	var ev Event
	if err := json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Node != "n1" || ev.Height != 42 || ev.Root != "ab" || ev.Txs != 2 {
		t.Fatalf("decoded %+v", ev)
	}
	if l := next(); l != "" {
		t.Fatalf("third line = %q, want blank", l)
	}

	// Client disconnect must unsubscribe.
	resp.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for b.subscriberCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscriber not removed after client disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestKVHandler(t *testing.T) {
	kv := func(ctx context.Context, key string) (string, int64, bool, error) {
		if key == "a" {
			return "va", 5, true, nil
		}
		return "", 0, false, nil
	}
	srv := httptest.NewServer(Handler(NewBroadcaster("n"), kv, nil))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/kv?key=a")
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Value   string `json:"value"`
		Version int64  `json:"version"`
	}
	json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if resp.StatusCode != 200 || got.Value != "va" || got.Version != 5 {
		t.Fatalf("status %d body %+v", resp.StatusCode, got)
	}

	resp, err = http.Get(srv.URL + "/kv?key=missing")
	if err != nil {
		t.Fatal(err)
	}
	var e struct {
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&e)
	resp.Body.Close()
	if resp.StatusCode != 404 || e.Error == "" {
		t.Fatalf("status %d error %q", resp.StatusCode, e.Error)
	}

	resp, err = http.Get(srv.URL + "/kv")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("missing key status %d, want 400", resp.StatusCode)
	}
}

func TestProveHandler(t *testing.T) {
	prove := func(key []byte) ([]byte, merkletree.SparseMerkleProof, error) {
		return []byte{0x01, 0x02}, merkletree.SparseMerkleProof{
			SideNodes:             [][]byte{{0xaa}, {0xbb, 0xcc}},
			NonMembershipLeafData: []byte{0xff},
		}, nil
	}
	srv := httptest.NewServer(Handler(NewBroadcaster("n"), nil, prove))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/prove?key=k")
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Root                  string   `json:"root"`
		SideNodes             []string `json:"side_nodes"`
		NonMembershipLeafData string   `json:"non_membership_leaf_data"`
	}
	json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if resp.StatusCode != 200 || got.Root != "0102" || len(got.SideNodes) != 2 ||
		got.SideNodes[0] != "aa" || got.SideNodes[1] != "bbcc" || got.NonMembershipLeafData != hex.EncodeToString([]byte{0xff}) {
		t.Fatalf("status %d body %+v", resp.StatusCode, got)
	}

	resp, err = http.Get(srv.URL + "/prove")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("missing key status %d, want 400", resp.StatusCode)
	}
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(Handler(NewBroadcaster("n"), nil, nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 8)
	n, _ := resp.Body.Read(buf)
	if resp.StatusCode != 200 || string(buf[:n]) != "ok" {
		t.Fatalf("status %d body %q", resp.StatusCode, buf[:n])
	}
}
