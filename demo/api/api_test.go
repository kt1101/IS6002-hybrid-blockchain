package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hybrid/demo/cluster"
	"hybrid/demo/hub"
	"hybrid/demo/load"
	"hybrid/demo/verify"
)

type env struct {
	url    string
	hub    *hub.Hub
	runner *load.Runner
}

// newEnv serves the API over a real Sim, hub and runner.
func newEnv(t *testing.T) *env {
	t.Helper()
	c, err := cluster.NewSim(cluster.SimConfig{N: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	h := hub.New(c, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	r := load.NewRunner(c, h.RecordOp)
	t.Cleanup(r.Stop)
	static := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "static page") })
	srv := httptest.NewServer(New(c, h, r, static))
	t.Cleanup(srv.Close)
	return &env{url: srv.URL, hub: h, runner: r}
}

// do sends a request with a JSON body (nil for none) and returns the status
// and raw response body.
func (e *env) do(t *testing.T, method, path string, body interface{}) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.url+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

type obj = map[string]interface{}

// call is do plus decoding a JSON object response (empty body gives nil).
func (e *env) call(t *testing.T, method, path string, body interface{}) (int, obj) {
	t.Helper()
	status, raw := e.do(t, method, path, body)
	var out obj
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: body %q is not a JSON object: %v", method, path, raw, err)
		}
	}
	return status, out
}

func (e *env) snapshot(t *testing.T) hub.Snapshot {
	t.Helper()
	status, raw := e.do(t, "GET", "/api/state", nil)
	if status != 200 {
		t.Fatalf("GET /api/state = %d: %s", status, raw)
	}
	var s hub.Snapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// eventually polls cond until it holds or d passes.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (e *env) mustSet(t *testing.T, node, key, value string) {
	t.Helper()
	status, out := e.call(t, "POST", "/api/tx", obj{"key": key, "value": value, "node": node})
	if status != 200 || out["ok"] != true {
		t.Fatalf("tx %s on %s = %d %v", key, node, status, out)
	}
}

func TestTxCommits(t *testing.T) {
	e := newEnv(t)
	status, out := e.call(t, "POST", "/api/tx", obj{"key": "alice", "value": "100", "node": "node1"})
	if status != 200 || out["ok"] != true {
		t.Fatalf("tx = %d %v, want 200 ok", status, out)
	}
	if _, ok := out["latency_ms"].(float64); !ok {
		t.Fatalf("latency_ms missing or not a number: %v", out)
	}
	if _, ok := out["height"].(float64); !ok {
		t.Fatalf("height missing or not a number: %v", out)
	}
	s := e.snapshot(t)
	if s.TrackedKey != "alice" {
		t.Fatalf("tracked_key = %q, want alice", s.TrackedKey)
	}
	var logged bool
	for _, l := range s.Log {
		if l.Level == "ok" && strings.Contains(l.Text, "Set alice on node1 committed") {
			logged = true
		}
	}
	if !logged {
		t.Fatalf("no commit log line: %+v", s.Log)
	}
	if s.Metrics.Errors != 0 {
		t.Fatalf("successful op counted as error: %+v", s.Metrics)
	}
}

func TestTxValidation(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name string
		body interface{}
		want int
	}{
		{"empty key", obj{"key": "", "value": "v", "node": "node1"}, 400},
		{"129-byte key", obj{"key": strings.Repeat("k", 129), "value": "v", "node": "node1"}, 400},
		{"128-byte key ok", obj{"key": strings.Repeat("k", 128), "value": "v", "node": "node1"}, 200},
		{"value too big", obj{"key": "k", "value": strings.Repeat("v", 4097), "node": "node1"}, 400},
		{"max value ok", obj{"key": "k", "value": strings.Repeat("v", 4096), "node": "node1"}, 200},
		{"unknown node", obj{"key": "k", "value": "v", "node": "node9"}, 404},
		{"missing node", obj{"key": "k", "value": "v"}, 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, out := e.call(t, "POST", "/api/tx", tc.body)
			if status != tc.want {
				t.Fatalf("status = %d (%v), want %d", status, out, tc.want)
			}
			if status >= 400 {
				if msg, _ := out["error"].(string); msg == "" {
					t.Fatalf("error body missing: %v", out)
				}
			}
		})
	}
	// Malformed JSON.
	req, _ := http.NewRequest("POST", e.url+"/api/tx", strings.NewReader("{nope"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("malformed JSON = %d, want 400", resp.StatusCode)
	}
}

func TestMethodsAndStatic(t *testing.T) {
	e := newEnv(t)
	if status, _ := e.do(t, "GET", "/api/tx", nil); status != 405 {
		t.Fatalf("GET /api/tx = %d, want 405", status)
	}
	if status, _ := e.do(t, "POST", "/api/state", nil); status != 405 {
		t.Fatalf("POST /api/state = %d, want 405", status)
	}
	if status, _ := e.do(t, "PUT", "/api/load", nil); status != 405 {
		t.Fatalf("PUT /api/load = %d, want 405", status)
	}
	if status, _ := e.do(t, "GET", "/api/nope", nil); status != 404 {
		t.Fatalf("GET /api/nope = %d, want 404", status)
	}
	status, body := e.do(t, "GET", "/index.html", nil)
	if status != 200 || string(body) != "static page" {
		t.Fatalf("static = %d %q", status, body)
	}
}

func TestVerifyEndpoint(t *testing.T) {
	e := newEnv(t)
	e.mustSet(t, "node1", "alice", "100")
	status, raw := e.do(t, "POST", "/api/verify", obj{"key": "alice", "node": "node2"})
	if status != 200 {
		t.Fatalf("verify = %d: %s", status, raw)
	}
	var res verify.Result
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if !res.Valid || !res.Found || res.Value != "100" || res.Node != "node2" || len(res.Steps) == 0 {
		t.Fatalf("result = %+v", res)
	}
	if len(res.Replicas) != 4 {
		t.Fatalf("replicas = %d, want 4", len(res.Replicas))
	}
	if got := e.snapshot(t).TrackedKey; got != "alice" {
		t.Fatalf("tracked_key = %q, want alice", got)
	}

	for name, body := range map[string]obj{
		"empty key": {"key": "", "node": "node1"},
		"long key":  {"key": strings.Repeat("k", 129), "node": "node1"},
	} {
		if status, _ := e.do(t, "POST", "/api/verify", body); status != 400 {
			t.Fatalf("%s: status = %d, want 400", name, status)
		}
	}
	if status, _ := e.do(t, "POST", "/api/verify", obj{"key": "alice", "node": "nodeX"}); status != 404 {
		t.Fatalf("unknown node status = %d, want 404", status)
	}
}

func TestFaultAndHeal(t *testing.T) {
	e := newEnv(t)
	e.mustSet(t, "node1", "alice", "100")
	status, out := e.call(t, "POST", "/api/fault", obj{"kind": "corrupt", "node": "node3"})
	if status != 200 || out["ok"] != true {
		t.Fatalf("fault = %d %v", status, out)
	}
	// A corrupt node diverges from the next block it applies.
	e.mustSet(t, "node1", "alice", "101")
	eventually(t, 3*time.Second, "node3 DIVERGENT", func() bool {
		c := e.snapshot(t).Consistency
		return c.Verdict == "DIVERGENT" && len(c.Divergent) == 1 && c.Divergent[0] == "node3"
	})

	// Corrupt is not healable.
	status, out = e.call(t, "POST", "/api/heal", obj{"node": "node3"})
	if status != 501 {
		t.Fatalf("heal corrupt = %d %v, want 501", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatalf("error body missing: %v", out)
	}

	for name, body := range map[string]obj{
		"bad kind":   {"kind": "meltdown", "node": "node1"},
		"empty kind": {"node": "node1"},
	} {
		if status, _ := e.do(t, "POST", "/api/fault", body); status != 400 {
			t.Fatalf("%s: status = %d, want 400", name, status)
		}
	}
	if status, _ := e.do(t, "POST", "/api/fault", obj{"kind": "crash", "node": "node9"}); status != 404 {
		t.Fatalf("fault on unknown node = %d, want 404", status)
	}
	if status, _ := e.do(t, "POST", "/api/heal", obj{"node": "node9"}); status != 404 {
		t.Fatalf("heal on unknown node = %d, want 404", status)
	}

	var warned bool
	for _, l := range e.snapshot(t).Log {
		if l.Level == "warn" && l.Text == "corrupt injected on node3" {
			warned = true
		}
	}
	if !warned {
		t.Fatal("injection not logged")
	}
}

func TestCrashHealViaAPI(t *testing.T) {
	e := newEnv(t)
	e.mustSet(t, "node1", "alice", "100")
	if status, out := e.call(t, "POST", "/api/fault", obj{"kind": "crash", "node": "node2"}); status != 200 {
		t.Fatalf("crash = %d %v", status, out)
	}
	eventually(t, 3*time.Second, "node2 down", func() bool {
		for _, n := range e.snapshot(t).Nodes {
			if n.Name == "node2" {
				return n.Status == "down"
			}
		}
		return false
	})

	status, out := e.call(t, "POST", "/api/tx", obj{"key": "bob", "value": "1", "node": "node2"})
	if status != 503 || out["ok"] != false || out["error"] == "" {
		t.Fatalf("tx on down node = %d %v, want 503 ok=false", status, out)
	}
	if n := e.snapshot(t).Metrics.Errors; n != 1 {
		t.Fatalf("metrics errors = %d, want 1 (the failed tx)", n)
	}
	if status, _ := e.do(t, "POST", "/api/verify", obj{"key": "alice", "node": "node2"}); status != 503 {
		t.Fatalf("verify on down node = %d, want 503", status)
	}
	// The other nodes keep working.
	e.mustSet(t, "node1", "bob", "2")

	if status, out := e.call(t, "POST", "/api/heal", obj{"node": "node2"}); status != 200 || out["ok"] != true {
		t.Fatalf("heal = %d %v", status, out)
	}
	eventually(t, 3*time.Second, "node2 up", func() bool {
		for _, n := range e.snapshot(t).Nodes {
			if n.Name == "node2" {
				return n.Status != "down"
			}
		}
		return false
	})
	e.mustSet(t, "node2", "carol", "3")
	var labels []string
	for _, m := range e.snapshot(t).Metrics.Markers {
		labels = append(labels, m.Label)
	}
	if strings.Join(labels, ",") != "crash @ node2,heal @ node2" {
		t.Fatalf("markers = %v", labels)
	}
}

func TestLoadConflict(t *testing.T) {
	e := newEnv(t)
	body := obj{"clients": 2, "workload": "a", "duration_s": 5}
	status, out := e.call(t, "POST", "/api/load", body)
	if status != 202 || out["ok"] != true {
		t.Fatalf("first load = %d %v, want 202", status, out)
	}
	eventually(t, 3*time.Second, "load state running", func() bool {
		l := e.snapshot(t).Load
		return l.Running && l.Clients == 2 && l.Workload == "a" && l.RemainingS > 0
	})
	status, out = e.call(t, "POST", "/api/load", body)
	if status != 409 || out["error"] == "" {
		t.Fatalf("second load = %d %v, want 409", status, out)
	}
	if status, _ := e.do(t, "DELETE", "/api/load", nil); status != 204 {
		t.Fatalf("DELETE load = %d, want 204", status)
	}
	if e.runner.Running() {
		t.Fatal("runner still running after DELETE")
	}
	if l := e.snapshot(t).Load; l.Running {
		t.Fatalf("hub load state still running: %+v", l)
	}
	// Stopping when idle is fine too.
	if status, _ := e.do(t, "DELETE", "/api/load", nil); status != 204 {
		t.Fatalf("idle DELETE load = %d, want 204", status)
	}
}

func TestLoadValidation(t *testing.T) {
	e := newEnv(t)
	for name, body := range map[string]obj{
		"zero clients": {"clients": 0, "workload": "a", "duration_s": 10},
		"257 clients":  {"clients": 257, "workload": "a", "duration_s": 10},
		"bad workload": {"clients": 4, "workload": "z", "duration_s": 10},
		"short":        {"clients": 4, "workload": "a", "duration_s": 4},
		"long":         {"clients": 4, "workload": "a", "duration_s": 301},
		"negative":     {"clients": 4, "workload": "a", "duration_s": -10},
		"overflowing":  {"clients": 4, "workload": "a", "duration_s": 9223372036},
	} {
		status, out := e.call(t, "POST", "/api/load", body)
		if status != 400 {
			t.Fatalf("%s: status = %d %v, want 400", name, status, out)
		}
	}
	if e.runner.Running() {
		t.Fatal("a rejected request started a run")
	}
}

func TestScenario(t *testing.T) {
	e := newEnv(t)
	if status, _ := e.do(t, "POST", "/api/scenario", obj{"step": 3}); status != 204 {
		t.Fatalf("scenario 3 = %d, want 204", status)
	}
	if got := e.snapshot(t).Step; got != 3 {
		t.Fatalf("scenario_step = %d, want 3", got)
	}
	for _, step := range []int{0, 6, -1} {
		if status, _ := e.do(t, "POST", "/api/scenario", obj{"step": step}); status != 400 {
			t.Fatalf("scenario %d = %d, want 400", step, status)
		}
	}
	if got := e.snapshot(t).Step; got != 3 {
		t.Fatalf("rejected step changed state to %d", got)
	}
}

// sseReader reads events from an SSE response.
type sseReader struct {
	sc *bufio.Scanner
}

type sseEvent struct {
	name, data string
}

// next returns the next event, skipping comment lines.
func (r *sseReader) next() (sseEvent, bool) {
	var ev sseEvent
	for r.sc.Scan() {
		line := r.sc.Text()
		switch {
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event: "):
			ev.name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			ev.data = strings.TrimPrefix(line, "data: ")
		case line == "" && ev.name != "":
			return ev, true
		}
	}
	return ev, false
}

func TestEventsStreamStartsWithState(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", e.url+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("Cache-Control = %q", cc)
	}

	rd := &sseReader{sc: bufio.NewScanner(resp.Body)}
	rd.sc.Buffer(make([]byte, 1<<20), 1<<20)
	first, ok := rd.next()
	if !ok || first.name != "state" {
		t.Fatalf("first event = %+v, want state", first)
	}
	var snap hub.Snapshot
	if err := json.Unmarshal([]byte(first.data), &snap); err != nil {
		t.Fatalf("state data is not a Snapshot: %v", err)
	}
	if len(snap.Nodes) != 4 || snap.Info.N != 4 {
		t.Fatalf("snapshot = %+v", snap)
	}

	// Later changes arrive as events named after the hub update kind.
	e.mustSet(t, "node1", "alice", "100")
	got := make(chan sseEvent, 1)
	go func() {
		for {
			ev, ok := rd.next()
			if !ok {
				return
			}
			if ev.name == "log" && strings.Contains(ev.data, "Set alice on node1 committed") {
				got <- ev
				return
			}
		}
	}()
	select {
	case ev := <-got:
		var line hub.LogLine
		if err := json.Unmarshal([]byte(ev.data), &line); err != nil || line.Level != "ok" {
			t.Fatalf("log event data = %q (%v)", ev.data, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no log event after a committed Set")
	}
}

func TestResetSim(t *testing.T) {
	e := newEnv(t)
	e.mustSet(t, "node1", "alice", "100")
	e.do(t, "POST", "/api/scenario", obj{"step": 4})
	if status, _ := e.do(t, "POST", "/api/load", obj{"clients": 2, "workload": "a", "duration_s": 30}); status != 202 {
		t.Fatalf("load = %d, want 202", status)
	}

	if status, body := e.do(t, "POST", "/api/reset", nil); status != 204 {
		t.Fatalf("reset = %d: %s, want 204", status, body)
	}
	if e.runner.Running() {
		t.Fatal("load still running after reset")
	}
	s := e.snapshot(t)
	if s.TrackedKey != "" || s.Step != 0 || s.Load.Running || len(s.Metrics.Markers) != 0 {
		t.Fatalf("hub not cleared: tracked=%q step=%d load=%+v markers=%v", s.TrackedKey, s.Step, s.Load, s.Metrics.Markers)
	}
	if len(s.Log) != 1 || !strings.Contains(s.Log[0].Text, "reset") {
		t.Fatalf("log after reset = %+v", s.Log)
	}
	// The ledger is empty again: alice is gone, and the cluster still commits.
	status, raw := e.do(t, "POST", "/api/verify", obj{"key": "alice", "node": "node1"})
	if status != 200 {
		t.Fatalf("verify after reset = %d: %s", status, raw)
	}
	var res verify.Result
	if err := json.Unmarshal(raw, &res); err != nil || res.Found {
		t.Fatalf("alice survived the reset: %+v (%v)", res, err)
	}
	var notFound, invalid bool
	for _, l := range e.snapshot(t).Log {
		if l.Level == "info" && l.Text == "Verify alice on node1: key not found" {
			notFound = true
		}
		if strings.Contains(l.Text, "proof invalid") {
			invalid = true
		}
	}
	if !notFound || invalid {
		t.Fatalf("log for a missing key: notFound=%v invalid=%v: %+v", notFound, invalid, e.snapshot(t).Log)
	}
	e.mustSet(t, "node2", "bob", "1")
}

// unsupported wraps a Cluster whose Reset is unavailable, as in docker mode.
type unsupported struct{ cluster.Cluster }

func (unsupported) Reset(context.Context) error { return cluster.ErrUnsupported }

func TestResetUnsupported(t *testing.T) {
	c, err := cluster.NewSim(cluster.SimConfig{N: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	cl := unsupported{c}
	h := hub.New(cl, nil)
	r := load.NewRunner(cl, h.RecordOp)
	srv := httptest.NewServer(New(cl, h, r, nil))
	t.Cleanup(srv.Close)
	e := &env{url: srv.URL, hub: h, runner: r}
	status, out := e.call(t, "POST", "/api/reset", nil)
	if status != 501 || out["error"] == "" {
		t.Fatalf("reset = %d %v, want 501", status, out)
	}
}
