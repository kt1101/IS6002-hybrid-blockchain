// Package api is the live demo's HTTP layer: JSON endpoints that drive the
// cluster, the load generator and verification, plus a Server-Sent Events
// stream of the hub's state for the browser UI.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"hybrid/demo/cluster"
	"hybrid/demo/hub"
	"hybrid/demo/load"
	"hybrid/demo/verify"
)

const (
	maxKeyBytes   = 128
	maxValueBytes = 4096
	maxBodyBytes  = 1 << 20

	minStep = 1
	maxStep = 5

	txTimeout     = 10 * time.Second // one Set
	verifyTimeout = 10 * time.Second // a whole verify
	faultTimeout  = 30 * time.Second // inject / heal / reset
)

// server holds what the handlers share.
type server struct {
	c      cluster.Cluster
	hub    *hub.Hub
	runner *load.Runner
}

// New returns the demo's HTTP handler: the /api routes backed by c, h and r,
// and static (may be nil) for every other path.
//
// /api/events is a long-lived response, so the http.Server that mounts this
// handler must not set WriteTimeout (it would cut the stream); use
// ReadHeaderTimeout and similar instead.
func New(c cluster.Cluster, h *hub.Hub, r *load.Runner, static http.Handler) http.Handler {
	s := &server{c: c, hub: h, runner: r}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", method(http.MethodGet, s.state))
	mux.HandleFunc("/api/events", method(http.MethodGet, s.events))
	mux.HandleFunc("/api/tx", method(http.MethodPost, s.tx))
	mux.HandleFunc("/api/load", s.loadRoute)
	mux.HandleFunc("/api/verify", method(http.MethodPost, s.verify))
	mux.HandleFunc("/api/fault", method(http.MethodPost, s.fault))
	mux.HandleFunc("/api/heal", method(http.MethodPost, s.heal))
	mux.HandleFunc("/api/reset", method(http.MethodPost, s.reset))
	mux.HandleFunc("/api/scenario", method(http.MethodPost, s.scenario))
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, errors.New("no such endpoint"))
	})
	if static != nil {
		mux.Handle("/", static)
	}
	return mux
}

// method restricts a handler to one HTTP method.
func method(m string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != m {
			methodNotAllowed(w, m)
			return
		}
		next(w, r)
	}
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}

// loadRoute serves POST (start) and DELETE (stop) on /api/load.
func (s *server) loadRoute(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.startLoad(w, r)
	case http.MethodDelete:
		s.stopLoad(w, r)
	default:
		methodNotAllowed(w, "POST, DELETE")
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// writeClusterError maps a cluster failure to a status code.
func writeClusterError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, cluster.ErrUnsupported):
		writeError(w, http.StatusNotImplemented, err)
	case errors.Is(err, cluster.ErrNodeDown):
		writeError(w, http.StatusServiceUnavailable, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

// decode reads a JSON request body into v.
func decode(w http.ResponseWriter, r *http.Request, v interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %v", err)
	}
	return nil
}

// checkNode reports whether name is one of the cluster's nodes.
func (s *server) checkNode(name string) bool {
	for _, n := range s.c.Nodes() {
		if n.Name == name {
			return true
		}
	}
	return false
}

// validateKey enforces the key limits.
func validateKey(key string) error {
	if key == "" {
		return errors.New("key must not be empty")
	}
	if len(key) > maxKeyBytes {
		return fmt.Errorf("key must be at most %d bytes, got %d", maxKeyBytes, len(key))
	}
	return nil
}

// requestNode validates a node name: false means the response has been written.
func (s *server) requestNode(w http.ResponseWriter, name string) bool {
	if !s.checkNode(name) {
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown node %q", name))
		return false
	}
	return true
}

func (s *server) state(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.hub.Snapshot())
}

type txRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Node  string `json:"node"`
}

func (s *server) tx(w http.ResponseWriter, r *http.Request) {
	var req txRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := validateKey(req.Key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Value) > maxValueBytes {
		writeError(w, http.StatusBadRequest, fmt.Errorf("value must be at most %d bytes, got %d", maxValueBytes, len(req.Value)))
		return
	}
	if !s.requestNode(w, req.Node) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), txTimeout)
	defer cancel()
	start := time.Now()
	err := s.c.Set(ctx, req.Node, req.Key, req.Value)
	lat := time.Since(start)
	s.hub.RecordOp(lat, err)
	if err != nil {
		s.hub.Logf("error", "Set %s on %s failed: %v", req.Key, req.Node, err)
		status := http.StatusOK // the cluster rejected it; the UI shows the reason in place
		if errors.Is(err, cluster.ErrNodeDown) {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	s.hub.SetTrackedKey(req.Key)
	s.hub.Logf("ok", "Set %s on %s committed (%d ms)", req.Key, req.Node, lat.Milliseconds())
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":         true,
		"latency_ms": lat.Milliseconds(),
		"height":     s.referenceHeight(),
	})
}

// referenceHeight is the height of the lowest-numbered up node, the one whose
// blocks the UI shows.
func (s *server) referenceHeight() int64 {
	for _, n := range s.hub.Snapshot().Nodes { // sorted by name
		if n.Status != "down" {
			return n.Height
		}
	}
	return 0
}

type loadRequest struct {
	Clients   int    `json:"clients"`
	Workload  string `json:"workload"`
	DurationS int    `json:"duration_s"`
}

func (s *server) startLoad(w http.ResponseWriter, r *http.Request) {
	var req loadRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	// Bound the seconds before converting so the Duration cannot overflow.
	if req.DurationS < 0 || req.DurationS > 1<<20 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("duration_s out of range: %d", req.DurationS))
		return
	}
	cfg := load.Config{Clients: req.Clients, Workload: req.Workload, Duration: time.Duration(req.DurationS) * time.Second}
	if err := cfg.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	// onState must not call runner.Stop: Stop waits for the run, which is
	// the goroutine delivering this callback.
	onState := func(running bool, remaining time.Duration) {
		remS := 0
		if running {
			remS = int((remaining + time.Second - 1) / time.Second)
		}
		s.hub.SetLoad(hub.LoadState{Running: running, Clients: cfg.Clients, Workload: cfg.Workload, RemainingS: remS})
	}
	if err := s.runner.Start(cfg, onState); err != nil {
		if errors.Is(err, load.ErrRunning) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.hub.Logf("info", "Load started: %d clients, workload %s, %d s", cfg.Clients, cfg.Workload, req.DurationS)
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *server) stopLoad(w http.ResponseWriter, r *http.Request) {
	s.runner.Stop()
	w.WriteHeader(http.StatusNoContent)
}

type verifyRequest struct {
	Key  string `json:"key"`
	Node string `json:"node"`
}

func (s *server) verify(w http.ResponseWriter, r *http.Request) {
	var req verifyRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := validateKey(req.Key); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !s.requestNode(w, req.Node) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), verifyTimeout)
	defer cancel()
	res, err := verify.Run(ctx, s.c, req.Node, req.Key)
	if err != nil {
		s.hub.Logf("error", "Verify %s on %s failed: %v", req.Key, req.Node, err)
		writeClusterError(w, err)
		return
	}
	s.hub.SetTrackedKey(req.Key)
	switch {
	case !res.Found:
		s.hub.Logf("info", "Verify %s on %s: key not found", req.Key, req.Node)
	case res.Valid:
		s.hub.Logf("ok", "Verify %s on %s: proof valid", req.Key, req.Node)
	default:
		s.hub.Logf("warn", "Verify %s on %s: proof invalid", req.Key, req.Node)
	}
	writeJSON(w, http.StatusOK, res)
}

type faultRequest struct {
	Kind string `json:"kind"`
	Node string `json:"node"`
}

func (s *server) fault(w http.ResponseWriter, r *http.Request) {
	var req faultRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	kind := cluster.Fault(req.Kind)
	switch kind {
	case cluster.FaultCrash, cluster.FaultSilent, cluster.FaultSlow, cluster.FaultCorrupt:
	default:
		writeError(w, http.StatusBadRequest, fmt.Errorf("kind must be crash, silent, slow or corrupt, got %q", req.Kind))
		return
	}
	if !s.requestNode(w, req.Node) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), faultTimeout)
	defer cancel()
	if err := s.c.Inject(ctx, req.Node, kind); err != nil {
		s.hub.Logf("error", "Injecting %s on %s failed: %v", kind, req.Node, err)
		writeClusterError(w, err)
		return
	}
	s.hub.Logf("warn", "%s injected on %s", kind, req.Node)
	s.hub.Mark(fmt.Sprintf("%s @ %s", kind, req.Node))
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

type healRequest struct {
	Node string `json:"node"`
}

func (s *server) heal(w http.ResponseWriter, r *http.Request) {
	var req healRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !s.requestNode(w, req.Node) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), faultTimeout)
	defer cancel()
	if err := s.c.Heal(ctx, req.Node); err != nil {
		s.hub.Logf("error", "Healing %s failed: %v", req.Node, err)
		writeClusterError(w, err)
		return
	}
	s.hub.Logf("info", "%s healed", req.Node)
	s.hub.Mark("heal @ " + req.Node)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) reset(w http.ResponseWriter, r *http.Request) {
	s.runner.Stop()
	ctx, cancel := context.WithTimeout(r.Context(), faultTimeout)
	defer cancel()
	if err := s.c.Reset(ctx); err != nil {
		writeClusterError(w, err)
		return
	}
	s.hub.Reset()
	s.runner.ResetPreload()
	w.WriteHeader(http.StatusNoContent)
}

type scenarioRequest struct {
	Step int `json:"step"`
}

func (s *server) scenario(w http.ResponseWriter, r *http.Request) {
	var req scenarioRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Step < minStep || req.Step > maxStep {
		writeError(w, http.StatusBadRequest, fmt.Errorf("step must be %d..%d, got %d", minStep, maxStep, req.Step))
		return
	}
	s.hub.SetStep(req.Step)
	w.WriteHeader(http.StatusNoContent)
}
