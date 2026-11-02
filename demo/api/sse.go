package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const pingEvery = 15 * time.Second

// events streams the hub's state: one "state" event with the full Snapshot,
// then every hub Update as an event named after its Kind. A comment line is
// sent every 15 s to keep idle connections open.
func (s *server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("streaming is not supported"))
		return
	}
	// Subscribe before taking the snapshot so no update falls in between.
	updates, cancel := s.hub.Subscribe()
	defer cancel()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	if !writeEvent(w, "state", s.hub.Snapshot()) {
		return
	}
	flusher.Flush()

	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case u, open := <-updates:
			if !open {
				return
			}
			if !writeEvent(w, u.Kind, u.Data) {
				return
			}
		case <-ping.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
		}
		flusher.Flush()
	}
}

// writeEvent writes one SSE event whose data is the JSON of v. It reports
// false if the event could not be written.
func writeEvent(w http.ResponseWriter, name string, v interface{}) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, data)
	return err == nil
}
