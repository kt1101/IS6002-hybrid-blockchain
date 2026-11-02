package web

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, h http.Handler, path string) (*http.Response, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	res := rec.Result()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("GET %s: read body: %v", path, err)
	}
	return res, string(body)
}

func TestHandlerServesIndex(t *testing.T) {
	h := Handler()

	res, body := get(t, h, "/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "Veritas Live Demo") {
		t.Errorf("GET / body does not contain %q", "Veritas Live Demo")
	}

	res, _ = get(t, h, "/js/app.js")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET /js/app.js = %d, want 200", res.StatusCode)
	}
	ct := res.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/javascript") && !strings.HasPrefix(ct, "text/javascript") {
		t.Errorf("GET /js/app.js Content-Type = %q, want a JavaScript type", ct)
	}
}

// Every file index.html references must be embedded.
func TestIndexAssetsExist(t *testing.T) {
	h := Handler()
	_, body := get(t, h, "/")
	for _, path := range []string{
		"/css/app.css", "/js/api.js", "/js/dom.js", "/js/render-cluster.js",
		"/js/render-verify.js", "/js/app.js",
	} {
		if !strings.Contains(body, `"`+strings.TrimPrefix(path, "/")+`"`) {
			t.Errorf("index.html does not reference %s", path)
			continue
		}
		if res, _ := get(t, h, path); res.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, res.StatusCode)
		}
	}
}
