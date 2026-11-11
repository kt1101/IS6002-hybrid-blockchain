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

// presentPages are the presentation pages (static/present/), in deck order.
// The table of contents is the directory URL /present/: http.FileServer
// redirects /present/index.html there (301), so it is listed as "".
var presentPages = []string{
	"", "1-bai-toan.html", "2-kien-truc.html", "3-co-che.html",
	"4-ket-qua.html", "5-gioi-han.html", "6-kich-ban.html",
}

// Every presentation page is served as HTML, is in Vietnamese, and loads
// the shared stylesheet plus its own CSS, data and script, all embedded.
func TestPresentPagesServed(t *testing.T) {
	h := Handler()
	for _, p := range presentPages {
		path := "/present/" + p
		res, body := get(t, h, path)
		if res.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, res.StatusCode)
			continue
		}
		if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Errorf("GET %s Content-Type = %q, want text/html", path, ct)
		}
		if !strings.Contains(body, `<html lang="vi">`) {
			t.Errorf("GET %s is not marked lang=vi", path)
		}
		for _, ref := range []string{`href="../css/app.css"`, `href="present.css"`, `src="data.js"`, `src="present.js"`, `href="../"`, `href="../overview"`, `href="./"`} {
			if !strings.Contains(body, ref) {
				t.Errorf("%s does not reference %s", path, ref)
			}
		}
		// A link to index.html would cost a 301 to ./ on every click.
		if strings.Contains(body, `href="index.html"`) {
			t.Errorf("%s links to index.html; link the table of contents as ./", path)
		}
	}
	for _, path := range []string{"/present/present.css", "/present/present.js", "/present/data.js"} {
		if res, _ := get(t, h, path); res.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, res.StatusCode)
		}
	}
	// The directory URL serves the table of contents.
	if _, body := get(t, h, "/present/"); !strings.Contains(body, "Trình bày đồ án") {
		t.Errorf("GET /present/ does not serve the table of contents")
	}
	if _, js := get(t, h, "/present/present.js"); strings.Contains(js, "'index.html'") {
		t.Errorf("present.js still lists index.html as a page")
	}
	// data.js is generated from the results commit; the pages read it.
	if _, body := get(t, h, "/present/data.js"); !strings.Contains(body, "window.PRESENT_DATA = ") {
		t.Errorf("/present/data.js does not define window.PRESENT_DATA")
	}
}

// The live demo and the results overview link to the presentation.
func TestPagesLinkPresentation(t *testing.T) {
	h := Handler()
	for _, path := range []string{"/", "/overview"} {
		if _, body := get(t, h, path); !strings.Contains(body, `<a href="/present/">`) {
			t.Errorf("GET %s has no link to /present/", path)
		}
	}
}

func TestOverviewServed(t *testing.T) {
	res, body := get(t, Handler(), "/overview")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET /overview = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("GET /overview Content-Type = %q, want text/html", ct)
	}
	if !strings.Contains(body, "Results overview") {
		t.Errorf("GET /overview body does not contain %q", "Results overview")
	}
}
