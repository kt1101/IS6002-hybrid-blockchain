// Package web serves the live demo's browser UI: static HTML, CSS, JavaScript
// and fonts embedded in the binary, so the demo needs no network at run time.
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var static embed.FS

// Handler serves the embedded UI: / is index.html, other paths map to files
// under static/.
func Handler() http.Handler {
	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err) // "static" is embedded above, so this cannot happen
	}
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Revalidate on every load so a rebuilt binary is picked up by an
		// open projector tab on refresh.
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}
