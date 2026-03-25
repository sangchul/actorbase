// Package console provides the web console HTTP server embedded in PM.
package console

//go:generate sh -c "cd web && npm install && npm run build"

import (
	"context"
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

//go:embed web/dist
var webFS embed.FS

// Server serves the Actorbase web console.
type Server struct {
	httpAddr string
	api      *apiHandler
}

// NewServer creates a console Server.
// httpAddr is the HTTP listen address (e.g. ":8080").
// pmGRPCAddr is the PM's own gRPC address used to call PM APIs internally.
func NewServer(httpAddr, pmGRPCAddr string) *Server {
	return &Server{
		httpAddr: httpAddr,
		api:      newAPIHandler(pmGRPCAddr),
	}
}

// Start starts the HTTP server. Blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) {
	go s.api.startRoutingWatcher(ctx)

	distFS, err := fs.Sub(webFS, "web/dist")
	if err != nil {
		slog.Error("console: web/dist not available", "err", err)
		return
	}

	mux := http.NewServeMux()
	s.api.register(mux)
	mux.Handle("/", &spaHandler{underlying: distFS})

	srv := &http.Server{
		Addr:    s.httpAddr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	slog.Info("console: HTTP server starting", "addr", s.httpAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("console: HTTP server error", "err", err)
	}
}

// spaHandler serves static files from an fs.FS.
// For unknown paths it falls back to index.html (SPA client-side routing).
type spaHandler struct {
	underlying fs.FS
}

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}

	// Check if the file exists.
	if _, err := fs.Stat(h.underlying, path); err != nil {
		// Not found → serve index.html for SPA client-side routing.
		http.ServeFileFS(w, r, h.underlying, "index.html")
		return
	}
	http.FileServerFS(h.underlying).ServeHTTP(w, r)
}
