package profile

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/rs/zerolog"
)

// Server serves pprof endpoints over HTTP.
type Server struct {
	httpSrv *http.Server
	log     zerolog.Logger
}

// NewServer creates a profiling HTTP server listening on addr.
func NewServer(addr string, log zerolog.Logger) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return &Server{
		httpSrv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		log: log.With().Str("component", "profile-server").Logger(),
	}
}

// Start begins serving profiles. It blocks until the server is shut down.
func (s *Server) Start() error {
	s.log.Info().Str("addr", s.httpSrv.Addr).Msg("profiling server listening")
	if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully stops the profiling server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}
