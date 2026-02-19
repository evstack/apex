package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Server serves Prometheus metrics over HTTP.
type Server struct {
	httpSrv *http.Server
	log     zerolog.Logger
}

// NewServer creates a metrics HTTP server listening on addr.
func NewServer(addr string, log zerolog.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	return &Server{
		httpSrv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		log: log.With().Str("component", "metrics-server").Logger(),
	}
}

// Start begins serving metrics. It blocks until the server is shut down.
func (s *Server) Start() error {
	s.log.Info().Str("addr", s.httpSrv.Addr).Msg("metrics server listening")
	if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully stops the metrics server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}
