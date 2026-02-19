package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// StatusProvider returns the current sync status.
type StatusProvider interface {
	Status() types.SyncStatus
}

// HealthStatus is the JSON response for health endpoints.
type HealthStatus struct {
	Healthy       bool   `json:"healthy"`
	SyncState     string `json:"sync_state"`
	LatestHeight  uint64 `json:"latest_height"`
	NetworkHeight uint64 `json:"network_height"`
	SyncLag       uint64 `json:"sync_lag"`
	Uptime        string `json:"uptime"`
	Version       string `json:"version"`
	Subscribers   int    `json:"subscribers"`
}

// HealthHandler serves /health and /health/ready endpoints.
type HealthHandler struct {
	status    StatusProvider
	store     store.Store
	notifier  *Notifier
	version   string
	startTime time.Time
}

// NewHealthHandler creates a HealthHandler.
func NewHealthHandler(sp StatusProvider, s store.Store, n *Notifier, version string) *HealthHandler {
	return &HealthHandler{
		status:    sp,
		store:     s,
		notifier:  n,
		version:   version,
		startTime: time.Now(),
	}
}

// Register mounts the health endpoints on the given mux.
func (h *HealthHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/health/ready", h.handleReady)
}

func (h *HealthHandler) buildStatus() HealthStatus {
	ss := h.status.Status()

	var lag uint64
	if ss.NetworkHeight > ss.LatestHeight {
		lag = ss.NetworkHeight - ss.LatestHeight
	}

	healthy := ss.State == types.Streaming || ss.State == types.Backfilling

	return HealthStatus{
		Healthy:       healthy,
		SyncState:     ss.State.String(),
		LatestHeight:  ss.LatestHeight,
		NetworkHeight: ss.NetworkHeight,
		SyncLag:       lag,
		Uptime:        time.Since(h.startTime).Truncate(time.Second).String(),
		Version:       h.version,
		Subscribers:   h.notifier.SubscriberCount(),
	}
}

func (h *HealthHandler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	hs := h.buildStatus()

	w.Header().Set("Content-Type", "application/json")
	if !hs.Healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(hs) //nolint:errcheck
}

func (h *HealthHandler) handleReady(w http.ResponseWriter, r *http.Request) {
	hs := h.buildStatus()

	// Additional readiness check: store must be accessible.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	_, err := h.store.GetSyncState(ctx)
	storeOK := err == nil || errors.Is(err, store.ErrNotFound)

	ready := hs.Healthy && storeOK

	w.Header().Set("Content-Type", "application/json")
	if !ready {
		hs.Healthy = false
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(hs) //nolint:errcheck
}
