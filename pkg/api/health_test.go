package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

type mockStatusProvider struct {
	status types.SyncStatus
}

func (m *mockStatusProvider) Status() types.SyncStatus {
	return m.status
}

func TestHealthEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		state      types.SyncState
		wantCode   int
		wantHealth bool
	}{
		{
			name:       "streaming is healthy",
			state:      types.Streaming,
			wantCode:   http.StatusOK,
			wantHealth: true,
		},
		{
			name:       "backfilling is healthy",
			state:      types.Backfilling,
			wantCode:   http.StatusOK,
			wantHealth: true,
		},
		{
			name:       "initializing is unhealthy",
			state:      types.Initializing,
			wantCode:   http.StatusServiceUnavailable,
			wantHealth: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &mockStatusProvider{status: types.SyncStatus{
				State:         tt.state,
				LatestHeight:  100,
				NetworkHeight: 105,
			}}
			notifier := NewNotifier(64, 1024, zerolog.Nop())
			h := NewHealthHandler(sp, newMockStore(), notifier, "test")

			mux := http.NewServeMux()
			h.Register(mux)

			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Errorf("status code = %d, want %d", rec.Code, tt.wantCode)
			}

			var hs HealthStatus
			if err := json.NewDecoder(rec.Body).Decode(&hs); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if hs.Healthy != tt.wantHealth {
				t.Errorf("healthy = %v, want %v", hs.Healthy, tt.wantHealth)
			}
			if hs.SyncLag != 5 {
				t.Errorf("sync_lag = %d, want 5", hs.SyncLag)
			}
		})
	}
}

func TestReadyEndpoint(t *testing.T) {
	sp := &mockStatusProvider{status: types.SyncStatus{
		State:         types.Streaming,
		LatestHeight:  100,
		NetworkHeight: 100,
	}}
	notifier := NewNotifier(64, 1024, zerolog.Nop())
	h := NewHealthHandler(sp, newMockStore(), notifier, "test")

	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status code = %d, want 200", rec.Code)
	}
}
