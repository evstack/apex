package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPromRecorderRegisters(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewPromRecorder(reg, "test-version")

	r.SetSyncState("streaming")
	r.SetLatestHeight(100)
	r.SetNetworkHeight(105)
	r.IncBlobsProcessed(10)
	r.IncHeadersProcessed(5)
	r.ObserveBatchDuration(500 * time.Millisecond)
	r.ObserveBackfillStageDuration("fetch_header", 3*time.Millisecond)
	r.IncBackfillStageErrors("store_header")
	r.IncAPIRequest("BlobGet", "ok")
	r.ObserveAPIRequestDuration("BlobGet", 10*time.Millisecond)
	r.ObserveStoreQueryDuration("GetBlobs", 2*time.Millisecond)
	r.SetActiveSubscriptions(3)
	r.IncEventsDropped()

	// Verify metrics were gathered without error.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) == 0 {
		t.Fatal("expected at least one metric family")
	}

	// Verify new and existing key metrics are present.
	foundInfo := false
	foundBackfillDur := false
	foundBackfillErr := false
	for _, fam := range families {
		switch fam.GetName() {
		case "apex_info":
			foundInfo = true
		case "apex_backfill_stage_duration_seconds":
			foundBackfillDur = true
		case "apex_backfill_stage_errors_total":
			foundBackfillErr = true
		}
	}
	if !foundInfo {
		t.Error("apex_info metric not found")
	}
	if !foundBackfillDur {
		t.Error("apex_backfill_stage_duration_seconds metric not found")
	}
	if !foundBackfillErr {
		t.Error("apex_backfill_stage_errors_total metric not found")
	}
}

func TestNopRecorderDoesNotPanic(t *testing.T) {
	r := Nop()
	r.SetSyncState("streaming")
	r.SetLatestHeight(100)
	r.SetNetworkHeight(105)
	r.IncBlobsProcessed(10)
	r.IncHeadersProcessed(5)
	r.ObserveBatchDuration(500 * time.Millisecond)
	r.ObserveBackfillStageDuration("fetch_header", 3*time.Millisecond)
	r.IncBackfillStageErrors("store_header")
	r.IncAPIRequest("BlobGet", "ok")
	r.ObserveAPIRequestDuration("BlobGet", 10*time.Millisecond)
	r.ObserveStoreQueryDuration("GetBlobs", 2*time.Millisecond)
	r.SetActiveSubscriptions(3)
	r.IncEventsDropped()
}
