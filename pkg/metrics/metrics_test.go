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

	// Verify apex_info is present.
	found := false
	for _, fam := range families {
		if fam.GetName() == "apex_info" {
			found = true
			break
		}
	}
	if !found {
		t.Error("apex_info metric not found")
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
	r.IncAPIRequest("BlobGet", "ok")
	r.ObserveAPIRequestDuration("BlobGet", 10*time.Millisecond)
	r.ObserveStoreQueryDuration("GetBlobs", 2*time.Millisecond)
	r.SetActiveSubscriptions(3)
	r.IncEventsDropped()
}
