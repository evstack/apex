package metrics

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Recorder defines the metrics interface for all Apex components.
// Implementations must be safe for concurrent use. A nil Recorder is
// valid — callers should use the package-level Nop() helper.
type Recorder interface {
	// Sync metrics
	SetSyncState(state string)
	SetLatestHeight(h uint64)
	SetNetworkHeight(h uint64)
	IncBlobsProcessed(n int)
	IncHeadersProcessed(n int)
	ObserveBatchDuration(d time.Duration)
	ObserveBackfillStageDuration(stage string, d time.Duration)
	IncBackfillStageErrors(stage string)

	// API metrics
	IncAPIRequest(method, status string)
	ObserveAPIRequestDuration(method string, d time.Duration)

	// Store metrics
	ObserveStoreQueryDuration(op string, d time.Duration)

	// Subscription metrics
	SetActiveSubscriptions(n int)
	IncEventsDropped()
}

// nopRecorder is a no-op implementation of Recorder.
type nopRecorder struct{}

// Nop returns a Recorder that discards all metrics.
func Nop() Recorder { return nopRecorder{} }

func (nopRecorder) SetSyncState(string)                                {}
func (nopRecorder) SetLatestHeight(uint64)                             {}
func (nopRecorder) SetNetworkHeight(uint64)                            {}
func (nopRecorder) IncBlobsProcessed(int)                              {}
func (nopRecorder) IncHeadersProcessed(int)                            {}
func (nopRecorder) ObserveBatchDuration(time.Duration)                 {}
func (nopRecorder) ObserveBackfillStageDuration(string, time.Duration) {}
func (nopRecorder) IncBackfillStageErrors(string)                      {}
func (nopRecorder) IncAPIRequest(string, string)                       {}
func (nopRecorder) ObserveAPIRequestDuration(string, time.Duration)    {}
func (nopRecorder) ObserveStoreQueryDuration(string, time.Duration)    {}
func (nopRecorder) SetActiveSubscriptions(int)                         {}
func (nopRecorder) IncEventsDropped()                                  {}

// syncStateLabels lists the known sync states. Index must match the
// syncStateGauges array in PromRecorder.
var syncStateLabels = [3]string{"initializing", "backfilling", "streaming"}

// PromRecorder implements Recorder using Prometheus metrics.
//
// All *Vec metrics are pre-resolved at construction time via WithLabelValues
// for known label sets. This eliminates per-call map lookups and slice
// allocations on hot paths.
type PromRecorder struct {
	latestHeight     prometheus.Gauge
	networkHeight    prometheus.Gauge
	syncLag          prometheus.Gauge
	blobsProcessed   prometheus.Counter
	headersProcessed prometheus.Counter
	batchDuration    prometheus.Histogram
	activeSubs       prometheus.Gauge
	eventsDropped    prometheus.Counter

	// Pre-resolved label combinations.
	syncStateGauges [3]prometheus.Gauge // indexed by syncStateLabels

	// Vec metrics kept for dynamic labels not known at init.
	backfillStageDurVec *prometheus.HistogramVec
	backfillStageErrVec *prometheus.CounterVec
	apiRequestsVec      *prometheus.CounterVec
	apiDurationVec      *prometheus.HistogramVec
	storeQueryDurVec    *prometheus.HistogramVec

	// Pre-resolved observers/counters for known label values.
	backfillStageDurOb map[string]prometheus.Observer
	backfillStageErrCt map[string]prometheus.Counter
	storeQueryDurOb    map[string]prometheus.Observer

	// cached for lag calculation
	lastLatest  atomic.Uint64
	lastNetwork atomic.Uint64
}

// NewPromRecorder creates a PromRecorder and registers metrics with the
// provided Prometheus registerer. Pass nil to use the default registerer.
func NewPromRecorder(reg prometheus.Registerer, version string) *PromRecorder {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	syncStateVec := factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "apex_sync_state",
		Help: "Current sync state (1 = active for the labeled state).",
	}, []string{"state"})

	backfillStageDurVec := factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "apex_backfill_stage_duration_seconds",
		Help:    "Per-stage duration during backfill height processing.",
		Buckets: prometheus.DefBuckets,
	}, []string{"stage"})

	backfillStageErrVec := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "apex_backfill_stage_errors_total",
		Help: "Total backfill stage errors by stage.",
	}, []string{"stage"})

	storeQueryDurVec := factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "apex_store_query_duration_seconds",
		Help:    "Store query duration by operation.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	apiRequestsVec := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "apex_api_requests_total",
		Help: "Total API requests by method and status.",
	}, []string{"method", "status"})

	apiDurationVec := factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "apex_api_request_duration_seconds",
		Help:    "API request duration by method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	r := &PromRecorder{
		latestHeight: factory.NewGauge(prometheus.GaugeOpts{
			Name: "apex_sync_latest_height",
			Help: "Latest locally synced block height.",
		}),
		networkHeight: factory.NewGauge(prometheus.GaugeOpts{
			Name: "apex_sync_network_height",
			Help: "Latest known network block height.",
		}),
		syncLag: factory.NewGauge(prometheus.GaugeOpts{
			Name: "apex_sync_lag",
			Help: "Difference between network height and latest synced height.",
		}),
		blobsProcessed: factory.NewCounter(prometheus.CounterOpts{
			Name: "apex_sync_blobs_processed_total",
			Help: "Total number of blobs processed.",
		}),
		headersProcessed: factory.NewCounter(prometheus.CounterOpts{
			Name: "apex_sync_headers_processed_total",
			Help: "Total number of headers processed.",
		}),
		batchDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "apex_sync_batch_duration_seconds",
			Help:    "Duration of backfill batch processing.",
			Buckets: prometheus.DefBuckets,
		}),
		activeSubs: factory.NewGauge(prometheus.GaugeOpts{
			Name: "apex_subscriptions_active",
			Help: "Number of active event subscriptions.",
		}),
		eventsDropped: factory.NewCounter(prometheus.CounterOpts{
			Name: "apex_subscriptions_events_dropped_total",
			Help: "Total number of subscription events dropped.",
		}),

		backfillStageDurVec: backfillStageDurVec,
		backfillStageErrVec: backfillStageErrVec,
		apiRequestsVec:      apiRequestsVec,
		apiDurationVec:      apiDurationVec,
		storeQueryDurVec:    storeQueryDurVec,
	}

	// Pre-resolve sync state gauges.
	for i, label := range syncStateLabels {
		r.syncStateGauges[i] = syncStateVec.WithLabelValues(label)
	}

	// Pre-resolve known backfill stage labels.
	knownStages := []string{"fetch_height", "store_header", "store_blobs", "observer"}
	r.backfillStageDurOb = make(map[string]prometheus.Observer, len(knownStages))
	r.backfillStageErrCt = make(map[string]prometheus.Counter, len(knownStages))
	for _, s := range knownStages {
		r.backfillStageDurOb[s] = backfillStageDurVec.WithLabelValues(s)
		r.backfillStageErrCt[s] = backfillStageErrVec.WithLabelValues(s)
	}

	// Pre-resolve known store operations.
	knownOps := []string{"PutBlobs", "GetBlobs", "GetBlobByCommitment", "PutHeader", "GetHeader", "GetSyncState"}
	r.storeQueryDurOb = make(map[string]prometheus.Observer, len(knownOps))
	for _, op := range knownOps {
		r.storeQueryDurOb[op] = storeQueryDurVec.WithLabelValues(op)
	}

	// Info metric (set once).
	infoVec := factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "apex_info",
		Help: "Build information.",
	}, []string{"version", "go_version"})
	infoVec.WithLabelValues(version, runtime.Version()).Set(1)

	return r
}

func (r *PromRecorder) SetSyncState(state string) {
	for i, label := range syncStateLabels {
		if label == state {
			r.syncStateGauges[i].Set(1)
		} else {
			r.syncStateGauges[i].Set(0)
		}
	}
}

func (r *PromRecorder) SetLatestHeight(h uint64) {
	r.latestHeight.Set(float64(h))
	r.lastLatest.Store(h)
	if lag := float64(r.lastNetwork.Load()) - float64(h); lag > 0 {
		r.syncLag.Set(lag)
	} else {
		r.syncLag.Set(0)
	}
}

func (r *PromRecorder) SetNetworkHeight(h uint64) {
	r.networkHeight.Set(float64(h))
	r.lastNetwork.Store(h)
	if lag := float64(h) - float64(r.lastLatest.Load()); lag > 0 {
		r.syncLag.Set(lag)
	} else {
		r.syncLag.Set(0)
	}
}

func (r *PromRecorder) IncBlobsProcessed(n int) {
	if n > 0 {
		r.blobsProcessed.Add(float64(n))
	}
}

func (r *PromRecorder) IncHeadersProcessed(n int) {
	if n > 0 {
		r.headersProcessed.Add(float64(n))
	}
}

func (r *PromRecorder) ObserveBatchDuration(d time.Duration) {
	r.batchDuration.Observe(d.Seconds())
}

func (r *PromRecorder) ObserveBackfillStageDuration(stage string, d time.Duration) {
	if ob, ok := r.backfillStageDurOb[stage]; ok {
		ob.Observe(d.Seconds())
		return
	}
	r.backfillStageDurVec.WithLabelValues(stage).Observe(d.Seconds())
}

func (r *PromRecorder) IncBackfillStageErrors(stage string) {
	if ct, ok := r.backfillStageErrCt[stage]; ok {
		ct.Inc()
		return
	}
	r.backfillStageErrVec.WithLabelValues(stage).Inc()
}

func (r *PromRecorder) IncAPIRequest(method, status string) {
	r.apiRequestsVec.WithLabelValues(method, status).Inc()
}

func (r *PromRecorder) ObserveAPIRequestDuration(method string, d time.Duration) {
	r.apiDurationVec.WithLabelValues(method).Observe(d.Seconds())
}

func (r *PromRecorder) ObserveStoreQueryDuration(op string, d time.Duration) {
	if ob, ok := r.storeQueryDurOb[op]; ok {
		ob.Observe(d.Seconds())
		return
	}
	r.storeQueryDurVec.WithLabelValues(op).Observe(d.Seconds())
}

func (r *PromRecorder) SetActiveSubscriptions(n int) {
	r.activeSubs.Set(float64(n))
}

func (r *PromRecorder) IncEventsDropped() {
	r.eventsDropped.Inc()
}
