package metrics

import (
	"runtime"
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

func (nopRecorder) SetSyncState(string)                             {}
func (nopRecorder) SetLatestHeight(uint64)                          {}
func (nopRecorder) SetNetworkHeight(uint64)                         {}
func (nopRecorder) IncBlobsProcessed(int)                           {}
func (nopRecorder) IncHeadersProcessed(int)                         {}
func (nopRecorder) ObserveBatchDuration(time.Duration)              {}
func (nopRecorder) IncAPIRequest(string, string)                    {}
func (nopRecorder) ObserveAPIRequestDuration(string, time.Duration) {}
func (nopRecorder) ObserveStoreQueryDuration(string, time.Duration) {}
func (nopRecorder) SetActiveSubscriptions(int)                      {}
func (nopRecorder) IncEventsDropped()                               {}

// PromRecorder implements Recorder using Prometheus metrics.
type PromRecorder struct {
	syncState        *prometheus.GaugeVec
	latestHeight     prometheus.Gauge
	networkHeight    prometheus.Gauge
	syncLag          prometheus.Gauge
	blobsProcessed   prometheus.Counter
	headersProcessed prometheus.Counter
	batchDuration    prometheus.Histogram
	apiRequests      *prometheus.CounterVec
	apiDuration      *prometheus.HistogramVec
	storeQueryDur    *prometheus.HistogramVec
	activeSubs       prometheus.Gauge
	eventsDropped    prometheus.Counter
	info             *prometheus.GaugeVec

	// cached for lag calculation
	lastLatest  uint64
	lastNetwork uint64
}

// NewPromRecorder creates a PromRecorder and registers metrics with the
// provided Prometheus registerer. Pass nil to use the default registerer.
func NewPromRecorder(reg prometheus.Registerer, version string) *PromRecorder {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	r := &PromRecorder{
		syncState: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "apex_sync_state",
			Help: "Current sync state (1 = active for the labeled state).",
		}, []string{"state"}),

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

		apiRequests: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "apex_api_requests_total",
			Help: "Total API requests by method and status.",
		}, []string{"method", "status"}),

		apiDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "apex_api_request_duration_seconds",
			Help:    "API request duration by method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),

		storeQueryDur: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "apex_store_query_duration_seconds",
			Help:    "Store query duration by operation.",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation"}),

		activeSubs: factory.NewGauge(prometheus.GaugeOpts{
			Name: "apex_subscriptions_active",
			Help: "Number of active event subscriptions.",
		}),

		eventsDropped: factory.NewCounter(prometheus.CounterOpts{
			Name: "apex_subscriptions_events_dropped_total",
			Help: "Total number of subscription events dropped.",
		}),

		info: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "apex_info",
			Help: "Build information.",
		}, []string{"version", "go_version"}),
	}

	r.info.WithLabelValues(version, runtime.Version()).Set(1)

	return r
}

func (r *PromRecorder) SetSyncState(state string) {
	for _, s := range []string{"initializing", "backfilling", "streaming"} {
		if s == state {
			r.syncState.WithLabelValues(s).Set(1)
		} else {
			r.syncState.WithLabelValues(s).Set(0)
		}
	}
}

func (r *PromRecorder) SetLatestHeight(h uint64) {
	r.latestHeight.Set(float64(h))
	r.lastLatest = h
	r.syncLag.Set(float64(r.lastNetwork) - float64(h))
}

func (r *PromRecorder) SetNetworkHeight(h uint64) {
	r.networkHeight.Set(float64(h))
	r.lastNetwork = h
	r.syncLag.Set(float64(h) - float64(r.lastLatest))
}

func (r *PromRecorder) IncBlobsProcessed(n int) {
	r.blobsProcessed.Add(float64(n))
}

func (r *PromRecorder) IncHeadersProcessed(n int) {
	r.headersProcessed.Add(float64(n))
}

func (r *PromRecorder) ObserveBatchDuration(d time.Duration) {
	r.batchDuration.Observe(d.Seconds())
}

func (r *PromRecorder) IncAPIRequest(method, status string) {
	r.apiRequests.WithLabelValues(method, status).Inc()
}

func (r *PromRecorder) ObserveAPIRequestDuration(method string, d time.Duration) {
	r.apiDuration.WithLabelValues(method).Observe(d.Seconds())
}

func (r *PromRecorder) ObserveStoreQueryDuration(op string, d time.Duration) {
	r.storeQueryDur.WithLabelValues(op).Observe(d.Seconds())
}

func (r *PromRecorder) SetActiveSubscriptions(n int) {
	r.activeSubs.Set(float64(n))
}

func (r *PromRecorder) IncEventsDropped() {
	r.eventsDropped.Inc()
}
