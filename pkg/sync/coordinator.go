package syncer

import (
	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// Coordinator manages the sync lifecycle between a data fetcher and a store.
type Coordinator struct {
	store       store.Store
	fetcher     fetch.DataFetcher
	state       types.SyncState
	batchSize   int
	concurrency int
	startHeight uint64
}

// Option configures a Coordinator.
type Option func(*Coordinator)

// WithBatchSize sets the number of headers fetched per batch.
// Values <= 0 are ignored.
func WithBatchSize(n int) Option {
	return func(c *Coordinator) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithConcurrency sets the number of concurrent fetch workers.
// Values <= 0 are ignored.
func WithConcurrency(n int) Option {
	return func(c *Coordinator) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithStartHeight sets the height at which syncing begins.
func WithStartHeight(h uint64) Option {
	return func(c *Coordinator) { c.startHeight = h }
}

// New creates a Coordinator with the given store, fetcher, and options.
func New(s store.Store, f fetch.DataFetcher, opts ...Option) *Coordinator {
	coord := &Coordinator{
		store:       s,
		fetcher:     f,
		state:       types.Initializing,
		batchSize:   64,
		concurrency: 4,
	}
	for _, opt := range opts {
		opt(coord)
	}
	return coord
}
