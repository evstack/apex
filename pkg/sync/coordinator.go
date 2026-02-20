package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/evstack/apex/pkg/backfill"
	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/metrics"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// ErrGapDetected is returned by SubscriptionManager when a height gap is found.
var ErrGapDetected = errors.New("gap detected")

// HeightObserver is called after a height is successfully processed and stored.
type HeightObserver func(height uint64, header *types.Header, blobs []types.Blob)

// Coordinator manages the sync lifecycle between a data fetcher and a store.
type Coordinator struct {
	store         store.Store
	fetcher       fetch.DataFetcher
	state         types.SyncState
	latestHeight  uint64
	networkHeight uint64
	stateMu       sync.RWMutex
	batchSize     int
	concurrency   int
	startHeight   uint64
	observer      HeightObserver
	backfillSrc   backfill.Source
	metrics       metrics.Recorder
	log           zerolog.Logger
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

// WithLogger sets the logger for the coordinator and its sub-components.
func WithLogger(log zerolog.Logger) Option {
	return func(c *Coordinator) { c.log = log }
}

// WithObserver sets a callback invoked after each height is successfully stored.
func WithObserver(obs HeightObserver) Option {
	return func(c *Coordinator) { c.observer = obs }
}

// WithBackfillSource sets the source used for historical backfill.
func WithBackfillSource(src backfill.Source) Option {
	return func(c *Coordinator) { c.backfillSrc = src }
}

// WithMetrics sets the metrics recorder for the coordinator.
func WithMetrics(m metrics.Recorder) Option {
	return func(c *Coordinator) { c.metrics = m }
}

// New creates a Coordinator with the given store, fetcher, and options.
func New(s store.Store, f fetch.DataFetcher, opts ...Option) *Coordinator {
	coord := &Coordinator{
		store:       s,
		fetcher:     f,
		state:       types.Initializing,
		batchSize:   64,
		concurrency: 4,
		metrics:     metrics.Nop(),
		log:         zerolog.Nop(),
	}
	for _, opt := range opts {
		opt(coord)
	}
	return coord
}

// Status returns the current sync status (concurrent-safe).
func (c *Coordinator) Status() types.SyncStatus {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return types.SyncStatus{
		State:         c.state,
		LatestHeight:  c.latestHeight,
		NetworkHeight: c.networkHeight,
	}
}

func (c *Coordinator) setState(s types.SyncState) {
	c.stateMu.Lock()
	c.state = s
	c.stateMu.Unlock()
	c.metrics.SetSyncState(s.String())
}

func (c *Coordinator) setLatestHeight(h uint64) {
	c.stateMu.Lock()
	c.latestHeight = h
	c.stateMu.Unlock()
	c.metrics.SetLatestHeight(h)
}

func (c *Coordinator) setNetworkHeight(h uint64) {
	c.stateMu.Lock()
	c.networkHeight = h
	c.stateMu.Unlock()
	c.metrics.SetNetworkHeight(h)
}

// Run executes the sync lifecycle: initialize -> backfill -> stream.
// It blocks until ctx is cancelled or a fatal error occurs.
// On ErrGapDetected during streaming, it re-backfills and re-enters streaming.
func (c *Coordinator) Run(ctx context.Context) error {
	for {
		fromHeight, networkHeight, err := c.initialize(ctx)
		if err != nil {
			return fmt.Errorf("initialize: %w", err)
		}
		c.setNetworkHeight(networkHeight)
		if fromHeight > 1 {
			c.setLatestHeight(fromHeight - 1)
		}

		if fromHeight <= networkHeight {
			c.setState(types.Backfilling)
			c.log.Info().
				Uint64("from", fromHeight).
				Uint64("to", networkHeight).
				Msg("starting backfill")

			// Wrap the user observer to also track latest height and metrics.
			wrappedObserver := func(height uint64, header *types.Header, blobs []types.Blob) {
				c.setLatestHeight(height)
				c.metrics.IncHeadersProcessed(1)
				c.metrics.IncBlobsProcessed(len(blobs))
				if c.observer != nil {
					c.observer(height, header, blobs)
				}
			}

			bf := &Backfiller{
				store:       c.store,
				fetcher:     c.fetcher,
				source:      c.backfillSrc,
				batchSize:   c.batchSize,
				concurrency: c.concurrency,
				observer:    wrappedObserver,
				metrics:     c.metrics,
				log:         c.log.With().Str("component", "backfiller").Logger(),
			}
			if err := bf.Run(ctx, fromHeight, networkHeight); err != nil {
				return fmt.Errorf("backfill: %w", err)
			}
			c.setLatestHeight(networkHeight)
			c.log.Info().Uint64("height", networkHeight).Msg("backfill complete")
		}

		c.setState(types.Streaming)
		c.log.Info().Msg("entering streaming mode")

		sm := &SubscriptionManager{
			store:   c.store,
			fetcher: c.fetcher,
			observer: func(height uint64, header *types.Header, blobs []types.Blob) {
				c.setLatestHeight(height)
				c.setNetworkHeight(height)
				c.metrics.IncHeadersProcessed(1)
				c.metrics.IncBlobsProcessed(len(blobs))
				if c.observer != nil {
					c.observer(height, header, blobs)
				}
			},
			log: c.log.With().Str("component", "subscription").Logger(),
		}
		err = sm.Run(ctx)
		if errors.Is(err, ErrGapDetected) {
			c.log.Warn().Msg("gap detected, re-entering backfill")
			continue
		}
		if err != nil {
			return fmt.Errorf("streaming: %w", err)
		}
		// ctx was cancelled; clean exit.
		return nil
	}
}

// initialize determines the starting height and network head.
func (c *Coordinator) initialize(ctx context.Context) (fromHeight, networkHeight uint64, err error) {
	c.setState(types.Initializing)

	// Try to resume from persisted state.
	ss, err := c.store.GetSyncState(ctx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return 0, 0, fmt.Errorf("get sync state: %w", err)
	}
	if ss != nil && ss.LatestHeight > 0 {
		fromHeight = ss.LatestHeight + 1
		c.log.Info().Uint64("resumed_from", fromHeight).Msg("resuming from checkpoint")
	} else {
		fromHeight = c.startHeight
		if fromHeight == 0 {
			fromHeight = 1
		}
		c.log.Info().Uint64("start_height", fromHeight).Msg("starting fresh")
	}

	head, err := c.fetcher.GetNetworkHead(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get network head: %w", err)
	}
	networkHeight = head.Height
	c.log.Info().Uint64("network_height", networkHeight).Msg("network head")

	return fromHeight, networkHeight, nil
}
