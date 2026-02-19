package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// ErrGapDetected is returned by SubscriptionManager when a height gap is found.
var ErrGapDetected = errors.New("gap detected")

// Coordinator manages the sync lifecycle between a data fetcher and a store.
type Coordinator struct {
	store       store.Store
	fetcher     fetch.DataFetcher
	state       types.SyncState
	stateMu     sync.RWMutex
	batchSize   int
	concurrency int
	startHeight uint64
	log         zerolog.Logger
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

// New creates a Coordinator with the given store, fetcher, and options.
func New(s store.Store, f fetch.DataFetcher, opts ...Option) *Coordinator {
	coord := &Coordinator{
		store:       s,
		fetcher:     f,
		state:       types.Initializing,
		batchSize:   64,
		concurrency: 4,
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
		State: c.state,
	}
}

func (c *Coordinator) setState(s types.SyncState) {
	c.stateMu.Lock()
	c.state = s
	c.stateMu.Unlock()
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

		if fromHeight <= networkHeight {
			c.setState(types.Backfilling)
			c.log.Info().
				Uint64("from", fromHeight).
				Uint64("to", networkHeight).
				Msg("starting backfill")

			bf := &Backfiller{
				store:       c.store,
				fetcher:     c.fetcher,
				batchSize:   c.batchSize,
				concurrency: c.concurrency,
				log:         c.log.With().Str("component", "backfiller").Logger(),
			}
			if err := bf.Run(ctx, fromHeight, networkHeight); err != nil {
				return fmt.Errorf("backfill: %w", err)
			}
			c.log.Info().Uint64("height", networkHeight).Msg("backfill complete")
		}

		c.setState(types.Streaming)
		c.log.Info().Msg("entering streaming mode")

		sm := &SubscriptionManager{
			store:   c.store,
			fetcher: c.fetcher,
			log:     c.log.With().Str("component", "subscription").Logger(),
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
