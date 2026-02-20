package syncer

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/backfill"
	backfillrpc "github.com/evstack/apex/pkg/backfill/rpc"
	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/metrics"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// Backfiller fetches historical blocks in batched, concurrent chunks.
type Backfiller struct {
	store       store.Store
	fetcher     fetch.DataFetcher
	source      backfill.Source
	batchSize   int
	concurrency int
	observer    HeightObserver
	metrics     metrics.Recorder
	log         zerolog.Logger
}

// Run backfills from fromHeight to toHeight (inclusive).
func (b *Backfiller) Run(ctx context.Context, fromHeight, toHeight uint64) error {
	src := b.source
	if src == nil {
		src = backfillrpc.NewSource(b.fetcher)
	}

	runner := backfill.Runner{
		Store:       b.store,
		Source:      src,
		BatchSize:   b.batchSize,
		Concurrency: b.concurrency,
		Observer: backfill.HeightObserver(func(height uint64, header *types.Header, blobs []types.Blob) {
			if b.observer != nil {
				b.observer(height, header, blobs)
			}
		}),
		Metrics: b.metrics,
		Log:     b.log,
	}

	return runner.Run(ctx, fromHeight, toHeight)
}
