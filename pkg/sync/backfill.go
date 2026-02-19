package syncer

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// Backfiller fetches historical blocks in batched, concurrent chunks.
type Backfiller struct {
	store       store.Store
	fetcher     fetch.DataFetcher
	batchSize   int
	concurrency int
	observer    HeightObserver
	log         zerolog.Logger
}

// Run backfills from fromHeight to toHeight (inclusive).
func (b *Backfiller) Run(ctx context.Context, fromHeight, toHeight uint64) error {
	namespaces, err := b.store.GetNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("get namespaces: %w", err)
	}

	for batchStart := fromHeight; batchStart <= toHeight; batchStart += uint64(b.batchSize) {
		batchEnd := batchStart + uint64(b.batchSize) - 1
		if batchEnd > toHeight {
			batchEnd = toHeight
		}

		if err := b.processBatch(ctx, batchStart, batchEnd, namespaces); err != nil {
			return err
		}

		// Checkpoint after each batch.
		if err := b.store.SetSyncState(ctx, types.SyncStatus{
			State:         types.Backfilling,
			LatestHeight:  batchEnd,
			NetworkHeight: toHeight,
		}); err != nil {
			return fmt.Errorf("checkpoint at height %d: %w", batchEnd, err)
		}

		b.log.Debug().
			Uint64("batch_start", batchStart).
			Uint64("batch_end", batchEnd).
			Msg("batch complete")
	}

	return nil
}

func (b *Backfiller) processBatch(ctx context.Context, from, to uint64, namespaces []types.Namespace) error {
	heights := make(chan uint64, to-from+1)
	for h := from; h <= to; h++ {
		heights <- h
	}
	close(heights)

	workers := b.concurrency
	if int(to-from+1) < workers {
		workers = int(to - from + 1)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for height := range heights {
				// Check for prior error or context cancellation.
				mu.Lock()
				failed := firstErr != nil
				mu.Unlock()
				if failed {
					return
				}

				if err := ctx.Err(); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}

				if err := b.processHeight(ctx, height, namespaces); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("height %d: %w", height, err)
					}
					mu.Unlock()
					return
				}
			}
		}()
	}

	wg.Wait()
	return firstErr
}

func (b *Backfiller) processHeight(ctx context.Context, height uint64, namespaces []types.Namespace) error {
	hdr, err := b.fetcher.GetHeader(ctx, height)
	if err != nil {
		return fmt.Errorf("get header: %w", err)
	}
	if err := b.store.PutHeader(ctx, hdr); err != nil {
		return fmt.Errorf("put header: %w", err)
	}

	var blobs []types.Blob
	if len(namespaces) > 0 {
		blobs, err = b.fetcher.GetBlobs(ctx, height, namespaces)
		if err != nil {
			return fmt.Errorf("get blobs: %w", err)
		}
		if len(blobs) > 0 {
			if err := b.store.PutBlobs(ctx, blobs); err != nil {
				return fmt.Errorf("put blobs: %w", err)
			}
		}
	}

	if b.observer != nil {
		b.observer(height, hdr, blobs)
	}

	return nil
}
