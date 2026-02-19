package syncer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/metrics"
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
	metrics     metrics.Recorder
	log         zerolog.Logger
}

// Run backfills from fromHeight to toHeight (inclusive).
func (b *Backfiller) Run(ctx context.Context, fromHeight, toHeight uint64) error {
	if b.metrics == nil {
		b.metrics = metrics.Nop()
	}

	namespaces, err := b.store.GetNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("get namespaces: %w", err)
	}

	totalHeights := toHeight - fromHeight + 1
	var processedHeights atomic.Uint64
	startTime := time.Now()

	// Periodic progress reporting.
	stopProgress := make(chan struct{})
	progressStopped := make(chan struct{})
	go func() {
		defer close(progressStopped)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				done := processedHeights.Load()
				if done == 0 {
					b.log.Info().
						Uint64("target", toHeight).
						Msg("backfill progress: waiting for first batch")
					continue
				}
				elapsed := time.Since(startTime)
				pct := float64(done) / float64(totalHeights) * 100
				rate := float64(done) / elapsed.Seconds()
				remaining := time.Duration(float64(totalHeights-done)/rate) * time.Second
				b.log.Info().
					Uint64("height", fromHeight+done).
					Uint64("target", toHeight).
					Str("progress", fmt.Sprintf("%.1f%%", pct)).
					Str("rate", fmt.Sprintf("%.0f heights/s", rate)).
					Str("eta", remaining.Truncate(time.Second).String()).
					Msg("backfill progress")
			case <-stopProgress:
				return
			}
		}
	}()

	for batchStart := fromHeight; batchStart <= toHeight; batchStart += uint64(b.batchSize) {
		batchStartTime := time.Now()
		batchEnd := batchStart + uint64(b.batchSize) - 1
		if batchEnd > toHeight {
			batchEnd = toHeight
		}

		if err := b.processBatch(ctx, batchStart, batchEnd, namespaces); err != nil {
			close(stopProgress)
			<-progressStopped
			return err
		}

		processedHeights.Store(batchEnd - fromHeight + 1)

		// Checkpoint after each batch.
		if err := b.store.SetSyncState(ctx, types.SyncStatus{
			State:         types.Backfilling,
			LatestHeight:  batchEnd,
			NetworkHeight: toHeight,
		}); err != nil {
			close(stopProgress)
			<-progressStopped
			return fmt.Errorf("checkpoint at height %d: %w", batchEnd, err)
		}

		b.log.Debug().
			Uint64("batch_start", batchStart).
			Uint64("batch_end", batchEnd).
			Msg("batch complete")
		b.metrics.ObserveBatchDuration(time.Since(batchStartTime))
	}

	// Stop progress goroutine and log final summary.
	close(stopProgress)
	<-progressStopped
	elapsed := time.Since(startTime)
	rate := float64(totalHeights) / elapsed.Seconds()
	b.log.Info().
		Uint64("heights", totalHeights).
		Str("elapsed", elapsed.Truncate(time.Second).String()).
		Str("rate", fmt.Sprintf("%.0f heights/s", rate)).
		Msg("backfill finished")

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
	stageStart := time.Now()
	hdr, err := b.fetcher.GetHeader(ctx, height)
	b.metrics.ObserveBackfillStageDuration("fetch_header", time.Since(stageStart))
	if err != nil {
		b.metrics.IncBackfillStageErrors("fetch_header")
		return fmt.Errorf("get header: %w", err)
	}

	stageStart = time.Now()
	if err := b.store.PutHeader(ctx, hdr); err != nil {
		b.metrics.ObserveBackfillStageDuration("store_header", time.Since(stageStart))
		b.metrics.IncBackfillStageErrors("store_header")
		return fmt.Errorf("put header: %w", err)
	}
	b.metrics.ObserveBackfillStageDuration("store_header", time.Since(stageStart))

	var blobs []types.Blob
	if len(namespaces) > 0 {
		stageStart = time.Now()
		blobs, err = b.fetcher.GetBlobs(ctx, height, namespaces)
		b.metrics.ObserveBackfillStageDuration("fetch_blobs", time.Since(stageStart))
		if err != nil {
			b.metrics.IncBackfillStageErrors("fetch_blobs")
			return fmt.Errorf("get blobs: %w", err)
		}
		if len(blobs) > 0 {
			stageStart = time.Now()
			if err := b.store.PutBlobs(ctx, blobs); err != nil {
				b.metrics.ObserveBackfillStageDuration("store_blobs", time.Since(stageStart))
				b.metrics.IncBackfillStageErrors("store_blobs")
				return fmt.Errorf("put blobs: %w", err)
			}
			b.metrics.ObserveBackfillStageDuration("store_blobs", time.Since(stageStart))
		}
	}

	if b.observer != nil {
		stageStart = time.Now()
		b.observer(height, hdr, blobs)
		b.metrics.ObserveBackfillStageDuration("observer", time.Since(stageStart))
	}

	return nil
}
