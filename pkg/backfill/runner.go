package backfill

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/metrics"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// Runner fetches historical blocks in batched, concurrent chunks.
type Runner struct {
	Store       store.Store
	Source      Source
	BatchSize   int
	Concurrency int
	Observer    HeightObserver
	Metrics     metrics.Recorder
	Log         zerolog.Logger
}

// Run backfills from fromHeight to toHeight (inclusive).
func (r *Runner) Run(ctx context.Context, fromHeight, toHeight uint64) error {
	if fromHeight > toHeight {
		return nil
	}

	if r.Metrics == nil {
		r.Metrics = metrics.Nop()
	}

	namespaces, err := r.Store.GetNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("get namespaces: %w", err)
	}

	totalHeights := toHeight - fromHeight + 1
	var processedHeights atomic.Uint64
	startTime := time.Now()

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
					r.Log.Info().
						Uint64("target", toHeight).
						Msg("backfill progress: waiting for first batch")
					continue
				}
				elapsed := time.Since(startTime)
				pct := float64(done) / float64(totalHeights) * 100
				rate := float64(done) / elapsed.Seconds()
				remaining := time.Duration(float64(totalHeights-done)/rate) * time.Second
				r.Log.Info().
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

	for batchStart := fromHeight; batchStart <= toHeight; batchStart += uint64(r.BatchSize) {
		batchStartTime := time.Now()
		batchEnd := batchStart + uint64(r.BatchSize) - 1
		if batchEnd > toHeight {
			batchEnd = toHeight
		}

		if err := r.processBatch(ctx, batchStart, batchEnd, namespaces); err != nil {
			close(stopProgress)
			<-progressStopped
			return fmt.Errorf("batch [%d,%d]: %w", batchStart, batchEnd, err)
		}

		processedHeights.Store(batchEnd - fromHeight + 1)

		if err := r.Store.SetSyncState(ctx, types.SyncStatus{
			State:         types.Backfilling,
			LatestHeight:  batchEnd,
			NetworkHeight: toHeight,
		}); err != nil {
			close(stopProgress)
			<-progressStopped
			return fmt.Errorf("checkpoint at height %d: %w", batchEnd, err)
		}

		r.Log.Debug().
			Uint64("batch_start", batchStart).
			Uint64("batch_end", batchEnd).
			Msg("batch complete")
		r.Metrics.ObserveBatchDuration(time.Since(batchStartTime))
	}

	close(stopProgress)
	<-progressStopped
	elapsed := time.Since(startTime)
	rate := float64(totalHeights) / elapsed.Seconds()
	r.Log.Info().
		Uint64("heights", totalHeights).
		Str("elapsed", elapsed.Truncate(time.Second).String()).
		Str("rate", fmt.Sprintf("%.0f heights/s", rate)).
		Msg("backfill finished")

	return nil
}

func (r *Runner) processBatch(ctx context.Context, from, to uint64, namespaces []types.Namespace) error {
	heights := make(chan uint64, to-from+1)
	for h := from; h <= to; h++ {
		heights <- h
	}
	close(heights)

	workers := r.Concurrency
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

				if err := r.processHeight(ctx, height, namespaces); err != nil {
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

func (r *Runner) processHeight(ctx context.Context, height uint64, namespaces []types.Namespace) error {
	stageStart := time.Now()
	hdr, blobs, err := r.Source.FetchHeight(ctx, height, namespaces)
	r.Metrics.ObserveBackfillStageDuration("fetch_height", time.Since(stageStart))
	if err != nil {
		r.Metrics.IncBackfillStageErrors("fetch_height")
		return fmt.Errorf("fetch height: %w", err)
	}

	stageStart = time.Now()
	if err := r.Store.PutHeader(ctx, hdr); err != nil {
		r.Metrics.ObserveBackfillStageDuration("store_header", time.Since(stageStart))
		r.Metrics.IncBackfillStageErrors("store_header")
		return fmt.Errorf("put header: %w", err)
	}
	r.Metrics.ObserveBackfillStageDuration("store_header", time.Since(stageStart))

	if len(blobs) > 0 {
		stageStart = time.Now()
		if err := r.Store.PutBlobs(ctx, blobs); err != nil {
			r.Metrics.ObserveBackfillStageDuration("store_blobs", time.Since(stageStart))
			r.Metrics.IncBackfillStageErrors("store_blobs")
			return fmt.Errorf("put blobs: %w", err)
		}
		r.Metrics.ObserveBackfillStageDuration("store_blobs", time.Since(stageStart))
	}

	if r.Observer != nil {
		stageStart = time.Now()
		r.Observer(height, hdr, blobs)
		r.Metrics.ObserveBackfillStageDuration("observer", time.Since(stageStart))
	}

	return nil
}
