package syncer

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// SubscriptionManager processes new headers from a live subscription.
type SubscriptionManager struct {
	store    store.Store
	fetcher  fetch.DataFetcher
	observer HeightObserver
	log      zerolog.Logger
}

// Run subscribes to new headers and processes them sequentially.
// Returns ErrGapDetected if a height discontinuity is found.
// Returns nil when ctx is cancelled.
func (sm *SubscriptionManager) Run(ctx context.Context) error {
	ch, err := sm.fetcher.SubscribeHeaders(ctx)
	if err != nil {
		return fmt.Errorf("subscribe headers: %w", err)
	}

	namespaces, err := sm.store.GetNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("get namespaces: %w", err)
	}

	// Determine the last processed height and network height from the store.
	var lastHeight, networkHeight uint64
	ss, err := sm.store.GetSyncState(ctx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("get sync state: %w", err)
	}
	if ss != nil {
		lastHeight = ss.LatestHeight
		networkHeight = ss.NetworkHeight
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case hdr, ok := <-ch:
			if !ok {
				// Channel closed (disconnect or ctx cancelled).
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("header subscription closed unexpectedly")
			}

			// Check contiguity.
			if lastHeight > 0 && hdr.Height != lastHeight+1 {
				sm.log.Warn().
					Uint64("expected", lastHeight+1).
					Uint64("got", hdr.Height).
					Msg("gap detected")
				return ErrGapDetected
			}

			if err := sm.processHeader(ctx, hdr, namespaces, networkHeight); err != nil {
				return fmt.Errorf("process height %d: %w", hdr.Height, err)
			}

			lastHeight = hdr.Height
		}
	}
}

func (sm *SubscriptionManager) processHeader(ctx context.Context, hdr *types.Header, namespaces []types.Namespace, networkHeight uint64) error {
	if err := sm.store.PutHeader(ctx, hdr); err != nil {
		return fmt.Errorf("put header: %w", err)
	}

	var blobs []types.Blob
	if len(namespaces) > 0 {
		var err error
		blobs, err = sm.fetcher.GetBlobs(ctx, hdr.Height, namespaces)
		if err != nil {
			return fmt.Errorf("get blobs: %w", err)
		}
		if len(blobs) > 0 {
			if err := sm.store.PutBlobs(ctx, blobs); err != nil {
				return fmt.Errorf("put blobs: %w", err)
			}
		}
	}

	if err := sm.store.SetSyncState(ctx, types.SyncStatus{
		State:         types.Streaming,
		LatestHeight:  hdr.Height,
		NetworkHeight: networkHeight,
	}); err != nil {
		return fmt.Errorf("set sync state: %w", err)
	}

	if sm.observer != nil {
		sm.observer(hdr.Height, hdr, blobs)
	}

	sm.log.Debug().Uint64("height", hdr.Height).Msg("processed header")
	return nil
}
