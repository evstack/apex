package backfill

import (
	"context"

	"github.com/evstack/apex/pkg/types"
)

// Source fetches header/blob data for historical heights.
type Source interface {
	FetchHeight(ctx context.Context, height uint64, namespaces []types.Namespace) (*types.Header, []types.Blob, error)
	Close() error
}

// HeightObserver is called after a height is fetched and persisted.
type HeightObserver func(height uint64, header *types.Header, blobs []types.Blob)
