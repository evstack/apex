package store

import (
	"context"
	"errors"

	"github.com/evstack/apex/pkg/types"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// Store defines the persistence interface for indexed data.
type Store interface {
	PutBlobs(ctx context.Context, blobs []types.Blob) error
	GetBlob(ctx context.Context, ns types.Namespace, height uint64, index int) (*types.Blob, error)

	// GetBlobs returns blobs for the given namespace in the height range
	// [startHeight, endHeight] (both inclusive).
	// limit=0 means no limit; offset=0 means no offset.
	GetBlobs(ctx context.Context, ns types.Namespace, startHeight, endHeight uint64, limit, offset int) ([]types.Blob, error)

	PutHeader(ctx context.Context, header *types.Header) error
	GetHeader(ctx context.Context, height uint64) (*types.Header, error)

	PutNamespace(ctx context.Context, ns types.Namespace) error
	GetNamespaces(ctx context.Context) ([]types.Namespace, error)

	// GetSyncState returns the persisted sync status.
	// Returns (nil, ErrNotFound) if no state has been persisted yet.
	GetSyncState(ctx context.Context) (*types.SyncStatus, error)
	SetSyncState(ctx context.Context, status types.SyncStatus) error

	Close() error
}
