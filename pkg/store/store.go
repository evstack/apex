package store

import (
	"context"

	"github.com/evstack/apex/pkg/types"
)

// Store defines the persistence interface for indexed data.
type Store interface {
	PutBlobs(ctx context.Context, blobs []types.Blob) error
	GetBlob(ctx context.Context, ns types.Namespace, height uint64, index int) (*types.Blob, error)
	GetBlobs(ctx context.Context, ns types.Namespace, startHeight, endHeight uint64) ([]types.Blob, error)

	PutHeader(ctx context.Context, header *types.Header) error
	GetHeader(ctx context.Context, height uint64) (*types.Header, error)

	PutNamespace(ctx context.Context, ns types.Namespace) error
	GetNamespaces(ctx context.Context) ([]types.Namespace, error)

	GetSyncState(ctx context.Context) (types.SyncStatus, error)
	SetSyncState(ctx context.Context, status types.SyncStatus) error

	Close() error
}
