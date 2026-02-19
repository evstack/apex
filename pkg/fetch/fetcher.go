package fetch

import (
	"context"

	"github.com/evstack/apex/pkg/types"
)

// DataFetcher defines the interface for retrieving data from a Celestia node.
type DataFetcher interface {
	GetHeader(ctx context.Context, height uint64) (*types.Header, error)
	GetBlobs(ctx context.Context, height uint64, namespaces []types.Namespace) ([]types.Blob, error)
	GetNetworkHead(ctx context.Context) (*types.Header, error)
	SubscribeHeaders(ctx context.Context) (<-chan *types.Header, error)
	Close() error
}
