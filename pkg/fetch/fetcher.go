package fetch

import (
	"context"
	"encoding/json"

	"github.com/evstack/apex/pkg/types"
)

// DataFetcher defines the interface for retrieving data from a Celestia node.
type DataFetcher interface {
	GetHeader(ctx context.Context, height uint64) (*types.Header, error)
	GetBlobs(ctx context.Context, height uint64, namespaces []types.Namespace) ([]types.Blob, error)
	GetNetworkHead(ctx context.Context) (*types.Header, error)

	// SubscribeHeaders returns a channel of new block headers. The channel is
	// closed when ctx is cancelled or Close is called. Callers should drain
	// the channel after cancellation to avoid blocking senders.
	SubscribeHeaders(ctx context.Context) (<-chan *types.Header, error)

	Close() error
}

// ProofForwarder forwards proof-related requests to an upstream Celestia node.
type ProofForwarder interface {
	GetProof(ctx context.Context, height uint64, namespace []byte, commitment []byte) (json.RawMessage, error)
	Included(ctx context.Context, height uint64, namespace []byte, proof json.RawMessage, commitment []byte) (bool, error)
}
