package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cometpb "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/base/tendermint/v1beta1"
	"github.com/evstack/apex/pkg/types"
)

// CelestiaAppFetcher implements DataFetcher using Cosmos SDK gRPC endpoints
// exposed by celestia-app (consensus node). This enables indexing without
// a Celestia DA node.
type CelestiaAppFetcher struct {
	conn      *grpc.ClientConn
	client    cometpb.ServiceClient
	log       zerolog.Logger
	mu        sync.Mutex
	closed    bool
	cancelSub context.CancelFunc
}

// bearerCreds implements grpc.PerRPCCredentials for bearer token auth.
type bearerCreds struct {
	token string
}

func (b bearerCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerCreds) RequireTransportSecurity() bool { return false }

// NewCelestiaAppFetcher creates a fetcher that reads from celestia-app's
// Cosmos SDK gRPC endpoint. No connection is established at construction time.
func NewCelestiaAppFetcher(grpcAddr, authToken string, log zerolog.Logger) (*CelestiaAppFetcher, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if authToken != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearerCreds{token: authToken}))
	}

	conn, err := grpc.NewClient(grpcAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}

	return &CelestiaAppFetcher{
		conn:   conn,
		client: cometpb.NewServiceClient(conn),
		log:    log.With().Str("component", "celestia-app-fetcher").Logger(),
	}, nil
}

// newCelestiaAppFetcherFromClient creates a fetcher from an existing gRPC
// client connection. Used in tests with bufconn.
func newCelestiaAppFetcherFromClient(conn *grpc.ClientConn, log zerolog.Logger) *CelestiaAppFetcher {
	return &CelestiaAppFetcher{
		conn:   conn,
		client: cometpb.NewServiceClient(conn),
		log:    log.With().Str("component", "celestia-app-fetcher").Logger(),
	}
}

// GetHeader fetches a block at the given height and returns a Header.
func (f *CelestiaAppFetcher) GetHeader(ctx context.Context, height uint64) (*types.Header, error) {
	hdr, _, err := f.GetHeightData(ctx, height, nil)
	if err != nil {
		return nil, err
	}
	return hdr, nil
}

// GetBlobs fetches a block and extracts blobs matching the given namespaces.
func (f *CelestiaAppFetcher) GetBlobs(ctx context.Context, height uint64, namespaces []types.Namespace) ([]types.Blob, error) {
	_, blobs, err := f.GetHeightData(ctx, height, namespaces)
	if err != nil {
		return nil, err
	}
	return blobs, nil
}

// GetHeightData fetches header and namespace-filtered blobs for a single height.
func (f *CelestiaAppFetcher) GetHeightData(ctx context.Context, height uint64, namespaces []types.Namespace) (*types.Header, []types.Blob, error) {
	resp, err := f.client.GetBlockByHeight(ctx, &cometpb.GetBlockByHeightRequest{
		Height: int64(height),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("get block at height %d: %w", height, err)
	}

	hdr, err := mapBlockResponse(resp.BlockId, resp.Block)
	if err != nil {
		return nil, nil, err
	}

	if len(namespaces) == 0 {
		return hdr, nil, nil
	}
	if resp.Block == nil || resp.Block.Data == nil {
		return hdr, nil, nil
	}

	txs := resp.Block.Data.Txs
	blobs, err := extractBlobsFromBlock(txs, namespaces, height)
	if err != nil {
		return nil, nil, fmt.Errorf("extract blobs at height %d: %w", height, err)
	}
	if len(blobs) == 0 {
		return hdr, nil, nil
	}
	return hdr, blobs, nil
}

// GetNetworkHead returns the header at the latest block height.
func (f *CelestiaAppFetcher) GetNetworkHead(ctx context.Context) (*types.Header, error) {
	resp, err := f.client.GetLatestBlock(ctx, &cometpb.GetLatestBlockRequest{})
	if err != nil {
		return nil, fmt.Errorf("get latest block: %w", err)
	}
	return mapBlockResponse(resp.BlockId, resp.Block)
}

// SubscribeHeaders polls GetLatestBlock at 1s intervals and emits new headers
// when the height advances. The coordinator already handles gaps, so polling
// is sufficient.
func (f *CelestiaAppFetcher) SubscribeHeaders(ctx context.Context) (<-chan *types.Header, error) {
	subCtx, cancel := context.WithCancel(ctx)

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("fetcher is closed")
	}
	if f.cancelSub != nil {
		f.cancelSub()
	}
	f.cancelSub = cancel
	f.mu.Unlock()

	out := make(chan *types.Header, 64)
	go f.pollLoop(subCtx, out)

	return out, nil
}

func (f *CelestiaAppFetcher) pollLoop(ctx context.Context, out chan<- *types.Header) {
	defer close(out)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastHeight uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hdr, err := f.GetNetworkHead(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				f.log.Warn().Err(err).Msg("poll latest block failed")
				continue
			}
			if hdr.Height <= lastHeight {
				continue
			}
			lastHeight = hdr.Height
			select {
			case out <- hdr:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Close cancels any active subscription and closes the gRPC connection.
func (f *CelestiaAppFetcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.cancelSub != nil {
		f.cancelSub()
	}
	return f.conn.Close()
}

// mapBlockResponse converts a gRPC block response into a types.Header.
func mapBlockResponse(blockID *cometpb.BlockID, block *cometpb.Block) (*types.Header, error) {
	if block == nil || block.Header == nil {
		return nil, fmt.Errorf("nil block or header in response")
	}
	if blockID == nil {
		return nil, fmt.Errorf("nil block_id in response")
	}

	hdr := block.Header
	t := hdr.Time.AsTime()

	raw, err := json.Marshal(block)
	if err != nil {
		return nil, fmt.Errorf("marshal raw header: %w", err)
	}

	return &types.Header{
		Height:    uint64(hdr.Height),
		Hash:      blockID.Hash,
		DataHash:  hdr.DataHash,
		Time:      t,
		RawHeader: raw,
	}, nil
}
