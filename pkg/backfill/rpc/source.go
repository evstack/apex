package rpc

import (
	"context"
	"fmt"

	"github.com/evstack/apex/pkg/backfill"
	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/types"
)

// heightDataFetcher is implemented by fetchers that can return header and blobs
// from one upstream request.
type heightDataFetcher interface {
	GetHeightData(ctx context.Context, height uint64, namespaces []types.Namespace) (*types.Header, []types.Blob, error)
}

// Source is an RPC-backed backfill data source.
type Source struct {
	fetcher fetch.DataFetcher
}

var _ backfill.Source = (*Source)(nil)

// NewSource wraps a DataFetcher as a backfill source.
func NewSource(fetcher fetch.DataFetcher) *Source {
	return &Source{fetcher: fetcher}
}

// FetchHeight fetches header and blobs for a single height.
func (s *Source) FetchHeight(ctx context.Context, height uint64, namespaces []types.Namespace) (*types.Header, []types.Blob, error) {
	if combined, ok := s.fetcher.(heightDataFetcher); ok {
		return combined.GetHeightData(ctx, height, namespaces)
	}

	hdr, err := s.fetcher.GetHeader(ctx, height)
	if err != nil {
		return nil, nil, fmt.Errorf("get header: %w", err)
	}

	var blobs []types.Blob
	if len(namespaces) > 0 {
		blobs, err = s.fetcher.GetBlobs(ctx, height, namespaces)
		if err != nil {
			return nil, nil, fmt.Errorf("get blobs: %w", err)
		}
	}
	return hdr, blobs, nil
}

// Close closes the wrapped fetcher.
func (s *Source) Close() error {
	return s.fetcher.Close()
}
