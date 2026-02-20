package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

type mockFetcher struct {
	hdrCalls      int
	blobCalls     int
	combinedCalls int

	hdr   *types.Header
	blobs []types.Blob
	err   error
}

func (m *mockFetcher) GetHeader(_ context.Context, _ uint64) (*types.Header, error) {
	m.hdrCalls++
	if m.err != nil {
		return nil, m.err
	}
	return m.hdr, nil
}

func (m *mockFetcher) GetBlobs(_ context.Context, _ uint64, _ []types.Namespace) ([]types.Blob, error) {
	m.blobCalls++
	if m.err != nil {
		return nil, m.err
	}
	return m.blobs, nil
}

func (m *mockFetcher) GetNetworkHead(_ context.Context) (*types.Header, error) {
	return &types.Header{Height: 1}, nil
}

func (m *mockFetcher) SubscribeHeaders(_ context.Context) (<-chan *types.Header, error) {
	ch := make(chan *types.Header)
	close(ch)
	return ch, nil
}

func (m *mockFetcher) Close() error { return nil }

func (m *mockFetcher) GetHeightData(_ context.Context, _ uint64, _ []types.Namespace) (*types.Header, []types.Blob, error) {
	m.combinedCalls++
	if m.err != nil {
		return nil, nil, m.err
	}
	return m.hdr, m.blobs, nil
}

func TestSourceFetchHeight_UsesCombinedFetcherWhenAvailable(t *testing.T) {
	f := &mockFetcher{
		hdr:   &types.Header{Height: 10},
		blobs: []types.Blob{{Height: 10}},
	}
	s := NewSource(f)

	hdr, blobs, err := s.FetchHeight(context.Background(), 10, []types.Namespace{{}})
	if err != nil {
		t.Fatalf("FetchHeight: %v", err)
	}
	if hdr == nil || hdr.Height != 10 {
		t.Fatalf("unexpected header: %#v", hdr)
	}
	if len(blobs) != 1 {
		t.Fatalf("expected 1 blob, got %d", len(blobs))
	}
	if f.combinedCalls != 1 {
		t.Fatalf("combined calls = %d, want 1", f.combinedCalls)
	}
	if f.hdrCalls != 0 || f.blobCalls != 0 {
		t.Fatalf("fallback methods were called: hdr=%d blobs=%d", f.hdrCalls, f.blobCalls)
	}
}

type fallbackFetcher struct {
	hdr   *types.Header
	blobs []types.Blob
	err   error
}

func (f *fallbackFetcher) GetHeader(_ context.Context, _ uint64) (*types.Header, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.hdr, nil
}

func (f *fallbackFetcher) GetBlobs(_ context.Context, _ uint64, _ []types.Namespace) ([]types.Blob, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.blobs, nil
}

func (f *fallbackFetcher) GetNetworkHead(_ context.Context) (*types.Header, error) {
	return &types.Header{Height: 1}, nil
}

func (f *fallbackFetcher) SubscribeHeaders(_ context.Context) (<-chan *types.Header, error) {
	ch := make(chan *types.Header)
	close(ch)
	return ch, nil
}

func (f *fallbackFetcher) Close() error { return nil }

func TestSourceFetchHeight_FallbackMode(t *testing.T) {
	f := &fallbackFetcher{hdr: &types.Header{Height: 5}}
	s := NewSource(f)

	hdr, blobs, err := s.FetchHeight(context.Background(), 5, nil)
	if err != nil {
		t.Fatalf("FetchHeight: %v", err)
	}
	if hdr == nil || hdr.Height != 5 {
		t.Fatalf("unexpected header: %#v", hdr)
	}
	if len(blobs) != 0 {
		t.Fatalf("expected no blobs, got %d", len(blobs))
	}
}

func TestSourceFetchHeight_PropagatesErrors(t *testing.T) {
	f := &fallbackFetcher{err: store.ErrNotFound}
	s := NewSource(f)

	_, _, err := s.FetchHeight(context.Background(), 99, []types.Namespace{{}})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
