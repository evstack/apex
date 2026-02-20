package syncer

import (
	"bytes"
	"context"
	"slices"
	"sync"

	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// mockStore is an in-memory Store for testing.
type mockStore struct {
	mu         sync.Mutex
	headers    map[uint64]*types.Header
	blobs      []types.Blob
	namespaces []types.Namespace
	syncState  *types.SyncStatus

	putHeaderErr error
	putBlobsErr  error
}

func newMockStore() *mockStore {
	return &mockStore{
		headers: make(map[uint64]*types.Header),
	}
}

func (m *mockStore) PutHeader(_ context.Context, h *types.Header) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putHeaderErr != nil {
		return m.putHeaderErr
	}
	m.headers[h.Height] = h
	return nil
}

func (m *mockStore) GetHeader(_ context.Context, height uint64) (*types.Header, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.headers[height]
	if !ok {
		return nil, store.ErrNotFound
	}
	return h, nil
}

func (m *mockStore) PutBlobs(_ context.Context, blobs []types.Blob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putBlobsErr != nil {
		return m.putBlobsErr
	}
	m.blobs = append(m.blobs, blobs...)
	return nil
}

func (m *mockStore) GetBlob(_ context.Context, ns types.Namespace, height uint64, index int) (*types.Blob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.blobs {
		b := &m.blobs[i]
		if b.Namespace == ns && b.Height == height && b.Index == index {
			return b, nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *mockStore) GetBlobs(_ context.Context, ns types.Namespace, startHeight, endHeight uint64, _, _ int) ([]types.Blob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.Blob
	for _, b := range m.blobs {
		if b.Namespace == ns && b.Height >= startHeight && b.Height <= endHeight {
			result = append(result, b)
		}
	}
	return result, nil
}

func (m *mockStore) GetBlobByCommitment(_ context.Context, commitment []byte) (*types.Blob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.blobs {
		if bytes.Equal(m.blobs[i].Commitment, commitment) {
			return &m.blobs[i], nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *mockStore) PutNamespace(_ context.Context, ns types.Namespace) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slices.Contains(m.namespaces, ns) {
		return nil
	}
	m.namespaces = append(m.namespaces, ns)
	return nil
}

func (m *mockStore) GetNamespaces(_ context.Context) ([]types.Namespace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]types.Namespace, len(m.namespaces))
	copy(out, m.namespaces)
	return out, nil
}

func (m *mockStore) GetSyncState(_ context.Context) (*types.SyncStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.syncState == nil {
		return nil, store.ErrNotFound
	}
	cp := *m.syncState
	return &cp, nil
}

func (m *mockStore) SetSyncState(_ context.Context, status types.SyncStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncState = &status
	return nil
}

func (m *mockStore) Close() error { return nil }

// mockFetcher is a test DataFetcher that serves data from maps.
type mockFetcher struct {
	mu      sync.Mutex
	headers map[uint64]*types.Header
	blobs   map[uint64][]types.Blob
	headH   uint64

	subCh    chan *types.Header
	getHdrFn func(ctx context.Context, height uint64) (*types.Header, error)
}

func newMockFetcher(headHeight uint64) *mockFetcher {
	return &mockFetcher{
		headers: make(map[uint64]*types.Header),
		blobs:   make(map[uint64][]types.Blob),
		headH:   headHeight,
	}
}

func (f *mockFetcher) addHeader(h *types.Header) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headers[h.Height] = h
}

func (f *mockFetcher) addBlobs(height uint64, blobs []types.Blob) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs[height] = blobs
}

func (f *mockFetcher) GetHeader(ctx context.Context, height uint64) (*types.Header, error) {
	f.mu.Lock()
	fn := f.getHdrFn
	h, ok := f.headers[height]
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, height)
	}
	if !ok {
		return nil, store.ErrNotFound
	}
	return h, nil
}

func (f *mockFetcher) GetBlobs(_ context.Context, height uint64, _ []types.Namespace) ([]types.Blob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blobs[height], nil
}

func (f *mockFetcher) GetNetworkHead(_ context.Context) (*types.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &types.Header{Height: f.headH}, nil
}

func (f *mockFetcher) SubscribeHeaders(_ context.Context) (<-chan *types.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subCh == nil {
		f.subCh = make(chan *types.Header, 64)
	}
	return f.subCh, nil
}

func (f *mockFetcher) Close() error { return nil }
