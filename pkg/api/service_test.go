package api

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// mockStore for service tests.
type mockStore struct {
	headers   map[uint64]*types.Header
	blobs     map[uint64][]types.Blob
	syncState *types.SyncStatus
}

func newMockStore() *mockStore {
	return &mockStore{
		headers: make(map[uint64]*types.Header),
		blobs:   make(map[uint64][]types.Blob),
	}
}

func (m *mockStore) PutBlobs(_ context.Context, blobs []types.Blob) error {
	for _, b := range blobs {
		m.blobs[b.Height] = append(m.blobs[b.Height], b)
	}
	return nil
}

func (m *mockStore) GetBlob(_ context.Context, ns types.Namespace, height uint64, index int) (*types.Blob, error) {
	for _, b := range m.blobs[height] {
		if b.Namespace == ns && b.Index == index {
			return &b, nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *mockStore) GetBlobs(_ context.Context, ns types.Namespace, startHeight, endHeight uint64, _, _ int) ([]types.Blob, error) {
	var result []types.Blob
	for h := startHeight; h <= endHeight; h++ {
		for _, b := range m.blobs[h] {
			if b.Namespace == ns {
				result = append(result, b)
			}
		}
	}
	return result, nil
}

func (m *mockStore) PutHeader(_ context.Context, h *types.Header) error {
	m.headers[h.Height] = h
	return nil
}

func (m *mockStore) GetHeader(_ context.Context, height uint64) (*types.Header, error) {
	h, ok := m.headers[height]
	if !ok {
		return nil, store.ErrNotFound
	}
	return h, nil
}

func (m *mockStore) PutNamespace(_ context.Context, _ types.Namespace) error { return nil }

func (m *mockStore) GetNamespaces(_ context.Context) ([]types.Namespace, error) {
	return nil, nil
}

func (m *mockStore) GetSyncState(_ context.Context) (*types.SyncStatus, error) {
	if m.syncState == nil {
		return nil, store.ErrNotFound
	}
	return m.syncState, nil
}

func (m *mockStore) SetSyncState(_ context.Context, s types.SyncStatus) error {
	m.syncState = &s
	return nil
}

func (m *mockStore) Close() error { return nil }

// mockFetcher for service tests.
type mockFetcher struct {
	networkHead *types.Header
}

func (m *mockFetcher) GetHeader(_ context.Context, _ uint64) (*types.Header, error) {
	return nil, errors.New("not implemented")
}

func (m *mockFetcher) GetBlobs(_ context.Context, _ uint64, _ []types.Namespace) ([]types.Blob, error) {
	return nil, nil
}

func (m *mockFetcher) GetNetworkHead(_ context.Context) (*types.Header, error) {
	if m.networkHead == nil {
		return nil, errors.New("no network head")
	}
	return m.networkHead, nil
}

func (m *mockFetcher) SubscribeHeaders(_ context.Context) (<-chan *types.Header, error) {
	return make(chan *types.Header), nil
}

func (m *mockFetcher) Close() error { return nil }

func TestServiceBlobGet(t *testing.T) {
	st := newMockStore()
	ns := testNamespace(1)
	commitment := []byte("c1")

	st.blobs[10] = []types.Blob{
		{Height: 10, Namespace: ns, Data: []byte("d1"), Commitment: commitment, Index: 0},
		{Height: 10, Namespace: ns, Data: []byte("d2"), Commitment: []byte("c2"), Index: 1},
	}

	svc := NewService(st, &mockFetcher{}, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	raw, err := svc.BlobGet(context.Background(), 10, ns, commitment)
	if err != nil {
		t.Fatalf("BlobGet: %v", err)
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal blob: %v", err)
	}
	if _, ok := m["commitment"]; !ok {
		t.Error("blob JSON missing 'commitment' field")
	}
}

func TestServiceBlobGetNotFound(t *testing.T) {
	st := newMockStore()
	ns := testNamespace(1)
	svc := NewService(st, &mockFetcher{}, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	_, err := svc.BlobGet(context.Background(), 10, ns, []byte("missing"))
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestServiceBlobGetAll(t *testing.T) {
	st := newMockStore()
	ns1 := testNamespace(1)
	ns2 := testNamespace(2)

	st.blobs[10] = []types.Blob{
		{Height: 10, Namespace: ns1, Data: []byte("d1"), Commitment: []byte("c1"), Index: 0},
		{Height: 10, Namespace: ns2, Data: []byte("d2"), Commitment: []byte("c2"), Index: 0},
	}

	svc := NewService(st, &mockFetcher{}, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	raw, err := svc.BlobGetAll(context.Background(), 10, []types.Namespace{ns1, ns2}, 0, 0)
	if err != nil {
		t.Fatalf("BlobGetAll: %v", err)
	}

	var blobs []json.RawMessage
	if err := json.Unmarshal(raw, &blobs); err != nil {
		t.Fatalf("unmarshal blobs: %v", err)
	}
	if len(blobs) != 2 {
		t.Errorf("got %d blobs, want 2", len(blobs))
	}
}

func TestServiceBlobGetAllEmpty(t *testing.T) {
	st := newMockStore()
	svc := NewService(st, &mockFetcher{}, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	raw, err := svc.BlobGetAll(context.Background(), 10, []types.Namespace{testNamespace(1)}, 0, 0)
	if err != nil {
		t.Fatalf("BlobGetAll: %v", err)
	}
	if string(raw) != "null" {
		t.Errorf("expected null for empty blobs, got %s", raw)
	}
}

func TestServiceHeaderGetByHeight(t *testing.T) {
	st := newMockStore()
	st.headers[42] = &types.Header{
		Height:    42,
		RawHeader: []byte(`{"height":"42"}`),
	}

	svc := NewService(st, &mockFetcher{}, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	raw, err := svc.HeaderGetByHeight(context.Background(), 42)
	if err != nil {
		t.Fatalf("HeaderGetByHeight: %v", err)
	}
	if string(raw) != `{"height":"42"}` {
		t.Errorf("got %s, want raw header JSON", raw)
	}
}

func TestServiceHeaderLocalHead(t *testing.T) {
	st := newMockStore()
	st.syncState = &types.SyncStatus{LatestHeight: 100}
	st.headers[100] = &types.Header{
		Height:    100,
		RawHeader: []byte(`{"height":"100"}`),
	}

	svc := NewService(st, &mockFetcher{}, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	raw, err := svc.HeaderLocalHead(context.Background())
	if err != nil {
		t.Fatalf("HeaderLocalHead: %v", err)
	}
	if string(raw) != `{"height":"100"}` {
		t.Errorf("got %s", raw)
	}
}

func TestServiceHeaderNetworkHead(t *testing.T) {
	ft := &mockFetcher{
		networkHead: &types.Header{
			Height:    200,
			RawHeader: []byte(`{"height":"200"}`),
		},
	}

	svc := NewService(newMockStore(), ft, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	raw, err := svc.HeaderNetworkHead(context.Background())
	if err != nil {
		t.Fatalf("HeaderNetworkHead: %v", err)
	}
	if string(raw) != `{"height":"200"}` {
		t.Errorf("got %s", raw)
	}
}

func TestServiceProofForwardingUnavailable(t *testing.T) {
	svc := NewService(newMockStore(), &mockFetcher{}, nil, NewNotifier(16, zerolog.Nop()), zerolog.Nop())

	_, err := svc.BlobGetProof(context.Background(), 1, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil proof forwarder")
	}

	_, err = svc.BlobIncluded(context.Background(), 1, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil proof forwarder")
	}
}
