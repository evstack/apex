package grpcapi

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/evstack/apex/pkg/api"
	pb "github.com/evstack/apex/pkg/api/grpc/gen/apex/v1"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// mockStore for gRPC tests.
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

func (m *mockStore) PutNamespace(_ context.Context, _ types.Namespace) error    { return nil }
func (m *mockStore) GetNamespaces(_ context.Context) ([]types.Namespace, error) { return nil, nil }

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

type mockFetcher struct {
	networkHead *types.Header
}

func (f *mockFetcher) GetHeader(_ context.Context, _ uint64) (*types.Header, error) {
	return nil, errors.New("not implemented")
}

func (f *mockFetcher) GetBlobs(_ context.Context, _ uint64, _ []types.Namespace) ([]types.Blob, error) {
	return nil, nil
}

func (f *mockFetcher) GetNetworkHead(_ context.Context) (*types.Header, error) {
	if f.networkHead == nil {
		return nil, errors.New("no network head")
	}
	return f.networkHead, nil
}

func (f *mockFetcher) SubscribeHeaders(_ context.Context) (<-chan *types.Header, error) {
	return make(chan *types.Header), nil
}

func (f *mockFetcher) Close() error { return nil }

func testNamespace(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}

func startTestServer(t *testing.T, svc *api.Service) pb.BlobServiceClient {
	t.Helper()

	srv := NewServer(svc, zerolog.Nop())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewBlobServiceClient(conn)
}

func startTestHeaderServer(t *testing.T, svc *api.Service) pb.HeaderServiceClient {
	t.Helper()

	srv := NewServer(svc, zerolog.Nop())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewHeaderServiceClient(conn)
}

func TestGRPCBlobGet(t *testing.T) {
	st := newMockStore()
	ns := testNamespace(1)
	commitment := []byte("c1")

	st.blobs[10] = []types.Blob{
		{Height: 10, Namespace: ns, Data: []byte("d1"), Commitment: commitment, Index: 0},
	}

	notifier := api.NewNotifier(16, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	client := startTestServer(t, svc)

	resp, err := client.Get(context.Background(), &pb.GetRequest{
		Height:     10,
		Namespace:  ns[:],
		Commitment: commitment,
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Blob.Height != 10 {
		t.Errorf("Height = %d, want 10", resp.Blob.Height)
	}
	if string(resp.Blob.Data) != "d1" {
		t.Errorf("Data = %q, want %q", resp.Blob.Data, "d1")
	}
}

func TestGRPCBlobGetAll(t *testing.T) {
	st := newMockStore()
	ns := testNamespace(1)

	st.blobs[10] = []types.Blob{
		{Height: 10, Namespace: ns, Data: []byte("d1"), Commitment: []byte("c1"), Index: 0},
		{Height: 10, Namespace: ns, Data: []byte("d2"), Commitment: []byte("c2"), Index: 1},
	}

	notifier := api.NewNotifier(16, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	client := startTestServer(t, svc)

	resp, err := client.GetAll(context.Background(), &pb.GetAllRequest{
		Height:     10,
		Namespaces: [][]byte{ns[:]},
	})
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(resp.Blobs) != 2 {
		t.Errorf("got %d blobs, want 2", len(resp.Blobs))
	}
}

func TestGRPCHeaderGetByHeight(t *testing.T) {
	st := newMockStore()
	now := time.Now().UTC().Truncate(time.Second)
	st.headers[42] = &types.Header{
		Height:    42,
		Hash:      []byte("hash"),
		DataHash:  []byte("dh"),
		Time:      now,
		RawHeader: []byte("raw"),
	}

	notifier := api.NewNotifier(16, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	client := startTestHeaderServer(t, svc)

	resp, err := client.GetByHeight(context.Background(), &pb.GetByHeightRequest{Height: 42})
	if err != nil {
		t.Fatalf("GetByHeight: %v", err)
	}
	if resp.Header.Height != 42 {
		t.Errorf("Height = %d, want 42", resp.Header.Height)
	}
	if string(resp.Header.Hash) != "hash" {
		t.Errorf("Hash = %q, want %q", resp.Header.Hash, "hash")
	}
}

func TestGRPCHeaderLocalHead(t *testing.T) {
	st := newMockStore()
	st.syncState = &types.SyncStatus{LatestHeight: 100}
	st.headers[100] = &types.Header{
		Height:    100,
		Hash:      []byte("hash100"),
		RawHeader: []byte("raw"),
	}

	notifier := api.NewNotifier(16, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	client := startTestHeaderServer(t, svc)

	resp, err := client.LocalHead(context.Background(), &pb.LocalHeadRequest{})
	if err != nil {
		t.Fatalf("LocalHead: %v", err)
	}
	if resp.Header.Height != 100 {
		t.Errorf("Height = %d, want 100", resp.Header.Height)
	}
}

func TestGRPCHeaderNetworkHead(t *testing.T) {
	ft := &mockFetcher{
		networkHead: &types.Header{
			Height:    200,
			Hash:      []byte("hash200"),
			RawHeader: []byte("raw"),
		},
	}

	notifier := api.NewNotifier(16, zerolog.Nop())
	svc := api.NewService(newMockStore(), ft, nil, notifier, zerolog.Nop())
	client := startTestHeaderServer(t, svc)

	resp, err := client.NetworkHead(context.Background(), &pb.NetworkHeadRequest{})
	if err != nil {
		t.Fatalf("NetworkHead: %v", err)
	}
	if resp.Header.Height != 200 {
		t.Errorf("Height = %d, want 200", resp.Header.Height)
	}
}

func TestGRPCBlobSubscribe(t *testing.T) {
	st := newMockStore()
	ns := testNamespace(1)

	notifier := api.NewNotifier(16, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	client := startTestServer(t, svc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Subscribe(ctx, &pb.BlobServiceSubscribeRequest{
		Namespace: ns[:],
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for server-side subscription to be established.
	deadline := time.After(5 * time.Second)
	for notifier.SubscriberCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for subscriber registration")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Publish an event.
	notifier.Publish(api.HeightEvent{
		Height: 1,
		Header: &types.Header{Height: 1},
		Blobs: []types.Blob{
			{Height: 1, Namespace: ns, Data: []byte("d1"), Index: 0},
		},
	})

	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Height != 1 {
		t.Errorf("Height = %d, want 1", ev.Height)
	}
	if len(ev.Blobs) != 1 {
		t.Errorf("Blobs = %d, want 1", len(ev.Blobs))
	}
}
