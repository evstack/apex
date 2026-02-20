package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/api"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// mockStore for JSON-RPC handler tests.
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

func (m *mockStore) GetBlobByCommitment(_ context.Context, commitment []byte) (*types.Blob, error) {
	for _, blobs := range m.blobs {
		for i := range blobs {
			if bytes.Equal(blobs[i].Commitment, commitment) {
				return &blobs[i], nil
			}
		}
	}
	return nil, store.ErrNotFound
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

type jsonRPCRequest struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int    `json:"id"`
}

type jsonRPCResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonRPCError   `json:"error,omitempty"`
	ID      int             `json:"id"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func doRPC(t *testing.T, srv http.Handler, method string, params ...any) jsonRPCResponse {
	t.Helper()

	if params == nil {
		params = []any{}
	}

	req := jsonRPCRequest{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httpReq)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		t.Fatalf("unmarshal response: %v (body: %s)", err, respBody)
	}
	return rpcResp
}

func TestJSONRPCHeaderGetByHeight(t *testing.T) {
	st := newMockStore()
	st.headers[42] = &types.Header{
		Height:    42,
		RawHeader: []byte(`{"height":"42"}`),
	}

	notifier := api.NewNotifier(16, 1024, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	srv := NewServer(svc, zerolog.Nop())

	resp := doRPC(t, srv, "header.GetByHeight", uint64(42))
	if resp.Error != nil {
		t.Fatalf("RPC error: %s", resp.Error.Message)
	}

	// Result should be the raw header JSON.
	if string(resp.Result) != `{"height":"42"}` {
		t.Errorf("result = %s, want raw header", resp.Result)
	}
}

func TestJSONRPCHeaderLocalHead(t *testing.T) {
	st := newMockStore()
	st.syncState = &types.SyncStatus{LatestHeight: 100}
	st.headers[100] = &types.Header{
		Height:    100,
		RawHeader: []byte(`{"height":"100"}`),
	}

	notifier := api.NewNotifier(16, 1024, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	srv := NewServer(svc, zerolog.Nop())

	resp := doRPC(t, srv, "header.LocalHead")
	if resp.Error != nil {
		t.Fatalf("RPC error: %s", resp.Error.Message)
	}
	if string(resp.Result) != `{"height":"100"}` {
		t.Errorf("result = %s", resp.Result)
	}
}

func TestJSONRPCHeaderNetworkHead(t *testing.T) {
	ft := &mockFetcher{
		networkHead: &types.Header{
			Height:    200,
			RawHeader: []byte(`{"height":"200"}`),
		},
	}

	notifier := api.NewNotifier(16, 1024, zerolog.Nop())
	svc := api.NewService(newMockStore(), ft, nil, notifier, zerolog.Nop())
	srv := NewServer(svc, zerolog.Nop())

	resp := doRPC(t, srv, "header.NetworkHead")
	if resp.Error != nil {
		t.Fatalf("RPC error: %s", resp.Error.Message)
	}
	if string(resp.Result) != `{"height":"200"}` {
		t.Errorf("result = %s", resp.Result)
	}
}

func TestJSONRPCBlobGetAll(t *testing.T) {
	st := newMockStore()
	ns := testNamespace(1)
	st.blobs[10] = []types.Blob{
		{Height: 10, Namespace: ns, Data: []byte("d1"), Commitment: []byte("c1"), Index: 0},
	}

	notifier := api.NewNotifier(16, 1024, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	srv := NewServer(svc, zerolog.Nop())

	resp := doRPC(t, srv, "blob.GetAll", uint64(10), [][]byte{ns[:]})
	if resp.Error != nil {
		t.Fatalf("RPC error: %s", resp.Error.Message)
	}

	var blobs []json.RawMessage
	if err := json.Unmarshal(resp.Result, &blobs); err != nil {
		t.Fatalf("unmarshal blobs: %v", err)
	}
	if len(blobs) != 1 {
		t.Errorf("got %d blobs, want 1", len(blobs))
	}
}

func TestJSONRPCBlobGetByCommitment(t *testing.T) {
	st := newMockStore()
	ns := testNamespace(1)
	st.blobs[10] = []types.Blob{
		{Height: 10, Namespace: ns, Data: []byte("d1"), Commitment: []byte("c1"), Index: 0},
	}

	notifier := api.NewNotifier(16, 1024, zerolog.Nop())
	svc := api.NewService(st, &mockFetcher{}, nil, notifier, zerolog.Nop())
	srv := NewServer(svc, zerolog.Nop())

	t.Run("found", func(t *testing.T) {
		resp := doRPC(t, srv, "blob.GetByCommitment", []byte("c1"))
		if resp.Error != nil {
			t.Fatalf("RPC error: %s", resp.Error.Message)
		}

		var blob map[string]json.RawMessage
		if err := json.Unmarshal(resp.Result, &blob); err != nil {
			t.Fatalf("unmarshal blob: %v", err)
		}
		if _, ok := blob["commitment"]; !ok {
			t.Error("blob JSON missing 'commitment' field")
		}
	})

	t.Run("not found", func(t *testing.T) {
		resp := doRPC(t, srv, "blob.GetByCommitment", []byte("missing"))
		if resp.Error == nil {
			t.Fatal("expected error for missing commitment")
		}
	})
}

func TestJSONRPCStubMethods(t *testing.T) {
	notifier := api.NewNotifier(16, 1024, zerolog.Nop())
	svc := api.NewService(newMockStore(), &mockFetcher{}, nil, notifier, zerolog.Nop())
	srv := NewServer(svc, zerolog.Nop())

	tests := []struct {
		method string
		params []any
	}{
		{"share.GetShare", []any{uint64(1), 0, 0}},
		{"share.GetEDS", []any{uint64(1)}},
		{"fraud.Get", []any{"befp"}},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			resp := doRPC(t, srv, tt.method, tt.params...)
			if resp.Error == nil {
				t.Error("expected error from stub method")
			}
		})
	}
}
