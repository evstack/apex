package fetch

import (
	"context"
	"encoding/base64"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	cometpb "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/base/tendermint/v1beta1"
	"github.com/evstack/apex/pkg/types"
)

// mockCometService implements the generated ServiceServer interface.
type mockCometService struct {
	cometpb.UnimplementedServiceServer
	blocks      map[int64]*cometpb.GetBlockByHeightResponse
	latestBlock *cometpb.GetLatestBlockResponse
}

func (m *mockCometService) GetBlockByHeight(_ context.Context, req *cometpb.GetBlockByHeightRequest) (*cometpb.GetBlockByHeightResponse, error) {
	resp, ok := m.blocks[req.Height]
	if !ok {
		return &cometpb.GetBlockByHeightResponse{
			BlockId: &cometpb.BlockID{Hash: []byte("default")},
			Block: &cometpb.Block{
				Header: &cometpb.Header{
					Height: req.Height,
					Time:   timestamppb.Now(),
				},
				Data: &cometpb.Data{},
			},
		}, nil
	}
	return resp, nil
}

func (m *mockCometService) GetLatestBlock(context.Context, *cometpb.GetLatestBlockRequest) (*cometpb.GetLatestBlockResponse, error) {
	return m.latestBlock, nil
}

func newTestFetcher(t *testing.T, mock *mockCometService) *CelestiaAppFetcher {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	cometpb.RegisterServiceServer(srv, mock)
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("grpc serve: %v", err)
		}
	}()
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("bufconn dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return newCelestiaAppFetcherFromClient(conn, zerolog.Nop())
}

func blockResponse(height int64, txs [][]byte, ts time.Time) *cometpb.GetBlockByHeightResponse {
	return &cometpb.GetBlockByHeightResponse{
		BlockId: &cometpb.BlockID{Hash: []byte("ABCD1234")},
		Block: &cometpb.Block{
			Header: &cometpb.Header{
				Height:   height,
				Time:     timestamppb.New(ts),
				DataHash: []byte("DDDD"),
			},
			Data: &cometpb.Data{Txs: txs},
		},
	}
}

func TestGetHeader(t *testing.T) {
	blockTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mock := &mockCometService{
		blocks: map[int64]*cometpb.GetBlockByHeightResponse{
			42: blockResponse(42, nil, blockTime),
		},
	}

	f := newTestFetcher(t, mock)

	hdr, err := f.GetHeader(context.Background(), 42)
	if err != nil {
		t.Fatalf("GetHeader: %v", err)
	}
	if hdr.Height != 42 {
		t.Errorf("Height = %d, want 42", hdr.Height)
	}
	if len(hdr.Hash) == 0 {
		t.Error("Hash is empty")
	}
	if !hdr.Time.Equal(blockTime) {
		t.Errorf("Time = %v, want %v", hdr.Time, blockTime)
	}
}

func TestGetBlobs(t *testing.T) {
	ns := testNS(1)
	blobTx := buildBlobTx("signer", [][]byte{[]byte("c1")},
		rawBlob{Namespace: ns, Data: []byte("blob-data")},
	)

	mock := &mockCometService{
		blocks: map[int64]*cometpb.GetBlockByHeightResponse{
			10: blockResponse(10, [][]byte{blobTx}, time.Now().UTC()),
		},
	}

	f := newTestFetcher(t, mock)

	var nsType types.Namespace
	copy(nsType[:], ns)

	blobs, err := f.GetBlobs(context.Background(), 10, []types.Namespace{nsType})
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if len(blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(blobs))
	}
	if string(blobs[0].Data) != "blob-data" {
		t.Errorf("Data = %q, want %q", blobs[0].Data, "blob-data")
	}
	if string(blobs[0].Commitment) != "c1" {
		t.Errorf("Commitment = %q, want %q", blobs[0].Commitment, "c1")
	}
	if string(blobs[0].Signer) != "signer" {
		t.Errorf("Signer = %q, want %q", blobs[0].Signer, "signer")
	}
}

func TestGetBlobsNoMatch(t *testing.T) {
	ns := testNS(1)
	blobTx := buildBlobTx("s", [][]byte{[]byte("c")}, rawBlob{Namespace: ns, Data: []byte("d")})

	mock := &mockCometService{
		blocks: map[int64]*cometpb.GetBlockByHeightResponse{
			10: blockResponse(10, [][]byte{blobTx}, time.Now().UTC()),
		},
	}

	f := newTestFetcher(t, mock)

	var ns99 types.Namespace
	ns99[types.NamespaceSize-1] = 99

	blobs, err := f.GetBlobs(context.Background(), 10, []types.Namespace{ns99})
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if blobs != nil {
		t.Errorf("expected nil for no matching blobs, got %d blobs", len(blobs))
	}
}

func TestGetNetworkHead(t *testing.T) {
	blockTime := time.Now().UTC().Truncate(time.Microsecond)
	mock := &mockCometService{
		latestBlock: &cometpb.GetLatestBlockResponse{
			BlockId: &cometpb.BlockID{Hash: []byte("HEAD")},
			Block: &cometpb.Block{
				Header: &cometpb.Header{
					Height:   50,
					Time:     timestamppb.New(blockTime),
					DataHash: []byte("DH"),
				},
				Data: &cometpb.Data{},
			},
		},
	}

	f := newTestFetcher(t, mock)

	hdr, err := f.GetNetworkHead(context.Background())
	if err != nil {
		t.Fatalf("GetNetworkHead: %v", err)
	}
	if hdr.Height != 50 {
		t.Errorf("Height = %d, want 50", hdr.Height)
	}
}

func TestSubscribeHeaders(t *testing.T) {
	blockTime := time.Now().UTC().Truncate(time.Microsecond)

	mock := &mockCometService{
		latestBlock: &cometpb.GetLatestBlockResponse{
			BlockId: &cometpb.BlockID{Hash: []byte("SUB")},
			Block: &cometpb.Block{
				Header: &cometpb.Header{
					Height:   100,
					Time:     timestamppb.New(blockTime),
					DataHash: []byte("DH"),
				},
				Data: &cometpb.Data{},
			},
		},
	}

	f := newTestFetcher(t, mock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := f.SubscribeHeaders(ctx)
	if err != nil {
		t.Fatalf("SubscribeHeaders: %v", err)
	}

	select {
	case hdr := <-ch:
		if hdr.Height != 100 {
			t.Errorf("Height = %d, want 100", hdr.Height)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for header")
	}
}

func TestCloseIdempotent(t *testing.T) {
	f, err := NewCelestiaAppFetcher("localhost:26657", "", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewCelestiaAppFetcher: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close (second): %v", err)
	}
}

// Ensure base64 is imported so tests compile (used by existing blobtx helpers).
var _ = base64.StdEncoding
