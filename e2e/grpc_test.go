package e2e

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/evstack/apex/pkg/api/grpc/gen/apex/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCBlobQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed e2e test in short mode")
	}

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), chainStartupTimeout)
	defer cancelSetup()

	newRPCCtx := func(t *testing.T) (context.Context, context.CancelFunc) {
		t.Helper()
		return context.WithTimeout(context.Background(), 10*time.Second)
	}

	grpcAddr, chainID, signerKeyHex, signerAddress := startSubmissionTestChain(t, setupCtx)
	namespace := testNamespace(t, []byte("apex-grpc"))
	data := []byte("apex grpc query e2e")
	commitment := mustBlobCommitment(t, namespace, data)

	apexBinary := buildApexBinary(t)
	apexRPCAddr := reserveTCPAddr(t)
	apexGRPCAddr := reserveTCPAddr(t)
	keyPath := writeSignerKey(t, signerKeyHex)
	configPath := writeApexConfig(t, apexConfig{
		Namespace:       namespace,
		DataGRPCAddr:    grpcAddr,
		SubmissionGRPC:  grpcAddr,
		ChainID:         chainID,
		SignerKeyPath:   keyPath,
		StoragePath:     filepath.Join(t.TempDir(), "apex.db"),
		RPCListenAddr:   apexRPCAddr,
		GRPCListenAddr:  apexGRPCAddr,
		GasPrice:        submissionGasPrice,
		MaxGasPrice:     submissionGasPrice,
		ConfirmTimeoutS: submissionConfirmTimeout,
	})

	proc := startApexProcess(t, apexBinary, configPath)
	defer proc.Stop(t)
	waitForApexHTTP(t, proc, apexRPCAddr)

	resp := doRPC(t, proc, apexRPCAddr, "blob.Submit",
		[]map[string]any{{
			"namespace":     namespace,
			"data":          data,
			"share_version": 0,
			"commitment":    commitment,
			"index":         -1,
		}},
		map[string]any{
			"gas_price":        submissionGasPrice,
			"is_gas_price_set": true,
		},
	)
	if resp.Error != nil {
		t.Fatalf("blob.Submit error: %s", resp.Error.Message)
	}
	height := decodeSubmitHeight(t, resp.Result)
	if height == 0 {
		t.Fatal("submission height must be positive")
	}

	waitForIndexedBlob(t, proc, apexRPCAddr, commitment, data, namespace, signerAddress)

	conn, err := grpc.NewClient(apexGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial apex gRPC: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	blobClient := pb.NewBlobServiceClient(conn)
	headerClient := pb.NewHeaderServiceClient(conn)

	t.Run("BlobService.GetByCommitment", func(t *testing.T) {
		rpcCtx, cancel := newRPCCtx(t)
		defer cancel()
		r, err := blobClient.GetByCommitment(rpcCtx, &pb.GetByCommitmentRequest{Commitment: commitment})
		if err != nil {
			t.Fatalf("GetByCommitment: %v", err)
		}
		if r.GetBlob() == nil {
			t.Fatal("expected non-nil blob")
		}
		if !bytes.Equal(r.GetBlob().GetData(), data) {
			t.Fatalf("data mismatch: got %q want %q", r.GetBlob().GetData(), data)
		}
		if !bytes.Equal(r.GetBlob().GetCommitment(), commitment) {
			t.Fatalf("commitment mismatch: got %x want %x", r.GetBlob().GetCommitment(), commitment)
		}
	})

	t.Run("BlobService.Get", func(t *testing.T) {
		rpcCtx, cancel := newRPCCtx(t)
		defer cancel()
		r, err := blobClient.Get(rpcCtx, &pb.GetRequest{
			Height:     height,
			Namespace:  namespace,
			Commitment: commitment,
		})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(r.GetBlob().GetData(), data) {
			t.Fatalf("data mismatch: got %q want %q", r.GetBlob().GetData(), data)
		}
		if !bytes.Equal(r.GetBlob().GetCommitment(), commitment) {
			t.Fatalf("commitment mismatch: got %x want %x", r.GetBlob().GetCommitment(), commitment)
		}
	})

	t.Run("BlobService.GetAll", func(t *testing.T) {
		rpcCtx, cancel := newRPCCtx(t)
		defer cancel()
		r, err := blobClient.GetAll(rpcCtx, &pb.GetAllRequest{
			Height:     height,
			Namespaces: [][]byte{namespace},
		})
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(r.GetBlobs()) == 0 {
			t.Fatal("expected at least one blob")
		}
		found := false
		for _, b := range r.GetBlobs() {
			if bytes.Equal(b.GetCommitment(), commitment) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("blob with commitment %x not in GetAll result", commitment)
		}
	})

	t.Run("HeaderService.GetByHeight", func(t *testing.T) {
		rpcCtx, cancel := newRPCCtx(t)
		defer cancel()
		r, err := headerClient.GetByHeight(rpcCtx, &pb.GetByHeightRequest{Height: height})
		if err != nil {
			t.Fatalf("GetByHeight: %v", err)
		}
		if r.GetHeader() == nil {
			t.Fatal("expected non-nil header")
		}
		if r.GetHeader().GetHeight() != height {
			t.Fatalf("header height = %d, want %d", r.GetHeader().GetHeight(), height)
		}
	})

	t.Run("HeaderService.LocalHead", func(t *testing.T) {
		rpcCtx, cancel := newRPCCtx(t)
		defer cancel()
		r, err := headerClient.LocalHead(rpcCtx, &pb.LocalHeadRequest{})
		if err != nil {
			t.Fatalf("LocalHead: %v", err)
		}
		if r.GetHeader() == nil {
			t.Fatal("expected non-nil header")
		}
		if r.GetHeader().GetHeight() < height {
			t.Fatalf("local head height %d < submission height %d", r.GetHeader().GetHeight(), height)
		}
	})
}
