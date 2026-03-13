package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	authv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/auth/v1beta1"
	abciv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/base/abci/v1beta1"
	txv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/v1beta1"
	"github.com/evstack/apex/pkg/submit"
	"github.com/evstack/apex/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evstack/apex/config"
)

const testSignerKeyHex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

func TestOpenBlobSubmitterDisabled(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Submission.Enabled = false

	submitter, err := openBlobSubmitter(&cfg)
	if err != nil {
		t.Fatalf("openBlobSubmitter: %v", err)
	}
	if submitter != nil {
		t.Fatalf("expected nil submitter when submission disabled, got %T", submitter)
	}
}

type submissionAuthServer struct {
	authv1beta1.UnimplementedQueryServer
	info        *authv1beta1.QueryAccountInfoResponse
	lastAddress string
}

func (s *submissionAuthServer) AccountInfo(_ context.Context, req *authv1beta1.QueryAccountInfoRequest) (*authv1beta1.QueryAccountInfoResponse, error) {
	s.lastAddress = req.GetAddress()
	return s.info, nil
}

type submissionTxServer struct {
	txv1beta1.UnimplementedServiceServer
	broadcastReq *txv1beta1.BroadcastTxRequest
	getTxHashes  []string
}

func (s *submissionTxServer) BroadcastTx(_ context.Context, req *txv1beta1.BroadcastTxRequest) (*txv1beta1.BroadcastTxResponse, error) {
	s.broadcastReq = req
	return &txv1beta1.BroadcastTxResponse{
		TxResponse: &abciv1beta1.TxResponse{Txhash: "ABC123"},
	}, nil
}

func (s *submissionTxServer) GetTx(_ context.Context, req *txv1beta1.GetTxRequest) (*txv1beta1.GetTxResponse, error) {
	s.getTxHashes = append(s.getTxHashes, req.GetHash())
	if len(s.getTxHashes) == 1 {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &txv1beta1.GetTxResponse{
		TxResponse: &abciv1beta1.TxResponse{
			Height: 101,
			Txhash: req.GetHash(),
		},
	}, nil
}

func TestOpenBlobSubmitterSubmitsViaConfiguredBackend(t *testing.T) {
	t.Parallel()

	signer, err := submit.LoadSigner(writeSignerKey(t, testSignerKeyHex))
	if err != nil {
		t.Fatalf("LoadSigner: %v", err)
	}

	authSrv := &submissionAuthServer{
		info: &authv1beta1.QueryAccountInfoResponse{
			Info: &authv1beta1.BaseAccount{
				Address:       signer.Address(),
				AccountNumber: 7,
				Sequence:      1,
			},
		},
	}
	txSrv := &submissionTxServer{}

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer lis.Close() //nolint:errcheck

	srv := grpc.NewServer()
	authv1beta1.RegisterQueryServer(srv, authSrv)
	txv1beta1.RegisterServiceServer(srv, txSrv)
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	cfg := submissionConfigWithAddr(t, lis.Addr().String())
	submitter, err := openBlobSubmitter(cfg)
	if err != nil {
		t.Fatalf("openBlobSubmitter: %v", err)
	}
	defer submitter.Close() //nolint:errcheck

	result, err := submitter.Submit(context.Background(), &submit.Request{
		Blobs: []submit.Blob{{
			Namespace:    testNamespace(1),
			Data:         []byte("payload"),
			ShareVersion: 0,
			Commitment:   []byte("commitment"),
		}},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Height != 101 {
		t.Fatalf("height = %d, want 101", result.Height)
	}
	if authSrv.lastAddress != signer.Address() {
		t.Fatalf("account lookup address = %q, want %q", authSrv.lastAddress, signer.Address())
	}
	if txSrv.broadcastReq == nil {
		t.Fatal("broadcast request = nil")
	}
	if txSrv.broadcastReq.GetMode() != txv1beta1.BroadcastMode_BROADCAST_MODE_SYNC {
		t.Fatalf("broadcast mode = %v, want %v", txSrv.broadcastReq.GetMode(), txv1beta1.BroadcastMode_BROADCAST_MODE_SYNC)
	}
	if len(txSrv.broadcastReq.GetTxBytes()) == 0 {
		t.Fatal("broadcast tx bytes are empty")
	}
	if got := len(txSrv.getTxHashes); got < 2 {
		t.Fatalf("GetTx calls = %d, want at least 2", got)
	}
	if txSrv.getTxHashes[0] != "ABC123" {
		t.Fatalf("first GetTx hash = %q, want %q", txSrv.getTxHashes[0], "ABC123")
	}
}

func TestOpenBlobSubmitterRejectsBadSignerKey(t *testing.T) {
	t.Parallel()

	cfg := submissionConfigWithAddr(t, "127.0.0.1:65535")
	cfg.Submission.SignerKey = writeSignerKey(t, "zzzz")

	_, err := openBlobSubmitter(cfg)
	if err == nil {
		t.Fatal("expected error when signer key is invalid")
	}
}

func submissionConfigWithAddr(t *testing.T, addr string) *config.Config {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.Submission.Enabled = true
	cfg.Submission.CelestiaAppGRPCAddr = addr
	cfg.Submission.ChainID = "mocha-4"
	cfg.Submission.GasPrice = 0.001
	cfg.Submission.MaxGasPrice = 0.01
	cfg.Submission.ConfirmationTimeout = 5
	cfg.Submission.SignerKey = writeSignerKey(t, testSignerKeyHex)
	return &cfg
}

func testNamespace(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}

func writeSignerKey(t *testing.T, signerKeyHex string) string {
	t.Helper()

	keyPath := filepath.Join(t.TempDir(), "submission.key")
	if err := os.WriteFile(keyPath, []byte(signerKeyHex), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return keyPath
}
