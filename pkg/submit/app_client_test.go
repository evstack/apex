package submit

import (
	"bytes"
	"context"
	"net"
	"testing"

	authv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/auth/v1beta1"
	abciv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/base/abci/v1beta1"
	txv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type authServer struct {
	authv1beta1.UnimplementedQueryServer
	info        *authv1beta1.QueryAccountInfoResponse
	lastAddress string
}

func (s *authServer) AccountInfo(_ context.Context, req *authv1beta1.QueryAccountInfoRequest) (*authv1beta1.QueryAccountInfoResponse, error) {
	s.lastAddress = req.GetAddress()
	return s.info, nil
}

type txServer struct {
	txv1beta1.UnimplementedServiceServer
	broadcast        *txv1beta1.BroadcastTxResponse
	getTx            *txv1beta1.GetTxResponse
	lastBroadcastReq *txv1beta1.BroadcastTxRequest
	lastGetTxHash    string
}

func (s *txServer) BroadcastTx(_ context.Context, req *txv1beta1.BroadcastTxRequest) (*txv1beta1.BroadcastTxResponse, error) {
	s.lastBroadcastReq = req
	return s.broadcast, nil
}

func (s *txServer) GetTx(_ context.Context, req *txv1beta1.GetTxRequest) (*txv1beta1.GetTxResponse, error) {
	s.lastGetTxHash = req.GetHash()
	return s.getTx, nil
}

func TestGRPCAppClient(t *testing.T) {
	t.Parallel()

	client, authSrv, txSrv := newTestGRPCAppClient(t)
	defer client.Close() //nolint:errcheck

	info, err := client.AccountInfo(context.Background(), "celestia1test")
	if err != nil {
		t.Fatalf("AccountInfo: %v", err)
	}
	assertAccountInfoResponse(t, info, authSrv)

	broadcast, err := client.BroadcastTx(context.Background(), []byte("tx"))
	if err != nil {
		t.Fatalf("BroadcastTx: %v", err)
	}
	assertBroadcastResponse(t, broadcast, txSrv)

	queried, err := client.GetTx(context.Background(), "ABC")
	if err != nil {
		t.Fatalf("GetTx: %v", err)
	}
	assertGetTxResponse(t, queried, txSrv)
}

func newTestGRPCAppClient(t *testing.T) (*GRPCAppClient, *authServer, *txServer) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	authSrv := &authServer{
		info: &authv1beta1.QueryAccountInfoResponse{
			Info: &authv1beta1.BaseAccount{
				Address:       "celestia1test",
				AccountNumber: 7,
				Sequence:      11,
			},
		},
	}
	txSrv := &txServer{
		broadcast: &txv1beta1.BroadcastTxResponse{
			TxResponse: &abciv1beta1.TxResponse{
				Height:    101,
				Txhash:    "ABC",
				Code:      0,
				RawLog:    "",
				GasWanted: 200,
				GasUsed:   180,
			},
		},
		getTx: &txv1beta1.GetTxResponse{
			TxResponse: &abciv1beta1.TxResponse{
				Height:    102,
				Txhash:    "DEF",
				Code:      3,
				RawLog:    "failed",
				GasWanted: 250,
				GasUsed:   190,
			},
		},
	}
	authv1beta1.RegisterQueryServer(srv, authSrv)
	txv1beta1.RegisterServiceServer(srv, txSrv)
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	return &GRPCAppClient{
		conn: conn,
		auth: authv1beta1.NewQueryClient(conn),
		tx:   txv1beta1.NewServiceClient(conn),
	}, authSrv, txSrv
}

func assertAccountInfoResponse(t *testing.T, info *AccountInfo, authSrv *authServer) {
	t.Helper()

	if info.AccountNumber != 7 || info.Sequence != 11 {
		t.Fatalf("info = %#v", info)
	}
	if authSrv.lastAddress != "celestia1test" {
		t.Fatalf("account lookup address = %q, want %q", authSrv.lastAddress, "celestia1test")
	}
}

func assertBroadcastResponse(t *testing.T, broadcast *TxStatus, txSrv *txServer) {
	t.Helper()

	if broadcast.Hash != "ABC" || broadcast.Height != 101 {
		t.Fatalf("broadcast = %#v", broadcast)
	}
	if txSrv.lastBroadcastReq == nil {
		t.Fatal("broadcast request = nil")
	}
	if !bytes.Equal(txSrv.lastBroadcastReq.GetTxBytes(), []byte("tx")) {
		t.Fatalf("broadcast tx bytes = %x, want %x", txSrv.lastBroadcastReq.GetTxBytes(), []byte("tx"))
	}
	if txSrv.lastBroadcastReq.GetMode() != txv1beta1.BroadcastMode_BROADCAST_MODE_SYNC {
		t.Fatalf("broadcast mode = %v, want %v", txSrv.lastBroadcastReq.GetMode(), txv1beta1.BroadcastMode_BROADCAST_MODE_SYNC)
	}
}

func assertGetTxResponse(t *testing.T, queried *TxStatus, txSrv *txServer) {
	t.Helper()

	if queried.Hash != "DEF" || queried.Code != 3 {
		t.Fatalf("queried = %#v", queried)
	}
	if txSrv.lastGetTxHash != "ABC" {
		t.Fatalf("GetTx hash = %q, want %q", txSrv.lastGetTxHash, "ABC")
	}
}
