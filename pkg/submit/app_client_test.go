package submit

import (
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
	info *authv1beta1.QueryAccountInfoResponse
}

func (s *authServer) AccountInfo(context.Context, *authv1beta1.QueryAccountInfoRequest) (*authv1beta1.QueryAccountInfoResponse, error) {
	return s.info, nil
}

type txServer struct {
	txv1beta1.UnimplementedServiceServer
	broadcast *txv1beta1.BroadcastTxResponse
	getTx     *txv1beta1.GetTxResponse
}

func (s *txServer) BroadcastTx(context.Context, *txv1beta1.BroadcastTxRequest) (*txv1beta1.BroadcastTxResponse, error) {
	return s.broadcast, nil
}

func (s *txServer) GetTx(context.Context, *txv1beta1.GetTxRequest) (*txv1beta1.GetTxResponse, error) {
	return s.getTx, nil
}

func TestGRPCAppClient(t *testing.T) {
	t.Parallel()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	authv1beta1.RegisterQueryServer(srv, &authServer{
		info: &authv1beta1.QueryAccountInfoResponse{
			Info: &authv1beta1.BaseAccount{
				Address:       "celestia1test",
				AccountNumber: 7,
				Sequence:      11,
			},
		},
	})
	txv1beta1.RegisterServiceServer(srv, &txServer{
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
	})
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	client := &GRPCAppClient{
		conn: conn,
		auth: authv1beta1.NewQueryClient(conn),
		tx:   txv1beta1.NewServiceClient(conn),
	}
	defer client.Close() //nolint:errcheck

	info, err := client.AccountInfo(context.Background(), "celestia1test")
	if err != nil {
		t.Fatalf("AccountInfo: %v", err)
	}
	if info.AccountNumber != 7 || info.Sequence != 11 {
		t.Fatalf("info = %#v", info)
	}

	broadcast, err := client.BroadcastTx(context.Background(), []byte("tx"))
	if err != nil {
		t.Fatalf("BroadcastTx: %v", err)
	}
	if broadcast.Hash != "ABC" || broadcast.Height != 101 {
		t.Fatalf("broadcast = %#v", broadcast)
	}

	queried, err := client.GetTx(context.Background(), "ABC")
	if err != nil {
		t.Fatalf("GetTx: %v", err)
	}
	if queried.Hash != "DEF" || queried.Code != 3 {
		t.Fatalf("queried = %#v", queried)
	}
}
