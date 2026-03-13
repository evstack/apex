package submit

import (
	"context"
	"fmt"

	authv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/auth/v1beta1"
	txv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// AccountInfo is the Apex-owned account view returned by celestia-app.
type AccountInfo struct {
	Address       string
	AccountNumber uint64
	Sequence      uint64
}

// TxStatus is the Apex-owned subset of tx execution status used by the submit
// pipeline for broadcast and confirmation.
type TxStatus struct {
	Height    int64
	Hash      string
	Code      uint32
	Codespace string
	RawLog    string
	GasWanted int64
	GasUsed   int64
}

// AppClient is the narrow direct-to-celestia-app transport surface needed by
// the submission pipeline.
type AppClient interface {
	AccountInfo(ctx context.Context, address string) (*AccountInfo, error)
	BroadcastTx(ctx context.Context, txBytes []byte) (*TxStatus, error)
	GetTx(ctx context.Context, hash string) (*TxStatus, error)
	Close() error
}

// GRPCAppClient implements AppClient against celestia-app's Cosmos SDK gRPC.
type GRPCAppClient struct {
	conn *grpc.ClientConn
	auth authv1beta1.QueryClient
	tx   txv1beta1.ServiceClient
}

// NewGRPCAppClient opens a gRPC client for direct celestia-app submission
// primitives.
func NewGRPCAppClient(grpcAddr string) (*GRPCAppClient, error) {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create celestia-app submit client: %w", err)
	}
	return &GRPCAppClient{
		conn: conn,
		auth: authv1beta1.NewQueryClient(conn),
		tx:   txv1beta1.NewServiceClient(conn),
	}, nil
}

func (c *GRPCAppClient) AccountInfo(ctx context.Context, address string) (*AccountInfo, error) {
	resp, err := c.auth.AccountInfo(ctx, &authv1beta1.QueryAccountInfoRequest{Address: address})
	if err != nil {
		return nil, fmt.Errorf("query account info: %w", err)
	}
	info := resp.GetInfo()
	if info == nil {
		return nil, fmt.Errorf("query account info: empty response for %q", address)
	}
	return &AccountInfo{
		Address:       info.GetAddress(),
		AccountNumber: info.GetAccountNumber(),
		Sequence:      info.GetSequence(),
	}, nil
}

func (c *GRPCAppClient) BroadcastTx(ctx context.Context, txBytes []byte) (*TxStatus, error) {
	resp, err := c.tx.BroadcastTx(ctx, &txv1beta1.BroadcastTxRequest{
		TxBytes: txBytes,
		Mode:    txv1beta1.BroadcastMode_BROADCAST_MODE_SYNC,
	})
	if err != nil {
		return nil, fmt.Errorf("broadcast tx: %w", err)
	}
	return mapTxStatus(resp.GetTxResponse()), nil
}

func (c *GRPCAppClient) GetTx(ctx context.Context, hash string) (*TxStatus, error) {
	resp, err := c.tx.GetTx(ctx, &txv1beta1.GetTxRequest{Hash: hash})
	if err != nil {
		return nil, fmt.Errorf("get tx: %w", err)
	}
	return mapTxStatus(resp.GetTxResponse()), nil
}

func (c *GRPCAppClient) Close() error {
	return c.conn.Close()
}

func mapTxStatus(resp interface {
	GetHeight() int64
	GetTxhash() string
	GetCode() uint32
	GetCodespace() string
	GetRawLog() string
	GetGasWanted() int64
	GetGasUsed() int64
},
) *TxStatus {
	if resp == nil {
		return nil
	}
	return &TxStatus{
		Height:    resp.GetHeight(),
		Hash:      resp.GetTxhash(),
		Code:      resp.GetCode(),
		Codespace: resp.GetCodespace(),
		RawLog:    resp.GetRawLog(),
		GasWanted: resp.GetGasWanted(),
		GasUsed:   resp.GetGasUsed(),
	}
}
