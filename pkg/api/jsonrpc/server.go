package jsonrpc

import (
	gorpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/api"
)

// NewServer creates a JSON-RPC server with celestia-node compatible method
// names. The returned server implements http.Handler and supports both
// HTTP and WebSocket connections.
func NewServer(svc *api.Service, log zerolog.Logger) *gorpc.RPCServer {
	srv := gorpc.NewServer()

	srv.Register("blob", &BlobHandler{svc: svc})
	srv.Register("header", &HeaderHandler{svc: svc})
	srv.Register("share", &ShareStub{})
	srv.Register("fraud", &FraudStub{})
	srv.Register("blobstream", &BlobstreamStub{})

	log.Info().Msg("JSON-RPC server initialized")
	return srv
}
