package grpcapi

import (
	"github.com/rs/zerolog"
	"google.golang.org/grpc"

	"github.com/evstack/apex/pkg/api"
	pb "github.com/evstack/apex/pkg/api/grpc/gen/apex/v1"
)

// NewServer creates a gRPC server with blob and header services registered.
func NewServer(svc *api.Service, log zerolog.Logger) *grpc.Server {
	srv := grpc.NewServer()

	pb.RegisterBlobServiceServer(srv, &BlobServiceServer{svc: svc})
	pb.RegisterHeaderServiceServer(srv, &HeaderServiceServer{svc: svc})

	log.Info().Msg("gRPC server initialized")
	return srv
}
