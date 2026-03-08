package grpcapi

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evstack/apex/pkg/api"
	pb "github.com/evstack/apex/pkg/api/grpc/gen/apex/v1"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// BlobServiceServer implements the BlobService gRPC interface.
type BlobServiceServer struct {
	pb.UnimplementedBlobServiceServer
	svc *api.Service
}

func (s *BlobServiceServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	ns, err := bytesToNamespace(req.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid namespace: %v", err)
	}

	b, err := s.svc.GetBlob(ctx, req.GetHeight(), ns, req.GetCommitment())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Error(codes.NotFound, store.ErrNotFound.Error())
		}
		return nil, status.Errorf(codes.Internal, "get blob: %v", err)
	}
	return &pb.GetResponse{Blob: blobToProto(b)}, nil
}

func (s *BlobServiceServer) GetByCommitment(ctx context.Context, req *pb.GetByCommitmentRequest) (*pb.GetByCommitmentResponse, error) {
	if len(req.GetCommitment()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "commitment is required")
	}

	b, err := s.svc.GetBlobByCommitment(ctx, req.GetCommitment())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Error(codes.NotFound, store.ErrNotFound.Error())
		}
		return nil, status.Errorf(codes.Internal, "get blob by commitment: %v", err)
	}

	return &pb.GetByCommitmentResponse{Blob: blobToProto(b)}, nil
}

func (s *BlobServiceServer) GetAll(ctx context.Context, req *pb.GetAllRequest) (*pb.GetAllResponse, error) {
	const maxNamespaces = 16
	if len(req.GetNamespaces()) > maxNamespaces {
		return nil, status.Errorf(codes.InvalidArgument, "too many namespaces: %d (max %d)", len(req.GetNamespaces()), maxNamespaces)
	}

	nsList := make([]types.Namespace, len(req.GetNamespaces()))
	for i, nsBytes := range req.GetNamespaces() {
		ns, err := bytesToNamespace(nsBytes)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid namespace %d: %v", i, err)
		}
		nsList[i] = ns
	}

	allBlobs, err := s.svc.GetAllBlobs(ctx, req.GetHeight(), nsList, int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get blobs: %v", err)
	}

	pbBlobs := make([]*pb.Blob, len(allBlobs))
	for i := range allBlobs {
		pbBlobs[i] = blobToProto(&allBlobs[i])
	}

	return &pb.GetAllResponse{Blobs: pbBlobs}, nil
}

func (s *BlobServiceServer) Subscribe(req *pb.BlobServiceSubscribeRequest, stream grpc.ServerStreamingServer[pb.BlobServiceSubscribeResponse]) error {
	ns, err := bytesToNamespace(req.GetNamespace())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid namespace: %v", err)
	}

	sub, err := s.svc.Notifier().Subscribe([]types.Namespace{ns})
	if err != nil {
		return status.Errorf(codes.ResourceExhausted, "subscribe: %v", err)
	}
	defer s.svc.Notifier().Unsubscribe(sub)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-sub.Events():
			if !ok {
				return nil
			}
			if len(ev.Blobs) == 0 {
				continue
			}
			pbBlobs := make([]*pb.Blob, len(ev.Blobs))
			for i := range ev.Blobs {
				pbBlobs[i] = blobToProto(&ev.Blobs[i])
			}
			if err := stream.Send(&pb.BlobServiceSubscribeResponse{
				Height: ev.Height,
				Blobs:  pbBlobs,
			}); err != nil {
				return err
			}
		}
	}
}

func blobToProto(b *types.Blob) *pb.Blob {
	return &pb.Blob{
		Height:       b.Height,
		Namespace:    b.Namespace[:],
		Data:         b.Data,
		Commitment:   b.Commitment,
		ShareVersion: b.ShareVersion,
		Signer:       b.Signer,
		Index:        int32(b.Index),
	}
}

func bytesToNamespace(b []byte) (types.Namespace, error) {
	if len(b) != types.NamespaceSize {
		return types.Namespace{}, fmt.Errorf("expected %d bytes, got %d", types.NamespaceSize, len(b))
	}
	var ns types.Namespace
	copy(ns[:], b)
	return ns, nil
}
