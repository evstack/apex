package grpcapi

import (
	"bytes"
	"context"
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

func (s *BlobServiceServer) Get(ctx context.Context, req *pb.GetBlobRequest) (*pb.GetBlobResponse, error) {
	ns, err := bytesToNamespace(req.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid namespace: %v", err)
	}

	blobs, err := s.svc.Store().GetBlobs(ctx, ns, req.Height, req.Height, 0, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get blobs: %v", err)
	}

	for i := range blobs {
		if bytes.Equal(blobs[i].Commitment, req.Commitment) {
			return &pb.GetBlobResponse{Blob: blobToProto(&blobs[i])}, nil
		}
	}

	return nil, status.Error(codes.NotFound, store.ErrNotFound.Error())
}

func (s *BlobServiceServer) GetAll(ctx context.Context, req *pb.GetAllBlobsRequest) (*pb.GetAllBlobsResponse, error) {
	const maxNamespaces = 16
	if len(req.Namespaces) > maxNamespaces {
		return nil, status.Errorf(codes.InvalidArgument, "too many namespaces: %d (max %d)", len(req.Namespaces), maxNamespaces)
	}

	nsList := make([]types.Namespace, len(req.Namespaces))
	for i, nsBytes := range req.Namespaces {
		ns, err := bytesToNamespace(nsBytes)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid namespace %d: %v", i, err)
		}
		nsList[i] = ns
	}

	var allBlobs []types.Blob
	for _, ns := range nsList {
		blobs, err := s.svc.Store().GetBlobs(ctx, ns, req.Height, req.Height, int(req.Limit), int(req.Offset))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get blobs: %v", err)
		}
		allBlobs = append(allBlobs, blobs...)
	}

	pbBlobs := make([]*pb.Blob, len(allBlobs))
	for i := range allBlobs {
		pbBlobs[i] = blobToProto(&allBlobs[i])
	}

	return &pb.GetAllBlobsResponse{Blobs: pbBlobs}, nil
}

func (s *BlobServiceServer) Subscribe(req *pb.SubscribeBlobsRequest, stream grpc.ServerStreamingServer[pb.SubscribeBlobsResponse]) error {
	ns, err := bytesToNamespace(req.Namespace)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid namespace: %v", err)
	}

	sub := s.svc.Notifier().Subscribe([]types.Namespace{ns})
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
			pbBlobs := make([]*pb.Blob, len(ev.Blobs))
			for i := range ev.Blobs {
				pbBlobs[i] = blobToProto(&ev.Blobs[i])
			}
			if err := stream.Send(&pb.SubscribeBlobsResponse{
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
