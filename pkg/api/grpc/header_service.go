package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/evstack/apex/pkg/api"
	pb "github.com/evstack/apex/pkg/api/grpc/gen/apex/v1"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// HeaderServiceServer implements the HeaderService gRPC interface.
type HeaderServiceServer struct {
	pb.UnimplementedHeaderServiceServer
	svc *api.Service
}

func (s *HeaderServiceServer) GetByHeight(ctx context.Context, req *pb.GetByHeightRequest) (*pb.GetByHeightResponse, error) {
	hdr, err := s.svc.Store().GetHeader(ctx, req.Height)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "header at height %d not found", req.Height)
		}
		return nil, status.Errorf(codes.Internal, "get header: %v", err)
	}
	return &pb.GetByHeightResponse{Header: headerToProto(hdr)}, nil
}

func (s *HeaderServiceServer) LocalHead(ctx context.Context, _ *pb.LocalHeadRequest) (*pb.LocalHeadResponse, error) {
	ss, err := s.svc.Store().GetSyncState(ctx)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "no sync state available")
		}
		return nil, status.Errorf(codes.Internal, "get sync state: %v", err)
	}
	hdr, err := s.svc.Store().GetHeader(ctx, ss.LatestHeight)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "header at height %d not found", ss.LatestHeight)
		}
		return nil, status.Errorf(codes.Internal, "get header: %v", err)
	}
	return &pb.LocalHeadResponse{Header: headerToProto(hdr)}, nil
}

func (s *HeaderServiceServer) NetworkHead(ctx context.Context, _ *pb.NetworkHeadRequest) (*pb.NetworkHeadResponse, error) {
	hdr, err := s.svc.Fetcher().GetNetworkHead(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get network head: %v", err)
	}
	return &pb.NetworkHeadResponse{Header: headerToProto(hdr)}, nil
}

func (s *HeaderServiceServer) Subscribe(_ *pb.HeaderServiceSubscribeRequest, stream grpc.ServerStreamingServer[pb.HeaderServiceSubscribeResponse]) error {
	sub, err := s.svc.HeaderSubscribe()
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
			if ev.Header != nil {
				if err := stream.Send(&pb.HeaderServiceSubscribeResponse{Header: headerToProto(ev.Header)}); err != nil {
					return err
				}
			}
		}
	}
}

func headerToProto(h *types.Header) *pb.Header {
	return &pb.Header{
		Height:    h.Height,
		Hash:      h.Hash,
		DataHash:  h.DataHash,
		Time:      timestamppb.New(h.Time),
		RawHeader: h.RawHeader,
	}
}
