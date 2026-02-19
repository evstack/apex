package grpcapi

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/evstack/apex/pkg/api"
	pb "github.com/evstack/apex/pkg/api/grpc/gen/apex/v1"
	"github.com/evstack/apex/pkg/types"
)

// HeaderServiceServer implements the HeaderService gRPC interface.
type HeaderServiceServer struct {
	pb.UnimplementedHeaderServiceServer
	svc *api.Service
}

func (s *HeaderServiceServer) GetByHeight(ctx context.Context, req *pb.GetHeaderRequest) (*pb.Header, error) {
	hdr, err := s.svc.Store().GetHeader(ctx, req.Height)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get header: %v", err)
	}
	return headerToProto(hdr), nil
}

func (s *HeaderServiceServer) LocalHead(ctx context.Context, _ *emptypb.Empty) (*pb.Header, error) {
	ss, err := s.svc.Store().GetSyncState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get sync state: %v", err)
	}
	hdr, err := s.svc.Store().GetHeader(ctx, ss.LatestHeight)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get header: %v", err)
	}
	return headerToProto(hdr), nil
}

func (s *HeaderServiceServer) NetworkHead(ctx context.Context, _ *emptypb.Empty) (*pb.Header, error) {
	hdr, err := s.svc.Fetcher().GetNetworkHead(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get network head: %v", err)
	}
	return headerToProto(hdr), nil
}

func (s *HeaderServiceServer) Subscribe(_ *pb.SubscribeHeadersRequest, stream grpc.ServerStreamingServer[pb.Header]) error {
	sub := s.svc.HeaderSubscribe()
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
				if err := stream.Send(headerToProto(ev.Header)); err != nil {
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
