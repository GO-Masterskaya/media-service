package api

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"mediaservice/internal/media"
	"mediaservice/internal/storage"
	mediav1 "mediaservice/proto/media/v1"
)

type MediaServer struct {
	mediav1.UnimplementedMediaServiceServer
	svc *media.Service
}

func NewMediaServer(svc *media.Service) *MediaServer {
	return &MediaServer{svc: svc}
}

// callerIDFromMetadata извлекает owner_id из gRPC metadata.
// По ТЗ v1 сервис не валидирует токен, но использует identity из metadata
// для проверки доступа к объекту (IDOR-fix).
func callerIDFromMetadata(ctx context.Context) (uuid.UUID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.Nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("x-owner-id")
	if len(vals) == 0 {
		return uuid.Nil, status.Error(codes.Unauthenticated, "missing owner_id")
	}
	id, err := uuid.Parse(vals[0])
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid owner_id")
	}
	return id, nil
}

func (s *MediaServer) GetDownloadURL(ctx context.Context, req *mediav1.GetDownloadURLRequest) (*mediav1.GetDownloadURLResponse, error) {
	if req.MediaId == "" {
		return nil, status.Error(codes.InvalidArgument, "media_id is required")
	}

	mediaID, err := uuid.Parse(req.MediaId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid media_id")
	}

	variant := storage.Variant(req.Variant)
	if variant == "" {
		variant = storage.VariantOriginal
	}

	switch variant {
	case storage.VariantOriginal, storage.VariantThumb, storage.VariantR720:
		// ok
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid variant: %s", req.Variant)
	}

	callerID, err := callerIDFromMetadata(ctx)
	if err != nil {
		return nil, err
	}

	url, err := s.svc.GetDownloadURL(ctx, callerID, mediaID, variant)
	if err != nil {
		return nil, err
	}

	return &mediav1.GetDownloadURLResponse{
		Url:       url.URL,
		ExpiresAt: timestamppb.New(url.ExpiresAt),
	}, nil
}
