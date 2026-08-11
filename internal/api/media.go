package api

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	mediav1 "mediaservice/proto/media/v1"
	"mediaservice/internal/media"
	"mediaservice/internal/storage"
)

type MediaServer struct {
	mediav1.UnimplementedMediaServiceServer
	svc *media.Service
}

func NewMediaServer(svc *media.Service) *MediaServer {
	return &MediaServer{svc: svc}
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
		// допустимые варианты
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid variant: %s", req.Variant)
	}

	url, err := s.svc.GetDownloadURL(ctx, mediaID, variant)
	if err != nil {
		return nil, err // уже *status.Status
	}

	return &mediav1.GetDownloadURLResponse{
		Url:       url.URL,
		ExpiresAt: timestamppb.New(url.ExpiresAt),
	}, nil
}