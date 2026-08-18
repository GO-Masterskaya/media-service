package api

import (
	"context"
	"errors"

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
	svc              *media.Service
	strictOwnerCheck bool
}

func NewMediaServer(svc *media.Service, strictOwnerCheck bool) *MediaServer {
	return &MediaServer{
		svc:              svc,
		strictOwnerCheck: strictOwnerCheck,
	}
}

// callerIDFromMetadata извлекает owner_id из gRPC metadata.
// При отсутствии metadata или x-owner-id возвращает uuid.Nil, nil —
// проверка обязательности лежит на вызывающем (strictOwnerCheck).
//
// По ТЗ v1 сервис не валидирует токен — он доверяет вызывающему (gateway/mesh).
// x-owner-id должен инжектиться доверенным посредником; прямой доступ клиента
// к gRPC media-service исключён архитектурно. Полноценная валидация auth —
// TODO (#5, флаг GRPC_AUTH_VALIDATE).
func callerIDFromMetadata(ctx context.Context) (uuid.UUID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.Nil, nil
	}
	vals := md.Get("x-owner-id")
	if len(vals) == 0 {	
		return uuid.Nil, nil
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
		return nil, err // только InvalidArgument
	}
	if callerID == uuid.Nil && s.strictOwnerCheck {
		return nil, status.Error(codes.Unauthenticated, "strict owner check enabled: missing trusted caller identity")
	}

	url, err := s.svc.GetDownloadURL(ctx, callerID, mediaID, variant)
	if err != nil {
		if errors.Is(err, media.ErrAccessDenied) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		return nil, err
	}

	return &mediav1.GetDownloadURLResponse{
		Url:       url.URL,
		ExpiresAt: timestamppb.New(url.ExpiresAt),
	}, nil
}
