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
	svc              *media.Service
	strictOwnerCheck bool
	deleteBatchSize  int
}

func NewMediaServer(svc *media.Service, strictOwnerCheck bool, deleteBatchSize int) *MediaServer {
	return &MediaServer{
		svc:              svc,
		strictOwnerCheck: strictOwnerCheck,
		deleteBatchSize:  deleteBatchSize,
	}
}

// callerIDFromMetadata извлекает owner_id из gRPC metadata.
//
// По ТЗ v1 сервис не валидирует токен — он доверяет вызывающему (gateway/mesh).
// x-owner-id должен инжектиться доверенным посредником; прямой доступ клиента
// к gRPC media-service исключён архитектурно. Полноценная валидация auth —
// TODO (#5, флаг GRPC_AUTH_VALIDATE).
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
		if s.strictOwnerCheck {
			// В strict mode gateway ОБЯЗАН прокидывать x-owner-id.
			// Если нет — значит deploy-модель нарушена.
			return nil, status.Error(codes.Unauthenticated, "strict owner check enabled: missing trusted caller identity")
		}
		// ТЗ v1: без strict mode пропускаем (но owner-проверка в сервисе всё равно отрежет IDOR).
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

// DeleteMedia — issue #13. Владелец берётся из доверенной metadata (см.
// callerIDFromMetadata) и сверяется с owner_id media внутри media.Service.
func (s *MediaServer) DeleteMedia(ctx context.Context, req *mediav1.DeleteMediaRequest) (*mediav1.DeleteMediaResponse, error) {
	if req.MediaId == "" {
		return nil, status.Error(codes.InvalidArgument, "media_id is required")
	}
	mediaID, err := uuid.Parse(req.MediaId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid media_id")
	}

	callerID, err := callerIDFromMetadata(ctx)
	if err != nil {
		if s.strictOwnerCheck {
			return nil, status.Error(codes.Unauthenticated, "strict owner check enabled: missing trusted caller identity")
		}
		return nil, err
	}

	if err := s.svc.DeleteMedia(ctx, callerID, mediaID); err != nil {
		return nil, err
	}
	return &mediav1.DeleteMediaResponse{Deleted: true}, nil
}

// DeleteByOwner — issue #13. Каскадно удаляет всё media вызывающего owner'а.
// v1 разрешает удалять только СВОИ данные (callerID == owner_id из запроса) —
// административное удаление чужого owner_id вне области ТЗ §5 (auth transport
// v1 не валидируется, доверяем gateway, а не отдельному admin-flow).
func (s *MediaServer) DeleteByOwner(ctx context.Context, req *mediav1.DeleteByOwnerRequest) (*mediav1.DeleteByOwnerResponse, error) {
	if req.OwnerId == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_id is required")
	}
	ownerID, err := uuid.Parse(req.OwnerId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid owner_id")
	}

	callerID, err := callerIDFromMetadata(ctx)
	if err != nil {
		if s.strictOwnerCheck {
			return nil, status.Error(codes.Unauthenticated, "strict owner check enabled: missing trusted caller identity")
		}
		return nil, err
	}
	if callerID != ownerID {
		return nil, status.Error(codes.PermissionDenied, "access denied")
	}

	count, err := s.svc.DeleteByOwner(ctx, ownerID, s.deleteBatchSize)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &mediav1.DeleteByOwnerResponse{DeletedCount: uint32(count)}, nil
}
