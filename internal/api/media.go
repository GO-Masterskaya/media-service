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

	callerID, err := s.resolveCaller(ctx)
	if err != nil {
		return nil, err
	}

	url, err := s.svc.GetDownloadURL(ctx, callerID, mediaID, variant)
	if err != nil {
		return nil, mapMediaError(err)
	}

	return &mediav1.GetDownloadURLResponse{
		Url:       url.URL,
		ExpiresAt: timestamppb.New(url.ExpiresAt),
	}, nil
}

// DeleteMedia — issue #18/#50 (см. main): снимает привязку media→callerID и,
// если usages_count после этого дошёл до нуля, чистит файлы и запись.
// Это НЕ безусловный hard-delete по владельцу — та семантика, которую issue
// #13/#17 изначально предлагал на это же имя, во избежание дублирования
// метода Service.DeleteMedia переехала во внутренний конвейер deleteByID
// (используется только DeleteByOwner/Reaper, наружу не выставлен) — см.
// обсуждение конфликта PR #13/#17 vs #50.
func (s *MediaServer) DeleteMedia(ctx context.Context, req *mediav1.DeleteMediaRequest) (*mediav1.DeleteMediaResponse, error) {
	if req.MediaId == "" {
		return nil, status.Error(codes.InvalidArgument, "media_id is required")
	}
	mediaID, err := uuid.Parse(req.MediaId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid media_id")
	}

	callerID, err := s.resolveCaller(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.svc.DeleteMedia(ctx, callerID, mediaID); err != nil {
		return nil, mapMediaError(err)
	}
	return &mediav1.DeleteMediaResponse{Deleted: true}, nil
}

// DeleteByOwner — issue #13, ВРЕМЕННО ОТКЛЮЧЕНО (ревью PR #13/#17).
//
// callerID здесь берётся из x-owner-id в metadata, которая НИКЕМ не
// валидируется — auth interceptor ещё не написан (см. TODO #5 в конфиге).
// Для одиночного DeleteMedia это принятый в v1 риск (см. ТЗ §5, как и у
// GetDownloadURL): максимум один чужой объект за раз. Для DeleteByOwner —
// необратимое МАССОВОЕ удаление — тот же риск неприемлем: злоумышленнику
// достаточно подставить чужой uuid и в заголовок, и в тело запроса, чтобы
// стереть чужую медиатеку целиком.
//
// Доменная логика (Service.DeleteByOwner) и её тесты (delete_test.go)
// оставлены нетронутыми — RPC включится обратно, как только появится
// настоящая проверка токена (#5). До тех пор — Unimplemented.
func (s *MediaServer) DeleteByOwner(ctx context.Context, req *mediav1.DeleteByOwnerRequest) (*mediav1.DeleteByOwnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteByOwner is disabled until request-level auth validation ships (#5): x-owner-id is currently caller-supplied and unverified, unsafe for irreversible bulk delete")
}

// mapMediaError — доменные ошибки media.* → gRPC status (Upload/Download/…).
func mapMediaError(err error) error {
	switch {
	case errors.Is(err, media.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, media.ErrAccessDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, media.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, media.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, media.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		// GetDownloadURL уже может вернуть готовый status.Error.
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, err.Error())
	}
}

// resolveCaller — единая точка решения strict/non-strict.
func (s *MediaServer) resolveCaller(ctx context.Context) (uuid.UUID, error) {
	callerID, err := callerIDFromMetadata(ctx)
	if err != nil {
		return uuid.Nil, err // только InvalidArgument
	}
	if callerID == uuid.Nil && s.strictOwnerCheck {
		return uuid.Nil, status.Error(codes.Unauthenticated, "strict owner check enabled: missing trusted caller identity")
	}
	return callerID, nil
}
