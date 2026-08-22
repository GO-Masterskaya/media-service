package media

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// ChunkSender отправляет один чанк данных в транспорт.
// Реализация предоставляется gRPC handler'ом (internal/api).
type ChunkSender func([]byte) error

var (
	ErrNotFound           = errors.New("media not found")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrFailedPrecondition = errors.New("media not ready")
	ErrAccessDenied       = errors.New("access denied")
)

type Service struct {
	mediaRepo  repo.MediaRepo
	derivRepo  repo.DerivativeRepo
	storage    storage.Interface
	presignTTL time.Duration
	log        *slog.Logger
}

func NewService(
	mediaRepo repo.MediaRepo,
	derivRepo repo.DerivativeRepo,
	storage storage.Interface,
	presignTTL time.Duration,
	log *slog.Logger,
) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		mediaRepo:  mediaRepo,
		derivRepo:  derivRepo,
		storage:    storage,
		presignTTL: presignTTL,
		log:        log,
	}
}

// GetDownloadURL возвращает presigned URL для original/thumbnail/r_720.
// Ошибки:
//   - NotFound           — нет media или нет derivative для variant.
//   - FailedPrecondition — вариант существует, но не готов.
//   - PermissionDenied   — caller не является владельцем объекта.
//   - Internal           — ошибка хранилища/БД.
func (s *Service) GetDownloadURL(ctx context.Context, callerID uuid.UUID, mediaID uuid.UUID, variant storage.Variant) (*storage.PresignedURL, error) {
	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, status.Error(codes.NotFound, ErrNotFound.Error())
		}
		s.log.Error("get media failed", slog.Any("error", err), slog.String("media_id", mediaID.String()))
		return nil, status.Error(codes.Internal, "internal error")
	}

	// IDOR fix: проверяем владельца до любой другой логики.
	if callerID != uuid.Nil && media.OwnerID != callerID {
		return nil, ErrAccessDenied
	}

	var storageKey string

	switch variant {
	case storage.VariantOriginal:
		switch media.Status {
		case repo.MediaStatusFailed, repo.MediaStatusDeleting:
			return nil, status.Errorf(codes.FailedPrecondition, "media not available, status: %s", media.Status)
		}
		storageKey = media.StorageKey

	case storage.VariantThumb, storage.VariantR720:
		if media.Status != repo.MediaStatusReady {
			return nil, status.Errorf(codes.FailedPrecondition, "media not ready, status: %s", media.Status)
		}
		deriv, err := s.derivRepo.GetByMediaAndVariant(ctx, mediaID, string(variant))
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "derivative %s not found", variant)
			}
			s.log.Error("get derivative failed",
				slog.Any("error", err),
				slog.String("media_id", mediaID.String()),
				slog.String("variant", string(variant)),
			)
			return nil, status.Error(codes.Internal, "internal error")
		}
		storageKey = deriv.StorageKey

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported variant: %s", variant)
	}

	if storageKey == "" {
		s.log.Error("empty storage key",
			slog.String("media_id", mediaID.String()),
			slog.String("variant", string(variant)),
		)
		return nil, status.Error(codes.Internal, "internal error")
	}

	presigned, err := s.storage.PresignGetObject(ctx, storageKey, s.presignTTL)
	if err != nil {
		s.log.Error("presign failed", slog.Any("error", err), slog.String("key", storageKey))
		return nil, status.Error(codes.Internal, "internal error")
	}
	return presigned, nil
}

// AttachMedia проверяет/проставляет owner для media.
// Idempotent: повторный вызов с тем же owner = nil; другой owner = ошибка.
func (s *Service) AttachMedia(ctx context.Context, mediaID uuid.UUID, ownerID uuid.UUID) error {
	if ownerID == uuid.Nil {
		return status.Error(codes.InvalidArgument, "owner_id required")
	}

	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return status.Error(codes.NotFound, "media not found")
		}
		s.log.Error("attach: get media failed", slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}

	if media.OwnerID == uuid.Nil {
		if err := s.mediaRepo.UpdateOwner(ctx, mediaID, ownerID); err != nil {
			s.log.Error("attach: update owner failed", slog.Any("error", err))
			return status.Error(codes.Internal, "internal error")
		}
		return nil
	}

	if media.OwnerID != ownerID {
		return status.Errorf(codes.PermissionDenied, "owner mismatch: media belongs to %s", media.OwnerID)
	}

	return nil
}
