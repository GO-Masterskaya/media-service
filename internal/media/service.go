package media

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

type Service struct {
	mediaRepo  repo.MediaRepo
	derivRepo  repo.DerivativeRepo
	storage    storage.Interface
	presignTTL time.Duration
}

func NewService(
	mediaRepo repo.MediaRepo,
	derivRepo repo.DerivativeRepo,
	storage storage.Interface,
	presignTTL time.Duration,
) *Service {
	return &Service{
		mediaRepo:  mediaRepo,
		derivRepo:  derivRepo,
		storage:    storage,
		presignTTL: presignTTL,
	}
}

// GetDownloadURL возвращает presigned URL для original/thumbnail/r_720.
// Ошибки:
//   - NotFound        — нет media или нет derivative для запрошенного variant.
//   - FailedPrecondition — вариант существует, но не готов (media failed/deleting/не ready).
func (s *Service) GetDownloadURL(ctx context.Context, mediaID uuid.UUID, variant storage.Variant) (*storage.PresignedURL, error) {
	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "media not found")
		}
		return nil, status.Errorf(codes.Internal, "get media: %v", err)
	}

	var storageKey string

	switch variant {
	case storage.VariantOriginal:
		// Original есть в хранилище сразу после upload, но недоступен
		// если медиа в процессе жёсткого удаления или упала навсегда.
		switch media.Status {
		case repo.MediaStatusFailed, repo.MediaStatusDeleting:
			return nil, status.Errorf(codes.FailedPrecondition, "media status is %s", media.Status)
		}
		storageKey = media.StorageKey

	case storage.VariantThumb, storage.VariantR720, storage.VariantPreview, storage.VariantR360:
		// Производные появляются только при статусе ready.
		if media.Status != repo.MediaStatusReady {
			return nil, status.Errorf(codes.FailedPrecondition, "media not ready, status: %s", media.Status)
		}
		deriv, err := s.derivRepo.GetByMediaAndVariant(ctx, mediaID, string(variant))
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "derivative %s not found", variant)
			}
			return nil, status.Errorf(codes.Internal, "get derivative: %v", err)
		}
		storageKey = deriv.StorageKey

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported variant: %s", variant)
	}

	if storageKey == "" {
		return nil, status.Error(codes.Internal, "empty storage key")
	}

	presigned, err := s.storage.PresignGetObject(ctx, storageKey, s.presignTTL)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "presign: %v", err)
	}
	return presigned, nil
}
