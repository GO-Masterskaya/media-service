package media

import (
	"context"
	"errors"
	"log/slog"
	"path"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

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

func (s *Service) GetDownloadURL(ctx context.Context, callerID uuid.UUID, mediaID uuid.UUID, variant storage.Variant) (*storage.PresignedURL, error) {
	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, status.Error(codes.NotFound, ErrNotFound.Error())
		}
		s.log.Error("get media failed", slog.Any("error", err), slog.String("media_id", mediaID.String()))
		return nil, status.Error(codes.Internal, "internal error")
	}

	if callerID != uuid.Nil && media.OwnerID != callerID {
		return nil, status.Error(codes.PermissionDenied, ErrAccessDenied.Error())
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

func (s *Service) GetMedia(ctx context.Context, mediaID uuid.UUID) (*repo.Media, error) {
	m, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "media not found")
		}
		s.log.Error("get media failed", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "internal error")
	}
	return m, nil
}

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

	// Идемпотентность: уже привязано к этому owner'у — не ошибка.
	if media.OwnerID == ownerID {
		return nil
	}

	// Перепривязка чужого запрещена.
	if media.OwnerID != uuid.Nil {
		return status.Errorf(codes.PermissionDenied, "owner mismatch: media belongs to %s", media.OwnerID)
	}

	// Нельзя привязать media в неподходящем статусе.
	switch media.Status {
	case repo.MediaStatusFailed, repo.MediaStatusDeleting:
		return status.Errorf(codes.FailedPrecondition, "media not available for attach, status: %s", media.Status)
	}

	if err := s.mediaRepo.UpdateOwner(ctx, mediaID, ownerID); err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return status.Error(codes.NotFound, "media not found")
		}
		// Race guard: между GetByID и UpdateOwner другой запрос мог установить owner.
		if errors.Is(err, repo.ErrOwnerMismatch) {
			media, err2 := s.mediaRepo.GetByID(ctx, mediaID)
			if err2 != nil {
				s.log.Error("attach: race check get media failed", slog.Any("error", err2))
				return status.Error(codes.Internal, "internal error")
			}
			if media.OwnerID == ownerID {
				return nil // идемпотентность: наш owner уже записан
			}
			return status.Errorf(codes.PermissionDenied, "owner mismatch: media belongs to %s", media.OwnerID)
		}
		s.log.Error("attach: update owner failed", slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}
	return nil
}

func (s *Service) DeleteMedia(ctx context.Context, callerID, mediaID uuid.UUID) error {
	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return status.Error(codes.NotFound, "media not found")
		}
		s.log.Error("get media for delete", slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}

	if callerID != uuid.Nil && media.OwnerID != callerID {
		return status.Error(codes.PermissionDenied, "access denied")
	}

	// Нельзя удалять media, пока оно обрабатывается — worker потеряет файл.
	switch media.Status {
	case repo.MediaStatusProcessing:
		return status.Errorf(codes.FailedPrecondition, "media is processing, cannot delete")
	}

	prefix := path.Join(media.OwnerID.String(), media.ID.String()) + "/"
	if err := s.storage.DeletePrefix(ctx, prefix); err != nil {
		s.log.Error("delete prefix failed", slog.Any("error", err), slog.String("prefix", prefix))
		return status.Error(codes.Internal, "storage delete failed")
	}

	if err := s.mediaRepo.HardDelete(ctx, mediaID); err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil
		}
		s.log.Error("hard delete failed", slog.Any("error", err))
		return status.Error(codes.Internal, "db delete failed")
	}
	return nil
}
