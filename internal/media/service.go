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
	// ErrAlreadyExists — тот же (owner_id, idempotency_key) с другим fingerprint
	// (или media_id занят другим idempotency_key). gRPC: codes.AlreadyExists.
	ErrAlreadyExists = errors.New("already exists")
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

// GetMedia возвращает метаданные медиаобъекта.
//
// Проверка владельца применяется так же, как в GetDownloadURL: при нулевом
// callerID она пропускается - это анонимный режим, включаемый отсутствием
// STRICT_OWNER_CHECK. Непустой callerID, не совпадающий с владельцем, даёт
// PermissionDenied.
//
// Деривативы не возвращаются: они лежат в отдельной таблице и читаются
// отдельным запросом.
func (s *Service) GetMedia(ctx context.Context, callerID, mediaID uuid.UUID) (*repo.Media, error) {
	m, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "media not found")
		}
		s.log.Error("get media failed", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "internal error")
	}

	if callerID != uuid.Nil && m.OwnerID != callerID {
		return nil, status.Error(codes.PermissionDenied, ErrAccessDenied.Error())
	}
	return m, nil
}

// AttachMedia создаёт привязку media к owner через таблицу media_attachments.
// Идемпотентна: повторный attach того же owner возвращает nil.
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

	// Guard: нельзя привязать media в неподходящем статусе.
	switch media.Status {
	case repo.MediaStatusFailed, repo.MediaStatusDeleting:
		return status.Errorf(codes.FailedPrecondition, "media not available for attach, status: %s", media.Status)
	}

	// Атомарно: INSERT в media_attachments + UPDATE usages_count.
	// AttachMedia — исправленный блок CreateAttachment
	if err := s.mediaRepo.CreateAttachment(ctx, mediaID, ownerID); err != nil {
		if errors.Is(err, repo.ErrMediaDeleting) {
			return status.Error(codes.FailedPrecondition, "media is being deleted")
		}
		if errors.Is(err, repo.ErrNotFound) {
			return status.Error(codes.NotFound, "media not found")
		}
		s.log.Error("attach: create attachment failed", slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}
	return nil
}

// DeleteMedia удаляет привязку media→callerID и, если usages_count становится 0,
// callerID обязателен: nil вызовет InvalidArgument. Force delete (без проверки
// привязок) доступен только через repo напрямую, не через этот метод.
func (s *Service) DeleteMedia(ctx context.Context, callerID, mediaID uuid.UUID) error {
	if callerID == uuid.Nil {
		return status.Error(codes.InvalidArgument, "caller_id required")
	}
	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return status.Error(codes.NotFound, "media not found")
		}
		s.log.Error("get media for delete", slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}

	// Guard: нельзя удалять media, пока оно обрабатывается — worker потеряет файл.
	switch media.Status {
	case repo.MediaStatusProcessing:
		return status.Errorf(codes.FailedPrecondition, "media is processing, cannot delete")
	}

	// Удаляем конкретную привязку. Если её нет — NotFound (handler превратит в nil).
	usages, err := s.mediaRepo.DeleteAttachment(ctx, mediaID, callerID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return status.Error(codes.NotFound, "attachment not found")
		}
		s.log.Error("delete: delete attachment failed", slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}

	// Media ещё используется другими сущностями — файлы и запись не трогаем.
	if usages > 0 {
		s.log.Info("media still in use after detach",
			slog.String("media_id", mediaID.String()),
			slog.Int("usages", usages),
		)
		return nil
	}

	// usages == 0 — DeleteAttachment уже установил status = 'deleting' в транзакции.
	// Новые attach'и заблокированы. Чистим файлы и БД.
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
