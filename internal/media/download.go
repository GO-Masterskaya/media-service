// Package media содержит доменную логику медиа-сервиса.
// Полный сервис и остальные методы (upload, delete и т.д.) — задачи #9–#13.
// repo.Media/repo.Derivative - заглушки, ожидаются в #10).
package media

import (
	"context"
	"errors"
	"fmt"
	"io"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"

	"github.com/google/uuid"
)

var (
	ErrNotFound           = errors.New("media not found")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrFailedPrecondition = errors.New("media not ready")
)

// ChunkSender отправляет один чанк данных в транспорт.
// Реализация предоставляется gRPC handler'ом (internal/api).
type ChunkSender func([]byte) error

// Service содержит доменную логику медиа.
// Полный конструктор и методы нужно добавить по мере реализации задач #9–#13.
type Service struct {
	mediaRepo repo.MediaRepo
	derivRepo repo.DerivativeRepo
	storage   storage.Interface
}

func NewService(mediaRepo repo.MediaRepo, derivRepo repo.DerivativeRepo, storage storage.Interface) *Service {
	return &Service{
		mediaRepo: mediaRepo,
		derivRepo: derivRepo,
		storage:   storage,
	}
}

// DownloadStream отдаёт содержимое объекта чанками.
// Все проверки (media, variant, доступность) выполняются ДО первого вызова sender.
func (s *Service) DownloadStream(ctx context.Context, mediaID uuid.UUID, variant string, sender ChunkSender) error {
	if mediaID == uuid.Nil {
		return fmt.Errorf("media_id is required: %w", ErrInvalidArgument)
	}
	if variant == "" {
		variant = "original"
	}

	// 1. Проверяем метаданные медиа в БД (#10).
	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return fmt.Errorf("media %s: %w", mediaID, ErrNotFound)
		}
		return fmt.Errorf("get media: %w", err)
	}

	// 2. Определяем storage key.
	key, err := s.resolveKey(ctx, media, variant)
	if err != nil {
		return err
	}

	// 3. Открываем объект в хранилище (#7).
	rc, err := s.storage.GetObject(ctx, key)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer rc.Close()

	// 4. Потоковая передача чанками. Файл не загружается целиком в RAM.
	return s.streamChunks(ctx, rc, sender)
}

// resolveKey определяет storage key в зависимости от варианта и проверяет статусы.
func (s *Service) resolveKey(ctx context.Context, media *repo.Media, variant string) (string, error) {
	switch variant {
	case "original":
		if media.Status == repo.MediaStatusDeleting || media.Status == repo.MediaStatusFailed {
			return "", fmt.Errorf("media status %q: %w", media.Status, ErrFailedPrecondition)
		}
		if media.StorageKey == "" {
			return "", fmt.Errorf("storage key empty for original: %w", ErrNotFound)
		}
		return media.StorageKey, nil
	case "thumbnail", "r_720", "preview":
		deriv, err := s.derivRepo.GetByMediaAndVariant(ctx, media.ID, variant)
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return "", fmt.Errorf("derivative %q: %w", variant, ErrNotFound)
			}
			return "", fmt.Errorf("get derivative: %w", err)
		}
		if deriv.StorageKey == "" {
			return "", fmt.Errorf("storage key empty for %q: %w", variant, ErrNotFound)
		}
		return deriv.StorageKey, nil
	default:
		return "", fmt.Errorf("unknown variant %q: %w", variant, ErrInvalidArgument)
	}
}

// streamChunks читает объект чанками и передаёт их в sender.
func (s *Service) streamChunks(ctx context.Context, rc io.ReadCloser, sender ChunkSender) error {
	// TODO: размер чанка потенциально можно вынести в env
	const chunkSize = 64 * 1024 // 64 KB
	buf := make([]byte, chunkSize)

	for {
		// Быстрая реакция на отмену клиента между чтениями.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		n, err := rc.Read(buf)
		if n > 0 {
			// Отправляем чанк. Блокировка sender обеспечивает backpressure:
			// если клиент не успевает читать, Send ждёт.
			// Per-caller stream limit обеспечивается interceptor'ом задачи #21.
			if sendErr := sender(buf[:n]); sendErr != nil {
				return fmt.Errorf("send chunk: %w", sendErr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			// Если объект отсутствует в MinIO, ошибка вылезает здесь.
			// При n==0 это отклонение до начала ответа (Send ещё не вызывался).
			return fmt.Errorf("read object: %w", err)
		}
	}
	return nil
}
