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

// DownloadStream отдаёт содержимое объекта чанками.
// Все проверки (media, variant, доступность) выполняются ДО первого вызова sender.
func (s *Service) DownloadStream(ctx context.Context, callerID uuid.UUID, mediaID uuid.UUID, variant string, sender ChunkSender) (err error) {
	rc, err := s.OpenMedia(ctx, callerID, mediaID, variant)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close object reader: %w", closeErr)
		}
	}()

	// Потоковая передача чанками. Файл не загружается целиком в RAM.
	return s.streamChunks(ctx, rc, sender)
}

// OpenMedia открывает содержимое медиаобъекта для чтения.
//
// Все проверки - существование записи, права вызывающего, доступность
// варианта - выполняются до возврата. Непустой io.ReadCloser означает, что
// доступ уже подтверждён: ошибка при чтении будет относиться к сети или
// хранилищу, а не к правам.
//
// Закрыть поток обязан вызывающий.
//
// Пустой variant трактуется как оригинал.
func (s *Service) OpenMedia(ctx context.Context, callerID uuid.UUID, mediaID uuid.UUID, variant string) (io.ReadCloser, error) {
	if mediaID == uuid.Nil {
		return nil, fmt.Errorf("media_id is required: %w", ErrInvalidArgument)
	}
	if variant == "" {
		variant = string(storage.VariantOriginal)
	}

	// 1. Проверяем метаданные медиа в БД.
	media, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, fmt.Errorf("media %s: %w", mediaID, ErrNotFound)
		}
		return nil, fmt.Errorf("get media: %w", err)
	}

	// 2. Проверяем владельца до открытия объекта.
	if callerID != uuid.Nil && media.OwnerID != callerID {
		return nil, ErrAccessDenied
	}

	// 3. Определяем storage key.
	key, err := s.resolveKey(ctx, media, variant)
	if err != nil {
		return nil, err
	}

	// 4. Открываем объект в хранилище.
	return s.storage.GetObject(ctx, key)
}

// resolveKey определяет storage key в зависимости от варианта и проверяет статусы.
func (s *Service) resolveKey(ctx context.Context, media *repo.Media, variant string) (string, error) {
	switch variant {
	case string(storage.VariantOriginal):
		if media.Status == repo.MediaStatusDeleting || media.Status == repo.MediaStatusFailed {
			return "", fmt.Errorf("media status %q: %w", media.Status, ErrFailedPrecondition)
		}
		if media.StorageKey == "" {
			return "", fmt.Errorf("storage key empty for original: %w", ErrNotFound)
		}
		return media.StorageKey, nil
	case string(storage.VariantThumb), string(storage.VariantR360), string(storage.VariantR720), string(storage.VariantPreview):
		if media.Status != repo.MediaStatusReady {
			return "", fmt.Errorf("media status %q: %w", media.Status, ErrFailedPrecondition)
		}
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
