package media

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// defaultDeleteBatchSize используется, если вызывающий код передал batchSize<=0.
const defaultDeleteBatchSize = 100

// deleteByID — низкоуровневая идемпотентная hard-delete команда (issue #13).
// Шаги: атомарно пометить deleting -> удалить объекты MinIO по префиксу
// {owner_id}/{media_id}/ -> удалить строку (derivatives уходят каскадом FK).
//
// БЕЗ проверки владельца: вызывающий код обязан сам гарантировать право на
// удаление (single-delete API проверяет владельца до вызова; DeleteByOwner и
// TTL reaper — уже owner-/TTL-scoped запросом, которым получен id).
//
// Общая точка входа для DeleteMedia, DeleteByOwner и Reaper — так удовлетворяем
// требование issue #17 "удалять истёкшие media через ту же доменную команду".
func (s *Service) deleteByID(ctx context.Context, mediaID uuid.UUID) error {
	m, found, err := s.mediaRepo.MarkDeleting(ctx, mediaID)
	if err != nil {
		return fmt.Errorf("mark deleting: %w", err)
	}
	if !found {
		return nil // идемпотентность: записи не было или уже удалена
	}

	prefix, err := storage.MediaPrefix(m.OwnerID, m.ID)
	if err != nil {
		return fmt.Errorf("build media prefix: %w", err)
	}

	if err := s.storage.DeletePrefix(ctx, prefix); err != nil {
		// Запись остаётся в status=deleting — это ОЖИДАЕМО: она видна для
		// фоновой сверки orphan/deleting (issue #24), а не потеряна.
		return fmt.Errorf("delete storage objects: %w", err)
	}

	if err := s.mediaRepo.HardDelete(ctx, mediaID); err != nil {
		return fmt.Errorf("hard delete row: %w", err)
	}
	return nil
}

// DeleteMedia — доменная команда для gRPC DeleteMedia. Проверяет владельца
// тем же паттерном, что и GetDownloadURL (fetch + сравнение OwnerID), затем
// делегирует в deleteByID.
//
// Ошибки: PermissionDenied — caller не владелец; Internal — ошибка БД/хранилища.
// Отсутствие media НЕ ошибка (idempotent success), как и требуют критерии
// приёмки issue #13.
func (s *Service) DeleteMedia(ctx context.Context, callerID, mediaID uuid.UUID) error {
	m, err := s.mediaRepo.GetByID(ctx, mediaID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil // повторный delete отсутствующего media — успех
		}
		s.log.Error("get media failed", slog.Any("error", err), slog.String("media_id", mediaID.String()))
		return status.Error(codes.Internal, "internal error")
	}

	if m.OwnerID != callerID {
		return status.Error(codes.PermissionDenied, "access denied")
	}

	if err := s.deleteByID(ctx, mediaID); err != nil {
		s.log.Error("delete media failed", slog.Any("error", err), slog.String("media_id", mediaID.String()))
		return status.Error(codes.Internal, "internal error")
	}
	return nil
}

// DeleteByOwner удаляет батчами всё media данного owner, используя ту же
// deleteByID-команду. batchSize<=0 -> defaultDeleteBatchSize.
//
// Возвращает количество ФАКТИЧЕСКИ удалённых записей в этом вызове
// (детерминированно отражает эффект вызова; повтор на том же owner вернёт
// меньше или 0 — это идемпотентность на уровне эффекта, а не одинаковое число
// при каждом вызове).
//
// Одна ошибка (например, временная недоступность MinIO для одной записи) не
// останавливает весь batch — она просто остаётся в status=deleting и не
// участвует в следующей выборке этого же вызова (WHERE status <> 'deleting'),
// что также не даёт зациклиться на одной и той же битой записи внутри вызова.
func (s *Service) DeleteByOwner(ctx context.Context, ownerID uuid.UUID, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = defaultDeleteBatchSize
	}

	deleted := 0
	for {
		ids, err := s.mediaRepo.ListDeletableByOwner(ctx, ownerID, batchSize)
		if err != nil {
			return deleted, fmt.Errorf("list deletable by owner: %w", err)
		}
		if len(ids) == 0 {
			return deleted, nil
		}

		progressed := 0
		for _, id := range ids {
			if err := s.deleteByID(ctx, id); err != nil {
				s.log.Error("delete by owner: item failed",
					slog.Any("error", err),
					slog.String("owner_id", ownerID.String()),
					slog.String("media_id", id.String()),
				)
				continue
			}
			deleted++
			progressed++
		}

		if progressed == 0 {
			// Ни одна запись из страницы не удалилась (например, БД/MinIO
			// стабильно недоступны) — прекращаем, чтобы не уйти в бесконечный
			// цикл повторной выборки той же страницы.
			return deleted, fmt.Errorf("delete by owner: no progress in batch, aborting")
		}
		if len(ids) < batchSize {
			return deleted, nil // последняя (неполная) страница
		}
	}
}
