package media

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// defaultDeleteBatchSize используется, если вызывающий код передал batchSize<=0.
const defaultDeleteBatchSize = 100

// deleteByID — низкоуровневая идемпотентная hard-delete команда (issue #13).
// Шаги: атомарно claim'ить deleting -> удалить объекты MinIO по префиксу
// {owner_id}/{media_id}/ -> удалить строку (derivatives уходят каскадом FK).
//
// resumeStuck определяет поведение при repo.ClaimAlreadyDeleting (запись уже
// в status=deleting — из чужого claim'а ИЛИ из прошлой прерванной попытки,
// на уровне одного запроса это неразличимо, см. MediaRepo.MarkDeleting):
//   - true  — довести очистку до конца самому. Подходит для явного,
//     единичного, инициированного пользователем повтора (DeleteMedia,
//     DeleteByOwner): если это ЕГО собственная зависшая запись, повтор
//     должен реально дочистить, а не молча вернуть "успех" без изменений.
//   - false — пропустить, ничего не трогать. Обязательно для периодических
//     фоновых обходов (Reaper): несколько реплик сервиса могут одновременно
//     наткнуться на один и тот же истёкший id. Если бы reaper тоже доводил
//     ClaimAlreadyDeleting до конца, любая реплика, не выигравшая исходный
//     claim, всё равно повторила бы DeletePrefix+HardDelete — гонка и потеря
//     дедупликации между репликами. С resumeStuck=false claim этого тика
//     получает ровно одна реплика; отставшая просто выходит. Зависшие
//     (осиротевшие) deleting-записи — зона ответственности фоновой сверки
//     (#24, ListDeleting), а не reaper'а.
//
// БЕЗ проверки владельца: вызывающий код обязан сам гарантировать право на
// удаление (single-delete API проверяет владельца до вызова; DeleteByOwner и
// TTL reaper — уже owner-/TTL-scoped запросом, которым получен id).
func (s *Service) deleteByID(ctx context.Context, mediaID uuid.UUID, resumeStuck bool) error {
	m, claim, err := s.mediaRepo.MarkDeleting(ctx, mediaID)
	if err != nil {
		return fmt.Errorf("mark deleting: %w", err)
	}
	switch claim {
	case repo.ClaimNone:
		return nil // идемпотентность: записи не было
	case repo.ClaimAlreadyDeleting:
		if !resumeStuck {
			return nil // не наш claim — не трогаем (см. doc-комментарий выше)
		}
		// иначе — доводим очистку до конца ниже, как и для ClaimTaken.
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

	if err := s.mediaRepo.HardDelete(ctx, mediaID); err != nil && !errors.Is(err, repo.ErrNotFound) {
		// ErrNotFound здесь ожидаем: фоновая сверка (#24) могла удалить эту же
		// зависшую deleting-запись параллельно — это идемпотентный успех, а не
		// ошибка.
		return fmt.Errorf("hard delete row: %w", err)
	}
	return nil
}

// PurgeMedia больше не выставлена как отдельный публичный метод: конфликт
// имени Service.DeleteMedia с семантикой "снять привязку, удалить при
// usages_count==0" из issue #18/#50 разрешён в пользу той версии (см.
// внизу service.go) — она уже стала публичным gRPC DeleteMedia и вызывается
// из Kafka-detach хендлера. Наш безусловный hard-delete-по-владельцу остаётся
// полностью внутренним конвейером (deleteByID выше), используемым только
// DeleteByOwner и Reaper — оба и так вызывают deleteByID напрямую, без
// отдельной публичной обёртки.

// DeleteByOwner удаляет батчами всё media данного owner, используя ту же
// deleteByID-команду (resumeStuck=true — как и DeleteMedia, это явный
// единичный вызов, а не периодический фоновый обход). batchSize<=0 ->
// defaultDeleteBatchSize.
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
		select {
		case <-ctx.Done():
			return deleted, ctx.Err()
		default:
		}

		ids, err := s.mediaRepo.ListDeletableByOwner(ctx, ownerID, batchSize)
		if err != nil {
			return deleted, fmt.Errorf("list deletable by owner: %w", err)
		}
		if len(ids) == 0 {
			return deleted, nil
		}

		progressed := 0
		for _, id := range ids {
			select {
			case <-ctx.Done():
				return deleted, ctx.Err()
			default:
			}

			if err := s.deleteByID(ctx, id, true); err != nil {
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
