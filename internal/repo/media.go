package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MediaStatus string

const (
	MediaStatusStored     MediaStatus = "stored"
	MediaStatusProcessing MediaStatus = "processing"
	MediaStatusReady      MediaStatus = "ready"
	MediaStatusFailed     MediaStatus = "failed"
	MediaStatusDeleting   MediaStatus = "deleting"
)

type Media struct {
	ID         uuid.UUID
	OwnerID    uuid.UUID
	Status     MediaStatus
	StorageKey string
	Error      string
}

type MediaRepo interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Media, error)

	// MarkDeleting атомарно переводит media в status=deleting — единственный
	// "claim" в конвейере удаления (issues #13, #17): UPDATE ... WHERE status
	// <> 'deleting' атомарен на уровне БД, поэтому конкурирующие вызовы
	// (обычный delete, DeleteByOwner, TTL reaper, несколько реплик сервиса)
	// не могут оба посчитать себя владельцами одной и той же очистки.
	//
	//   - found=true  — этот вызов выполнил переход; возвращённая *Media
	//     содержит owner_id/id, нужные для построения MinIO-префикса.
	//   - found=false, err=nil — записи нет вовсе ИЛИ она уже была deleting
	//     (из прошлой прерванной попытки/гонки с другим claim'ом). Оба случая
	//     вызывающий трактует как идемпотентный успех; повторную очистку
	//     зависшей deleting-записи выполняет фоновая сверка (#24) через
	//     ListDeleting.
	MarkDeleting(ctx context.Context, id uuid.UUID) (m *Media, found bool, err error)

	// HardDelete удаляет строку media. media_derivative уходит каскадом через
	// FK ON DELETE CASCADE. Возвращает ErrNotFound, если строки уже не
	// было — ОЖИДАЕМЫЙ путь при гонке с фоновой сверкой (#24), которая может
	// параллельно удалить ту же зависшую deleting-запись; вызывающий код
	// обязан трактовать ErrNotFound здесь как идемпотентный успех.
	HardDelete(ctx context.Context, id uuid.UUID) error

	// ListDeletableByOwner отдаёт очередной batch id media заданного owner,
	// ещё не находящихся в status=deleting. После того как MarkDeleting
	// заклеймил запись, следующий вызов её уже не вернёт — это и обеспечивает
	// отсутствие повторной обработки в рамках одного DeleteByOwner.
	ListDeletableByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error)

	// ListExpiredIDs отдаёт очередной batch id media с истёкшим TTL
	// (expires_at задан и в прошлом), ещё не находящихся в status=deleting.
	ListExpiredIDs(ctx context.Context, limit int) ([]uuid.UUID, error)

	// ListDeleting отдаёт media, зависшие в status=deleting дольше olderThan —
	// используется фоновой сверкой (#24) для докручивания прерванных удалений.
	ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*Media, error)

	// ExistsBatch проверяет, какие из переданных id всё ещё существуют —
	// используется фоновой сверкой (#24) для поиска "осиротевших" объектов
	// в MinIO без соответствующей строки в БД.
	ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error)
}

type PgMediaRepo struct {
	pool *pgxpool.Pool
}

func NewPgMediaRepo(pool *pgxpool.Pool) *PgMediaRepo {
	return &PgMediaRepo{pool: pool}
}

func (r *PgMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*Media, error) {
	const q = `SELECT id, owner_id, status, storage_key, COALESCE(error, '') FROM media WHERE id = $1`
	row := r.pool.QueryRow(ctx, q, id)

	var m Media
	if err := row.Scan(&m.ID, &m.OwnerID, &m.Status, &m.StorageKey, &m.Error); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get media by id: %w", err)
	}
	return &m, nil
}

func (r *PgMediaRepo) MarkDeleting(ctx context.Context, id uuid.UUID) (*Media, bool, error) {
	const q = `
		UPDATE media
		SET status = 'deleting'
		WHERE id = $1 AND status <> 'deleting'
		RETURNING id, owner_id, status, storage_key`

	var m Media
	err := r.pool.QueryRow(ctx, q, id).Scan(&m.ID, &m.OwnerID, &m.Status, &m.StorageKey)
	if err == nil {
		return &m, true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		// Либо записи нет вовсе, либо она уже deleting — оба случая безопасно
		// трактуются вызывающим как идемпотентный успех (см. интерфейс выше).
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("mark deleting: %w", err)
}

// ListDeleting возвращает записи со статусом deleting, обновлённые раньше cutoff.
func (r *PgMediaRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*Media, error) {
	const q = `SELECT id, owner_id, status, storage_key FROM media 
	           WHERE status = 'deleting' AND updated_at < $1 
	           ORDER BY updated_at LIMIT $2`
	rows, err := r.pool.Query(ctx, q, olderThan, limit)
	if err != nil {
		return nil, fmt.Errorf("list deleting: %w", err)
	}
	defer rows.Close()

	var result []*Media
	for rows.Next() {
		var m Media
		if err := rows.Scan(&m.ID, &m.OwnerID, &m.Status, &m.StorageKey); err != nil {
			return nil, fmt.Errorf("scan deleting: %w", err)
		}
		result = append(result, &m)
	}
	return result, rows.Err()
}

// HardDelete безвозвратно удаляет запись. Возвращает ErrNotFound, если строки
// уже не было — ожидаемый путь при гонке с фоновой сверкой (#24).
func (r *PgMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error {
	const q = `DELETE FROM media WHERE id = $1`
	tag, err := r.pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("hard delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *PgMediaRepo) ListDeletableByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
	const q = `
		SELECT id FROM media
		WHERE owner_id = $1 AND status <> 'deleting'
		ORDER BY id
		LIMIT $2`
	return r.listIDs(ctx, q, ownerID, limit)
}

func (r *PgMediaRepo) ListExpiredIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	const q = `
		SELECT id FROM media
		WHERE expires_at IS NOT NULL AND expires_at <= now() AND status <> 'deleting'
		ORDER BY expires_at
		LIMIT $1`
	return r.listIDs(ctx, q, limit)
}

func (r *PgMediaRepo) listIDs(ctx context.Context, q string, args ...any) ([]uuid.UUID, error) {
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list ids: %w", err)
	}
	return ids, nil
}

// ExistsBatch проверяет существование записей по ID.
func (r *PgMediaRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	if len(ids) == 0 {
		return map[uuid.UUID]struct{}{}, nil
	}
	const q = `SELECT id FROM media WHERE id = ANY($1)`
	rows, err := r.pool.Query(ctx, q, ids)
	if err != nil {
		return nil, fmt.Errorf("exists batch: %w", err)
	}
	defer rows.Close()

	exists := make(map[uuid.UUID]struct{}, len(ids))
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan exists: %w", err)
		}
		exists[id] = struct{}{}
	}
	return exists, rows.Err()
}
