package repo

import (
	"context"
	"errors"
	"fmt"

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
	//     зависшей deleting-записи выполняет фоновая сверка (#24).
	MarkDeleting(ctx context.Context, id uuid.UUID) (m *Media, found bool, err error)

	// HardDelete удаляет строку media. media_derivative уходит каскадом через
	// FK ON DELETE CASCADE (см. миграцию 000001). Идемпотентно: отсутствие
	// строки — не ошибка.
	HardDelete(ctx context.Context, id uuid.UUID) error

	// ListDeletableByOwner отдаёт очередной batch id media заданного owner,
	// ещё не находящихся в status=deleting. После того как MarkDeleting
	// заклеймил запись, следующий вызов её уже не вернёт — это и обеспечивает
	// отсутствие повторной обработки в рамках одного DeleteByOwner.
	ListDeletableByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error)

	// ListExpiredIDs отдаёт очередной batch id media с истёкшим TTL
	// (expires_at задан и в прошлом), ещё не находящихся в status=deleting.
	ListExpiredIDs(ctx context.Context, limit int) ([]uuid.UUID, error)
}

type PgMediaRepo struct {
	pool *pgxpool.Pool
}

func NewPgMediaRepo(pool *pgxpool.Pool) *PgMediaRepo {
	return &PgMediaRepo{pool: pool}
}

func (r *PgMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*Media, error) {
	const q = `SELECT id, owner_id, status, storage_key FROM media WHERE id = $1`
	row := r.pool.QueryRow(ctx, q, id)

	var m Media
	if err := row.Scan(&m.ID, &m.OwnerID, &m.Status, &m.StorageKey); err != nil {
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

func (r *PgMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error {
	const q = `DELETE FROM media WHERE id = $1`
	if _, err := r.pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("hard delete media: %w", err)
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
