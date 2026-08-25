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
	ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*Media, error)
	HardDelete(ctx context.Context, id uuid.UUID) error
	ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error)
	// CreateAttachment создаёт привязку media→owner и инкрементирует usages_count.
	// Идемпотентна: если привязка уже есть — возвращает nil.
	CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error

	DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (usagesRemaining int, err error)
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

// HardDelete безвозвратно удаляет запись.
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

func (r *PgMediaRepo) CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Сериализация с DeleteAttachment: блокируем media.
	var status string
	err = tx.QueryRow(ctx,
		`SELECT status FROM media WHERE id = $1 FOR UPDATE`, mediaID,
	).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("select media for update: %w", err)
	}
	if status == string(MediaStatusDeleting) {
		// Бизнес-ошибка: retry не поможет, медиа всё ещё будет deleting.
		// Service мапит в FailedPrecondition → PermanentError.
		return ErrMediaDeleting
	}

	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO media_attachments (media_id, owner_id)
		VALUES ($1, $2)
		ON CONFLICT (media_id, owner_id) DO NOTHING
		RETURNING true
	`, mediaID, ownerID).Scan(&inserted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("insert attachment: %w", err)
	}
	if !inserted {
		return nil // уже привязано
	}

	if _, err := tx.Exec(ctx, `
		UPDATE media SET usages_count = usages_count + 1 WHERE id = $1
	`, mediaID); err != nil {
		return fmt.Errorf("increment usages: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attachment: %w", err)
	}
	return nil
}

func (r *PgMediaRepo) DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Сериализация с CreateAttachment: блокируем media.
	var dummy string
	err = tx.QueryRow(ctx,
		`SELECT status FROM media WHERE id = $1 FOR UPDATE`, mediaID,
	).Scan(&dummy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("select media for update: %w", err)
	}

	var deleted bool
	err = tx.QueryRow(ctx, `
		DELETE FROM media_attachments
		WHERE media_id = $1 AND owner_id = $2
		RETURNING true
	`, mediaID, ownerID).Scan(&deleted)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("delete attachment: %w", err)
	}

	var usages int
	err = tx.QueryRow(ctx, `
		UPDATE media
		SET usages_count = GREATEST(0, usages_count - 1),
		    status = CASE WHEN usages_count = 1 THEN 'deleting' ELSE status END
		WHERE id = $1
		RETURNING usages_count
	`, mediaID).Scan(&usages)
	if err != nil {
		return 0, fmt.Errorf("decrement usages: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit detach: %w", err)
	}
	return usages, nil
}
