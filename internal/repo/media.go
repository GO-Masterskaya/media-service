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
}

type MediaRepo interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Media, error)
	ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*Media, error)
	HardDelete(ctx context.Context, id uuid.UUID) error
	ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error)
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
