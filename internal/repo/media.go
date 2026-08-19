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
	Error      string
}

type MediaRepo interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Media, error)
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
