package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Derivative struct {
	ID         uuid.UUID
	MediaID    uuid.UUID
	Variant    string
	StorageKey string
}

type DerivativeRepo interface {
	GetByMediaAndVariant(ctx context.Context, mediaID uuid.UUID, variant string) (*Derivative, error)
}

type PgDerivativeRepo struct {
	pool *pgxpool.Pool
}

func NewPgDerivativeRepo(pool *pgxpool.Pool) *PgDerivativeRepo {
	return &PgDerivativeRepo{pool: pool}
}

func (r *PgDerivativeRepo) GetByMediaAndVariant(ctx context.Context, mediaID uuid.UUID, variant string) (*Derivative, error) {
	const q = `SELECT id, media_id, variant, storage_key FROM media_derivative WHERE media_id = $1 AND variant = $2`
	row := r.pool.QueryRow(ctx, q, mediaID, variant)

	var d Derivative
	if err := row.Scan(&d.ID, &d.MediaID, &d.Variant, &d.StorageKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get derivative: %w", err)
	}
	return &d, nil
}
