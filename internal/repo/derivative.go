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

type Derivative struct {
	ID         uuid.UUID
	MediaID    uuid.UUID
	Variant    string
	Mime       string
	SizeBytes  int64
	StorageKey string
	Width      int
	Height     int
	Duration   time.Duration
}

type DerivativeRepo interface {
	GetByMediaAndVariant(ctx context.Context, mediaID uuid.UUID, variant string) (*Derivative, error)
	Insert(ctx context.Context, d Derivative) (*Derivative, error)
	UpsertDerivative(ctx context.Context, d *Derivative) (*Derivative, error)
}

type PgDerivativeRepo struct {
	pool *pgxpool.Pool
}

func NewPgDerivativeRepo(pool *pgxpool.Pool) *PgDerivativeRepo {
	return &PgDerivativeRepo{pool: pool}
}

func (r *PgDerivativeRepo) GetByMediaAndVariant(ctx context.Context, mediaID uuid.UUID, variant string) (*Derivative, error) {
	const q = `
		SELECT id, media_id, variant, mime, size_bytes, storage_key
		FROM media_derivative
		WHERE media_id = $1 AND variant = $2
	`
	row := r.pool.QueryRow(ctx, q, mediaID, variant)

	var d Derivative
	if err := row.Scan(&d.ID, &d.MediaID, &d.Variant, &d.Mime, &d.SizeBytes, &d.StorageKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get derivative: %w", err)
	}
	return &d, nil
}

func (r *PgDerivativeRepo) Insert(ctx context.Context, d Derivative) (*Derivative, error) {
	if d.ID == uuid.Nil {
		d.ID = uuid.New()
	}

	const q = `
		INSERT INTO media_derivative (id, media_id, variant, mime, size_bytes, storage_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (media_id, variant) DO NOTHING
		RETURNING id, media_id, variant, mime, size_bytes, storage_key
	`

	row := r.pool.QueryRow(ctx, q, d.ID, d.MediaID, d.Variant, d.Mime, d.SizeBytes, d.StorageKey)
	var out Derivative
	err := row.Scan(&out.ID, &out.MediaID, &out.Variant, &out.Mime, &out.SizeBytes, &out.StorageKey)
	if err == nil {
		return &out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("insert derivative: %w", err)
	}
	return r.GetByMediaAndVariant(ctx, d.MediaID, d.Variant)
}

func (r *PgDerivativeRepo) UpsertDerivative(ctx context.Context, d *Derivative) (*Derivative, error) {
	if d.ID == uuid.Nil {
		d.ID = uuid.New()
	}

	const q = `
		INSERT INTO media_derivative (id, media_id, variant, mime, size_bytes, storage_key, width, height, duration)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (media_id, variant) DO UPDATE SET
			mime = EXCLUDED.mime,
			size_bytes = EXCLUDED.size_bytes,
			storage_key = EXCLUDED.storage_key,
			width = EXCLUDED.width,
			height = EXCLUDED.height,
			duration = EXCLUDED.duration
		RETURNING id, media_id, variant, mime, size_bytes, storage_key, width, height, duration
	`

	row := r.pool.QueryRow(ctx, q, d.ID, d.MediaID, d.Variant,
		d.Mime, d.SizeBytes, d.StorageKey, d.Width, d.Height, d.Duration)
	var out Derivative
	if err := row.Scan(
		&out.ID,
		&out.MediaID,
		&out.Variant,
		&out.Mime,
		&out.SizeBytes,
		&out.StorageKey,
		&out.Width,
		&out.Height,
		&out.Duration,
	); err != nil {
		return nil, fmt.Errorf("upsert derivative: %w", err)
	}

	return &out, nil
}
