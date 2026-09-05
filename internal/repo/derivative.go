package repo

import (
	"context"
	"encoding/json"
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
	Mime       string
	SizeBytes  int64
	StorageKey string
	Metadata   json.RawMessage
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

func (r *PgDerivativeRepo) ListByMediaIDs(ctx context.Context, mediaIDs []uuid.UUID) (map[uuid.UUID][]*Derivative, error) {
	result := make(map[uuid.UUID][]*Derivative, len(mediaIDs))
	if len(mediaIDs) == 0 {
		return result, nil
	}

	const q = `
		SELECT id, media_id, variant, mime, size_bytes, storage_key, metadata
		FROM media_derivative
		WHERE media_id = ANY($1)
		ORDER BY media_id, created_at ASC, id ASC
	`
	rows, err := r.pool.Query(ctx, q, mediaIDs)
	if err != nil {
		return nil, fmt.Errorf("list derivatives: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d Derivative
		if err := rows.Scan(&d.ID, &d.MediaID, &d.Variant, &d.Mime, &d.SizeBytes, &d.StorageKey, &d.Metadata); err != nil {
			return nil, fmt.Errorf("scan derivative: %w", err)
		}
		result[d.MediaID] = append(result[d.MediaID], &d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate derivatives: %w", err)
	}
	return result, nil
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
		INSERT INTO media_derivative (id, media_id, variant, mime, size_bytes, storage_key, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (media_id, variant) DO UPDATE SET
			mime = EXCLUDED.mime,
			size_bytes = EXCLUDED.size_bytes,
			storage_key = EXCLUDED.storage_key,
			metadata = EXCLUDED.metadata
		RETURNING id, media_id, variant, mime, size_bytes, storage_key, metadata
	`

	row := r.pool.QueryRow(ctx, q, d.ID, d.MediaID, d.Variant,
		d.Mime, d.SizeBytes, d.StorageKey, d.Metadata)
	var out Derivative
	if err := row.Scan(
		&out.ID,
		&out.MediaID,
		&out.Variant,
		&out.Mime,
		&out.SizeBytes,
		&out.StorageKey,
		&out.Metadata,
	); err != nil {
		return nil, fmt.Errorf("upsert derivative: %w", err)
	}

	return &out, nil
}
