package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

type MediaKind string

const (
	MediaKindImage MediaKind = "image"
	MediaKindVideo MediaKind = "video"
	MediaKindAudio MediaKind = "audio"
)

type Media struct {
	ID                uuid.UUID
	OwnerID           uuid.UUID
	Kind              MediaKind
	OrigFilename      string
	Mime              string
	SizeBytes         int64
	Status            MediaStatus
	StorageKey        string
	Metadata          json.RawMessage
	IdempotencyKey    string
	BodyFingerprint   string
	ParamsFingerprint string
	ExpiresAt         *time.Time
	Error             string
	CreatedAt         time.Time
}

type MediaRepo interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Media, error)
	GetByOwnerIdempotency(ctx context.Context, ownerID uuid.UUID, idempotencyKey string) (*Media, error)
	InsertWithJobs(ctx context.Context, m Media, jobTypes []string) (*Media, error)
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

const mediaSelectCols = `
	id, owner_id, kind, orig_filename, mime, size_bytes, status, storage_key,
	metadata, idempotency_key, body_fingerprint, params_fingerprint,
	expires_at, COALESCE(error, ''), created_at
`

func scanMedia(row pgx.Row) (*Media, error) {
	var m Media
	var meta []byte
	if err := row.Scan(
		&m.ID,
		&m.OwnerID,
		&m.Kind,
		&m.OrigFilename,
		&m.Mime,
		&m.SizeBytes,
		&m.Status,
		&m.StorageKey,
		&meta,
		&m.IdempotencyKey,
		&m.BodyFingerprint,
		&m.ParamsFingerprint,
		&m.ExpiresAt,
		&m.Error,
		&m.CreatedAt,
	); err != nil {
		return nil, err
	}
	if len(meta) == 0 {
		m.Metadata = json.RawMessage(`{}`)
	} else {
		m.Metadata = meta
	}
	return &m, nil
}

func (r *PgMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*Media, error) {
	q := `SELECT ` + mediaSelectCols + ` FROM media WHERE id = $1`
	m, err := scanMedia(r.pool.QueryRow(ctx, q, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get media by id: %w", err)
	}
	return m, nil
}

func (r *PgMediaRepo) GetByOwnerIdempotency(ctx context.Context, ownerID uuid.UUID, idempotencyKey string) (*Media, error) {
	q := `SELECT ` + mediaSelectCols + ` FROM media WHERE owner_id = $1 AND idempotency_key = $2`
	m, err := scanMedia(r.pool.QueryRow(ctx, q, ownerID, idempotencyKey))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get media by owner idempotency: %w", err)
	}
	return m, nil
}

// InsertWithJobs атомарно создаёт media и requested jobs в одной TX.
// Статус: processing если есть jobs, иначе stored.
// При unique (owner_id, idempotency_key) возвращает ErrConcurrentConflict.
func (r *PgMediaRepo) InsertWithJobs(ctx context.Context, m Media, jobTypes []string) (*Media, error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	if len(m.Metadata) == 0 {
		m.Metadata = json.RawMessage(`{}`)
	}
	if len(jobTypes) > 0 {
		m.Status = MediaStatusProcessing
	} else {
		m.Status = MediaStatusStored
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin insert media: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	insertMedia := `
		INSERT INTO media (
			id, owner_id, kind, orig_filename, mime, size_bytes, status, storage_key,
			metadata, idempotency_key, body_fingerprint, params_fingerprint, expires_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13
		)
		RETURNING ` + mediaSelectCols

	created, err := scanMedia(tx.QueryRow(ctx, insertMedia,
		m.ID,
		m.OwnerID,
		m.Kind,
		m.OrigFilename,
		m.Mime,
		m.SizeBytes,
		m.Status,
		m.StorageKey,
		[]byte(m.Metadata),
		m.IdempotencyKey,
		m.BodyFingerprint,
		m.ParamsFingerprint,
		m.ExpiresAt,
	))
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrConcurrentConflict
		}
		return nil, fmt.Errorf("insert media: %w", err)
	}

	const insertJob = `
		INSERT INTO processing_jobs (id, media_id, type, status)
		VALUES ($1, $2, $3, 'queued')
		ON CONFLICT (media_id, type) DO NOTHING
	`
	for _, jt := range jobTypes {
		if _, err := tx.Exec(ctx, insertJob, uuid.New(), created.ID, jt); err != nil {
			return nil, fmt.Errorf("insert job %q: %w", jt, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit insert media: %w", err)
	}
	return created, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
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
