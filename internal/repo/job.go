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

const DefaultJobLease = 30 * time.Second

type JobStatus string

const (
	JobStatusQueued  JobStatus = "queued"
	JobStatusRunning JobStatus = "running"
	JobStatusDone    JobStatus = "done"
	JobStatusFailed  JobStatus = "failed"
)

type Job struct {
	ID         uuid.UUID
	MediaID    uuid.UUID
	Type       string
	Status     JobStatus
	LockedBy   string
	LeaseUntil time.Time
}

type JobRepo interface {
	Enqueue(ctx context.Context, mediaID uuid.UUID, jobType string) (*Job, error)
	ClaimNext(ctx context.Context, owner string) (*Job, error)
	MarkDone(ctx context.Context, jobID uuid.UUID, owner string) error
	MarkFailed(ctx context.Context, jobID uuid.UUID, owner, reason string) error
	Release(ctx context.Context, jobID uuid.UUID, owner string) error
}

type PgJobRepo struct {
	pool *pgxpool.Pool
}

func NewPgJobRepo(pool *pgxpool.Pool) *PgJobRepo {
	return &PgJobRepo{pool: pool}
}

func ClaimNextJob(ctx context.Context, pool *pgxpool.Pool, owner string) (*Job, error) {
	return NewPgJobRepo(pool).ClaimNext(ctx, owner)
}

func (r *PgJobRepo) Enqueue(ctx context.Context, mediaID uuid.UUID, jobType string) (*Job, error) {
	id := uuid.New()
	const insert = `
		INSERT INTO processing_jobs (id, media_id, type, status)
		VALUES ($1, $2, $3, 'queued')
		ON CONFLICT (media_id, type) DO NOTHING
		RETURNING id, media_id, type, status, locked_by, lease_until
	`

	job, err := scanJob(r.pool.QueryRow(ctx, insert, id, mediaID, jobType))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("enqueue job: %w", err)
	}

	const sel = `
		SELECT id, media_id, type, status, locked_by, lease_until
		FROM processing_jobs
		WHERE media_id = $1 AND type = $2
	`
	job, err = scanJob(r.pool.QueryRow(ctx, sel, mediaID, jobType))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("enqueue job lookup: %w", err)
	}
	return job, nil
}

func (r *PgJobRepo) ClaimNext(ctx context.Context, owner string) (*Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const q = `
		WITH next AS (
			SELECT id
			FROM processing_jobs
			WHERE status = 'queued'
				AND run_after <= now()
			ORDER BY run_after
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE processing_jobs j
		SET status = 'running',
		    locked_at = now(),
		    locked_by = $1,
		    lease_until = now() + ($2 * interval '1 millisecond')
		FROM next
		WHERE j.id = next.id
		RETURNING j.id, j.media_id, j.type, j.status, j.locked_by, j.lease_until
	`

	job, err := scanJob(tx.QueryRow(ctx, q, owner, DefaultJobLease.Milliseconds()))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("claim job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return job, nil
}

func (r *PgJobRepo) MarkDone(ctx context.Context, jobID uuid.UUID, owner string) error {
	return r.complete(ctx, jobID, owner, JobStatusDone, "")
}

func (r *PgJobRepo) MarkFailed(ctx context.Context, jobID uuid.UUID, owner, reason string) error {
	return r.complete(ctx, jobID, owner, JobStatusFailed, reason)
}

func (r *PgJobRepo) Release(ctx context.Context, jobID uuid.UUID, owner string) error {
	return r.complete(ctx, jobID, owner, JobStatusQueued, "")
}

func (r *PgJobRepo) complete(ctx context.Context, jobID uuid.UUID, owner string, to JobStatus, reason string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var from JobStatus
	var lockedBy *string
	var leaseUntil *time.Time
	var mediaID uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT media_id, status, locked_by, lease_until
		FROM processing_jobs
		WHERE id = $1
		FOR UPDATE
	`, jobID).Scan(&mediaID, &from, &lockedBy, &leaseUntil)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("load job: %w", err)
	}

	if !CanTransition(string(from), string(to)) {
		return ErrInvalidTransition
	}
	if from != JobStatusRunning {
		return ErrInvalidTransition
	}
	if lockedBy == nil || *lockedBy != owner || leaseUntil == nil || !leaseUntil.After(time.Now()) {
		return ErrLeaseMismatch
	}

	_, err = tx.Exec(ctx, `
		UPDATE processing_jobs
		SET status = $2,
		    last_error = NULLIF($3, ''),
		    locked_by = NULL,
		    lease_until = NULL,
		    locked_at = NULL
		WHERE id = $1
	`, jobID, to, reason)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}

	if err := recalcMedia(ctx, tx, mediaID, reason); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func recalcMedia(ctx context.Context, tx pgx.Tx, mediaID uuid.UUID, failReason string) error {
	var mediaStatus MediaStatus
	if err := tx.QueryRow(ctx, `
		SELECT status FROM media WHERE id = $1 FOR UPDATE
	`, mediaID).Scan(&mediaStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("lock media: %w", err)
	}

	var failedCount, notDoneCount int
	if err := tx.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status <> 'done')
		FROM processing_jobs
		WHERE media_id = $1
	`, mediaID).Scan(&failedCount, &notDoneCount); err != nil {
		return fmt.Errorf("count jobs: %w", err)
	}

	switch {
	case failedCount > 0:
		if failReason != "" {
			_, err := tx.Exec(ctx, `
				UPDATE media SET status = 'failed', error = $2 WHERE id = $1
			`, mediaID, failReason)
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE media SET status = 'failed' WHERE id = $1`, mediaID)
		return err
	case notDoneCount == 0:
		_, err := tx.Exec(ctx, `
			UPDATE media SET status = 'ready', error = NULL WHERE id = $1
		`, mediaID)
		return err
	default:
		if mediaStatus == MediaStatusFailed {
			return nil
		}
		_, err := tx.Exec(ctx, `UPDATE media SET status = 'processing' WHERE id = $1`, mediaID)
		return err
	}
}

func scanJob(row pgx.Row) (*Job, error) {
	var job Job
	var lockedBy *string
	var leaseUntil *time.Time
	if err := row.Scan(&job.ID, &job.MediaID, &job.Type, &job.Status, &lockedBy, &leaseUntil); err != nil {
		return nil, err
	}
	if lockedBy != nil {
		job.LockedBy = *lockedBy
	}
	if leaseUntil != nil {
		job.LeaseUntil = *leaseUntil
	}
	return &job, nil
}

var availableTransitions = []struct {
	from, to string
}{
	{"queued", "running"},
	{"running", "done"},
	{"running", "failed"},
	{"running", "queued"},
}

func CanTransition(from, to string) bool {
	for _, transition := range availableTransitions {
		if transition.from == from && transition.to == to {
			return true
		}
	}
	return false
}
