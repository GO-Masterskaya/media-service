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
	Attempts   int
	CreatedAt  time.Time
}

type JobRepo interface {
	Enqueue(ctx context.Context, mediaID uuid.UUID, jobType string) (*Job, error)
	ClaimNext(ctx context.Context, owner string, leaseDuration time.Duration) (*Job, error)
	MarkDone(ctx context.Context, jobID uuid.UUID, owner string) error
	MarkFailed(ctx context.Context, jobID uuid.UUID, owner, reason string) error
	ReleaseForRetry(ctx context.Context, jobID uuid.UUID, owner string, runAfter time.Time) error
	ReleaseForShutdown(ctx context.Context, jobID uuid.UUID, owner string) error
	ExtendLease(ctx context.Context, jobID uuid.UUID, owner string, d time.Duration) error
	ReapExpiredLeases(ctx context.Context, maxAttempts int, backoff JobBackoffConfig) (int64, error)
}

type PgJobRepo struct {
	pool *pgxpool.Pool
}

func NewPgJobRepo(pool *pgxpool.Pool) *PgJobRepo {
	return &PgJobRepo{pool: pool}
}

func ClaimNextJob(ctx context.Context, pool *pgxpool.Pool, owner string, leaseDuration time.Duration) (*Job, error) {
	return NewPgJobRepo(pool).ClaimNext(ctx, owner, leaseDuration)
}

func (r *PgJobRepo) Enqueue(ctx context.Context, mediaID uuid.UUID, jobType string) (*Job, error) {
	id := uuid.New()
	const insert = `
		INSERT INTO processing_jobs (id, media_id, type, status)
		VALUES ($1, $2, $3, 'queued')
		ON CONFLICT (media_id, type) DO NOTHING
		RETURNING id, media_id, type, status, locked_by, lease_until, attempts, created_at
	`

	job, err := scanJob(r.pool.QueryRow(ctx, insert, id, mediaID, jobType))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("enqueue job: %w", err)
	}

	const sel = `
		SELECT id, media_id, type, status, locked_by, lease_until, attempts, created_at
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

func (r *PgJobRepo) ClaimNext(ctx context.Context, owner string, leaseDuration time.Duration) (*Job, error) {
	if leaseDuration <= 0 {
		leaseDuration = DefaultJobLease
	}

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
		RETURNING j.id, j.media_id, j.type, j.status, j.locked_by, j.lease_until, j.attempts, j.created_at
	`

	job, err := scanJob(tx.QueryRow(ctx, q, owner, leaseDuration.Milliseconds()))
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

func (r *PgJobRepo) ReleaseForRetry(ctx context.Context, jobID uuid.UUID, owner string, runAfter time.Time) error {
	return r.release(ctx, jobID, owner, runAfter, true, "")
}

func (r *PgJobRepo) ReleaseForShutdown(ctx context.Context, jobID uuid.UUID, owner string) error {
	return r.release(ctx, jobID, owner, time.Now(), false, "")
}

func (r *PgJobRepo) release(ctx context.Context, jobID uuid.UUID, owner string, runAfter time.Time, incrementAttempts bool, reason string) error {
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

	if !CanTransition(string(from), string(JobStatusQueued)) {
		return ErrInvalidTransition
	}
	if from != JobStatusRunning {
		return ErrInvalidTransition
	}
	if lockedBy == nil || *lockedBy != owner || leaseUntil == nil || !leaseUntil.After(time.Now()) {
		return ErrLeaseMismatch
	}

	if incrementAttempts {
		_, err = tx.Exec(ctx, `
			UPDATE processing_jobs
			SET status = 'queued',
			    last_error = NULLIF($2, ''),
			    locked_by = NULL,
			    lease_until = NULL,
			    locked_at = NULL,
			    attempts = attempts + 1,
			    run_after = $3
			WHERE id = $1
		`, jobID, reason, runAfter)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE processing_jobs
			SET status = 'queued',
			    last_error = NULLIF($2, ''),
			    locked_by = NULL,
			    lease_until = NULL,
			    locked_at = NULL,
			    run_after = $3
			WHERE id = $1
		`, jobID, reason, runAfter)
	}
	if err != nil {
		return fmt.Errorf("release job: %w", err)
	}

	if err := recalcMedia(ctx, tx, mediaID, ""); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ExtendLease атомарно продлевает lease для running-задачи, принадлежащей owner.
// Новый lease_until = now() + d. Если задача не найдена, не принадлежит owner,
// не в статусе running или lease уже истёк — возвращает ErrLeaseMismatch.
func (r *PgJobRepo) ExtendLease(ctx context.Context, jobID uuid.UUID, owner string, d time.Duration) error {
	const q = `
		UPDATE processing_jobs
		SET lease_until = now() + ($3 * interval '1 millisecond')
		WHERE id = $1
		  AND locked_by = $2
		  AND status = 'running'
		  AND lease_until > now()
	`
	tag, err := r.pool.Exec(ctx, q, jobID, owner, d.Milliseconds())
	if err != nil {
		return fmt.Errorf("extend lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseMismatch
	}
	return nil
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
	if err := row.Scan(&job.ID, &job.MediaID, &job.Type, &job.Status, &lockedBy, &leaseUntil, &job.Attempts, &job.CreatedAt); err != nil {
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

// ClaimBatch атомарно забирает до limit задач со статусом queued, переводя их в running.
// Используется движком обработки (Engine) для пакетной загрузки задач.
func (r *PgJobRepo) ClaimBatch(ctx context.Context, owner string, limit int, leaseDuration time.Duration) ([]Job, error) {
	if limit <= 0 {
		return nil, nil
	}
	if leaseDuration <= 0 {
		leaseDuration = DefaultJobLease
	}

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
			LIMIT $3
		)
		UPDATE processing_jobs j
		SET status = 'running',
		    locked_at = now(),
		    locked_by = $1,
		    lease_until = now() + ($2 * interval '1 millisecond')
		FROM next
		WHERE j.id = next.id
		RETURNING j.id, j.media_id, j.type, j.status, j.locked_by, j.lease_until, j.attempts, j.created_at
	`

	rows, err := tx.Query(ctx, q, owner, leaseDuration.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim batch: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		var lockedBy *string
		var leaseUntil *time.Time
		if err := rows.Scan(&job.ID, &job.MediaID, &job.Type, &job.Status, &lockedBy, &leaseUntil, &job.Attempts, &job.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan claimed job: %w", err)
		}
		if lockedBy != nil {
			job.LockedBy = *lockedBy
		}
		if leaseUntil != nil {
			job.LeaseUntil = *leaseUntil
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim batch rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

// GetQueueDepth возвращает количество задач со статусом queued, готовых к выполнению.
func (r *PgJobRepo) GetQueueDepth(ctx context.Context) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM processing_jobs WHERE status = 'queued' AND run_after <= now()`,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("get queue depth: %w", err)
	}
	return count, nil
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

// ReapExpiredLeases возвращает running-задачи с протухшим lease обратно в queued
// (с инкрементом attempts и exponential backoff через run_after).
// Задачи, превысившие maxAttempts, переводятся в failed и пересчитывают статус media.
// Возвращает количество обработанных задач.
func (r *PgJobRepo) ReapExpiredLeases(ctx context.Context, maxAttempts int, backoff JobBackoffConfig) (int64, error) {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if backoff.Base <= 0 {
		backoff = DefaultJobBackoff()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("reap expired leases begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const selectQ = `
		SELECT id, attempts, media_id
		FROM processing_jobs
		WHERE status = 'running'
		  AND lease_until < now()
		FOR UPDATE
	`
	rows, err := tx.Query(ctx, selectQ)
	if err != nil {
		return 0, fmt.Errorf("reap expired leases select: %w", err)
	}
	defer rows.Close()

	type staleJob struct {
		id      uuid.UUID
		attempts int
		mediaID uuid.UUID
	}
	var stale []staleJob
	for rows.Next() {
		var j staleJob
		if err := rows.Scan(&j.id, &j.attempts, &j.mediaID); err != nil {
			return 0, fmt.Errorf("reap expired leases scan: %w", err)
		}
		stale = append(stale, j)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("reap expired leases rows: %w", err)
	}
	rows.Close()

	var processed int64
	mediaFailed := make(map[uuid.UUID]struct{})

	for _, j := range stale {
		if j.attempts >= maxAttempts {
			tag, err := tx.Exec(ctx, `
				UPDATE processing_jobs
				SET status = 'failed',
				    locked_by = NULL,
				    lease_until = NULL,
				    locked_at = NULL,
				    last_error = 'max attempts exceeded (lease expired)'
				WHERE id = $1
			`, j.id)
			if err != nil {
				return 0, fmt.Errorf("reap expired leases fail job %s: %w", j.id, err)
			}
			if tag.RowsAffected() > 0 {
				processed++
				mediaFailed[j.mediaID] = struct{}{}
			}
			continue
		}

		runAfter := backoff.NextRunAfter(j.attempts + 1)
		tag, err := tx.Exec(ctx, `
			UPDATE processing_jobs
			SET status = 'queued',
			    locked_by = NULL,
			    lease_until = NULL,
			    locked_at = NULL,
			    attempts = attempts + 1,
			    run_after = $2
			WHERE id = $1
		`, j.id, runAfter)
		if err != nil {
			return 0, fmt.Errorf("reap expired leases requeue job %s: %w", j.id, err)
		}
		if tag.RowsAffected() > 0 {
			processed++
		}
	}

	for mediaID := range mediaFailed {
		if err := recalcMedia(ctx, tx, mediaID, "max attempts exceeded (lease expired)"); err != nil {
			return 0, fmt.Errorf("reap expired leases recalc media %s: %w", mediaID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("reap expired leases commit tx: %w", err)
	}
	return processed, nil
}
