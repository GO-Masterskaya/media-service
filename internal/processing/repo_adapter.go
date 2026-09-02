package processing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"mediaservice/internal/repo"
)

// RepoAdapter адаптирует repo.PgJobRepo к интерфейсу processing.JobRepository.
// Маппит типы между пакетами repo и processing, хранит ownerID экземпляра Engine.
type RepoAdapter struct {
	jobRepo       *repo.PgJobRepo
	ownerID       string
	leaseDuration time.Duration
	maxAttempts   int
	backoff       BackoffConfig
}

// NewRepoAdapter создаёт адаптер. ownerID — уникальная строка, идентифицирующая
// этот экземпляр сервиса (например, hostname или UUID).
func NewRepoAdapter(
	jobRepo *repo.PgJobRepo,
	ownerID string,
	leaseDuration time.Duration,
	maxAttempts int,
	backoff BackoffConfig,
) *RepoAdapter {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	backoff = backoff.normalize()
	return &RepoAdapter{
		jobRepo:       jobRepo,
		ownerID:       ownerID,
		leaseDuration: leaseDuration,
		maxAttempts:   maxAttempts,
		backoff:       backoff,
	}
}

// ClaimOne забирает одну задачу из БД и маппит repo.Job → processing.Job.
func (a *RepoAdapter) ClaimOne(ctx context.Context, leaseDuration time.Duration) (*Job, error) {
	if leaseDuration <= 0 {
		leaseDuration = a.leaseDuration
	}

	repoJob, err := a.jobRepo.ClaimNext(ctx, a.ownerID, leaseDuration)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim one: %w", err)
	}

	return &Job{
		ID:        repoJob.ID,
		MediaID:   repoJob.MediaID,
		Type:      repoJob.Type,
		Attempts:  repoJob.Attempts,
		CreatedAt: repoJob.CreatedAt,
	}, nil
}

func (a *RepoAdapter) GetQueueDepth(ctx context.Context) (int64, error) {
	return a.jobRepo.GetQueueDepth(ctx)
}

func (a *RepoAdapter) MarkDone(ctx context.Context, jobID string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return fmt.Errorf("parse job id: %w", err)
	}
	err = a.jobRepo.MarkDone(ctx, id, a.ownerID)
	if err != nil {
		if errors.Is(err, repo.ErrLeaseMismatch) || errors.Is(err, repo.ErrNotFound) {
			return fmt.Errorf("mark done %s (non-critical): %w", jobID, err)
		}
		return fmt.Errorf("mark done: %w", err)
	}
	return nil
}

func (a *RepoAdapter) FailJob(ctx context.Context, jobID string, reason string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return fmt.Errorf("parse job id: %w", err)
	}
	err = a.jobRepo.MarkFailed(ctx, id, a.ownerID, reason)
	if err != nil {
		if errors.Is(err, repo.ErrLeaseMismatch) || errors.Is(err, repo.ErrNotFound) {
			return fmt.Errorf("fail job %s (non-critical): %w", jobID, err)
		}
		return fmt.Errorf("fail job: %w", err)
	}
	return nil
}

func (a *RepoAdapter) ReleaseJobForRetry(ctx context.Context, jobID string, attemptsAfterIncrement int, reason string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return fmt.Errorf("parse job id: %w", err)
	}
	return a.releaseForRetry(ctx, id, attemptsAfterIncrement, reason)
}

func (a *RepoAdapter) releaseForRetry(ctx context.Context, id uuid.UUID, attemptsAfterIncrement int, reason string) error {
	runAfter := a.backoff.nextRunAfter(attemptsAfterIncrement)
	err := a.jobRepo.ReleaseForRetry(ctx, id, a.ownerID, runAfter, reason)
	if err != nil {
		if errors.Is(err, repo.ErrLeaseMismatch) || errors.Is(err, repo.ErrNotFound) {
			return fmt.Errorf("release job %s (non-critical): %w", id, err)
		}
		return fmt.Errorf("release job: %w", err)
	}
	return nil
}

func (a *RepoAdapter) ReleaseJobOnShutdown(ctx context.Context, jobID string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return fmt.Errorf("parse job id: %w", err)
	}
	err = a.jobRepo.ReleaseForShutdown(ctx, id, a.ownerID)
	if err != nil {
		if errors.Is(err, repo.ErrLeaseMismatch) || errors.Is(err, repo.ErrNotFound) {
			return fmt.Errorf("release job on shutdown %s (non-critical): %w", jobID, err)
		}
		return fmt.Errorf("release job on shutdown: %w", err)
	}
	return nil
}

func (a *RepoAdapter) ExtendLease(ctx context.Context, jobID string, d time.Duration) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return fmt.Errorf("parse job id: %w", err)
	}
	err = a.jobRepo.ExtendLease(ctx, id, a.ownerID, d)
	if err != nil {
		return fmt.Errorf("extend lease: %w", err)
	}
	return nil
}

func (a *RepoAdapter) RecoverStaleJobs(ctx context.Context) (int64, error) {
	return a.jobRepo.ReapExpiredLeases(ctx, a.maxAttempts, a.backoff.toRepo())
}
