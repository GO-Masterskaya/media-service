package processing

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"mediaservice/internal/repo"
)

// RepoAdapter адаптирует repo.PgJobRepo к интерфейсу processing.JobRepository.
// Маппит типы между пакетами repo и processing, хранит ownerID экземпляра Engine.
type RepoAdapter struct {
	jobRepo *repo.PgJobRepo
	ownerID string // уникальный идентификатор этого экземпляра Engine (для lease)
}

// NewRepoAdapter создаёт адаптер. ownerID — уникальная строка, идентифицирующая
// этот экземпляр сервиса (например, hostname или UUID).
func NewRepoAdapter(jobRepo *repo.PgJobRepo, ownerID string) *RepoAdapter {
	return &RepoAdapter{
		jobRepo: jobRepo,
		ownerID: ownerID,
	}
}

// ClaimOne забирает одну задачу из БД и маппит repo.Job → processing.Job.
// Если очередь пуста, возвращает (nil, nil).
func (a *RepoAdapter) ClaimOne(ctx context.Context) (*Job, error) {
	repoJob, err := a.jobRepo.ClaimNext(ctx, a.ownerID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim one: %w", err)
	}

	return &Job{
		ID:      repoJob.ID,
		MediaID: repoJob.MediaID,
		Type:    repoJob.Type,
	}, nil
}

// GetQueueDepth возвращает количество задач в очереди БД.
func (a *RepoAdapter) GetQueueDepth(ctx context.Context) (int64, error) {
	return a.jobRepo.GetQueueDepth(ctx)
}

// MarkDone помечает задачу как done через repo.MarkDone.
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

// FailJob помечает задачу как failed через repo.MarkFailed.
func (a *RepoAdapter) FailJob(ctx context.Context, jobID string, reason string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return fmt.Errorf("parse job id: %w", err)
	}
	err = a.jobRepo.MarkFailed(ctx, id, a.ownerID, reason)
	if err != nil {
		// Lease mismatch или not found — не критично, логируем но не падаем.
		if errors.Is(err, repo.ErrLeaseMismatch) || errors.Is(err, repo.ErrNotFound) {
			return fmt.Errorf("fail job %s (non-critical): %w", jobID, err)
		}
		return fmt.Errorf("fail job: %w", err)
	}
	return nil
}

// ReleaseJob возвращает задачу из running обратно в queued через repo.Release.
func (a *RepoAdapter) ReleaseJob(ctx context.Context, jobID string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return fmt.Errorf("parse job id: %w", err)
	}
	err = a.jobRepo.Release(ctx, id, a.ownerID)
	if err != nil {
		if errors.Is(err, repo.ErrLeaseMismatch) || errors.Is(err, repo.ErrNotFound) {
			return fmt.Errorf("release job %s (non-critical): %w", jobID, err)
		}
		return fmt.Errorf("release job: %w", err)
	}
	return nil
}
