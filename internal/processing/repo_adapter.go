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

// ClaimQueued забирает до limit задач из БД и маппит repo.Job → processing.Job.
func (a *RepoAdapter) ClaimQueued(ctx context.Context, limit int) ([]Job, error) {
	repoJobs, err := a.jobRepo.ClaimBatch(ctx, a.ownerID, limit)
	if err != nil {
		return nil, fmt.Errorf("claim queued: %w", err)
	}

	jobs := make([]Job, 0, len(repoJobs))
	for _, rj := range repoJobs {
		jobs = append(jobs, Job{
			ID:      rj.ID,
			MediaID: rj.MediaID,
			Type:    rj.Type,
		})
	}
	return jobs, nil
}

// GetQueueDepth возвращает количество задач в очереди БД.
func (a *RepoAdapter) GetQueueDepth(ctx context.Context) (int64, error) {
	return a.jobRepo.GetQueueDepth(ctx)
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
