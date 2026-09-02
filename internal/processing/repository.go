package processing

import (
	"context"
	"time"

	"mediaservice/internal/repo"
)

// BackoffConfig задаёт exponential backoff для retry processing jobs.
type BackoffConfig struct {
	Base   time.Duration
	Max    time.Duration
	Jitter float64
}

func (c BackoffConfig) toRepo() repo.JobBackoffConfig {
	return repo.JobBackoffConfig{
		Base:   c.Base,
		Max:    c.Max,
		Jitter: c.Jitter,
	}
}

func (c BackoffConfig) nextRunAfter(attemptsAfterIncrement int) time.Time {
	return c.toRepo().NextRunAfter(attemptsAfterIncrement)
}

// JobRepository определяет интерфейс взаимодействия с базой данных (задача #25).
type JobRepository interface {
	// ClaimOne атомарно забирает одну задачу со статусом queued → running.
	// leaseDuration задаёт начальный lease при захвате.
	// Если очередь пуста, возвращает (nil, nil).
	ClaimOne(ctx context.Context, leaseDuration time.Duration) (*Job, error)

	// GetQueueDepth возвращает текущее количество задач со статусом queued в БД.
	GetQueueDepth(ctx context.Context) (int64, error)

	// MarkDone помечает задачу как done после успешного выполнения.
	MarkDone(ctx context.Context, jobID string) error

	// FailJob помечает задачу как failed с указанием причины.
	// Используется при неизвестном типе задачи или ошибке handler.
	FailJob(ctx context.Context, jobID string, reason string) error

	// ReleaseJobForRetry возвращает задачу в queued с backoff и инкрементом attempts.
	// attemptsAfterIncrement — значение attempts после инкремента (job.Attempts+1).
	ReleaseJobForRetry(ctx context.Context, jobID string, attemptsAfterIncrement int) error

	// ReleaseJobOnShutdown возвращает задачу в queued без инкремента attempts.
	// Используется при graceful shutdown для незавершённых jobs.
	ReleaseJobOnShutdown(ctx context.Context, jobID string) error

	// ExtendLease продлевает lease задачи на указанную длительность.
	// Используется heartbeat-горутиной для поддержания lease во время обработки.
	ExtendLease(ctx context.Context, jobID string, d time.Duration) error

	// RecoverStaleJobs возвращает running-задачи с протухшим lease обратно в очередь.
	// Задачи, превысившие maxAttempts, переводятся в failed.
	// Возвращает количество обработанных задач.
	RecoverStaleJobs(ctx context.Context) (int64, error)
}
