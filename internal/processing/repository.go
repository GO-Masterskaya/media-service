package processing

import (
	"context"
	"time"
)

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

	// ReleaseJob возвращает задачу в queued (running → queued).
	// Используется при graceful shutdown для задач, которые не успели выполниться.
	ReleaseJob(ctx context.Context, jobID string) error

	// ExtendLease продлевает lease задачи на указанную длительность.
	// Используется heartbeat-горутиной для поддержания lease во время обработки.
	ExtendLease(ctx context.Context, jobID string, d time.Duration) error

	// ReapExpiredLeases возвращает running-задачи с протухшим lease обратно в очередь.
	// Задачи, превысившие maxAttempts, переводятся в failed.
	// Возвращает количество обработанных задач.
	ReapExpiredLeases(ctx context.Context, maxAttempts int) (int64, error)
}
