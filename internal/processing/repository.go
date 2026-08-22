package processing

import "context"

// JobRepository определяет интерфейс взаимодействия с базой данных (задача #25).
type JobRepository interface {
	// ClaimQueued запрашивает и атомарно меняет статус queued -> running не более чем у limit задач.
	ClaimQueued(ctx context.Context, limit int) ([]Job, error)

	// GetQueueDepth возвращает текущее количество задач со статусом queued в БД.
	GetQueueDepth(ctx context.Context) (int64, error)

	// FailJob помечает задачу как failed с указанием причины.
	// Используется при неизвестном типе задачи или ошибке handler.
	FailJob(ctx context.Context, jobID string, reason string) error
}

