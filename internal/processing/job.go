package processing

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrUnknownJobType возвращается, когда в реестре нет обработчика для указанного типа задачи.
var ErrUnknownJobType = errors.New("unknown job type")

// Job представляет задачу обработки из таблицы processing_jobs.
type Job struct {
	ID        uuid.UUID
	MediaID   uuid.UUID
	Type      string
	Attempts  int       // Сколько раз задачу уже пытались выполнить (для retry в #26).
	CreatedAt time.Time // Время создания задачи (для логирования и диагностики).
}

// Handler описывает контракт обработчика отдельного типа задач.
type Handler interface {
	Handle(ctx context.Context, job Job) error
}

// HandlerFunc позволяет использовать обычную функцию в качестве Handler.
type HandlerFunc func(ctx context.Context, job Job) error

// Handle вызывает h(ctx, job).
func (h HandlerFunc) Handle(ctx context.Context, job Job) error {
	return h(ctx, job)
}
