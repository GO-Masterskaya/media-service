package media

import (
	"context"
	"log/slog"
	"time"
)

// defaultReapBatchSize используется, если вызывающий код передал batchSize<=0.
const defaultReapBatchSize = 100

// Reaper периодически удаляет media с истёкшим TTL через ту же deleteByID
// команду, что использует одиночный DeleteMedia и DeleteByOwner (issue #13).
// Без TTL (expires_at IS NULL) записи никогда не попадают в выборку и не
// затрагиваются.
type Reaper struct {
	svc       *Service
	interval  time.Duration
	batchSize int
	log       *slog.Logger
}

// NewReaper создаёт reaper. interval и batchSize конфигурируются извне
// (TTL_REAP_INTERVAL, TTL_REAP_BATCH_SIZE) — см. criterion "период и batch
// size конфигурируются".
func NewReaper(svc *Service, interval time.Duration, batchSize int, log *slog.Logger) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = defaultReapBatchSize
	}
	return &Reaper{svc: svc, interval: interval, batchSize: batchSize, log: log}
}

// Run блокируется до отмены ctx — предназначен для запуска в отдельной
// горутине (`go reaper.Run(ctx)`), останавливается вместе с graceful shutdown
// сервиса через тот же ctx, что и остальные компоненты.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runOnce(ctx)
		}
	}
}

// runOnce — один проход: одна страница истёкших media. Публичный для тестов
// (не привязан к тикеру).
func (r *Reaper) runOnce(ctx context.Context) {
	ids, err := r.svc.mediaRepo.ListExpiredIDs(ctx, r.batchSize)
	if err != nil {
		r.log.Error("reaper: list expired failed", slog.Any("error", err))
		return
	}

	for _, id := range ids {
		// MarkDeleting внутри deleteByID — атомарный claim (UPDATE ... WHERE
		// status <> 'deleting'). Он же гарантирует, что при нескольких
		// параллельных reaper-инстансах (несколько реплик сервиса) одну и ту
		// же запись фактически удалит только один из них — второй получит
		// found=true с уже 'deleting' и просто повторит (безопасно
		// идемпотентную) очистку MinIO+строки без риска двойного эффекта.
		if err := r.svc.deleteByID(ctx, id); err != nil {
			// Ошибка одной записи не должна останавливать остальной batch.
			r.log.Error("reaper: delete failed", slog.Any("error", err), slog.String("media_id", id.String()))
			continue
		}
	}
}
