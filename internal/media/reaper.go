package media

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// defaultReapBatchSize используется, если вызывающий код передал batchSize<=0.
const defaultReapBatchSize = 100

var (
	reaperMetricsOnce sync.Once
	reapScanned       prometheus.Counter
	reapDeleted       prometheus.Counter
	reapFailed        prometheus.Counter
)

func initReaperMetrics() {
	reaperMetricsOnce.Do(func() {
		reapScanned = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reaper_scanned_total",
			Help: "Total expired media records scanned by the TTL reaper",
		})
		reapDeleted = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reaper_deleted_total",
			Help: "Total media records deleted by the TTL reaper",
		})
		reapFailed = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reaper_failed_total",
			Help: "Total delete failures encountered by the TTL reaper",
		})
		prometheus.MustRegister(reapScanned, reapDeleted, reapFailed)
	})
}

// ReaperConfig — конфиг TTL reaper'а (issue #17), по образцу ReconcilerConfig.
type ReaperConfig struct {
	Interval  time.Duration
	BatchSize int
	// DryRun: только логировать/считать метрику "would delete", ничего не
	// удалять. Рекомендуется для первого выката (см. ревью PR #13/#17):
	// reaper безвозвратно удаляет данные, и по умолчанию должен сначала
	// просто посчитать объём, прежде чем реально включать удаление.
	DryRun bool
}

// Reaper периодически удаляет media с истёкшим TTL через ту же deleteByID
// команду, что использует одиночный DeleteMedia и DeleteByOwner (issue #13).
// Без TTL (expires_at IS NULL) записи никогда не попадают в выборку и не
// затрагиваются.
type Reaper struct {
	svc *Service
	cfg ReaperConfig
	log *slog.Logger

	scannedCounter prometheus.Counter
	deletedCounter prometheus.Counter
	failedCounter  prometheus.Counter

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// batchSize сохранён для обратной совместимости старых тестов/вызовов,
	// заглядывающих в r.batchSize напрямую; фактическое значение — cfg.BatchSize.
	batchSize int
}

// NewReaper создаёт reaper. interval и batchSize конфигурируются извне
// (TTL_REAP_INTERVAL, TTL_REAP_BATCH_SIZE) — см. criterion "период и batch
// size конфигурируются". Для dry-run используйте NewReaperWithConfig.
func NewReaper(svc *Service, interval time.Duration, batchSize int, log *slog.Logger) *Reaper {
	return NewReaperWithConfig(svc, ReaperConfig{Interval: interval, BatchSize: batchSize}, log)
}

// NewReaperWithConfig — полная форма конструктора, с поддержкой DryRun.
func NewReaperWithConfig(svc *Service, cfg ReaperConfig, log *slog.Logger) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultReapBatchSize
	}
	initReaperMetrics()

	return &Reaper{
		svc:            svc,
		cfg:            cfg,
		log:            log,
		scannedCounter: reapScanned,
		deletedCounter: reapDeleted,
		failedCounter:  reapFailed,
		stopCh:         make(chan struct{}),
		batchSize:      cfg.BatchSize,
	}
}

// Run блокируется до отмены ctx или Shutdown — предназначен для запуска в
// отдельной горутине (`go reaper.Run(ctx)`).
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopped (context done)")
			return
		case <-r.stopCh:
			r.log.Info("reaper stopped (shutdown requested)")
			return
		case <-ticker.C:
			r.wg.Add(1)
			func() {
				defer r.wg.Done()
				r.runOnce(ctx)
			}()
		}
	}
}

// Shutdown — kill-switch: останавливает тикер и дожидается завершения
// текущего прохода. Идемпотентна: повторный вызов не паникует. Тот же
// паттерн, что и у Reconciler.Shutdown.
func (r *Reaper) Shutdown(ctx context.Context) error {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		r.log.Info("reaper shutdown gracefully")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("reaper shutdown timeout: %w", ctx.Err())
	}
}

// runOnce — один проход: одна страница истёкших media. Публичный для тестов
// (не привязан к тикеру).
func (r *Reaper) runOnce(ctx context.Context) {
	ids, err := r.svc.mediaRepo.ListExpiredIDs(ctx, r.cfg.BatchSize)
	if err != nil {
		r.log.Error("reaper: list expired failed", slog.Any("error", err))
		r.failedCounter.Inc()
		return
	}
	r.scannedCounter.Add(float64(len(ids)))

	for _, id := range ids {
		select {
		case <-ctx.Done():
			r.log.Info("reaper: interrupted by shutdown")
			return
		default:
		}

		if r.cfg.DryRun {
			r.log.Info("dry-run: would delete expired media", slog.String("media_id", id.String()))
			continue
		}

		// MarkDeleting внутри deleteByID — атомарный claim (UPDATE ... WHERE
		// status <> 'deleting'). Он же гарантирует, что при нескольких
		// параллельных reaper-инстансах (несколько реплик сервиса) одну и ту
		// же запись фактически удалит только один из них.
		if err := r.svc.deleteByID(ctx, id); err != nil {
			r.log.Error("reaper: delete failed", slog.Any("error", err), slog.String("media_id", id.String()))
			r.failedCounter.Inc()
			continue
		}
		r.deletedCounter.Inc()
	}
}
