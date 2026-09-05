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

// defaultReapInterval используется, если вызывающий код передал Interval<=0.
// Актуально при прямом вызове NewReaperWithConfig в обход конфига/валидатора
// (time.NewTicker паникует на неположительном интервале) — см. ревью PR #13/#17.
const defaultReapInterval = time.Minute

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
// Interval<=0 и BatchSize<=0 подменяются дефолтами — конструктор безопасен
// при прямом вызове в обход конфига/валидатора (см. ревью PR #13/#17: голый
// time.NewTicker(0) в Run иначе паникует).
func NewReaperWithConfig(svc *Service, cfg ReaperConfig, log *slog.Logger) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultReapInterval
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
		// Приоритетная неблокирующая проверка: если стоп-сигнал уже пришёл
		// одновременно с тиком, обычный select ниже выбрал бы между ними
		// псевдослучайно (гарантий приоритета у Go select нет) — и мог бы
		// запустить ещё один полный проход runOnce уже после Shutdown (см.
		// ревью PR #13/#17). Эта проверка гарантирует, что стоп побеждает.
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopped (context done)")
			return
		case <-r.stopCh:
			r.log.Info("reaper stopped (shutdown requested)")
			return
		default:
		}

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

// runOnce — один тик: вычерпывает ВСЕ страницы истёкших media, а не только
// одну (см. ревью PR #13/#17 — при интервале в минуту и одной странице за
// тик накопленный backlog никогда не разгребается: потолок BatchSize
// удалений в минуту независимо от объёма просроченных записей). Продолжает,
// пока очередная страница полная (== BatchSize); последняя неполная/пустая
// страница завершает проход. Публичный для тестов (не привязан к тикеру).
func (r *Reaper) runOnce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper: interrupted by shutdown")
			return
		case <-r.stopCh:
			r.log.Info("reaper: interrupted by shutdown")
			return
		default:
		}

		ids, err := r.svc.mediaRepo.ListExpiredIDs(ctx, r.cfg.BatchSize)
		if err != nil {
			r.log.Error("reaper: list expired failed", slog.Any("error", err))
			r.failedCounter.Inc()
			return
		}
		if len(ids) == 0 {
			return
		}
		r.scannedCounter.Add(float64(len(ids)))

		progressed := 0
		for _, id := range ids {
			select {
			case <-ctx.Done():
				r.log.Info("reaper: interrupted by shutdown")
				return
			case <-r.stopCh:
				r.log.Info("reaper: interrupted by shutdown")
				return
			default:
			}

			if r.cfg.DryRun {
				r.log.Info("dry-run: would delete expired media", slog.String("media_id", id.String()))
				continue
			}

			// resumeStuck=false: claim этого тика получает ровно одна
			// реплика reaper'а. Если MarkDeleting вернул ClaimAlreadyDeleting
			// (кто-то другой уже взял эту запись — другая реплика прямо
			// сейчас, или это зависшая с прошлой прерванной попытки), эта
			// реплика её ПРОПУСКАЕТ, а не доводит очистку сама — иначе две
			// реплики параллельно вызовут DeletePrefix+HardDelete на одной
			// записи, и дедупликация между репликами исчезнет (см. ревью).
			// Зависшие записи — зона ответственности фоновой сверки (#24).
			deletedNow, err := r.svc.deleteByID(ctx, id, false)
			if err != nil {
				r.log.Error("reaper: delete failed", slog.Any("error", err), slog.String("media_id", id.String()))
				r.failedCounter.Inc()
				continue
			}
			// progressed считает любое успешное разрешение id (та же логика,
			// что в DeleteByOwner) — id не вернётся из ListExpiredIDs снова,
			// зацикливания не будет. deletedCounter — только реальные
			// удаления, не идемпотентные no-op'ы (см. ревью PR #13/#17).
			progressed++
			if deletedNow {
				r.deletedCounter.Inc()
			}
		}

		if progressed == 0 {
			// Ни одна запись за этот проход не сдвинулась — dry-run (никогда
			// не клеймит) или устойчивая ошибка MarkDeleting/БД. ListExpiredIDs
			// вернула бы ту же страницу снова — прекращаем, чтобы не уйти в
			// бесконечный цикл (см. ревью PR #13/#17).
			return
		}
		if len(ids) < r.cfg.BatchSize {
			return // последняя (неполная) страница
		}
	}
}
