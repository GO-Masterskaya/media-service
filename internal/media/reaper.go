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

// reaperMetrics — по образцу cleanerMetrics (internal/events/retention.go,
// issue #60): собственный экземпляр на каждый Reaper вместо package-level
// sync.Once/глобалов. Регистрация — через явно переданный Registerer, а не
// жёстко на prometheus.DefaultRegisterer (см. ревью PR #13/#17), что заодно
// снимает и проблему повторной регистрации в тестах.
type reaperMetrics struct {
	scanned prometheus.Counter
	deleted prometheus.Counter
	failed  prometheus.Counter
}

func buildReaperMetrics() *reaperMetrics {
	return &reaperMetrics{
		scanned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "media", Subsystem: "reaper", Name: "scanned_total",
			Help: "Total expired media records scanned by the TTL reaper.",
		}),
		deleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "media", Subsystem: "reaper", Name: "deleted_total",
			Help: "Total media records deleted by the TTL reaper.",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "media", Subsystem: "reaper", Name: "failed_total",
			Help: "Total delete failures encountered by the TTL reaper.",
		}),
	}
}

func newReaperMetrics(reg prometheus.Registerer) *reaperMetrics {
	m := buildReaperMetrics()
	if reg != nil {
		reg.MustRegister(m.scanned, m.deleted, m.failed)
	}
	return m
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
	svc     *Service
	cfg     ReaperConfig
	log     *slog.Logger
	metrics *reaperMetrics

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// batchSize сохранён для обратной совместимости старых тестов/вызовов,
	// заглядывающих в r.batchSize напрямую; фактическое значение — cfg.BatchSize.
	batchSize int
}

// NewReaper создаёт reaper для тестов/обратной совместимости — метрики не
// регистрируются нигде (reg=nil), только собираются в памяти. Для реального
// запуска (main.go) используйте NewReaperWithConfig с явным Registerer.
func NewReaper(svc *Service, interval time.Duration, batchSize int, log *slog.Logger) *Reaper {
	return newReaper(svc, ReaperConfig{Interval: interval, BatchSize: batchSize}, log, nil)
}

// NewReaperWithConfig — полная форма конструктора, с поддержкой DryRun и
// явно переданным Registerer (nil — не регистрировать метрики нигде, удобно
// для тестов, где повторная регистрация на одном Registerer иначе паникует).
// Interval<=0 и BatchSize<=0 подменяются дефолтами — конструктор безопасен
// при прямом вызове в обход конфига/валидатора (см. ревью PR #13/#17: голый
// time.NewTicker(0) в Run иначе паникует).
func NewReaperWithConfig(svc *Service, cfg ReaperConfig, log *slog.Logger, reg prometheus.Registerer) *Reaper {
	return newReaper(svc, cfg, log, reg)
}

func newReaper(svc *Service, cfg ReaperConfig, log *slog.Logger, reg prometheus.Registerer) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultReapInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultReapBatchSize
	}

	return &Reaper{
		svc:       svc,
		cfg:       cfg,
		log:       log,
		metrics:   newReaperMetrics(reg),
		stopCh:    make(chan struct{}),
		batchSize: cfg.BatchSize,
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
			// runOnce вызывается синхронно (не в go func) — обёртка в
			// анонимную функцию тут не нужна, defer scoping'а требовать
			// нечему (см. ревью PR #13/#17).
			r.wg.Add(1)
			r.runOnce(ctx)
			r.wg.Done()
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
			r.metrics.failed.Inc()
			return
		}
		if len(ids) == 0 {
			return
		}
		r.metrics.scanned.Add(float64(len(ids)))

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
				r.metrics.failed.Inc()
				continue
			}
			// progressed считает любое успешное разрешение id (та же логика,
			// что в DeleteByOwner) — id не вернётся из ListExpiredIDs снова,
			// зацикливания не будет. deleted-метрика — только реальные
			// удаления, не идемпотентные no-op'ы (см. ревью PR #13/#17).
			progressed++
			if deletedNow {
				r.metrics.deleted.Inc()
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
