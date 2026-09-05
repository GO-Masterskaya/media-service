package processing

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

const (
	finalizationTimeout   = 5 * time.Second
	heartbeatMaxRetries   = 2 // ретраи heartbeat перед отменой jobCtx
	minQueueDepthInterval = 5 * time.Second
)

// Config задаёт параметры работы ядра движка обработки.
type Config struct {
	WorkerConcurrency int
	PollInterval      time.Duration
	JobTimeout        time.Duration
	LeaseDuration     time.Duration
	MaxAttempts       int
}

// Engine представляет собой ядро движка обработки задач.
//
// Каждый воркер самостоятельно забирает задачу из БД (pull-on-demand),
// выполняет хендлер с отдельным контекстом и таймаутом, и финализирует
// результат (MarkDone/FailJob) на неотменяемом контексте.
// Reaper-горутина периодически подбирает задачи с протухшим lease.
type Engine struct {
	cfg      Config
	repo     JobRepository
	registry *Registry
	metrics  *Metrics

	cancel      context.CancelFunc
	wg          sync.WaitGroup
	once        sync.Once
	started     bool
	mu          sync.Mutex
	shutdownErr error
	workersDone chan struct{}
}

// NewEngine создаёт и инициализирует новый Engine.
func NewEngine(cfg Config, repo JobRepository, registry *Registry, metrics *Metrics) *Engine {
	if cfg.WorkerConcurrency <= 0 {
		cfg.WorkerConcurrency = 2
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 12 * time.Minute
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	return &Engine{
		cfg:      cfg,
		repo:     repo,
		registry: registry,
		metrics:  metrics,
	}
}

// Start запускает воркеры и reaper под переданным контекстом.
// Идемпотентен: повторный вызов возвращает ошибку.
func (e *Engine) Start(parentCtx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return fmt.Errorf("processing engine already started")
	}

	if e.registry.Len() == 0 {
		slog.Warn("processing engine not started: no handlers registered")
		return nil
	}

	// Startup recovery: stale running jobs с протухшим lease → queued.
	recCtx, recCancel := context.WithTimeout(parentCtx, 30*time.Second)
	recovered, recErr := e.repo.RecoverStaleJobs(recCtx)
	recCancel()
	if recErr != nil {
		return fmt.Errorf("startup recovery: %w", recErr)
	}
	if recovered > 0 {
		e.metrics.JobsRecoveredTotal.Add(float64(recovered))
		slog.Info("startup recovery completed", "count", recovered)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	e.cancel = cancel
	e.workersDone = make(chan struct{})
	e.started = true
	e.shutdownErr = nil

	for i := 0; i < e.cfg.WorkerConcurrency; i++ {
		e.wg.Add(1)
		go e.workerLoop(ctx, i)
	}

	// Reaper: подбирает задачи с протухшим lease.
	e.wg.Add(1)
	go e.reaperLoop(ctx)

	slog.Info("processing engine started",
		"concurrency", e.cfg.WorkerConcurrency,
		"job_timeout", e.cfg.JobTimeout,
		"lease_duration", e.cfg.LeaseDuration,
		"heartbeat_interval", e.cfg.LeaseDuration/3,
		"max_attempts", e.cfg.MaxAttempts,
		"poll_interval", e.cfg.PollInterval,
	)

	return nil
}

// Stop останавливает движок с ограниченным дедлайном (для тестов / ручного вызова).
func (e *Engine) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = e.Shutdown(ctx)
}

// Wait блокирует до завершения всех worker/reaper горутин или до отмены ctx.
// Безопасно вызывать после Shutdown (в т.ч. после timeout) перед закрытием pool.
// При таймауте возвращает ошибку — вызывающий может best-effort закрыть infra.
func (e *Engine) Wait(ctx context.Context) error {
	e.mu.Lock()
	done := e.workersDone
	e.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("processing engine wait timeout: %w", ctx.Err())
	}
}

// Shutdown останавливает воркеры и reaper, дожидаясь завершения in-flight jobs
// в пределах ctx. Идемпотентен: повторный вызов возвращает тот же результат.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.once.Do(func() {
		if e.cancel != nil {
			e.cancel()
		}

		e.mu.Lock()
		if e.workersDone == nil {
			e.workersDone = make(chan struct{})
			close(e.workersDone)
			e.mu.Unlock()
			return
		}
		done := e.workersDone
		e.mu.Unlock()

		go func() {
			e.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			e.metrics.ShutdownGracefulTotal.Inc()
			slog.Info("processing engine stopped gracefully")
		case <-ctx.Done():
			err := fmt.Errorf("processing engine shutdown timeout: %w", ctx.Err())
			e.mu.Lock()
			e.shutdownErr = err
			e.mu.Unlock()
			e.metrics.ShutdownTimeoutTotal.Inc()
			slog.Warn("processing engine shutdown timed out", "error", err)
		}
	})

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdownErr
}

// reaperLoop периодически подбирает running-задачи с протухшим lease.
// Интервал = LeaseDuration (проверяем не реже чем раз в lease).
func (e *Engine) reaperLoop(ctx context.Context) {
	defer e.wg.Done()

	interval := e.cfg.LeaseDuration
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reaped, err := e.repo.RecoverStaleJobs(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("reaper: failed to recover stale jobs", "error", err)
				continue
			}
			if reaped > 0 {
				e.metrics.JobsRecoveredTotal.Add(float64(reaped))
				slog.Info("reaper: recovered expired leases", "count", reaped)
			}
		}
	}
}

// workerLoop выполняет цикл работы отдельного воркера.
// Воркер сам клеймит задачу, когда свободен (pull-on-demand).
func (e *Engine) workerLoop(ctx context.Context, workerID int) {
	defer e.wg.Done()

	var lastDepthCheck time.Time

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := e.repo.ClaimOne(ctx, e.cfg.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("worker failed to claim job",
				"worker_id", workerID,
				"error", err,
			)
			e.sleep(ctx, e.cfg.PollInterval)
			continue
		}

		if job == nil {
			// Очередь пуста — обновляем метрику (не чаще minQueueDepthInterval).
			if time.Since(lastDepthCheck) >= minQueueDepthInterval {
				if depth, err := e.repo.GetQueueDepth(ctx); err == nil {
					e.metrics.DBQueueDepth.Set(float64(depth))
				}
				lastDepthCheck = time.Now()
			}
			e.sleep(ctx, e.cfg.PollInterval)
			continue
		}

		// Обновляем метрику глубины очереди в БД (не чаще minQueueDepthInterval).
		if time.Since(lastDepthCheck) >= minQueueDepthInterval {
			if depth, err := e.repo.GetQueueDepth(ctx); err == nil {
				e.metrics.DBQueueDepth.Set(float64(depth))
			}
			lastDepthCheck = time.Now()
		}

		e.processJob(ctx, job, workerID)
	}
}

// processJob выполняет хендлер с recover() и обновляет статус задачи.
//
// jobCtx привязан к ctx движка (ТЗ §3.6): при shutdown хендлер получает
// отмену и задача возвращается в queued через ReleaseJobOnShutdown.
//
// Retry: retryable failure → ReleaseJobForRetry с backoff;
// permanent failure или max attempts → FailJob.
func (e *Engine) processJob(engineCtx context.Context, job *Job, workerID int) {
	e.metrics.InFlightWorkers.Inc()
	defer e.metrics.InFlightWorkers.Dec()

	startedAt := time.Now()
	defer func() {
		e.metrics.ProcessingDuration.Observe(time.Since(startedAt).Seconds())
	}()

	handler, err := e.registry.Get(job.Type)
	if err != nil {
		slog.Error("job rejected: unknown job type",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"error", err,
		)
		finCtx, finCancel := context.WithTimeout(context.Background(), finalizationTimeout)
		defer finCancel()
		if failErr := e.repo.FailJob(finCtx, job.ID.String(), "unknown job type: "+job.Type); failErr != nil {
			slog.Error("failed to mark job as failed", "job_id", job.ID.String(), "error", failErr)
		}
		e.metrics.JobsFailedTotal.Inc()
		return
	}

	// jobCtx привязан к engineCtx: при shutdown движка хендлер получает отмену.
	// JobTimeout ограничивает максимальное время выполнения одной задачи.
	jobCtx, jobCancel := context.WithTimeout(engineCtx, e.cfg.JobTimeout)

	// Heartbeat: продлеваем lease в фоне, пока handler работает.
	// При потере lease — отменяем jobCtx.
	heartbeatDone := make(chan struct{})
	go e.heartbeatLoop(jobCtx, jobCancel, job.ID.String(), workerID, heartbeatDone)

	// Запускаем хендлер с recover().
	handlerErr := e.safeHandle(jobCtx, handler, *job, workerID)
	jobCancel()

	// Останавливаем heartbeat и ждём его завершения.
	<-heartbeatDone

	// Финализация на неотменяемом контексте — гарантирует запись в БД.
	finCtx, finCancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer finCancel()

	if handlerErr != nil {
		if engineCtx.Err() != nil {
			slog.Info("job interrupted by shutdown, releasing back to queue",
				"worker_id", workerID,
				"job_id", job.ID.String(),
				"job_type", job.Type,
			)
			if releaseErr := e.repo.ReleaseJobOnShutdown(finCtx, job.ID.String()); releaseErr != nil {
				slog.Error("failed to release job on shutdown", "job_id", job.ID.String(), "error", releaseErr)
			} else {
				e.metrics.ShutdownJobsReleasedTotal.Inc()
			}
			return
		}

		handlerErr = ClassifyHandlerError(handlerErr)

		if IsPermanent(handlerErr) {
			slog.Error("job permanent failure",
				"worker_id", workerID,
				"job_id", job.ID.String(),
				"job_type", job.Type,
				"error", handlerErr,
			)
			if failErr := e.repo.FailJob(finCtx, job.ID.String(), handlerErr.Error()); failErr != nil {
				slog.Error("failed to mark job as failed", "job_id", job.ID.String(), "error", failErr)
			}
			e.metrics.JobsFailedTotal.Inc()
			return
		}

		slog.Error("job handling error",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"attempt", job.Attempts+1,
			"max_attempts", e.cfg.MaxAttempts,
			"error", handlerErr,
		)

		nextAttempt := job.Attempts + 1
		if nextAttempt < e.cfg.MaxAttempts {
			if releaseErr := e.repo.ReleaseJobForRetry(finCtx, job.ID.String(), nextAttempt, handlerErr.Error()); releaseErr != nil {
				slog.Error("failed to release job for retry", "job_id", job.ID.String(), "error", releaseErr)
			} else {
				e.metrics.JobsRetriedTotal.Inc()
				slog.Info("job released for retry",
					"job_id", job.ID.String(),
					"attempt", nextAttempt,
					"max_attempts", e.cfg.MaxAttempts,
				)
			}
			return
		}

		if failErr := e.repo.FailJob(finCtx, job.ID.String(), handlerErr.Error()); failErr != nil {
			slog.Error("failed to mark job as failed", "job_id", job.ID.String(), "error", failErr)
		}
		e.metrics.JobsFailedTotal.Inc()
		return
	}

	if err := e.repo.MarkDone(finCtx, job.ID.String()); err != nil {
		slog.Error("failed to mark job as done",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"error", err,
		)
		// Метрику НЕ инкрементим — задача фактически не помечена как done.
		// Reaper подберёт её после протухания lease.
		return
	}
	e.metrics.JobsProcessedTotal.Inc()
}

// heartbeatLoop продлевает lease задачи каждые LeaseDuration/3.
// Останавливается при отмене ctx (handler завершился или таймаут).
// При невозможности продлить lease (после heartbeatMaxRetries) — отменяет jobCtx,
// чтобы хендлер не работал впустую.
// Закрывает done канал при выходе.
func (e *Engine) heartbeatLoop(ctx context.Context, cancelJob context.CancelFunc, jobID string, workerID int, done chan<- struct{}) {
	defer close(done)

	interval := e.cfg.LeaseDuration / 3
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Используем фоновый контекст, чтобы heartbeat мог завершиться
			// даже если jobCtx уже отменён (race между ticker и cancel).
			extendCtx, extendCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := e.repo.ExtendLease(extendCtx, jobID, e.cfg.LeaseDuration)
			extendCancel()

			if err != nil {
				consecutiveFailures++
				e.metrics.LeaseExtensionErrorsTotal.Inc()
				slog.Warn("heartbeat: failed to extend lease",
					"worker_id", workerID,
					"job_id", jobID,
					"error", err,
					"consecutive_failures", consecutiveFailures,
				)
				if consecutiveFailures > heartbeatMaxRetries {
					slog.Error("heartbeat: lease lost, cancelling job",
						"worker_id", workerID,
						"job_id", jobID,
					)
					cancelJob()
					return
				}
				continue
			}
			consecutiveFailures = 0
			e.metrics.LeaseExtensionsTotal.Inc()
		}
	}
}

// safeHandle оборачивает вызов handler.Handle в recover().
// При панике возвращает ошибку, воркер продолжает работу.
func (e *Engine) safeHandle(ctx context.Context, handler Handler, job Job, workerID int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			slog.Error("panic in job handler",
				"worker_id", workerID,
				"job_id", job.ID.String(),
				"job_type", job.Type,
				"panic", r,
				"stack", string(stack),
			)
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return handler.Handle(ctx, job)
}

// sleep ждёт указанную длительность или завершения контекста.
func (e *Engine) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
