package events

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"mediaservice/internal/repo"
)

type ProcessedEventCleaner interface {
	Start(ctx context.Context)
	Shutdown(ctx context.Context) error
}

type RetentionConfig struct {
	Interval   time.Duration
	OlderThan  time.Duration
	BatchLimit int
}

type processedEventCleaner struct {
	repo repo.ProcessedEventRepo
	cfg  RetentionConfig
	log  *slog.Logger

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewProcessedEventCleaner(
	repo repo.ProcessedEventRepo,
	cfg RetentionConfig,
	log *slog.Logger,
) ProcessedEventCleaner {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 1 * time.Hour
	}
	if cfg.OlderThan <= 0 {
		cfg.OlderThan = 30 * 24 * time.Hour // 30 days; must exceed Kafka topic retention
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 1000
	}
	return &processedEventCleaner{
		repo:   repo,
		cfg:    cfg,
		log:    log,
		stopCh: make(chan struct{}),
	}
}

func (c *processedEventCleaner) Start(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runOnce(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			c.log.Info("processed event cleaner stopped")
			return
		case <-c.stopCh:
			c.log.Info("processed event cleaner stopped (shutdown requested)")
			return
		case <-ticker.C:
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.runOnce(ctx)
			}()
		}
	}
}

func (c *processedEventCleaner) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.log.Info("processed event cleaner shutdown gracefully")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *processedEventCleaner) runOnce(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-c.cfg.OlderThan)
	var totalDeleted int64

	for {
		select {
		case <-ctx.Done():
			c.log.Info("retention cleanup interrupted by shutdown", slog.Int64("deleted_this_run", totalDeleted))
			return
		default:
		}

		deleted, err := c.repo.DeleteTerminalOlderThan(ctx, cutoff, c.cfg.BatchLimit)
		if err != nil {
			c.log.Error("retention cleanup failed", slog.Any("error", err))
			return
		}

		if deleted == 0 {
			break
		}

		totalDeleted += deleted
		c.log.Debug("retention cleanup batch", slog.Int64("deleted", deleted))

		if deleted < int64(c.cfg.BatchLimit) {
			break
		}
	}

	if totalDeleted > 0 {
		c.log.Info("retention cleanup completed",
			slog.Int64("deleted", totalDeleted),
			slog.Time("cutoff", cutoff),
		)
	} else {
		c.log.Debug("retention cleanup: nothing to delete")
	}
}
