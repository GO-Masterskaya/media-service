package events

import (
	"context"
	"log/slog"
	"time"

	"mediaservice/internal/repo"
)

// ProcessedEventCleaner периодически чистит терминальные записи (done/dlq)
// из processed_events, чтобы таблица не росла бесконечно.
type ProcessedEventCleaner interface {
	Start(ctx context.Context)
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
		cfg.OlderThan = 7 * 24 * time.Hour
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 1000
	}
	return &processedEventCleaner{
		repo: repo,
		cfg:  cfg,
		log:  log,
	}
}

func (c *processedEventCleaner) Start(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	c.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			c.log.Info("processed event cleaner stopped")
			return
		case <-ticker.C:
			c.runOnce(ctx)
		}
	}
}

func (c *processedEventCleaner) runOnce(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-c.cfg.OlderThan)

	deleted, err := c.repo.DeleteTerminalOlderThan(ctx, cutoff, c.cfg.BatchLimit)
	if err != nil {
		c.log.Error("retention cleanup failed", slog.Any("error", err))
		return
	}

	if deleted > 0 {
		c.log.Info("retention cleanup completed",
			slog.Int64("deleted", deleted),
			slog.Time("cutoff", cutoff),
		)
	} else {
		c.log.Debug("retention cleanup: nothing to delete")
	}
}
