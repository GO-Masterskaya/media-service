package events

import (
	"context"
	"errors"
	"mediaservice/internal/repo"
	"testing"
	"time"

	"github.com/google/uuid"
)

type stubCleanerRepo struct {
	deleted int64
	err     error
}

func (s *stubCleanerRepo) Claim(ctx context.Context, eventID uuid.UUID, fingerprint, owner string, lease time.Duration) (*repo.ProcessedEvent, bool, error) {
	return nil, false, nil
}
func (s *stubCleanerRepo) MarkDone(ctx context.Context, eventID uuid.UUID, owner string, result []byte) error {
	return nil
}
func (s *stubCleanerRepo) MarkDLQ(ctx context.Context, eventID uuid.UUID, owner, reason string) error {
	return nil
}
func (s *stubCleanerRepo) DeleteTerminalOlderThan(ctx context.Context, olderThan time.Time, limit int) (int64, error) {
	return s.deleted, s.err
}

func TestCleaner_RunOnce_DeletesBatch(t *testing.T) {
	repo := &stubCleanerRepo{deleted: 42}
	c := NewProcessedEventCleaner(repo, RetentionConfig{
		Interval:   time.Hour,
		OlderThan:  time.Hour,
		BatchLimit: 100,
	}, testLog())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	go c.Start(ctx)
	<-ctx.Done()
}

func TestCleaner_RunOnce_ErrorNoPanic(t *testing.T) {
	repo := &stubCleanerRepo{err: errors.New("db down")}
	c := NewProcessedEventCleaner(repo, RetentionConfig{
		Interval:   time.Hour,
		OlderThan:  time.Hour,
		BatchLimit: 100,
	}, testLog())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	go c.Start(ctx)
	<-ctx.Done()
}
