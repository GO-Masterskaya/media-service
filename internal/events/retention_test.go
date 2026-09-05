package events

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/repo"

	"github.com/prometheus/client_golang/prometheus"
)

type batchingCleanerRepo struct {
	totalRecords int
	deletedCalls []int
	err          error

	bumpAttempt int
	bumpErr     error
}

func (s *batchingCleanerRepo) Claim(
	ctx context.Context,
	eventID uuid.UUID,
	fingerprint string,
	owner string,
	lease time.Duration,
) (*repo.ProcessedEvent, bool, error) {
	return nil, false, nil
}

func (s *batchingCleanerRepo) MarkDone(
	ctx context.Context,
	eventID uuid.UUID,
	owner string,
	result []byte,
) error {
	return nil
}

func (s *batchingCleanerRepo) MarkDLQ(
	ctx context.Context,
	eventID uuid.UUID,
	owner string,
	reason string,
) error {
	return nil
}

func (s *batchingCleanerRepo) DeleteTerminalOlderThan(
	ctx context.Context,
	olderThan time.Time,
	limit int,
) (int64, error) {
	if s.err != nil {
		return 0, s.err
	}

	toDelete := min(limit, s.totalRecords)

	if toDelete <= 0 {
		return 0, nil
	}

	s.totalRecords -= toDelete
	s.deletedCalls = append(s.deletedCalls, toDelete)

	return int64(toDelete), nil
}

func (s *batchingCleanerRepo) BumpAttempt(
	ctx context.Context,
	eventID uuid.UUID,
	owner string,
) (int, error) {
	if s.bumpErr != nil {
		return 0, s.bumpErr
	}

	s.bumpAttempt++

	return s.bumpAttempt, nil
}

func TestCleaner_RunOnce_MultipleBatches(t *testing.T) {
	stub := &batchingCleanerRepo{
		totalRecords: 2500,
	}

	c := NewProcessedEventCleaner(
		stub,
		RetentionConfig{
			Interval:   time.Hour,
			OlderThan:  time.Hour,
			BatchLimit: 1000,
		},
		testLog(),
		prometheus.NewRegistry(),
	)

	c.(*processedEventCleaner).runOnce(context.Background())

	assert.Equal(t, []int{1000, 1000, 500}, stub.deletedCalls)
	assert.Equal(t, 0, stub.totalRecords)
}

func TestCleaner_RunOnce_ZeroRecords(t *testing.T) {
	stub := &batchingCleanerRepo{
		totalRecords: 0,
	}

	c := NewProcessedEventCleaner(
		stub,
		RetentionConfig{
			Interval:   time.Hour,
			OlderThan:  time.Hour,
			BatchLimit: 1000,
		},
		testLog(),
		prometheus.NewRegistry(),
	)

	c.(*processedEventCleaner).runOnce(context.Background())

	assert.Empty(t, stub.deletedCalls)
	assert.Equal(t, 0, stub.totalRecords)
}

func TestCleaner_RunOnce_ErrorStopsLoop(t *testing.T) {
	stub := &batchingCleanerRepo{
		totalRecords: 5000,
		err:          errors.New("db down"),
	}

	c := NewProcessedEventCleaner(
		stub,
		RetentionConfig{
			Interval:   time.Hour,
			OlderThan:  time.Hour,
			BatchLimit: 1000,
		},
		testLog(),
		prometheus.NewRegistry(),
	)

	c.(*processedEventCleaner).runOnce(context.Background())

	assert.Empty(t, stub.deletedCalls)
	assert.Equal(t, 5000, stub.totalRecords)
}

func TestCleaner_ContextCancel_BetweenBatches(t *testing.T) {
	stub := &batchingCleanerRepo{
		totalRecords: 5000,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := 0

	controlledStub := &controlledCancellingRepo{
		inner:       stub,
		cancelAfter: 1,
		cancel:      cancel,
		callCount:   &callCount,
	}

	c := NewProcessedEventCleaner(
		controlledStub,
		RetentionConfig{
			Interval:   time.Hour,
			OlderThan:  time.Hour,
			BatchLimit: 1000,
		},
		testLog(),
		prometheus.NewRegistry(),
	)

	c.(*processedEventCleaner).runOnce(ctx)

	assert.Equal(t, []int{1000}, stub.deletedCalls)
	assert.Equal(t, 4000, stub.totalRecords)
	assert.Equal(t, 1, callCount)
}

type controlledCancellingRepo struct {
	inner       *batchingCleanerRepo
	cancelAfter int
	cancel      context.CancelFunc
	callCount   *int
}

func (c *controlledCancellingRepo) DeleteTerminalOlderThan(
	ctx context.Context,
	olderThan time.Time,
	limit int,
) (int64, error) {
	*c.callCount++

	deleted, err := c.inner.DeleteTerminalOlderThan(
		ctx,
		olderThan,
		limit,
	)

	if *c.callCount >= c.cancelAfter {
		c.cancel()
	}

	return deleted, err
}

func (c *controlledCancellingRepo) Claim(
	ctx context.Context,
	eventID uuid.UUID,
	fingerprint string,
	owner string,
	lease time.Duration,
) (*repo.ProcessedEvent, bool, error) {
	return c.inner.Claim(ctx, eventID, fingerprint, owner, lease)
}

func (c *controlledCancellingRepo) MarkDone(
	ctx context.Context,
	eventID uuid.UUID,
	owner string,
	result []byte,
) error {
	return c.inner.MarkDone(ctx, eventID, owner, result)
}

func (c *controlledCancellingRepo) MarkDLQ(
	ctx context.Context,
	eventID uuid.UUID,
	owner string,
	reason string,
) error {
	return c.inner.MarkDLQ(ctx, eventID, owner, reason)
}

func (c *controlledCancellingRepo) BumpAttempt(
	ctx context.Context,
	eventID uuid.UUID,
	owner string,
) (int, error) {
	return c.inner.BumpAttempt(ctx, eventID, owner)
}

type periodicityRepo struct{ calls *atomic.Int32 }

func (p *periodicityRepo) Claim(ctx context.Context, eventID uuid.UUID, fingerprint, owner string, lease time.Duration) (*repo.ProcessedEvent, bool, error) {
	return nil, false, nil
}
func (p *periodicityRepo) MarkDone(ctx context.Context, eventID uuid.UUID, owner string, result []byte) error {
	return nil
}
func (p *periodicityRepo) MarkDLQ(ctx context.Context, eventID uuid.UUID, owner, reason string) error {
	return nil
}
func (p *periodicityRepo) DeleteTerminalOlderThan(ctx context.Context, olderThan time.Time, limit int) (int64, error) {
	p.calls.Add(1)
	return 0, nil
}
func (p *periodicityRepo) BumpAttempt(ctx context.Context, eventID uuid.UUID, owner string) (int, error) {
	return 0, nil
}

func TestCleaner_Start_Periodicity(t *testing.T) {
	var calls atomic.Int32
	pr := &periodicityRepo{calls: &calls}
	c := NewProcessedEventCleaner(
		pr,
		RetentionConfig{
			Interval:   50 * time.Millisecond,
			OlderThan:  time.Hour,
			BatchLimit: 1000,
		},
		testLog(),
		prometheus.NewRegistry(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { c.Start(ctx); close(done) }()

	// 50 мс интервал → за 3 секунды должно быть ~60 тиков, даже если половина пропущена.
	require.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, 3*time.Second, 10*time.Millisecond, "expected startup run + at least one ticker tick")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.Shutdown(shutdownCtx))
	<-done
}
