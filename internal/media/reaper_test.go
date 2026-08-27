package media

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/repo"
)

func TestReaper_TTL_DeletesExpiredOnly(t *testing.T) {
	expired1 := uuid.New()
	expired2 := uuid.New()

	deleted := map[uuid.UUID]bool{}
	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			return []uuid.UUID{expired1, expired2}, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
			return &repo.Media{ID: id, OwnerID: uuid.New()}, true, nil
		},
		hardDeleteFunc: func(ctx context.Context, id uuid.UUID) error {
			deleted[id] = true
			return nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaper(svc, 0, 100, svcTestLogger())

	r.runOnce(context.Background())

	assert.True(t, deleted[expired1])
	assert.True(t, deleted[expired2])
}

func TestReaper_EmptySelection_NoOp(t *testing.T) {
	calledMarkDeleting := false
	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			return nil, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
			calledMarkDeleting = true
			return nil, false, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaper(svc, 0, 100, svcTestLogger())

	require.NotPanics(t, func() { r.runOnce(context.Background()) })
	assert.False(t, calledMarkDeleting, "empty selection must not touch any record")
}

func TestReaper_OneItemFails_RestStillReaped(t *testing.T) {
	ok := uuid.New()
	failing := uuid.New()
	deleted := map[uuid.UUID]bool{}

	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			return []uuid.UUID{failing, ok}, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
			return &repo.Media{ID: id, OwnerID: uuid.New()}, true, nil
		},
		hardDeleteFunc: func(ctx context.Context, id uuid.UUID) error {
			if id == failing {
				return errors.New("db unavailable")
			}
			deleted[id] = true
			return nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaper(svc, 0, 100, svcTestLogger())

	require.NotPanics(t, func() { r.runOnce(context.Background()) })
	assert.True(t, deleted[ok], "failure on one record must not stop the rest of the batch")
}

func TestReaper_DefaultBatchSizeAppliedWhenNonPositive(t *testing.T) {
	svc := newTestSvc(&svcStubMediaRepo{}, &svcStubStorage{})
	r := NewReaper(svc, 0, 0, svcTestLogger())
	assert.Equal(t, defaultReapBatchSize, r.batchSize)
}

func TestReaper_DryRun_DoesNotDelete(t *testing.T) {
	expired := uuid.New()
	markDeletingCalled := false

	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			return []uuid.UUID{expired}, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
			markDeletingCalled = true
			return &repo.Media{ID: id}, true, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaperWithConfig(svc, ReaperConfig{BatchSize: 100, DryRun: true}, svcTestLogger())

	r.runOnce(context.Background())

	assert.False(t, markDeletingCalled, "dry-run must not touch any record, only report what it would delete")
}
