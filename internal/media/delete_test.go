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

func newTestSvc(mr *svcStubMediaRepo, st *svcStubStorage) *Service {
	return newTestService(mr, &svcStubDerivRepo{}, st)
}

// --- DeleteMedia: владение ---

func TestDeleteMedia_Own_Success(t *testing.T) {
	owner := uuid.New()
	mediaID := uuid.New()
	mr := &svcStubMediaRepo{
		media:             &repo.Media{ID: mediaID, OwnerID: owner, StorageKey: "orig"},
		markDeletingFound: true,
	}
	st := &svcStubStorage{}
	svc := newTestSvc(mr, st)

	err := svc.DeleteMedia(context.Background(), owner, mediaID)
	require.NoError(t, err)
}

func TestDeleteMedia_SomeoneElses_PermissionDenied(t *testing.T) {
	owner := uuid.New()
	stranger := uuid.New()
	mediaID := uuid.New()
	mr := &svcStubMediaRepo{
		media:             &repo.Media{ID: mediaID, OwnerID: owner, StorageKey: "orig"},
		markDeletingFound: true, // если бы дошло досюда — это была бы дыра
	}
	svc := newTestSvc(mr, &svcStubStorage{})

	err := svc.DeleteMedia(context.Background(), stranger, mediaID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PermissionDenied")
}

// --- Повтор удаления ---

func TestDeleteMedia_RepeatOnAlreadyGone_IsIdempotentSuccess(t *testing.T) {
	owner := uuid.New()
	mediaID := uuid.New()
	mr := &svcStubMediaRepo{err: repo.ErrNotFound} // GetByID больше не находит запись
	svc := newTestSvc(mr, &svcStubStorage{})

	err := svc.DeleteMedia(context.Background(), owner, mediaID)
	require.NoError(t, err)
}

// --- Ретрай зависшей deleting-записи (регрессия из ревью PR #13/#17) ---

func TestDeleteMedia_RetryStuckDeletingRecord_CompletesCleanup(t *testing.T) {
	owner := uuid.New()
	mediaID := uuid.New()
	stuck := &repo.Media{ID: mediaID, OwnerID: owner, Status: repo.MediaStatusDeleting, StorageKey: "orig"}

	deletePrefixCalled := false
	hardDeleteCalled := false

	mr := &svcStubMediaRepo{
		media: stuck, // GetByID (проверка владельца) видит зависшую запись
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
			// Имитируем ПРАВИЛЬНОЕ поведение репозитория: запись уже была
			// deleting, но found=true, потому что она реально существует —
			// а не found=false, как было в баге из ревью (когда повтор молча
			// "успевал" без факта удаления).
			return stuck, true, nil
		},
		hardDeleteFunc: func(ctx context.Context, id uuid.UUID) error {
			hardDeleteCalled = true
			return nil
		},
	}
	st := &svcStubStorage{
		deletePrefixFunc: func(ctx context.Context, prefix string) error {
			deletePrefixCalled = true
			return nil
		},
	}
	svc := newTestSvc(mr, st)

	err := svc.DeleteMedia(context.Background(), owner, mediaID)
	require.NoError(t, err)
	assert.True(t, deletePrefixCalled, "retry on a stuck deleting record must actually attempt storage cleanup")
	assert.True(t, hardDeleteCalled, "retry on a stuck deleting record must actually attempt row deletion")
}

// --- DeleteByOwner: батчи, изоляция по owner, частичные сбои ---

func TestDeleteByOwner_DeterministicCount(t *testing.T) {
	owner := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	calls := 0
	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
			return &repo.Media{ID: id, OwnerID: owner}, true, nil
		},
		listDeletableByOwner: func(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
			calls++
			if calls > 1 {
				return nil, nil // вторая страница пустая — batch завершён
			}
			return ids, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})

	n, err := svc.DeleteByOwner(context.Background(), owner, 100)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestDeleteByOwner_PartialFailure_ContinuesAndReportsPartialCount(t *testing.T) {
	owner := uuid.New()
	failing := uuid.New()
	ok1, ok2 := uuid.New(), uuid.New()
	calls := 0

	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
			return &repo.Media{ID: id, OwnerID: owner}, true, nil
		},
		hardDeleteFunc: func(ctx context.Context, id uuid.UUID) error {
			if id == failing {
				return errors.New("db unavailable for this row")
			}
			return nil
		},
		listDeletableByOwner: func(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
			calls++
			if calls > 1 {
				return nil, nil
			}
			return []uuid.UUID{ok1, failing, ok2}, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})

	n, err := svc.DeleteByOwner(context.Background(), owner, 100)
	require.NoError(t, err) // одна ошибка в batch не должна ронять весь вызов
	assert.Equal(t, 2, n, "two of three items should have been deleted")
}

func TestDeleteByOwner_EmptySelection_ReturnsZero(t *testing.T) {
	owner := uuid.New()
	mr := &svcStubMediaRepo{
		listDeletableByOwner: func(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
			return nil, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})

	n, err := svc.DeleteByOwner(context.Background(), owner, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}
