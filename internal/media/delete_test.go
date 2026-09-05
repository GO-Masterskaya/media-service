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

// --- deleteByID: базовый claim и идемпотентность ---
// Service.DeleteMedia больше нет (issue #18/#50 занял это имя своей
// attachment-семантикой) — deleteByID теперь приватный конвейер, вызываемый
// только DeleteByOwner и Reaper. Тестируем его напрямую.

func TestDeleteByID_ClaimTaken_Success(t *testing.T) {
	owner := uuid.New()
	mediaID := uuid.New()
	mr := &svcStubMediaRepo{
		media:             &repo.Media{ID: mediaID, OwnerID: owner, StorageKey: "orig"},
		markDeletingClaim: repo.ClaimTaken,
	}
	st := &svcStubStorage{}
	svc := newTestSvc(mr, st)

	deleted, err := svc.deleteByID(context.Background(), mediaID, true)
	require.NoError(t, err)
	assert.True(t, deleted, "ClaimTaken must result in an actual deletion")
}

func TestDeleteByID_ClaimNone_IsIdempotentSuccess(t *testing.T) {
	mediaID := uuid.New()
	mr := &svcStubMediaRepo{markDeletingClaim: repo.ClaimNone} // записи нет
	svc := newTestSvc(mr, &svcStubStorage{})

	deleted, err := svc.deleteByID(context.Background(), mediaID, true)
	require.NoError(t, err)
	assert.False(t, deleted, "ClaimNone is an idempotent no-op, not a deletion — see review PR #13/#17 counter-inflation fix")
}

// --- Ретрай зависшей deleting-записи (issue из первого раунда ревью) ---

func TestDeleteByID_ResumeStuckTrue_CompletesCleanup(t *testing.T) {
	owner := uuid.New()
	mediaID := uuid.New()
	stuck := &repo.Media{ID: mediaID, OwnerID: owner, Status: repo.MediaStatusDeleting, StorageKey: "orig"}

	deletePrefixCalled := false
	hardDeleteCalled := false

	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			// Запись уже была deleting -> ClaimAlreadyDeleting, а не ClaimTaken.
			// С resumeStuck=true очистка всё равно должна довестись до конца
			// (это и есть фикс из первого раунда ревью), в отличие от Reaper,
			// который вызывает deleteByID с resumeStuck=false (см. reaper_test.go).
			return stuck, repo.ClaimAlreadyDeleting, nil
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

	deleted, err := svc.deleteByID(context.Background(), mediaID, true)
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.True(t, deletePrefixCalled, "resumeStuck=true retry must actually attempt storage cleanup")
	assert.True(t, hardDeleteCalled, "resumeStuck=true retry must actually attempt row deletion")
}

func TestDeleteByID_ResumeStuckFalse_SkipsAlreadyClaimed(t *testing.T) {
	mediaID := uuid.New()
	deletePrefixCalled := false

	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			return &repo.Media{ID: id, OwnerID: uuid.New()}, repo.ClaimAlreadyDeleting, nil
		},
	}
	st := &svcStubStorage{
		deletePrefixFunc: func(ctx context.Context, prefix string) error {
			deletePrefixCalled = true
			return nil
		},
	}
	svc := newTestSvc(mr, st)

	deleted, err := svc.deleteByID(context.Background(), mediaID, false)
	require.NoError(t, err)
	assert.False(t, deleted, "skipped claim must not report as deleted")
	assert.False(t, deletePrefixCalled, "resumeStuck=false must skip a claim it doesn't own")
}

// --- DeleteByOwner: батчи, изоляция по owner, частичные сбои ---

func TestDeleteByOwner_DeterministicCount(t *testing.T) {
	owner := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	calls := 0
	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			return &repo.Media{ID: id, OwnerID: owner}, repo.ClaimTaken, nil
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

// TestDeleteByOwner_ClaimNoneRace_DoesNotInflateCount — регрессия из ревью:
// между ListDeletableByOwner и deleteByID запись мог успеть удалить кто-то
// другой (другой DeleteByOwner, DeleteMedia, Reaper, реконсилятор #24) —
// MarkDeleting в этом случае вернёт ClaimNone. Раньше deleteByID возвращал
// просто nil-ошибку на ClaimNone, и DeleteByOwner считал это удалением,
// хотя фактически ничего не произошло. Счётчик должен считать только
// реальные удаления.
func TestDeleteByOwner_ClaimNoneRace_DoesNotInflateCount(t *testing.T) {
	owner := uuid.New()
	alreadyGone := uuid.New()
	real1, real2 := uuid.New(), uuid.New()
	calls := 0

	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			if id == alreadyGone {
				return nil, repo.ClaimNone, nil // кто-то уже удалил её первым
			}
			return &repo.Media{ID: id, OwnerID: owner}, repo.ClaimTaken, nil
		},
		listDeletableByOwner: func(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
			calls++
			if calls > 1 {
				return nil, nil
			}
			return []uuid.UUID{real1, alreadyGone, real2}, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})

	n, err := svc.DeleteByOwner(context.Background(), owner, 100)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "only the 2 actually-deleted records should be counted, not the already-gone one")
}

func TestDeleteByOwner_PartialFailure_ContinuesAndReportsPartialCount(t *testing.T) {
	owner := uuid.New()
	failing := uuid.New()
	ok1, ok2 := uuid.New(), uuid.New()
	calls := 0

	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			return &repo.Media{ID: id, OwnerID: owner}, repo.ClaimTaken, nil
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

// TestDeleteByOwner_ContextCancelled_StopsPromptly — самостоятельно найденный
// аналог бага из ревью reaper (progressed/stopCh): у DeleteByOwner не было ни
// одной проверки ctx.Done() внутри цикла. Не бесконечный цикл (в конце концов
// все вызовы начали бы падать по отменённому ctx и сработал бы progressed==0),
// но лишняя, неотзывчивая на отмену работа. Тест фиксирует, что теперь
// остановка происходит сразу на следующей записи, а не после целой страницы
// гарантированно проваливающихся вызовов.
func TestDeleteByOwner_ContextCancelled_StopsPromptly(t *testing.T) {
	owner := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processed := 0
	mr := &svcStubMediaRepo{
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			processed++
			if processed == 2 {
				cancel() // отменяем сразу после второй успешно обработанной записи
			}
			return &repo.Media{ID: id, OwnerID: owner}, repo.ClaimTaken, nil
		},
		listDeletableByOwner: func(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
			ids := make([]uuid.UUID, limit)
			for i := range ids {
				ids[i] = uuid.New()
			}
			return ids, nil // "бесконечный" backlog — без ctx-check ушли бы вглубь
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})

	n, err := svc.DeleteByOwner(ctx, owner, 5)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, n, "must stop right after the item that triggered cancellation, not process the rest of the page")
}
