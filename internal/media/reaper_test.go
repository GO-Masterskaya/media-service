package media

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/repo"
)

func TestReaper_TTL_DeletesExpiredOnly(t *testing.T) {
	expired1 := uuid.New()
	expired2 := uuid.New()
	seen := map[uuid.UUID]bool{}

	deleted := map[uuid.UUID]bool{}
	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			if seen[expired1] && seen[expired2] {
				return nil, nil // все страницы вычерпаны
			}
			return []uuid.UUID{expired1, expired2}, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			seen[id] = true
			return &repo.Media{ID: id, OwnerID: uuid.New()}, repo.ClaimTaken, nil
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
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			calledMarkDeleting = true
			return nil, repo.ClaimNone, nil
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
	page := 0

	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			page++
			if page > 1 {
				return nil, nil
			}
			return []uuid.UUID{failing, ok}, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			return &repo.Media{ID: id, OwnerID: uuid.New()}, repo.ClaimTaken, nil
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

// TestReaper_DefaultIntervalAppliedWhenNonPositive — регрессия из ревью:
// time.NewTicker паникует на interval<=0, если конструктор вызван напрямую
// в обход конфига/валидатора.
func TestReaper_DefaultIntervalAppliedWhenNonPositive(t *testing.T) {
	svc := newTestSvc(&svcStubMediaRepo{}, &svcStubStorage{})
	r := NewReaperWithConfig(svc, ReaperConfig{}, svcTestLogger())
	assert.Equal(t, defaultReapInterval, r.cfg.Interval)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // сразу отменяем — Run должен выйти по ctx.Done(), не запаниковав на тикере
	require.NotPanics(t, func() { r.Run(ctx) })
}

func TestReaper_DryRun_DoesNotDelete(t *testing.T) {
	expired := uuid.New()
	markDeletingCalled := false

	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			return []uuid.UUID{expired}, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			markDeletingCalled = true
			return &repo.Media{ID: id}, repo.ClaimTaken, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaperWithConfig(svc, ReaperConfig{BatchSize: 100, DryRun: true}, svcTestLogger())

	r.runOnce(context.Background())

	assert.False(t, markDeletingCalled, "dry-run must not touch any record, only report what it would delete")
}

// TestReaper_DryRun_FullPage_DoesNotLoopForever — прямая регрессия из
// третьего раунда ревью PR #13/#17: в dry-run ничего не клеймится, поэтому
// ListExpiredIDs при полной странице возвращала бы её снова и снова —
// без guard'а на отсутствие прогресса цикл в runOnce был бесконечным.
func TestReaper_DryRun_FullPage_DoesNotLoopForever(t *testing.T) {
	callCount := 0
	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			callCount++
			ids := make([]uuid.UUID, limit)
			for i := range ids {
				ids[i] = uuid.New()
			}
			return ids, nil // всегда ПОЛНАЯ страница — состояние не меняется
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaperWithConfig(svc, ReaperConfig{BatchSize: 5, DryRun: true}, svcTestLogger())

	done := make(chan struct{})
	go func() {
		r.runOnce(context.Background())
		close(done)
	}()

	select {
	case <-done:
		assert.Equal(t, 1, callCount, "dry-run must process exactly one page and stop, not loop on an unchanging page")
	case <-time.After(2 * time.Second):
		t.Fatal("runOnce did not return — infinite loop regression (dry-run never claims, page never changes)")
	}
}

// TestReaper_PersistentMarkDeletingError_DoesNotLoopForever — тот же класс
// регрессии: устойчивая ошибка MarkDeleting (например, БД недоступна) тоже
// не меняет состояние записей, и без guard'а страница вычитывалась бы заново
// бесконечно.
func TestReaper_PersistentMarkDeletingError_DoesNotLoopForever(t *testing.T) {
	callCount := 0
	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			callCount++
			ids := make([]uuid.UUID, limit)
			for i := range ids {
				ids[i] = uuid.New()
			}
			return ids, nil
		},
		markDeletingErr: errors.New("db unavailable"),
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaper(svc, 0, 5, svcTestLogger())

	done := make(chan struct{})
	go func() {
		r.runOnce(context.Background())
		close(done)
	}()

	select {
	case <-done:
		assert.Equal(t, 1, callCount, "a page where every MarkDeleting call fails must not be re-fetched forever")
	case <-time.After(2 * time.Second):
		t.Fatal("runOnce did not return — infinite loop regression (persistent MarkDeleting failure)")
	}
}

// TestReaper_Shutdown_InterruptsRunOnce — регрессия из третьего раунда
// ревью: runOnce проверял только ctx.Done(), не stopCh, так что Shutdown без
// одновременной отмены контекста не прерывал текущий проход.
func TestReaper_Shutdown_InterruptsRunOnce(t *testing.T) {
	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			ids := make([]uuid.UUID, limit)
			for i := range ids {
				ids[i] = uuid.New()
			}
			return ids, nil // "бесконечный" backlog — всегда полная страница
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			return &repo.Media{ID: id, OwnerID: uuid.New()}, repo.ClaimTaken, nil
		},
	}
	svc := newTestSvc(mr, &svcStubStorage{})
	r := NewReaper(svc, 0, 5, svcTestLogger())

	done := make(chan struct{})
	go func() {
		r.runOnce(context.Background()) // ctx НЕ отменяется — только Shutdown
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // дать runOnce пройти хотя бы одну страницу
	require.NoError(t, r.Shutdown(context.Background()))

	select {
	case <-done:
		// ок — runOnce прервался по stopCh
	case <-time.After(2 * time.Second):
		t.Fatal("runOnce did not stop after Shutdown — stopCh not checked")
	}
}

// TestReaper_SkipsAlreadyClaimedRecord_NoDoubleCleanup — прямая регрессия из
// второго раунда ревью PR #13/#17: MarkDeleting может вернуть
// ClaimAlreadyDeleting, когда запись параллельно забрала другая реплика
// reaper'а (или это её собственный, но чужой по смыслу claim). Reaper
// обязан ПРОПУСТИТЬ такую запись (resumeStuck=false), а не повторно гонять
// DeletePrefix/HardDelete — иначе дедупликация между репликами теряется.
func TestReaper_SkipsAlreadyClaimedRecord_NoDoubleCleanup(t *testing.T) {
	claimedByOther := uuid.New()

	deletePrefixCalled := false
	hardDeleteCalled := false

	mr := &svcStubMediaRepo{
		listExpiredIDs: func(ctx context.Context, limit int) ([]uuid.UUID, error) {
			return []uuid.UUID{claimedByOther}, nil
		},
		markDeletingFunc: func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
			// Имитируем: другая реплика уже выиграла claim секунду назад.
			return &repo.Media{ID: id, OwnerID: uuid.New()}, repo.ClaimAlreadyDeleting, nil
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
	r := NewReaper(svc, 0, 100, svcTestLogger())

	r.runOnce(context.Background())

	assert.False(t, deletePrefixCalled, "reaper must not clean up storage for a claim it doesn't own")
	assert.False(t, hardDeleteCalled, "reaper must not hard-delete a row it doesn't own the claim for")
}
