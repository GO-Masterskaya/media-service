package media

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

type recStubMediaRepo struct {
	mediaList []*repo.Media
	exists    map[uuid.UUID]struct{}
	err       error
}

func (s *recStubMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	if s.err != nil {
		return nil, s.err
	}
	if _, ok := s.exists[id]; ok {
		return &repo.Media{ID: id}, nil
	}
	return nil, repo.ErrNotFound
}
func (s *recStubMediaRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.mediaList, nil
}
func (s *recStubMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error { return nil }

func (s *recStubMediaRepo) MarkDeleting(ctx context.Context, id uuid.UUID) (*repo.Media, bool, error) {
	return nil, false, nil
}

func (s *recStubMediaRepo) ListDeletableByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
	return nil, nil
}

func (s *recStubMediaRepo) ListExpiredIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	return nil, nil
}

func (s *recStubMediaRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.exists, nil
}

type recStubStorage struct {
	objects []storage.ObjectInfo
	deleted []string
	err     error
}

func (s *recStubStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return nil
}
func (s *recStubStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (s *recStubStorage) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*storage.PresignedURL, error) {
	return nil, nil
}
func (s *recStubStorage) DeleteObject(ctx context.Context, key string) error {
	if s.err != nil {
		return s.err
	}
	s.deleted = append(s.deleted, key)
	return nil
}
func (s *recStubStorage) DeletePrefix(ctx context.Context, prefix string) error {
	if s.err != nil {
		return s.err
	}
	s.deleted = append(s.deleted, prefix)
	return nil
}
func (s *recStubStorage) Close() error { return nil }
func (s *recStubStorage) ForEachObject(ctx context.Context, prefix string, fn func(storage.ObjectInfo) error) error {
	for _, obj := range s.objects {
		if err := fn(obj); err != nil {
			return err
		}
	}
	return nil
}

func recTestLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestReconciler_Deleting_Success(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	mr := &recStubMediaRepo{
		mediaList: []*repo.Media{{
			ID:         mediaID,
			OwnerID:    owner,
			Status:     repo.MediaStatusDeleting,
			StorageKey: owner.String() + "/" + mediaID.String() + "/original.png",
		}},
	}
	sr := &recStubStorage{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, recTestLog())
	rec.reconcileDeleting(context.Background())

	require.Len(t, sr.deleted, 1)
	assert.Contains(t, sr.deleted[0], owner.String())
	assert.Contains(t, sr.deleted[0], mediaID.String())
}

func TestReconciler_Deleting_OwnershipGuard(t *testing.T) {
	mr := &recStubMediaRepo{
		mediaList: []*repo.Media{{
			ID:         uuid.New(),
			OwnerID:    uuid.New(),
			Status:     repo.MediaStatusDeleting,
			StorageKey: "evil/path/file.png",
		}},
	}
	sr := &recStubStorage{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, recTestLog())
	rec.reconcileDeleting(context.Background())

	assert.Empty(t, sr.deleted)
}

func TestReconciler_Deleting_DryRun(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	mr := &recStubMediaRepo{
		mediaList: []*repo.Media{{
			ID:         mediaID,
			OwnerID:    owner,
			Status:     repo.MediaStatusDeleting,
			StorageKey: owner.String() + "/" + mediaID.String() + "/original.png",
		}},
	}
	sr := &recStubStorage{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100, DryRun: true}, recTestLog())
	rec.reconcileDeleting(context.Background())

	assert.Empty(t, sr.deleted) // ничего не удалилось
}

func TestReconciler_Orphan_Deletes(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	key := owner.String() + "/" + mediaID.String() + "/original.png"

	mr := &recStubMediaRepo{exists: map[uuid.UUID]struct{}{}}
	sr := &recStubStorage{
		objects: []storage.ObjectInfo{{
			Key:             key,
			LastModified:    time.Now().Add(-10 * time.Minute),
			UploadStartedAt: time.Now().Add(-10 * time.Minute), // +++ FIX
		}},
	}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, recTestLog())
	rec.reconcileOrphans(context.Background())

	require.Len(t, sr.deleted, 1)
	assert.Equal(t, key, sr.deleted[0])
}

func TestReconciler_Orphan_InFlightProtected(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	key := owner.String() + "/" + mediaID.String() + "/original.png"

	mr := &recStubMediaRepo{exists: map[uuid.UUID]struct{}{}}
	sr := &recStubStorage{
		objects: []storage.ObjectInfo{{
			Key:             key,
			LastModified:    time.Now().Add(-30 * time.Second),
			UploadStartedAt: time.Now().Add(-30 * time.Second), // +++ FIX
		}},
	}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, recTestLog())
	rec.reconcileOrphans(context.Background())

	assert.Empty(t, sr.deleted)
}

func TestReconciler_Orphan_ExistsInDB(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	key := owner.String() + "/" + mediaID.String() + "/original.png"

	mr := &recStubMediaRepo{exists: map[uuid.UUID]struct{}{mediaID: {}}}
	sr := &recStubStorage{
		objects: []storage.ObjectInfo{{
			Key:          key,
			LastModified: time.Now().Add(-10 * time.Minute),
		}},
	}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, recTestLog())
	rec.reconcileOrphans(context.Background())

	assert.Empty(t, sr.deleted)
}

func TestReconciler_RestartSafe(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	mr := &recStubMediaRepo{
		mediaList: []*repo.Media{{
			ID:         mediaID,
			OwnerID:    owner,
			Status:     repo.MediaStatusDeleting,
			StorageKey: owner.String() + "/" + mediaID.String() + "/original.png",
		}},
	}
	sr := &recStubStorage{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, recTestLog())

	rec.reconcileDeleting(context.Background())
	require.Len(t, sr.deleted, 1)

	rec.reconcileDeleting(context.Background())
}

func TestReconciler_Shutdown(t *testing.T) {
	mr := &recStubMediaRepo{}
	sr := &recStubStorage{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{Interval: time.Hour}, recTestLog())

	ctx, cancel := context.WithCancel(context.Background())
	go rec.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	err := rec.Shutdown(shutdownCtx)
	require.NoError(t, err)

	// Повторный Shutdown — идемпотентен, не паникует
	err = rec.Shutdown(shutdownCtx)
	require.NoError(t, err)

	cancel()
}
