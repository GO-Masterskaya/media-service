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

type stubMediaRepoForReconciler struct {
	mediaList []*repo.Media
	exists    map[uuid.UUID]struct{}
	err       error
}

func (s *stubMediaRepoForReconciler) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	if s.err != nil {
		return nil, s.err
	}
	if _, ok := s.exists[id]; ok {
		return &repo.Media{ID: id}, nil
	}
	return nil, repo.ErrNotFound
}
func (s *stubMediaRepoForReconciler) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.mediaList, nil
}
func (s *stubMediaRepoForReconciler) HardDelete(ctx context.Context, id uuid.UUID) error { return nil }
func (s *stubMediaRepoForReconciler) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.exists, nil
}

type stubStorageForReconciler struct {
	objects []storage.ObjectInfo
	deleted []string
	err     error
}

func (s *stubStorageForReconciler) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return nil
}
func (s *stubStorageForReconciler) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (s *stubStorageForReconciler) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*storage.PresignedURL, error) {
	return nil, nil
}
func (s *stubStorageForReconciler) DeleteObject(ctx context.Context, key string) error {
	if s.err != nil {
		return s.err
	}
	s.deleted = append(s.deleted, key)
	return nil
}
func (s *stubStorageForReconciler) DeletePrefix(ctx context.Context, prefix string) error {
	if s.err != nil {
		return s.err
	}
	s.deleted = append(s.deleted, prefix)
	return nil
}
func (s *stubStorageForReconciler) Close() error { return nil }
func (s *stubStorageForReconciler) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return s.objects, nil
}

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestReconciler_Deleting_Success(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	mr := &stubMediaRepoForReconciler{
		mediaList: []*repo.Media{{
			ID:         mediaID,
			OwnerID:    owner,
			Status:     repo.MediaStatusDeleting,
			StorageKey: owner.String() + "/" + mediaID.String() + "/original.png",
		}},
	}
	sr := &stubStorageForReconciler{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, testLog())
	rec.reconcileDeleting(context.Background())

	require.Len(t, sr.deleted, 1)
	assert.Contains(t, sr.deleted[0], owner.String())
	assert.Contains(t, sr.deleted[0], mediaID.String())
}

func TestReconciler_Deleting_OwnershipGuard(t *testing.T) {
	mr := &stubMediaRepoForReconciler{
		mediaList: []*repo.Media{{
			ID:         uuid.New(),
			OwnerID:    uuid.New(),
			Status:     repo.MediaStatusDeleting,
			StorageKey: "evil/path/file.png",
		}},
	}
	sr := &stubStorageForReconciler{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, testLog())
	rec.reconcileDeleting(context.Background())

	assert.Empty(t, sr.deleted)
}

func TestReconciler_Orphan_Deletes(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	key := owner.String() + "/" + mediaID.String() + "/original.png"

	mr := &stubMediaRepoForReconciler{exists: map[uuid.UUID]struct{}{}}
	sr := &stubStorageForReconciler{
		objects: []storage.ObjectInfo{{
			Key:          key,
			LastModified: time.Now().Add(-10 * time.Minute),
		}},
	}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, testLog())
	rec.reconcileOrphans(context.Background())

	require.Len(t, sr.deleted, 1)
	assert.Equal(t, key, sr.deleted[0])
}

func TestReconciler_Orphan_InFlightProtected(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	key := owner.String() + "/" + mediaID.String() + "/original.png"

	mr := &stubMediaRepoForReconciler{exists: map[uuid.UUID]struct{}{}}
	sr := &stubStorageForReconciler{
		objects: []storage.ObjectInfo{{
			Key:          key,
			LastModified: time.Now().Add(-30 * time.Second),
		}},
	}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, testLog())
	rec.reconcileOrphans(context.Background())

	assert.Empty(t, sr.deleted)
}

func TestReconciler_Orphan_ExistsInDB(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	key := owner.String() + "/" + mediaID.String() + "/original.png"

	mr := &stubMediaRepoForReconciler{exists: map[uuid.UUID]struct{}{mediaID: {}}}
	sr := &stubStorageForReconciler{
		objects: []storage.ObjectInfo{{
			Key:          key,
			LastModified: time.Now().Add(-10 * time.Minute),
		}},
	}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, testLog())
	rec.reconcileOrphans(context.Background())

	assert.Empty(t, sr.deleted)
}

func TestReconciler_RestartSafe(t *testing.T) {
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mediaID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	mr := &stubMediaRepoForReconciler{
		mediaList: []*repo.Media{{
			ID:         mediaID,
			OwnerID:    owner,
			Status:     repo.MediaStatusDeleting,
			StorageKey: owner.String() + "/" + mediaID.String() + "/original.png",
		}},
	}
	sr := &stubStorageForReconciler{}

	rec := NewReconciler(mr, sr, ReconcilerConfig{GracePeriod: time.Minute, BatchSize: 100}, testLog())

	rec.reconcileDeleting(context.Background())
	require.Len(t, sr.deleted, 1)

	rec.reconcileDeleting(context.Background())
}
