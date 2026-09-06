package media

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// ---------- stubs (уникальные имена, чтобы не конфликтовать с reconciler_test) ----------

type svcStubMediaRepo struct {
	media *repo.Media
	err   error

	// --- delete/TTL (issues #13, #17) ---
	markDeletingClaim repo.ClaimState
	markDeletingErr   error
	hardDeleteErr     error

	// markDeletingFunc, если задан, заменяет статичное markDeletingClaim —
	// нужен для сценариев вроде "ретрай зависшей deleting-записи", где
	// поведение должно зависеть от текущего состояния, а не быть константой.
	markDeletingFunc func(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error)

	// hardDeleteFunc, если задан, заменяет статичный hardDeleteErr — нужен
	// для сценариев вроде "одна запись в batch не удаляется, остальные да".
	hardDeleteFunc func(ctx context.Context, id uuid.UUID) error

	listDeletableByOwner func(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error)
	listExpiredIDs       func(ctx context.Context, limit int) ([]uuid.UUID, error)
}

func (s *svcStubMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	return s.media, s.err
}
func (s *svcStubMediaRepo) GetByOwnerIdempotency(ctx context.Context, ownerID uuid.UUID, idempotencyKey string) (*repo.Media, error) {
	return s.media, s.err
}
func (s *svcStubMediaRepo) InsertWithJobs(ctx context.Context, m repo.Media, jobTypes []string) (*repo.Media, error) {
	return &m, s.err
}
func (s *svcStubMediaRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	return nil, nil
}
func (s *svcStubMediaRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	return nil, nil
}

func (s *svcStubMediaRepo) MarkDeleting(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
	if s.markDeletingFunc != nil {
		return s.markDeletingFunc(ctx, id)
	}
	if s.markDeletingErr != nil {
		return nil, repo.ClaimNone, s.markDeletingErr
	}
	if s.markDeletingClaim == repo.ClaimNone {
		return nil, repo.ClaimNone, nil
	}
	return s.media, s.markDeletingClaim, nil
}

func (s *svcStubMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error {
	if s.hardDeleteFunc != nil {
		return s.hardDeleteFunc(ctx, id)
	}
	return s.hardDeleteErr
}

func (s *svcStubMediaRepo) ListDeletableByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
	if s.listDeletableByOwner != nil {
		return s.listDeletableByOwner(ctx, ownerID, limit)
	}
	return nil, nil
}

func (s *svcStubMediaRepo) ListExpiredIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if s.listExpiredIDs != nil {
		return s.listExpiredIDs(ctx, limit)
	}
	return nil, nil
}

func (s *svcStubMediaRepo) CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error {
	return nil
}
func (s *svcStubMediaRepo) DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (usagesRemaining int, err error) {
	return 0, nil
}

type svcStubDerivRepo struct {
	deriv *repo.Derivative
	err   error
}

func (s *svcStubDerivRepo) GetByMediaAndVariant(ctx context.Context, mediaID uuid.UUID, variant string) (*repo.Derivative, error) {
	return s.deriv, s.err
}

func (s *svcStubDerivRepo) Insert(ctx context.Context, d repo.Derivative) (*repo.Derivative, error) {
	return &d, s.err
}

func (s *svcStubStorage) Insert(ctx context.Context, d repo.Derivative) (*repo.Derivative, error) {
	return &d, s.err
}

func (s *svcStubDerivRepo) UpsertDerivative(ctx context.Context, d *repo.Derivative) (*repo.Derivative, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.deriv != nil {
		return s.deriv, nil
	}
	return d, nil
}

type svcStubStorage struct {
	url *storage.PresignedURL
	err error

	// deletePrefixFunc, если задан, заменяет заглушку DeletePrefix — нужен
	// для проверки, что удаление реально дошло до вызова к хранилищу.
	deletePrefixFunc func(ctx context.Context, prefix string) error
}

func (s *svcStubStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return nil
}
func (s *svcStubStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (s *svcStubStorage) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*storage.PresignedURL, error) {
	return s.url, s.err
}
func (s *svcStubStorage) DeleteObject(ctx context.Context, key string) error { return nil }
func (s *svcStubStorage) DeletePrefix(ctx context.Context, prefix string) error {
	if s.deletePrefixFunc != nil {
		return s.deletePrefixFunc(ctx, prefix)
	}
	return nil
}
func (s *svcStubStorage) ForEachObject(ctx context.Context, prefix string, fn func(storage.ObjectInfo) error) error {
	return nil
}
func (s *svcStubStorage) Close() error { return nil }

// ---------- helpers ----------

func svcTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func ownerID() uuid.UUID {
	return uuid.MustParse("22222222-2222-2222-2222-222222222222")
}

func otherOwnerID() uuid.UUID {
	return uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
}

func mediaWithStatus(st repo.MediaStatus) *repo.Media {
	return &repo.Media{
		ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		OwnerID:    ownerID(),
		Status:     st,
		StorageKey: "22222222-2222-2222-2222-222222222222/11111111-1111-1111-1111-111111111111/original.png",
	}
}

func derivFor(variant string) *repo.Derivative {
	return &repo.Derivative{
		ID:         uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		MediaID:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Variant:    variant,
		StorageKey: "22222222-2222-2222-2222-222222222222/11111111-1111-1111-1111-111111111111/" + variant + ".mp4",
	}
}

func requireGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error, got: %v", err)
	assert.Equal(t, want, st.Code(), "error message: %s", st.Message())
}

// ---------- tests ----------

func TestGetDownloadURL_Original_Success(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusStored)}
	sr := &svcStubStorage{url: &storage.PresignedURL{URL: "http://minio/presign", ExpiresAt: time.Now().Add(15 * time.Minute)}}

	svc := NewService(mr, &svcStubDerivRepo{}, sr, 15*time.Minute, svcTestLogger())
	url, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)

	require.NoError(t, err)
	assert.Equal(t, "http://minio/presign", url.URL)
}

func TestGetDownloadURL_Original_FailedStatus(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusFailed)}
	svc := NewService(mr, &svcStubDerivRepo{}, &svcStubStorage{}, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestGetDownloadURL_Original_DeletingStatus(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusDeleting)}
	svc := NewService(mr, &svcStubDerivRepo{}, &svcStubStorage{}, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestGetDownloadURL_Derivative_Success(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusReady)}
	dr := &svcStubDerivRepo{deriv: derivFor("r_720")}
	sr := &svcStubStorage{url: &storage.PresignedURL{URL: "http://minio/r720", ExpiresAt: time.Now().Add(15 * time.Minute)}}

	svc := NewService(mr, dr, sr, 15*time.Minute, svcTestLogger())
	url, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantR720)

	require.NoError(t, err)
	assert.Equal(t, "http://minio/r720", url.URL)
}

func TestGetDownloadURL_Derivative_NotReady(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusProcessing)}
	svc := NewService(mr, &svcStubDerivRepo{}, &svcStubStorage{}, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantR720)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestGetDownloadURL_Derivative_NotFound(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusReady)}
	dr := &svcStubDerivRepo{err: repo.ErrNotFound}
	svc := NewService(mr, dr, &svcStubStorage{}, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantThumb)
	requireGRPCCode(t, err, codes.NotFound)
}

func TestGetDownloadURL_MediaNotFound(t *testing.T) {
	mr := &svcStubMediaRepo{err: repo.ErrNotFound}
	svc := NewService(mr, &svcStubDerivRepo{}, &svcStubStorage{}, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), uuid.New(), storage.VariantOriginal)
	requireGRPCCode(t, err, codes.NotFound)
}

func TestGetDownloadURL_WrongOwner_PermissionDenied(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusStored)}
	svc := NewService(mr, &svcStubDerivRepo{}, &svcStubStorage{}, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), otherOwnerID(), mr.media.ID, storage.VariantOriginal)
	require.Error(t, err, ErrAccessDenied)
}

func TestGetDownloadURL_StorageError(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusStored)}
	sr := &svcStubStorage{err: errors.New("minio down")}
	svc := NewService(mr, &svcStubDerivRepo{}, sr, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)
	requireGRPCCode(t, err, codes.Internal)
}

func TestGetDownloadURL_UnsupportedVariant(t *testing.T) {
	mr := &svcStubMediaRepo{media: mediaWithStatus(repo.MediaStatusReady)}
	svc := NewService(mr, &svcStubDerivRepo{}, &svcStubStorage{}, time.Minute, svcTestLogger())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantPreview)
	requireGRPCCode(t, err, codes.InvalidArgument)
}
