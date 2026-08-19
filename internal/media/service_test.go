package media

import (
	"context"
	"errors"
	"io"
	"log/slog"
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

// ---------- stubs ----------

type stubMediaRepo struct {
	media *repo.Media
	err   error
}

func (s *stubMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	return s.media, s.err
}

type stubDerivRepo struct {
	deriv *repo.Derivative
	err   error
}

func (s *stubDerivRepo) GetByMediaAndVariant(ctx context.Context, mediaID uuid.UUID, variant string) (*repo.Derivative, error) {
	return s.deriv, s.err
}

func (s *stubDerivRepo) Insert(ctx context.Context, d repo.Derivative) (*repo.Derivative, error) {
	return &d, s.err
}

type stubStorage struct {
	url *storage.PresignedURL
	err error
}

func (s *stubStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return nil
}
func (s *stubStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (s *stubStorage) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*storage.PresignedURL, error) {
	return s.url, s.err
}
func (s *stubStorage) DeleteObject(ctx context.Context, key string) error    { return nil }
func (s *stubStorage) DeletePrefix(ctx context.Context, prefix string) error { return nil }
func (s *stubStorage) Close() error                                          { return nil }

// ---------- helpers ----------

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
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusStored)}
	sr := &stubStorage{url: &storage.PresignedURL{URL: "http://minio/presign", ExpiresAt: time.Now().Add(15 * time.Minute)}}

	svc := NewService(mr, &stubDerivRepo{}, sr, 15*time.Minute, slog.Default())
	url, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)

	require.NoError(t, err)
	assert.Equal(t, "http://minio/presign", url.URL)
}

func TestGetDownloadURL_Original_FailedStatus(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusFailed)}
	svc := NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, slog.Default())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestGetDownloadURL_Original_DeletingStatus(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusDeleting)}
	svc := NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, slog.Default())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestGetDownloadURL_Derivative_Success(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusReady)}
	dr := &stubDerivRepo{deriv: derivFor("r_720")}
	sr := &stubStorage{url: &storage.PresignedURL{URL: "http://minio/r720", ExpiresAt: time.Now().Add(15 * time.Minute)}}

	svc := NewService(mr, dr, sr, 15*time.Minute, slog.Default())
	url, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantR720)

	require.NoError(t, err)
	assert.Equal(t, "http://minio/r720", url.URL)
}

func TestGetDownloadURL_Derivative_NotReady(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusProcessing)}
	svc := NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, slog.Default())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantR720)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestGetDownloadURL_Derivative_NotFound(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusReady)}
	dr := &stubDerivRepo{err: repo.ErrNotFound}
	svc := NewService(mr, dr, &stubStorage{}, time.Minute, slog.Default())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantThumb)
	requireGRPCCode(t, err, codes.NotFound)
}

func TestGetDownloadURL_MediaNotFound(t *testing.T) {
	mr := &stubMediaRepo{err: repo.ErrNotFound}
	svc := NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, slog.Default())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), uuid.New(), storage.VariantOriginal)
	requireGRPCCode(t, err, codes.NotFound)
}

func TestGetDownloadURL_WrongOwner_PermissionDenied(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusReady)}
	svc := NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, slog.Default())

	_, err := svc.GetDownloadURL(context.Background(), otherOwnerID(), mr.media.ID, storage.VariantOriginal)
	requireGRPCCode(t, err, codes.PermissionDenied)
}

func TestGetDownloadURL_StorageError(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusStored)}
	sr := &stubStorage{err: errors.New("minio down")}
	svc := NewService(mr, &stubDerivRepo{}, sr, time.Minute, slog.Default())

	_, err := svc.GetDownloadURL(context.Background(), ownerID(), mr.media.ID, storage.VariantOriginal)
	requireGRPCCode(t, err, codes.Internal)
}
