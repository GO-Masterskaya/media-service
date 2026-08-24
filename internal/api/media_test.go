package api

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	mediav1 "mediaservice/proto/media/v1"
)

// ---------- stubs (копия из service_test.go) ----------

type stubMediaRepo struct {
	media *repo.Media
	err   error
}

func (s *stubMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	return s.media, s.err
}

func (s *stubMediaRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	return nil, nil
}
func (s *stubMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error {
	return nil
}
func (s *stubMediaRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	return nil, nil
}

func (s *stubMediaRepo) UpdateOwner(ctx context.Context, mediaID uuid.UUID, ownerID uuid.UUID) error {
	return nil
}

func (s *stubMediaRepo) CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error {
	return nil
}
func (s *stubMediaRepo) DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (usagesRemaining int, err error) {
	return 0, nil
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
func (s *stubStorage) ForEachObject(ctx context.Context, prefix string, fn func(storage.ObjectInfo) error) error {
	return nil
}
func (s *stubStorage) Close() error { return nil }

// ---------- helpers ----------

func ownerID() uuid.UUID {
	return uuid.MustParse("22222222-2222-2222-2222-222222222222")
}

func mediaWithStatus(st repo.MediaStatus) *repo.Media {
	return &repo.Media{
		ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		OwnerID:    ownerID(),
		Status:     st,
		StorageKey: "22222222-2222-2222-2222-222222222222/11111111-1111-1111-1111-111111111111/original.png",
	}
}

func ctxWithOwner(owner string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-owner-id", owner))
}

func requireGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error, got: %v", err)
	assert.Equal(t, want, st.Code(), "error message: %s", st.Message())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// ---------- tests ----------

func TestGetDownloadURL_Success(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusStored)}
	sr := &stubStorage{url: &storage.PresignedURL{URL: "http://minio/presign", ExpiresAt: time.Now().Add(15 * time.Minute)}}

	svc := media.NewService(mr, &stubDerivRepo{}, sr, 15*time.Minute, testLogger())
	server := NewMediaServer(svc, false)

	resp, err := server.GetDownloadURL(ctxWithOwner(ownerID().String()), &mediav1.GetDownloadURLRequest{
		MediaId: "11111111-1111-1111-1111-111111111111",
		Variant: "original",
	})

	require.NoError(t, err)
	assert.Equal(t, "http://minio/presign", resp.Url)
	assert.NotNil(t, resp.ExpiresAt)
}

func TestGetDownloadURL_MissingMediaID(t *testing.T) {
	svc := media.NewService(&stubMediaRepo{}, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger())
	server := NewMediaServer(svc, false)

	_, err := server.GetDownloadURL(ctxWithOwner(ownerID().String()), &mediav1.GetDownloadURLRequest{
		Variant: "original",
	})

	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestGetDownloadURL_InvalidMediaID(t *testing.T) {
	svc := media.NewService(&stubMediaRepo{}, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger())
	server := NewMediaServer(svc, false)

	_, err := server.GetDownloadURL(ctxWithOwner(ownerID().String()), &mediav1.GetDownloadURLRequest{
		MediaId: "not-a-uuid",
		Variant: "original",
	})

	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestGetDownloadURL_InvalidVariant(t *testing.T) {
	svc := media.NewService(&stubMediaRepo{}, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger())
	server := NewMediaServer(svc, false)

	_, err := server.GetDownloadURL(ctxWithOwner(ownerID().String()), &mediav1.GetDownloadURLRequest{
		MediaId: "11111111-1111-1111-1111-111111111111",
		Variant: "invalid",
	})

	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestGetDownloadURL_MissingOwnerID(t *testing.T) {
	svc := media.NewService(&stubMediaRepo{}, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger())
	server := NewMediaServer(svc, true) // <-- strict=true

	// Нет metadata вообще
	_, err := server.GetDownloadURL(context.Background(), &mediav1.GetDownloadURLRequest{
		MediaId: "11111111-1111-1111-1111-111111111111",
		Variant: "original",
	})

	requireGRPCCode(t, err, codes.Unauthenticated)
}

func TestGetDownloadURL_WrongOwner(t *testing.T) {
	mr := &stubMediaRepo{media: mediaWithStatus(repo.MediaStatusStored)}
	svc := media.NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger())
	server := NewMediaServer(svc, false)

	otherOwner := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	_, err := server.GetDownloadURL(ctxWithOwner(otherOwner), &mediav1.GetDownloadURLRequest{
		MediaId: "11111111-1111-1111-1111-111111111111",
		Variant: "original",
	})

	requireGRPCCode(t, err, codes.PermissionDenied)
}

func TestGetDownloadURL_ServiceError(t *testing.T) {
	mr := &stubMediaRepo{err: repo.ErrNotFound}
	svc := media.NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger())
	server := NewMediaServer(svc, false)

	_, err := server.GetDownloadURL(ctxWithOwner(ownerID().String()), &mediav1.GetDownloadURLRequest{
		MediaId: "11111111-1111-1111-1111-111111111111",
		Variant: "original",
	})

	requireGRPCCode(t, err, codes.NotFound)
}
