package api

import (
	"context"
	"io"
	"log/slog"
	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	mediav1 "mediaservice/proto/media/v1"
)

// ---------- stubs (копия из service_test.go) ----------

type stubMediaRepo struct {
	media    *repo.Media
	err      error
	listPage *repo.MediaPage
}

func (s *stubMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	return s.media, s.err
}

func (r *stubMediaRepo) ListByOwner(ctx context.Context, ownerID uuid.UUID, pageSize int, cursor *repo.MediaCursor) (*repo.MediaPage, error) {
	return r.listPage, r.err
}

func (s *stubMediaRepo) GetByOwnerIdempotency(ctx context.Context, ownerID uuid.UUID, idempotencyKey string) (*repo.Media, error) {
	return s.media, s.err
}

func (s *stubMediaRepo) InsertWithJobs(ctx context.Context, m repo.Media, jobTypes []string) (*repo.Media, error) {
	return &m, s.err
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

func (s *stubDerivRepo) ListByMediaIDs(ctx context.Context, mediaIDs []uuid.UUID) (map[uuid.UUID][]*repo.Derivative, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.deriv == nil {
		return map[uuid.UUID][]*repo.Derivative{}, nil
	}
	return map[uuid.UUID][]*repo.Derivative{s.deriv.MediaID: {s.deriv}}, nil
}

func (s *stubDerivRepo) GetByMediaAndVariant(ctx context.Context, mediaID uuid.UUID, variant string) (*repo.Derivative, error) {
	return s.deriv, s.err
}

func (s *stubDerivRepo) Insert(ctx context.Context, d repo.Derivative) (*repo.Derivative, error) {
	return &d, s.err
}

func (s *stubDerivRepo) UpsertDerivative(ctx context.Context, d *repo.Derivative) (*repo.Derivative, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.deriv != nil {
		return s.deriv, nil
	}
	return d, nil
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

// ---------- tests

func TestGetMedia_ReturnsMetadataStatusAndDerivatives(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	created := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	mr := &stubMediaRepo{media: &repo.Media{
		ID: id, OwnerID: ownerID(), Kind: repo.MediaKindVideo, Mime: "video/mp4",
		SizeBytes: 42, Status: repo.MediaStatusReady, Metadata: []byte(`{"duration":12.5}`), CreatedAt: created,
	}}
	dr := &stubDerivRepo{deriv: &repo.Derivative{MediaID: id, Variant: "r_720", Mime: "video/mp4", SizeBytes: 21}}
	server := NewMediaServer(media.NewService(mr, dr, &stubStorage{}, time.Minute, testLogger()), false)

	got, err := server.GetMedia(context.Background(), &mediav1.GetMediaRequest{MediaId: id.String()})
	require.NoError(t, err)
	assert.Equal(t, id.String(), got.Id)
	assert.Equal(t, mediav1.MediaStatus_READY, got.Status)
	assert.Equal(t, float64(12.5), got.Metadata.Fields["duration"].GetNumberValue())
	require.Len(t, got.Derivatives, 1)
	assert.Equal(t, "r_720", got.Derivatives[0].Variant)
}

func TestGetMedia_NotFound(t *testing.T) {
	server := NewMediaServer(media.NewService(&stubMediaRepo{err: repo.ErrNotFound}, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger()), false)
	_, err := server.GetMedia(context.Background(), &mediav1.GetMediaRequest{MediaId: "11111111-1111-1111-1111-111111111111"})
	requireGRPCCode(t, err, codes.NotFound)
}

func TestListMediaByOwner_UsesPageTokenAndKeepsOwnerInToken(t *testing.T) {
	first := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	second := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	created1 := time.Date(2026, 9, 4, 12, 0, 2, 0, time.UTC)
	created2 := time.Date(2026, 9, 4, 12, 0, 1, 0, time.UTC)
	owner := ownerID()
	mr := &stubMediaRepo{listPage: &repo.MediaPage{HasMore: true, Items: []*repo.Media{
		{ID: first, OwnerID: owner, Kind: repo.MediaKindImage, Mime: "image/png", Status: repo.MediaStatusStored, CreatedAt: created1},
	}}}
	server := NewMediaServer(media.NewService(mr, &stubDerivRepo{}, &stubStorage{}, time.Minute, testLogger()), false)

	resp, err := server.ListMediaByOwner(context.Background(), &mediav1.ListMediaByOwnerRequest{OwnerId: owner.String(), PageSize: 1})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	require.NotEmpty(t, resp.NextPageToken)

	mr.listPage = &repo.MediaPage{Items: []*repo.Media{{ID: second, OwnerID: owner, Kind: repo.MediaKindImage, Mime: "image/png", Status: repo.MediaStatusStored, CreatedAt: created2}}}
	resp2, err := server.ListMediaByOwner(context.Background(), &mediav1.ListMediaByOwnerRequest{OwnerId: owner.String(), PageSize: 1, PageToken: resp.NextPageToken})
	require.NoError(t, err)
	require.Len(t, resp2.Items, 1)
	assert.Equal(t, second.String(), resp2.Items[0].Id)

	otherOwner := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	_, err = server.ListMediaByOwner(context.Background(), &mediav1.ListMediaByOwnerRequest{OwnerId: otherOwner.String(), PageSize: 1, PageToken: resp.NextPageToken})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

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
