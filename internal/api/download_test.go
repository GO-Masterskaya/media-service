package api

import (
	"bytes"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	v1 "mediaservice/proto/media/v1"
)

func newTestServerWithStrict(mediaRepo repo.MediaRepo, derivRepo repo.DerivativeRepo, st storage.Interface, strict bool) *MediaServer {
	discardLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	svc := media.NewService(mediaRepo, derivRepo, st, 15*time.Minute, discardLog)
	return NewMediaServer(svc, strict)
}

func newTestServer(mediaRepo repo.MediaRepo, derivRepo repo.DerivativeRepo, st storage.Interface) *MediaServer {
	return newTestServerWithStrict(mediaRepo, derivRepo, st, false)
}

func incomingCtxWithOwnerID(ctx context.Context, ownerID string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.Pairs("x-owner-id", ownerID))
}

type mockMediaRepo struct {
	media    *repo.Media
	mediaErr error
}

func (m *mockMediaRepo) GetByID(_ context.Context, _ uuid.UUID) (*repo.Media, error) {
	return m.media, m.mediaErr
}

func (m *mockMediaRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	return nil, nil
}

func (s *mockMediaRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	return nil, nil
}

func (s *mockMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (s *mockMediaRepo) CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error {
	return nil
}

func (s *mockMediaRepo) DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (usagesRemaining int, err error) {
	return 0, nil
}

type mockDerivRepo struct {
	deriv    *repo.Derivative
	derivErr error
}

func (m *mockDerivRepo) GetByMediaAndVariant(_ context.Context, _ uuid.UUID, _ string) (*repo.Derivative, error) {
	return m.deriv, m.derivErr
}

func (m *mockDerivRepo) Insert(_ context.Context, d repo.Derivative) (*repo.Derivative, error) {
	return &d, nil
}

type mockStorage struct {
	reader io.ReadCloser
	err    error
}

func (m *mockStorage) PutObject(_ context.Context, _ string, _ io.Reader, _ int64, _ string) error {
	return nil
}
func (m *mockStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return m.reader, m.err
}
func (m *mockStorage) PresignGetObject(_ context.Context, _ string, _ time.Duration) (*storage.PresignedURL, error) {
	return nil, nil
}
func (m *mockStorage) DeleteObject(_ context.Context, _ string) error { return nil }
func (m *mockStorage) DeletePrefix(_ context.Context, _ string) error { return nil }
func (m *mockStorage) Close() error                                   { return nil }
func (s *mockStorage) ForEachObject(ctx context.Context, prefix string, fn func(storage.ObjectInfo) error) error {
	return nil
}

type mockStream struct {
	ctx  context.Context
	send func(*v1.DownloadChunk) error
}

func (m *mockStream) Context() context.Context       { return m.ctx }
func (m *mockStream) Send(c *v1.DownloadChunk) error { return m.send(c) }
func (m *mockStream) SendMsg(_ any) error            { return nil }
func (m *mockStream) RecvMsg(_ any) error            { return nil }
func (m *mockStream) SetHeader(_ metadata.MD) error  { return nil }
func (m *mockStream) SendHeader(_ metadata.MD) error { return nil }
func (m *mockStream) SetTrailer(_ metadata.MD)       {}

func TestMapDownloadError(t *testing.T) {
	tests := []struct {
		name     string
		in       error
		wantCode codes.Code
	}{
		{name: "not found", in: media.ErrNotFound, wantCode: codes.NotFound},
		{name: "access denied", in: media.ErrAccessDenied, wantCode: codes.PermissionDenied},
		{name: "invalid argument", in: media.ErrInvalidArgument, wantCode: codes.InvalidArgument},
		{name: "failed precondition", in: media.ErrFailedPrecondition, wantCode: codes.FailedPrecondition},
		{name: "deadline exceeded", in: context.DeadlineExceeded, wantCode: codes.DeadlineExceeded},
		{name: "internal", in: errors.New("boom"), wantCode: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapDownloadError(tt.in)
			assert.Equal(t, tt.wantCode, status.Code(err))
		})
	}
}

func TestDownloadStream_Handler_Success_Strict(t *testing.T) {
	data := []byte("handler test data")
	id := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	ownerID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}
	srv := newTestServerWithStrict(mediaRepo, &mockDerivRepo{}, storageMock, true)

	var received []byte
	stream := &mockStream{
		ctx: incomingCtxWithOwnerID(context.Background(), ownerID.String()),
		send: func(c *v1.DownloadChunk) error {
			received = append(received, c.Data...)
			return nil
		},
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)
	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_Handler_Success_NonStrict_NoMetadata(t *testing.T) {
	data := []byte("open link download")
	id := uuid.MustParse("33333334-3333-3333-3333-333333333334")
	ownerID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}
	srv := newTestServerWithStrict(mediaRepo, &mockDerivRepo{}, storageMock, false)

	var received []byte
	stream := &mockStream{
		ctx: context.Background(),
		send: func(c *v1.DownloadChunk) error {
			received = append(received, c.Data...)
			return nil
		},
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)
	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_Handler_InvalidUUID(t *testing.T) {
	srv := newTestServer(&mockMediaRepo{}, &mockDerivRepo{}, &mockStorage{})
	stream := &mockStream{ctx: context.Background(), send: func(*v1.DownloadChunk) error { return nil }}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: "not-a-uuid", Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDownloadStream_Handler_ContextCanceled_BeforeStart(t *testing.T) {
	id := uuid.New()
	ownerID := uuid.New()
	mediaRepo := &mockMediaRepo{media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"}}
	srv := newTestServer(mediaRepo, &mockDerivRepo{}, &mockStorage{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := &mockStream{ctx: ctx, send: func(*v1.DownloadChunk) error { return nil }}
	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)

	require.Error(t, err)
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestDownloadStream_Handler_ContextCanceled_DuringSend(t *testing.T) {
	data := []byte("some data")
	id := uuid.New()
	ownerID := uuid.New()
	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}
	srv := newTestServer(mediaRepo, &mockDerivRepo{}, storageMock)

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	stream := &mockStream{
		ctx: incomingCtxWithOwnerID(ctx, ownerID.String()),
		send: func(*v1.DownloadChunk) error {
			calls++
			if calls == 1 {
				cancel()
				return context.Canceled
			}
			return nil
		},
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestDownloadStream_Handler_NotFound(t *testing.T) {
	id := uuid.New()
	ownerID := uuid.New()
	mediaRepo := &mockMediaRepo{mediaErr: repo.ErrNotFound}
	srv := newTestServer(mediaRepo, &mockDerivRepo{}, &mockStorage{})
	stream := &mockStream{
		ctx:  incomingCtxWithOwnerID(context.Background(), ownerID.String()),
		send: func(*v1.DownloadChunk) error { return nil },
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestDownloadStream_Handler_Unauthenticated_StrictMissingMetadata(t *testing.T) {
	id := uuid.New()
	ownerID := uuid.New()
	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	srv := newTestServerWithStrict(mediaRepo, &mockDerivRepo{}, &mockStorage{}, true)
	stream := &mockStream{
		ctx:  context.Background(),
		send: func(*v1.DownloadChunk) error { return nil },
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestDownloadStream_Handler_PermissionDenied_Strict(t *testing.T) {
	id := uuid.New()
	ownerID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	callerID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	srv := newTestServerWithStrict(mediaRepo, &mockDerivRepo{}, &mockStorage{}, true)
	stream := &mockStream{
		ctx:  incomingCtxWithOwnerID(context.Background(), callerID.String()),
		send: func(*v1.DownloadChunk) error { return nil },
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestDownloadStream_Handler_PermissionDenied_NonStrict_WrongOwner(t *testing.T) {
	id := uuid.New()
	ownerID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	callerID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	srv := newTestServerWithStrict(mediaRepo, &mockDerivRepo{}, &mockStorage{}, false)
	stream := &mockStream{
		ctx:  incomingCtxWithOwnerID(context.Background(), callerID.String()),
		send: func(*v1.DownloadChunk) error { return nil },
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestDownloadStream_Handler_InvalidVariant(t *testing.T) {
	id := uuid.New()
	ownerID := uuid.New()
	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	srv := newTestServer(mediaRepo, &mockDerivRepo{}, &mockStorage{})
	stream := &mockStream{
		ctx:  incomingCtxWithOwnerID(context.Background(), ownerID.String()),
		send: func(*v1.DownloadChunk) error { return nil },
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: id.String(), Variant: "unknown_variant"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
