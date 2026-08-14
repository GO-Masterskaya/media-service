package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	"mediaservice/proto/media/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockRepo — заглушка repo.MediaRepository.
type mockRepo struct {
	media      *repo.Media
	derivative *repo.Derivative
	mediaErr   error
	derivErr   error
}

func (m *mockRepo) GetMedia(_ context.Context, _ string) (*repo.Media, error) {
	return m.media, m.mediaErr
}
func (m *mockRepo) GetDerivative(_ context.Context, _, _ string) (*repo.Derivative, error) {
	return m.derivative, m.derivErr
}

// mockStorage — заглушка storage.Interface.
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

// mockStream — заглушка v1.MediaService_DownloadStreamServer.
type mockStream struct {
	ctx  context.Context
	send func(*v1.DownloadChunk) error
}

func (m *mockStream) Context() context.Context       { return m.ctx }
func (m *mockStream) Send(c *v1.DownloadChunk) error { return m.send(c) }

func newTestServer(repo repo.MediaRepository, st storage.Interface) *MediaServer {
	return NewMediaServer(media.NewService(repo, st))
}

func TestMapDownloadError(t *testing.T) {
	tests := []struct {
		name     string
		in       error
		wantCode codes.Code
	}{
		{name: "not found", in: media.ErrNotFound, wantCode: codes.NotFound},
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

func TestDownloadStream_Handler_Success(t *testing.T) {
	data := []byte("handler test data")
	repoMock := &mockRepo{
		media: &repo.Media{ID: "m1", Status: "stored", StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}
	srv := newTestServer(repoMock, storageMock)

	var received []byte
	stream := &mockStream{
		ctx: context.Background(),
		send: func(c *v1.DownloadChunk) error {
			received = append(received, c.Data...)
			return nil
		},
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: "m1", Variant: "original"}, stream)
	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_Handler_ContextCanceled_BeforeStart(t *testing.T) {
	repoMock := &mockRepo{media: &repo.Media{ID: "m1", Status: "stored", StorageKey: "key"}}
	srv := newTestServer(repoMock, &mockStorage{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := &mockStream{ctx: ctx, send: func(*v1.DownloadChunk) error { return nil }}
	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: "m1", Variant: "original"}, stream)

	require.Error(t, err)
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestDownloadStream_Handler_ContextCanceled_DuringSend(t *testing.T) {
	data := []byte("some data")
	repoMock := &mockRepo{
		media: &repo.Media{ID: "m1", Status: "stored", StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}
	srv := newTestServer(repoMock, storageMock)

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	stream := &mockStream{
		ctx: ctx,
		send: func(*v1.DownloadChunk) error {
			calls++
			if calls == 1 {
				cancel()
				return context.Canceled
			}
			return nil
		},
	}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: "m1", Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestDownloadStream_Handler_NotFound(t *testing.T) {
	repoMock := &mockRepo{mediaErr: repo.ErrNotFound}
	srv := newTestServer(repoMock, &mockStorage{})
	stream := &mockStream{ctx: context.Background(), send: func(*v1.DownloadChunk) error { return nil }}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: "missing", Variant: "original"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestDownloadStream_Handler_InvalidArgument(t *testing.T) {
	repoMock := &mockRepo{media: &repo.Media{ID: "m1", Status: "stored", StorageKey: "key"}}
	srv := newTestServer(repoMock, &mockStorage{})
	stream := &mockStream{ctx: context.Background(), send: func(*v1.DownloadChunk) error { return nil }}

	err := srv.DownloadStream(&v1.DownloadStreamRequest{MediaId: "m1", Variant: "unknown_variant"}, stream)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}