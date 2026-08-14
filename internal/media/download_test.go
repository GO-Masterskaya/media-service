package media

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestDownloadStream_Success_Original(t *testing.T) {
	data := []byte("hello world from media service")
	r := &mockRepo{
		media: &repo.Media{ID: "media-1", Status: "stored", StorageKey: "owner-1/media-1/original.txt"},
	}
	s := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}

	var received []byte
	err := NewService(r, s).DownloadStream(context.Background(), "media-1", "original", func(chunk []byte) error {
		received = append(received, chunk...)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_Success_Derivative(t *testing.T) {
	data := []byte("thumbnail bytes")
	r := &mockRepo{
		media:      &repo.Media{ID: "m1", Status: "ready"},
		derivative: &repo.Derivative{MediaID: "m1", Variant: "thumbnail", Status: "ready", StorageKey: "k/thumb.jpg"},
	}
	s := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}

	var received []byte
	err := NewService(r, s).DownloadStream(context.Background(), "m1", "thumbnail", func(c []byte) error {
		received = append(received, c...)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_NotFound_BeforeSend(t *testing.T) {
	r := &mockRepo{mediaErr: repo.ErrNotFound}
	s := &mockStorage{}

	called := false
	err := NewService(r, s).DownloadStream(context.Background(), "missing", "original", func([]byte) error {
		called = true
		return nil
	})

	require.ErrorIs(t, err, ErrNotFound)
	assert.False(t, called, "sender must not be called for missing media")
}

func TestDownloadStream_VariantNotFound(t *testing.T) {
	r := &mockRepo{
		media:    &repo.Media{ID: "m1", Status: "ready"},
		derivErr: repo.ErrNotFound,
	}
	err := NewService(r, &mockStorage{}).DownloadStream(context.Background(), "m1", "r_720", func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrNotFound)
}

func TestDownloadStream_ClientCancel_ClosesReader(t *testing.T) {
	sr := &slowReader{data: make([]byte, 1024*1024), delay: 5 * time.Millisecond}
	r := &mockRepo{
		media: &repo.Media{ID: "m1", Status: "stored", StorageKey: "key"},
	}
	s := &mockStorage{reader: sr}

	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	err := NewService(r, s).DownloadStream(ctx, "m1", "original", func([]byte) error {
		calls++
		if calls == 2 {
			cancel()
			return context.Canceled
		}
		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, sr.closed, "reader must be closed after cancellation")
}

type slowReader struct {
	data   []byte
	offset int
	delay  time.Duration
	closed bool
}

func (r *slowReader) Read(p []byte) (int, error) {
	time.Sleep(r.delay)
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
func (r *slowReader) Close() error {
	r.closed = true
	return nil
}