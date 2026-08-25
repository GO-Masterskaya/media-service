package media

import (
	"bytes"
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

func newTestService(mediaRepo repo.MediaRepo, derivRepo repo.DerivativeRepo, st storage.Interface) *Service {
	discardLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	return NewService(mediaRepo, derivRepo, st, 15*time.Minute, discardLog)
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

func (m *mockDerivRepo) UpsertDerivative(ctx context.Context, d *repo.Derivative) (*repo.Derivative, error) {
	if m.derivErr != nil {
		return nil, m.derivErr
	}
	if m.deriv != nil {
		return m.deriv, nil
	}
	return d, nil
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

func TestDownloadStream_Success_Original_Strict(t *testing.T) {
	data := []byte("hello world from media service")
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	callerID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: callerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}

	var received []byte
	err := newTestService(mediaRepo, &mockDerivRepo{}, storageMock).
		DownloadStream(context.Background(), callerID, id, "original", func(chunk []byte) error {
			received = append(received, chunk...)
			return nil
		})

	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_Success_Original_NonStrict(t *testing.T) {
	data := []byte("non-strict download")
	id := uuid.MustParse("11111112-1111-1111-1111-111111111112")
	ownerID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}

	var received []byte
	err := newTestService(mediaRepo, &mockDerivRepo{}, storageMock).
		DownloadStream(context.Background(), uuid.Nil, id, "original", func(chunk []byte) error {
			received = append(received, chunk...)
			return nil
		})

	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_Success_Derivative(t *testing.T) {
	data := []byte("thumbnail bytes")
	id := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	callerID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: callerID, Status: repo.MediaStatusReady, StorageKey: "orig-key"},
	}
	derivRepo := &mockDerivRepo{
		deriv: &repo.Derivative{MediaID: id, Variant: "thumb", StorageKey: "thumb-key"},
	}
	storageMock := &mockStorage{reader: io.NopCloser(bytes.NewReader(data))}

	var received []byte
	err := newTestService(mediaRepo, derivRepo, storageMock).
		DownloadStream(context.Background(), callerID, id, "thumb", func(c []byte) error {
			received = append(received, c...)
			return nil
		})

	require.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestDownloadStream_NilUUID(t *testing.T) {
	callerID := uuid.New()
	err := newTestService(&mockMediaRepo{}, &mockDerivRepo{}, &mockStorage{}).
		DownloadStream(context.Background(), callerID, uuid.Nil, "original", func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestDownloadStream_NotFound_BeforeSend(t *testing.T) {
	id := uuid.New()
	callerID := uuid.New()
	mediaRepo := &mockMediaRepo{mediaErr: repo.ErrNotFound}
	storageMock := &mockStorage{}

	called := false
	err := newTestService(mediaRepo, &mockDerivRepo{}, storageMock).
		DownloadStream(context.Background(), callerID, id, "original", func([]byte) error {
			called = true
			return nil
		})

	require.ErrorIs(t, err, ErrNotFound)
	assert.False(t, called, "sender must not be called for missing media")
}

func TestDownloadStream_AccessDenied_Strict(t *testing.T) {
	id := uuid.New()
	callerID := uuid.New()
	ownerID := uuid.New()

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}

	err := newTestService(mediaRepo, &mockDerivRepo{}, &mockStorage{}).
		DownloadStream(context.Background(), callerID, id, "original", func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrAccessDenied)
}

func TestDownloadStream_AccessDenied_NonStrict_WithCallerID(t *testing.T) {
	id := uuid.New()
	callerID := uuid.New()
	ownerID := uuid.New()

	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: ownerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}

	err := newTestService(mediaRepo, &mockDerivRepo{}, &mockStorage{}).
		DownloadStream(context.Background(), callerID, id, "original", func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrAccessDenied)
}

func TestDownloadStream_VariantNotFound(t *testing.T) {
	id := uuid.New()
	callerID := uuid.New()
	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: callerID, Status: repo.MediaStatusReady, StorageKey: "key"},
	}
	derivRepo := &mockDerivRepo{derivErr: repo.ErrNotFound}

	err := newTestService(mediaRepo, derivRepo, &mockStorage{}).
		DownloadStream(context.Background(), callerID, id, "r_720", func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrNotFound)
}

func TestDownloadStream_ClientCancel_ClosesReader(t *testing.T) {
	sr := &slowReader{data: make([]byte, 1024*1024), delay: 5 * time.Millisecond}
	id := uuid.New()
	callerID := uuid.New()
	mediaRepo := &mockMediaRepo{
		media: &repo.Media{ID: id, OwnerID: callerID, Status: repo.MediaStatusStored, StorageKey: "key"},
	}
	storageMock := &mockStorage{reader: sr}

	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	err := newTestService(mediaRepo, &mockDerivRepo{}, storageMock).
		DownloadStream(ctx, callerID, id, "original", func([]byte) error {
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
