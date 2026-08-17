package processing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mediaservice/internal/config"
	"mediaservice/internal/storage"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

type memoryStorage struct {
	object map[string][]byte
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{
		object: make(map[string][]byte),
	}
}

func (m *memoryStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	res, err := io.ReadAll(reader)
	if err != nil {
		return errors.New("error read file")
	}

	m.object[key] = res
	return nil
}

func (m *memoryStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.object[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memoryStorage) DeleteObject(ctx context.Context, key string) error {
	delete(m.object, key)
	return nil
}

func (m *memoryStorage) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*storage.PresignedURL, error) {
	return nil, nil
}

func (m *memoryStorage) DeletePrefix(ctx context.Context, prefix string) error {
	return nil
}

func (m *memoryStorage) Close() error {
	return nil
}

type mockRepo struct {
	upsertFunc func(ctx context.Context, record *DerivativeRecord) (*DerivativeRecord, error)
}

func (m *mockRepo) UpsertDerivative(ctx context.Context, record *DerivativeRecord) (*DerivativeRecord, error) {
	if m.upsertFunc != nil {
		res, err := m.upsertFunc(ctx, record)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return record, nil
}

func TestThumbnailHandler_RollbackOnDBError(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/file"

	fileData, err := os.ReadFile(filepath.Join("testdata", "image.png"))
	if err != nil {
		t.Fatalf("failed to read test fixture: %v", err)
	}

	st.object[sourceKey] = fileData

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindImage,
		SourceKey: sourceKey,
	}

	m := &mockRepo{
		upsertFunc: func(ctx context.Context, record *DerivativeRecord) (*DerivativeRecord, error) {
			return nil, errors.New("db connection failed")
		},
	}

	ctx := context.Background()
	h := NewThumbnailHandler(st, m, &config.Config{}, nil)
	_, err = h.ProcessThumbnail(ctx, media)
	if err == nil {
		t.Fatal("expected error due to DB failure, got nil")
	}

	thumbKey, _ := storage.BuildKey(ownerID, mediaID, storage.VariantThumb, "image/jpeg", "")
	if _, exists := st.object[thumbKey]; exists {
		t.Fatalf("orphan file %s was not deleted from storage after DB error", thumbKey)
	}
}

func TestThumbnailHandler_Success(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/file.png"

	fileData, err := os.ReadFile(filepath.Join("testdata", "image.png"))
	if err != nil {
		t.Fatalf("failed to read test fixture: %v", err)
	}
	st.object[sourceKey] = fileData

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindImage,
		SourceKey: sourceKey,
	}

	m := &mockRepo{}

	ctx := context.Background()
	h := NewThumbnailHandler(st, m, &config.Config{}, nil)

	record, err := h.ProcessThumbnail(ctx, media)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if record == nil {
		t.Fatal("expected non-nil derivative record")
	}

	thumbKey, _ := storage.BuildKey(ownerID, mediaID, storage.VariantThumb, "image/jpeg", "")
	if record.StorageKey != thumbKey {
		t.Errorf("expected storage key %s, got %s", thumbKey, record.StorageKey)
	}

	if record.MediaID != mediaID {
		t.Errorf("expected media ID %s, got %s", mediaID, record.MediaID)
	}

	if record.SizeBytes <= 0 {
		t.Errorf("expected SizeBytes > 0, got %d", record.SizeBytes)
	}

	if record.MIME != "image/jpeg" {
		t.Errorf("expected MIME image/jpeg, got %s", record.MIME)
	}

	if _, exists := st.object[thumbKey]; !exists {
		t.Fatalf("expected file %s to exist in storage, but it was not found", thumbKey)
	}
}

func TestThumbnailHandler_Video(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/video.mp4"

	fileData, err := os.ReadFile(filepath.Join("testdata", "video.mp4"))
	if err != nil {
		t.Fatalf("failed to read video fixture: %v", err)
	}
	st.object[sourceKey] = fileData

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindVideo,
		SourceKey: sourceKey,
	}

	m := &mockRepo{}
	cfg := &config.Config{
		ThumbSecond: 0,
	}

	ctx := context.Background()
	h := NewThumbnailHandler(st, m, cfg, nil)

	record, err := h.ProcessThumbnail(ctx, media)
	if err != nil {
		t.Fatalf("expected no error for video thumbnail, got: %v", err)
	}

	if record == nil {
		t.Fatal("expected non-nil derivative record")
	}

	thumbKey, _ := storage.BuildKey(ownerID, mediaID, storage.VariantThumb, "image/jpeg", "")
	if record.StorageKey != thumbKey {
		t.Errorf("expected storage key %s, got %s", thumbKey, record.StorageKey)
	}

	if record.MIME != "image/jpeg" {
		t.Errorf("expected MIME image/jpeg, got %s", record.MIME)
	}

	if _, exists := st.object[thumbKey]; !exists {
		t.Fatalf("expected file %s to exist in storage, but it was not found", thumbKey)
	}
}

func TestThumbnailHandler_Audio(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/audio.mp3"

	fileData, err := os.ReadFile(filepath.Join("testdata", "audio.mp3"))
	if err != nil {
		t.Fatalf("failed to read audio fixture: %v", err)
	}
	st.object[sourceKey] = fileData

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindAudio,
		SourceKey: sourceKey,
	}

	m := &mockRepo{}
	ctx := context.Background()
	h := NewThumbnailHandler(st, m, &config.Config{}, nil)

	record, err := h.ProcessThumbnail(ctx, media)
	if err != nil {
		t.Fatalf("expected no error for audio waveform, got: %v", err)
	}

	if record == nil {
		t.Fatal("expected non-nil derivative record")
	}

	if record.SizeBytes <= 0 {
		t.Errorf("expected positive SizeBytes, got %d", record.SizeBytes)
	}

	if _, exists := st.object[record.StorageKey]; !exists {
		t.Fatalf("expected file %s to exist in storage, but it was not found", record.StorageKey)
	}
}

func TestThumbnailHandler_CorruptSource(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/corrupt.png"

	st.object[sourceKey] = []byte("this is corrupt data and not a valid image")

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindImage,
		SourceKey: sourceKey,
	}

	m := &mockRepo{}
	ctx := context.Background()
	h := NewThumbnailHandler(st, m, &config.Config{}, nil)

	_, err := h.ProcessThumbnail(ctx, media)
	if err == nil {
		t.Fatal("expected managed error for corrupt source, got nil")
	}
}

func TestThumbnailHandler_Idempotency(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/image.png"

	fileData, err := os.ReadFile(filepath.Join("testdata", "image.png"))
	if err != nil {
		t.Fatalf("failed to read image fixture: %v", err)
	}
	st.object[sourceKey] = fileData

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindImage,
		SourceKey: sourceKey,
	}

	m := &mockRepo{}
	ctx := context.Background()
	h := NewThumbnailHandler(st, m, &config.Config{}, nil)

	firstRecord, err := h.ProcessThumbnail(ctx, media)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	secondRecord, err := h.ProcessThumbnail(ctx, media)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	if firstRecord.StorageKey != secondRecord.StorageKey {
		t.Errorf("expected same storage key %s, got %s", firstRecord.StorageKey, secondRecord.StorageKey)
	}
}
