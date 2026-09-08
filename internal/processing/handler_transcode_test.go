package processing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"mediaservice/internal/config"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

func TestTranscodeHandler_ValidationError(t *testing.T) {
	st := newMemoryStorage()
	m := &mockRepo{}
	h := NewTranscodeHandler(st, m, &config.Config{}, nil)

	_, err := h.ProcessTranscode(context.Background(), MediaRecord{})
	if err == nil {
		t.Fatal("expected error for invalid media_id or owner_id, got nil")
	}
}

func TestTranscodeHandler_Video(t *testing.T) {
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
	ctx := context.Background()
	h := NewTranscodeHandler(st, m, &config.Config{}, nil)

	record, err := h.ProcessTranscode(ctx, media)
	if err != nil {
		t.Fatalf("expected no error for video transcode, got: %v", err)
	}

	if record == nil {
		t.Fatal("expected non-nil derivative record")
	}

	expectedKey, _ := storage.BuildKey(ownerID, mediaID, storage.VariantR720, "video/mp4", "")
	if record.StorageKey != expectedKey {
		t.Errorf("expected storage key %s, got %s", expectedKey, record.StorageKey)
	}

	if record.Variant != storage.VariantR720 {
		t.Errorf("expected variant %s, got %s", storage.VariantR720, record.Variant)
	}

	if record.MIME != "video/mp4" {
		t.Errorf("expected MIME video/mp4, got %s", record.MIME)
	}

	if _, exists := st.object[expectedKey]; !exists {
		t.Fatalf("expected file %s to exist in storage, but it was not found", expectedKey)
	}
}

func TestTranscodeHandler_Audio(t *testing.T) {
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
	h := NewTranscodeHandler(st, m, &config.Config{}, nil)

	record, err := h.ProcessTranscode(ctx, media)
	if err != nil {
		t.Fatalf("expected no error for audio transcode, got: %v", err)
	}

	if record == nil {
		t.Fatal("expected non-nil derivative record")
	}

	expectedKey, _ := storage.BuildKey(ownerID, mediaID, storage.VariantPreview, "audio/mp4", "")
	if record.StorageKey != expectedKey {
		t.Errorf("expected storage key %s, got %s", expectedKey, record.StorageKey)
	}

	if _, exists := st.object[expectedKey]; !exists {
		t.Fatalf("expected file %s to exist in storage, but it was not found", expectedKey)
	}
}

func TestTranscodeHandler_Image(t *testing.T) {
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
	h := NewTranscodeHandler(st, m, &config.Config{}, nil)

	record, err := h.ProcessTranscode(ctx, media)
	if err != nil {
		t.Fatalf("expected no error for image transcode, got: %v", err)
	}

	if record == nil {
		t.Fatal("expected non-nil derivative record")
	}

	expectedKey, _ := storage.BuildKey(ownerID, mediaID, storage.VariantPreview, "image/jpeg", "")
	if record.StorageKey != expectedKey {
		t.Errorf("expected storage key %s, got %s", expectedKey, record.StorageKey)
	}

	if _, exists := st.object[expectedKey]; !exists {
		t.Fatalf("expected file %s to exist in storage, but it was not found", expectedKey)
	}
}

func TestTranscodeHandler_RollbackOnDBError(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/video.mp4"

	fileData, err := os.ReadFile(filepath.Join("testdata", "video.mp4"))
	if err != nil {
		t.Fatalf("failed to read test fixture: %v", err)
	}
	st.object[sourceKey] = fileData

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindVideo,
		SourceKey: sourceKey,
	}

	m := &mockRepo{
		upsertFunc: func(ctx context.Context, record *repo.Derivative) (*repo.Derivative, error) {
			return nil, errors.New("db connection failed")
		},
	}

	ctx := context.Background()
	h := NewTranscodeHandler(st, m, &config.Config{}, nil)
	_, err = h.ProcessTranscode(ctx, media)
	if err == nil {
		t.Fatal("expected error due to DB failure, got nil")
	}

	expectedKey, _ := storage.BuildKey(ownerID, mediaID, storage.VariantR720, "video/mp4", "")
	if _, exists := st.object[expectedKey]; exists {
		t.Fatalf("orphan file %s was not deleted from storage after DB error", expectedKey)
	}
}

func TestTranscodeHandler_CorruptSource(t *testing.T) {
	st := newMemoryStorage()
	ownerID := uuid.New()
	mediaID := uuid.New()
	sourceKey := "test/source/corrupt.bin"

	st.object[sourceKey] = []byte("corrupt binary data")

	media := MediaRecord{
		ID:        mediaID,
		OwnerID:   ownerID,
		Kind:      KindVideo,
		SourceKey: sourceKey,
	}

	m := &mockRepo{}
	ctx := context.Background()
	h := NewTranscodeHandler(st, m, &config.Config{}, nil)

	_, err := h.ProcessTranscode(ctx, media)
	if err == nil {
		t.Fatal("expected error for corrupt source, got nil")
	}
}

func TestTranscodeHandler_Idempotency(t *testing.T) {
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
	ctx := context.Background()
	h := NewTranscodeHandler(st, m, &config.Config{}, nil)

	firstRecord, err := h.ProcessTranscode(ctx, media)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	secondRecord, err := h.ProcessTranscode(ctx, media)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	if firstRecord.StorageKey != secondRecord.StorageKey {
		t.Errorf("expected same storage key %s, got %s", firstRecord.StorageKey, secondRecord.StorageKey)
	}
}
