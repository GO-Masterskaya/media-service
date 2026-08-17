package processing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mediaservice/internal/config"
	"mediaservice/internal/storage"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type MediaRecord struct {
	ID        uuid.UUID
	OwnerID   uuid.UUID
	Kind      Kind
	SourceKey string
}

type DerivativeRecord struct {
	ID         uuid.UUID
	MediaID    uuid.UUID
	Variant    storage.Variant
	MIME       string
	SizeBytes  int64
	StorageKey string
	Metadata   map[string]any
}

type DerivativeRepository interface {
	UpsertDerivative(ctx context.Context, record *DerivativeRecord) (*DerivativeRecord, error)
}

type ThumbnailHandler struct {
	storage storage.Interface
	repo    DerivativeRepository
	cfg     *config.Config
	log     *slog.Logger
}

func NewThumbnailHandler(
	storage storage.Interface,
	repo DerivativeRepository,
	cfg *config.Config,
	log *slog.Logger,
) *ThumbnailHandler {
	if log == nil {
		log = slog.Default()
	}
	return &ThumbnailHandler{
		storage: storage,
		repo:    repo,
		cfg:     cfg,
		log:     log,
	}
}

func (h *ThumbnailHandler) downloadSource(ctx context.Context, key, targetPath string) error {
	res, err := h.storage.GetObject(ctx, key)
	if err != nil {
		return fmt.Errorf("error get object: %w", err)
	}
	defer func() { _ = res.Close() }()

	out, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("error create file: %w", err)
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, res); err != nil {
		return fmt.Errorf("error copy file: %w", err)
	}

	return nil
}

func (h *ThumbnailHandler) resolveThumbFormat(kind Kind) (ext string, mime string) {
	switch kind {
	case KindAudio:
		return "png", "image/png"
	default:
		return "jpg", "image/jpeg"
	}
}

func (h *ThumbnailHandler) ProcessThumbnail(ctx context.Context, media MediaRecord) (*DerivativeRecord, error) {
	if media.ID == uuid.Nil || media.OwnerID == uuid.Nil {
		return nil, errors.New("invalid media_id or owner_id")
	}

	tempDir, err := os.MkdirTemp("", "thumb_*")
	if err != nil {
		return nil, fmt.Errorf("error create dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	sourcePath := filepath.Join(tempDir, "source")
	if err := h.downloadSource(ctx, media.SourceKey, sourcePath); err != nil {
		return nil, fmt.Errorf("download source: %w", err)
	}
	ext, mime := h.resolveThumbFormat(media.Kind)
	outputPath := filepath.Join(tempDir, "thumb."+ext)

	sec := h.cfg.ThumbSecond
	if sec <= 0 {
		sec = 0
	}
	if err := GenerateThumbnail(ctx, sourcePath, outputPath, media.Kind, sec); err != nil {
		return nil, fmt.Errorf("error call ffmpeg: %w", err)
	}
	actualOutputPath := filepath.Join(os.TempDir(), filepath.Base(outputPath))
	defer func() { _ = os.Remove(actualOutputPath) }()

	file, err := os.Open(actualOutputPath)
	if err != nil {
		return nil, fmt.Errorf("error open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error get file info: %w", err)
	}
	key, err := storage.BuildKey(media.OwnerID, media.ID, storage.VariantThumb, mime, "")
	if err != nil {
		return nil, fmt.Errorf("error build key: %w", err)
	}

	if err = h.storage.PutObject(ctx, key, file, info.Size(), mime); err != nil {
		return nil, fmt.Errorf("error download file in db: %w", err)
	}

	var dbCommited bool
	defer func() {
		if !dbCommited {
			_ = h.storage.DeleteObject(context.Background(), key)
		}
	}()

	metadata := make(map[string]any)
	if infoProbe, err := Probe(ctx, actualOutputPath); err == nil && infoProbe != nil {
		metadata["width"] = infoProbe.Width
		metadata["height"] = infoProbe.Height
	}

	record := &DerivativeRecord{
		ID:         uuid.New(),
		MediaID:    media.ID,
		Variant:    storage.VariantThumb,
		MIME:       mime,
		SizeBytes:  info.Size(),
		StorageKey: key,
		Metadata:   metadata,
	}

	res, err := h.repo.UpsertDerivative(ctx, record)
	if err != nil {
		return nil, fmt.Errorf("save derivative in db: %w", err)
	}

	dbCommited = true

	return res, nil
}
