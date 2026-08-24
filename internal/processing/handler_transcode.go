package processing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mediaservice/internal/config"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type TranscodeHandler struct {
	storage storage.Interface
	repo    DerivativeRepository
	cfg     *config.Config
	log     *slog.Logger
}

func NewTranscodeHandler(st storage.Interface, repo DerivativeRepository, cfg *config.Config, log *slog.Logger) *TranscodeHandler {
	return &TranscodeHandler{
		storage: st,
		repo:    repo,
		cfg:     cfg,
		log:     log,
	}
}

func (h *TranscodeHandler) logError(msg string, mediaID uuid.UUID, err error) {
	if h.log != nil {
		h.log.Error(msg, "media_id", mediaID, "error", err)
	}
}

/*
resolveTranscodeFormat определяет расширение файла, MIME-тип и целевой вариант
хранения (storage.Variant) в зависимости от типа медиафайла (Kind).
*/
func (h *TranscodeHandler) resolveTranscodeFormat(kind Kind) (ext string, mime string, variant storage.Variant) {
	switch kind {
	case KindVideo:
		return "mp4", "video/mp4", storage.VariantR720
	case KindAudio:
		return "m4a", "audio/mp4", storage.VariantPreview
	case KindImage:
		return "jpg", "image/jpeg", storage.VariantPreview
	default:
		return "bin", "application/octet-stream", storage.VariantPreview
	}
}

/*
ProcessTranscode выполняет полный цикл обработка/транскодирования медиафайла:
 1. Проверяет валидность ID медиа и владельца.
 2. Создает изолированную временную папку и скачивает исходник из S3.
 3. Вызывает процесс транскодирования (FFmpeg) для получения нужного формата.
 4. Снимает метаданные с готового файла через Probe (ширина/высота).
 5. Загружает транскодированный файл в S3 с детерминированным ключом.
 6. Идемпотентно сохраняет запись о производной (DerivativeRecord) в БД.

Функция гарантирует очистку временных файлов на диске (через defer)
и транзакционный откат: если сохранение в БД завершится ошибкой,
уже загруженный объект будет удален из S3.
*/
func (h *TranscodeHandler) ProcessTranscode(ctx context.Context, media MediaRecord) (*DerivativeRecord, error) {
	if media.ID == uuid.Nil || media.OwnerID == uuid.Nil {
		err := errors.New("invalid media_id or owner_id")
		h.logError("validation failed", media.ID, err)
		return nil, err
	}

	workDir, err := os.MkdirTemp("", "transcode-*")
	if err != nil {
		h.logError("failed to create temp dir", media.ID, err)
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	res, err := h.storage.GetObject(ctx, media.SourceKey)
	if err != nil {
		h.logError("failed to get object from storage", media.ID, err)
		return nil, fmt.Errorf("error get object: %w", err)
	}
	defer func() { _ = res.Close() }()

	inputPath := filepath.Join(workDir, "input")
	inFile, err := os.Create(inputPath)
	if err != nil {
		h.logError("failed to create input file", media.ID, err)
		return nil, fmt.Errorf("error create input file: %w", err)
	}

	limitedReader := io.LimitReader(res, maxSourceSizeBytes+1)
	write, err := io.Copy(inFile, limitedReader)
	_ = inFile.Close()
	if err != nil {
		h.logError("failed to copy source data to input file", media.ID, err)
		return nil, fmt.Errorf("error copy file: %w", err)
	}
	if write > maxSourceSizeBytes {
		err := errors.New("file size exceeds maximum allowed limit")
		h.logError("source file too large", media.ID, err)
		return nil, err
	}

	ext, mime, variant := h.resolveTranscodeFormat(media.Kind)
	outputPath := filepath.Join(workDir, fmt.Sprintf("transcode_%s.%s", media.ID, ext))

	actualOutputPath, err := Transcode(ctx, inputPath, outputPath, media.Kind)
	if err != nil {
		h.logError("failed to transcode media via ffmpeg", media.ID, err)
		return nil, fmt.Errorf("error transcoding: %w", err)
	}
	defer func() { _ = os.Remove(actualOutputPath) }()

	file, err := os.Open(actualOutputPath)
	if err != nil {
		h.logError("failed to open transcoded file", media.ID, err)
		return nil, fmt.Errorf("error open transcoded file: %w", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		h.logError("failed to stat transcoded file", media.ID, err)
		return nil, fmt.Errorf("error stat transcoded file: %w", err)
	}

	key, err := storage.BuildKey(media.OwnerID, media.ID, variant, mime, "")
	if err != nil {
		h.logError("failed to build storage key", media.ID, err)
		return nil, fmt.Errorf("error build storage key: %w", err)
	}

	if err = h.storage.PutObject(ctx, key, file, info.Size(), mime); err != nil {
		h.logError("failed to upload transcoded file to storage", media.ID, err)
		return nil, fmt.Errorf("error upload transcoded file to storage: %w", err)
	}

	var dbCommitted bool
	defer func() {
		if !dbCommitted {
			_ = h.storage.DeleteObject(context.Background(), key)
		}
	}()

	metadata := make(map[string]any)
	if infoProbe, err := Probe(ctx, actualOutputPath); err == nil && infoProbe != nil {
		if infoProbe.Width > 0 && infoProbe.Height > 0 {
			metadata["width"] = infoProbe.Width
			metadata["height"] = infoProbe.Height
		}
		if infoProbe.Duration > 0 {
			metadata["duration"] = infoProbe.Duration
		}
	} else if err != nil && h.log != nil {
		h.log.Warn("failed to probe transcoded file metadata", "media_id", media.ID, "error", err)
	}

	record := &repo.Derivative{
		ID:         uuid.New(),
		MediaID:    media.ID,
		Variant:    string(variant),
		Mime:       mime,
		SizeBytes:  info.Size(),
		StorageKey: key,
	}

	resRecord, err := h.repo.UpsertDerivative(ctx, record)
	if err != nil {
		h.logError("failed to save derivative in db", media.ID, err)
		return nil, fmt.Errorf("save derivative in db: %w", err)
	}

	dbCommitted = true

	return &DerivativeRecord{
		ID:         resRecord.ID,
		MediaID:    resRecord.MediaID,
		Variant:    storage.Variant(resRecord.Variant),
		MIME:       record.Mime,
		SizeBytes:  record.SizeBytes,
		StorageKey: record.StorageKey,
		Metadata:   metadata,
	}, nil
}
