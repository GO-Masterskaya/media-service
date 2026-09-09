package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"mediaservice/internal/processing"
	"mediaservice/internal/repo"
	"mediaservice/internal/upload"
)

// Prober извлекает технические метаданные медиафайла.
type Prober interface {
	Probe(ctx context.Context, inputPath string) (*processing.MediaInfo, error)
}

// DefaultProber использует ffprobe через пакет processing.
type DefaultProber struct{}

func (DefaultProber) Probe(ctx context.Context, inputPath string) (*processing.MediaInfo, error) {
	return processing.Probe(ctx, inputPath)
}

// UploadRequestParams содержит параметры инициализации загрузки.
type UploadRequestParams struct {
	OwnerID        uuid.UUID
	CallerID       uuid.UUID
	Filename       string
	MIME           string
	ExpectedSize   uint64
	IdempotencyKey string
	MakeThumbnail  bool
	Transcode      bool
	ExpiresAt      *time.Time
}

// UploadResult содержит результат загрузки или идемпотентного повтора.
type UploadResult struct {
	MediaID uuid.UUID
	Status  repo.MediaStatus
	Replay  bool
}

// ChunkReceiver — функция получения очередного чанка из клиентского стрима.
// При завершении стрима возвращает io.EOF.
type ChunkReceiver func() ([]byte, error)

// ValidateMIME проверяет MIME-тип по белому списку (поддерживаются wildcard вида 'image/*', '*/*', а также точные типы).
func ValidateMIME(mime string, allowlist []string) bool {
	if mime == "" {
		return false
	}
	if len(allowlist) == 0 {
		return false
	}
	mime = strings.ToLower(strings.TrimSpace(mime))
	for _, pattern := range allowlist {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		if pattern == "*" || pattern == "*/*" || pattern == mime {
			return true
		}
		if strings.HasSuffix(pattern, "/*") {
			prefix := strings.TrimSuffix(pattern, "/*")
			if strings.HasPrefix(mime, prefix+"/") {
				return true
			}
		}
	}
	return false
}

// Upload оркестрирует приём стрима: запись во временный файл, валидацию,
// ffprobe-инспекцию и атомарное сохранение в MinIO и PostgreSQL.
func (s *Service) Upload(ctx context.Context, params UploadRequestParams, chunkRecv ChunkReceiver) (*UploadResult, error) {
	// 1. Валидация входных параметров
	if params.OwnerID == uuid.Nil {
		return nil, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if params.CallerID != uuid.Nil && params.CallerID != params.OwnerID {
		return nil, fmt.Errorf("%w: caller is not authorized for owner", ErrAccessDenied)
	}
	if params.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: idempotency_key is required", ErrInvalidArgument)
	}
	if params.MIME == "" {
		return nil, fmt.Errorf("%w: mime is required", ErrInvalidArgument)
	}
	if !ValidateMIME(params.MIME, s.mimeAllowlist) {
		return nil, fmt.Errorf("%w: mime type %q is not allowed", ErrInvalidArgument, params.MIME)
	}
	if s.maxUploadBytes > 0 && params.ExpectedSize > uint64(s.maxUploadBytes) {
		return nil, fmt.Errorf("%w: expected size %d exceeds limit %d", ErrInvalidArgument, params.ExpectedSize, s.maxUploadBytes)
	}
	if params.ExpiresAt != nil && !params.ExpiresAt.After(time.Now()) {
		return nil, fmt.Errorf("%w: expiration time must be in the future", ErrInvalidArgument)
	}
	if s.tempStore == nil {
		return nil, errors.New("temp store is not configured")
	}

	// Ранняя проверка идемпотентности (#78):
	// Проверяем пару (owner_id, idempotency_key) сразу после приёма первого пакета,
	// до создания временного файла, выкачивания байтов и запуска ffprobe.
	existing, err := s.mediaRepo.GetByOwnerIdempotency(ctx, params.OwnerID, params.IdempotencyKey)
	if err == nil {
		if existing.Status == repo.MediaStatusDeleting {
			return nil, fmt.Errorf("%w: media is deleting", ErrAlreadyExists)
		}
		currentParamsFP := ParamsFingerprint(params.MIME, params.MakeThumbnail, params.Transcode, params.ExpiresAt)
		if existing.ParamsFingerprint != "" && existing.ParamsFingerprint != currentParamsFP {
			return nil, fmt.Errorf("%w: params mismatch for idempotency key", ErrAlreadyExists)
		}
		if params.ExpectedSize > 0 && existing.SizeBytes != int64(params.ExpectedSize) {
			return nil, fmt.Errorf("%w: expected size mismatch for idempotency key", ErrAlreadyExists)
		}
		return &UploadResult{
			MediaID: existing.ID,
			Status:  existing.Status,
			Replay:  true,
		}, nil
	}
	if !errors.Is(err, repo.ErrNotFound) {
		return nil, fmt.Errorf("early idempotency check: %w", err)
	}

	// Проверка квоты перед началом загрузки по expected_size (#78)
	if params.ExpectedSize > 0 {
		if err := s.checkQuota(ctx, params.OwnerID, int64(params.ExpectedSize)); err != nil {
			return nil, err
		}
	}

	// 2. Создание временного файла (#22)
	tf, err := s.tempStore.Create(ctx)
	if err != nil {
		if errors.Is(err, upload.ErrDiskFull) {
			return nil, err
		}
		return nil, fmt.Errorf("create temp file: %w", err)
	}
	defer tf.Remove()

	hasher := sha256.New()
	var totalWritten int64

	const maxConsecutiveEmptyChunks = 3
	consecutiveEmptyChunks := 0

	var magicHeader []byte
	magicChecked := false

	// 3. Стриминг чанков во временный файл с расчетом SHA256 на лету
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		chunk, recvErr := chunkRecv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			return nil, recvErr
		}
		if len(chunk) == 0 {
			consecutiveEmptyChunks++
			if consecutiveEmptyChunks > maxConsecutiveEmptyChunks {
				return nil, fmt.Errorf("%w: excessive empty chunks received", ErrInvalidArgument)
			}
			continue
		}
		consecutiveEmptyChunks = 0

		if !magicChecked {
			toCopy := 512 - len(magicHeader)
			if toCopy > len(chunk) {
				toCopy = len(chunk)
			}
			magicHeader = append(magicHeader, chunk[:toCopy]...)
			if len(magicHeader) >= 512 {
				if err := checkMagicBytes(params.MIME, magicHeader); err != nil {
					return nil, err
				}
				magicChecked = true
			}
		}

		n, writeErr := tf.WriteChunk(chunk)
		if writeErr != nil {
			if errors.Is(writeErr, upload.ErrSizeLimitExceeded) {
				return nil, fmt.Errorf("%w: file size exceeds max limit: %v", ErrInvalidArgument, writeErr)
			}
			if errors.Is(writeErr, upload.ErrDiskFull) {
				return nil, writeErr
			}
			return nil, fmt.Errorf("write temp file: %w", writeErr)
		}
		hasher.Write(chunk[:n])
		totalWritten += int64(n)
	}

	if totalWritten == 0 {
		return nil, fmt.Errorf("%w: empty upload body", ErrInvalidArgument)
	}
	if !magicChecked && len(magicHeader) > 0 {
		if err := checkMagicBytes(params.MIME, magicHeader); err != nil {
			return nil, err
		}
		magicChecked = true
	}
	if params.ExpectedSize > 0 && totalWritten != int64(params.ExpectedSize) {
		return nil, fmt.Errorf("%w: actual size (%d) does not match expected size (%d)", ErrInvalidArgument, totalWritten, params.ExpectedSize)
	}

	// Проверка квоты по фактическому размеру (#78)
	if err := s.checkQuota(ctx, params.OwnerID, totalWritten); err != nil {
		return nil, err
	}

	// Закрываем дескриптор записи перед ffprobe и чтением
	if err := tf.Close(); err != nil {
		return nil, fmt.Errorf("close temp file: %w", err)
	}

	// 4. Media inspection через ffprobe (#8)
	prober := s.prober
	if prober == nil {
		prober = DefaultProber{}
	}

	probeInfo, probeErr := prober.Probe(ctx, tf.Name())
	if probeErr != nil {
		return nil, fmt.Errorf("%w: corrupt or unreadable media: %v", ErrInvalidArgument, probeErr)
	}

	// Проверка соответствия MIME-класса
	if err := validateMIMEClass(params.MIME, probeInfo.Kind); err != nil {
		return nil, err
	}

	// Валидация опций обработки
	jobTypes, err := validateProcessingOptions(params.MakeThumbnail, params.Transcode, probeInfo.Kind)
	if err != nil {
		return nil, err
	}

	metaJSON, err := buildProbeMetadata(probeInfo)
	if err != nil {
		return nil, fmt.Errorf("build metadata: %w", err)
	}

	bodyFingerprint := hex.EncodeToString(hasher.Sum(nil))
	paramsFingerprint := ParamsFingerprint(params.MIME, params.MakeThumbnail, params.Transcode, params.ExpiresAt)

	// 5. Атомарное сохранение в MinIO и Postgres (#23)
	rf, err := os.Open(tf.Name())
	if err != nil {
		return nil, fmt.Errorf("open temp file for reading: %w", err)
	}
	defer func() {
		_ = rf.Close()
	}()

	kind := repo.MediaKind(probeInfo.Kind)
	mediaID := uuid.New()

	persistIn := PersistUploadInput{
		OwnerID:           params.OwnerID,
		MediaID:           mediaID,
		IdempotencyKey:    params.IdempotencyKey,
		Filename:          params.Filename,
		Mime:              params.MIME,
		Kind:              kind,
		SizeBytes:         totalWritten,
		BodyFingerprint:   bodyFingerprint,
		ParamsFingerprint: paramsFingerprint,
		ExpiresAt:         params.ExpiresAt,
		Metadata:          metaJSON,
		JobTypes:          jobTypes,
		Reader:            rf,
		ContentType:       params.MIME,
	}

	res, err := s.PersistUpload(ctx, persistIn)
	if err != nil {
		return nil, err
	}

	return &UploadResult{
		MediaID: res.Media.ID,
		Status:  res.Media.Status,
		Replay:  res.Replay,
	}, nil
}

func validateMIMEClass(mime string, kind processing.Kind) error {
	parts := strings.SplitN(mime, "/", 2)
	major := strings.ToLower(parts[0])

	switch major {
	case "image":
		if kind != processing.KindImage {
			return fmt.Errorf("%w: mime class %q does not match inspected media kind %q", ErrInvalidArgument, major, kind)
		}
	case "video":
		if kind != processing.KindVideo {
			return fmt.Errorf("%w: mime class %q does not match inspected media kind %q", ErrInvalidArgument, major, kind)
		}
	case "audio":
		if kind != processing.KindAudio {
			return fmt.Errorf("%w: mime class %q does not match inspected media kind %q", ErrInvalidArgument, major, kind)
		}
	default:
		return fmt.Errorf("%w: unsupported mime class %q", ErrInvalidArgument, major)
	}
	return nil
}

func validateProcessingOptions(makeThumbnail, transcode bool, kind processing.Kind) ([]string, error) {
	if makeThumbnail && kind == processing.KindAudio {
		return nil, fmt.Errorf("%w: thumbnail is not supported for audio", ErrInvalidArgument)
	}
	if transcode && kind == processing.KindImage {
		return nil, fmt.Errorf("%w: transcode is not supported for images", ErrInvalidArgument)
	}

	var jobTypes []string
	if makeThumbnail {
		jobTypes = append(jobTypes, "thumbnail")
	}
	if transcode {
		jobTypes = append(jobTypes, "transcode")
	}
	return jobTypes, nil
}

type mediaMetadata struct {
	Width         int     `json:"width,omitempty"`
	Height        int     `json:"height,omitempty"`
	DurationSec   float64 `json:"duration_sec,omitempty"`
	Codec         string  `json:"codec,omitempty"`
	Bitrate       int64   `json:"bitrate,omitempty"`
	FrameCount    int64   `json:"frame_count,omitempty"`
	AudioChannels int     `json:"audio_channels,omitempty"`
	AudioCodec    string  `json:"audio_codec,omitempty"`
	Format        string  `json:"format,omitempty"`
}

func buildProbeMetadata(info *processing.MediaInfo) (json.RawMessage, error) {
	if info == nil {
		return json.RawMessage(`{}`), nil
	}
	m := mediaMetadata{
		Width:         info.Width,
		Height:        info.Height,
		Codec:         info.Codec,
		Bitrate:       info.Bitrate,
		FrameCount:    info.FrameCount,
		AudioChannels: info.AudioChannels,
		AudioCodec:    info.AudioCodec,
		Format:        info.FormatName,
	}
	if info.Duration > 0 {
		m.DurationSec = info.Duration.Seconds()
	}
	return json.Marshal(m)
}

func checkMagicBytes(declaredMIME string, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	detected := http.DetectContentType(data)
	parts := strings.Split(detected, ";")
	detectedType := strings.ToLower(strings.TrimSpace(parts[0]))
	declaredType := strings.ToLower(strings.TrimSpace(strings.Split(declaredMIME, ";")[0]))

	// Отсекаем подмену типов, когда http.DetectContentType точно распознал медиа-формат (#78):
	// например, image/png с телом JPEG или video с телом image.
	if strings.HasPrefix(detectedType, "image/") || strings.HasPrefix(detectedType, "video/") || strings.HasPrefix(detectedType, "audio/") {
		detectedMajor := strings.SplitN(detectedType, "/", 2)[0]
		declaredMajor := strings.SplitN(declaredType, "/", 2)[0]

		if detectedMajor != declaredMajor {
			return fmt.Errorf("%w: declared mime %q does not match detected media type %q",
				ErrInvalidArgument, declaredMIME, detectedType)
		}

		if detectedMajor == "image" && !isImageMIMECompatible(declaredType, detectedType) {
			return fmt.Errorf("%w: declared mime %q does not match detected image type %q",
				ErrInvalidArgument, declaredMIME, detectedType)
		}
	}
	return nil
}

func isImageMIMECompatible(declared, detected string) bool {
	if declared == detected {
		return true
	}
	if (declared == "image/jpeg" && detected == "image/jpg") ||
		(declared == "image/jpg" && detected == "image/jpeg") {
		return true
	}
	return false
}
