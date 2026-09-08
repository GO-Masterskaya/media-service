package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// compensateTimeout — отдельный дедлайн для DeleteObject после сбоя DB.
// Нельзя использовать исходный ctx: он часто уже отменён (cancel/timeout),
// а компенсация как раз нужна в этом случае.
const compensateTimeout = 10 * time.Second

var allowedJobTypes = map[string]struct{}{
	"thumbnail": {},
	"transcode": {},
}

// PersistUploadInput — уже провалидированные данные после upload.TempStore
// и ffprobe (вызывающий слой MediaService.Upload).
type PersistUploadInput struct {
	OwnerID           uuid.UUID
	MediaID           uuid.UUID // обязателен: задаёт Upload-слой на все попытки одного upload
	IdempotencyKey    string
	Filename          string
	Mime              string
	Kind              repo.MediaKind
	SizeBytes         int64
	BodyFingerprint   string
	ParamsFingerprint string
	ExpiresAt         *time.Time
	Metadata          json.RawMessage
	JobTypes          []string // "thumbnail", "transcode"
	Reader            io.Reader
	ContentType       string // для PutObject; если пусто — Mime
}

// PersistUploadResult — результат первого upload или идемпотентного replay.
type PersistUploadResult struct {
	Media  *repo.Media
	Replay bool
}

// PersistUpload кладёт оригинал в MinIO, затем атомарно пишет media+jobs.
// Порядок: MinIO → DB. При ошибке DB — best-effort storage.DeleteObject,
// но только если объект не принадлежит уже закоммиченной строке (тот же StorageKey).
// Необработанный orphan подхватывает media.Reconciler.
func (s *Service) PersistUpload(ctx context.Context, in PersistUploadInput) (*PersistUploadResult, error) {
	if in.OwnerID == uuid.Nil {
		return nil, fmt.Errorf("%w: owner_id required", ErrInvalidArgument)
	}
	if in.MediaID == uuid.Nil {
		return nil, fmt.Errorf("%w: media_id required", ErrInvalidArgument)
	}
	if in.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: idempotency_key required", ErrInvalidArgument)
	}
	if in.BodyFingerprint == "" || in.ParamsFingerprint == "" {
		return nil, fmt.Errorf("%w: fingerprints required", ErrInvalidArgument)
	}
	if in.Reader == nil {
		return nil, fmt.Errorf("%w: reader required", ErrInvalidArgument)
	}
	if err := validateJobTypes(in.JobTypes); err != nil {
		return nil, err
	}

	existing, err := s.mediaRepo.GetByOwnerIdempotency(ctx, in.OwnerID, in.IdempotencyKey)
	if err == nil {
		return matchIdempotent(existing, in)
	}
	if !errors.Is(err, repo.ErrNotFound) {
		s.log.Error("idempotency lookup failed", slog.Any("error", err))
		return nil, err
	}

	// До Put: тот же media_id с другим idempotency_key не должен перезаписать
	// чужой объект в MinIO (ключ строится из id/mime/filename).
	byID, err := s.mediaRepo.GetByID(ctx, in.MediaID)
	if err == nil {
		if byID.IdempotencyKey == in.IdempotencyKey {
			return matchIdempotent(byID, in)
		}
		return nil, ErrAlreadyExists
	}
	if !errors.Is(err, repo.ErrNotFound) {
		s.log.Error("media id lookup failed", slog.Any("error", err))
		return nil, err
	}

	key, err := storage.BuildKey(in.OwnerID, in.MediaID, storage.VariantOriginal, in.Mime, in.Filename)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}

	contentType := in.ContentType
	if contentType == "" {
		contentType = in.Mime
	}

	if err := s.storage.PutObject(ctx, key, in.Reader, in.SizeBytes, contentType); err != nil {
		s.log.Error("put object failed",
			slog.Any("error", err),
			slog.String("storage_key", key),
		)
		return nil, err
	}

	meta := in.Metadata
	if len(meta) == 0 {
		meta = json.RawMessage(`{}`)
	}

	row := repo.Media{
		ID:                in.MediaID,
		OwnerID:           in.OwnerID,
		Kind:              in.Kind,
		OrigFilename:      in.Filename,
		Mime:              in.Mime,
		SizeBytes:         in.SizeBytes,
		StorageKey:        key,
		Metadata:          meta,
		IdempotencyKey:    in.IdempotencyKey,
		BodyFingerprint:   in.BodyFingerprint,
		ParamsFingerprint: in.ParamsFingerprint,
		ExpiresAt:         in.ExpiresAt,
	}

	created, err := s.mediaRepo.InsertWithJobs(ctx, row, in.JobTypes)
	if err == nil {
		return &PersistUploadResult{Media: created, Replay: false}, nil
	}

	// Сначала резолвим конфликт: при совпадающем StorageKey объект уже принадлежит
	// победителю — удалять нельзя (типичный ретрай с тем же media_id).
	if errors.Is(err, repo.ErrConcurrentConflict) {
		existing, lookupErr := s.mediaRepo.GetByOwnerIdempotency(ctx, in.OwnerID, in.IdempotencyKey)
		if lookupErr != nil {
			s.compensateDelete(ctx, key)
			return nil, fmt.Errorf("concurrent insert resolve: %w", lookupErr)
		}
		if existing.StorageKey != key {
			s.compensateDelete(ctx, key)
		}
		return matchIdempotent(existing, in)
	}

	if errors.Is(err, repo.ErrIDConflict) {
		existing, lookupErr := s.mediaRepo.GetByID(ctx, in.MediaID)
		if lookupErr != nil {
			s.compensateDelete(ctx, key)
			return nil, fmt.Errorf("id conflict resolve: %w", lookupErr)
		}
		if existing.StorageKey != key {
			s.compensateDelete(ctx, key)
		}
		if existing.IdempotencyKey == in.IdempotencyKey {
			return matchIdempotent(existing, in)
		}
		return nil, ErrAlreadyExists
	}

	s.compensateDelete(ctx, key)
	s.log.Error("insert media failed",
		slog.Any("error", err),
		slog.String("media_id", in.MediaID.String()),
	)
	return nil, err
}

func (s *Service) compensateDelete(ctx context.Context, key string) {
	compensateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compensateTimeout)
	defer cancel()
	if delErr := s.storage.DeleteObject(compensateCtx, key); delErr != nil {
		s.log.Error("compensate delete after db failure",
			slog.Any("error", delErr),
			slog.String("storage_key", key),
		)
	}
}

func validateJobTypes(jobTypes []string) error {
	for _, jt := range jobTypes {
		if _, ok := allowedJobTypes[jt]; !ok {
			return fmt.Errorf("%w: unsupported job type %q", ErrInvalidArgument, jt)
		}
	}
	return nil
}

func matchIdempotent(existing *repo.Media, in PersistUploadInput) (*PersistUploadResult, error) {
	switch existing.Status {
	case repo.MediaStatusDeleting, repo.MediaStatusFailed:
		return nil, fmt.Errorf("%w: media status %s", ErrFailedPrecondition, existing.Status)
	}

	// Строки до миграции fingerprints: DEFAULT '' без бэкфилла. Пустые fp
	// не сравниваем с новыми — иначе любой ретрай станет AlreadyExists.
	legacyEmpty := existing.BodyFingerprint == "" && existing.ParamsFingerprint == ""
	if legacyEmpty ||
		(existing.BodyFingerprint == in.BodyFingerprint &&
			existing.ParamsFingerprint == in.ParamsFingerprint) {
		return &PersistUploadResult{Media: existing, Replay: true}, nil
	}
	return nil, ErrAlreadyExists
}
