package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	Region    string
}

type minioStorage struct {
	client *minio.Client
	bucket string
	log    *slog.Logger
}

func NewMinIO(cfg MinIOConfig, log *slog.Logger) (Interface, error) {
	if log == nil {
		log = slog.Default()
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client init: %w", err)
	}

	log.Info("minio storage initialized",
		slog.String("endpoint", cfg.Endpoint),
		slog.String("bucket", cfg.Bucket),
		slog.Bool("ssl", cfg.UseSSL),
	)

	return &minioStorage{
		client: client,
		bucket: cfg.Bucket,
		log:    log,
	}, nil
}

func (s *minioStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	start := time.Now()
	opts := minio.PutObjectOptions{
		ContentType: contentType,
		UserMetadata: map[string]string{
			"upload-started-at": time.Now().UTC().Format(time.RFC3339),
		},
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, size, opts)
	if err != nil {
		return fmt.Errorf("put object %q: %w", key, err)
	}
	s.log.Info("object uploaded",
		slog.String("key", key),
		slog.String("content_type", contentType),
		slog.Duration("duration", time.Since(start)),
	)
	return nil
}

func (s *minioStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	// Возвращаем ленивый reader без лишнего HEAD-запроса.
	// Ошибка NoSuchKey вылезет при первом Read.
	return s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
}

func (s *minioStorage) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*PresignedURL, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("ttl must be positive")
	}
	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, ttl, url.Values{})
	if err != nil {
		return nil, fmt.Errorf("presign object %q: %w", key, err)
	}
	expires := time.Now().UTC().Add(ttl)
	s.log.Info("presign generated",
		slog.String("key", key),
		slog.Time("expires_at", expires),
		// Полный URL с credentials в query НЕ логируем (секреты).
	)
	return &PresignedURL{
		URL:       u.String(),
		ExpiresAt: expires,
	}, nil
}

func (s *minioStorage) DeleteObject(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err == nil {
		s.log.Info("object deleted", slog.String("key", key))
		return nil
	}
	// Идемпотентность: NoSuchKey / NoSuchObject — не ошибка.
	resp := minio.ToErrorResponse(err)
	if resp.Code == "NoSuchKey" || resp.Code == "NoSuchObject" {
		return nil
	}
	return fmt.Errorf("delete object %q: %w", key, err)
}

func (s *minioStorage) DeletePrefix(ctx context.Context, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("delete prefix: empty prefix is not allowed")
	}

	objectsCh := make(chan minio.ObjectInfo)
	var listErr error

	go func() {
		defer close(objectsCh)
		opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
		for obj := range s.client.ListObjects(ctx, s.bucket, opts) {
			if obj.Err != nil {
				listErr = obj.Err
				return
			}
			select {
			case objectsCh <- obj:
			case <-ctx.Done():
				return
			}
		}
	}()

	var removeErrs []error
	deleted := 0
	for err := range s.client.RemoveObjects(ctx, s.bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if err.Err != nil {
			removeErrs = append(removeErrs, fmt.Errorf("remove %q: %w", err.ObjectName, err.Err))
		} else {
			deleted++
		}
	}

	if listErr != nil {
		return fmt.Errorf("list objects for prefix %q: %w", prefix, listErr)
	}
	if len(removeErrs) > 0 {
		return fmt.Errorf("delete prefix %q: %d errors, first: %w", prefix, len(removeErrs), removeErrs[0])
	}
	s.log.Info("prefix deleted", slog.String("prefix", prefix), slog.Int("count", deleted))
	return nil
}

func (s *minioStorage) Close() error { return nil }

func (s *minioStorage) ForEachObject(ctx context.Context, prefix string, fn func(ObjectInfo) error) error {
	opts := minio.ListObjectsOptions{
		Prefix:       prefix,
		Recursive:    true,
		WithMetadata: true, // +++ FIX: без этого UserMetadata пустой
	}
	for obj := range s.client.ListObjects(ctx, s.bucket, opts) {
		if obj.Err != nil {
			return fmt.Errorf("list objects: %w", obj.Err)
		}

		uploadStarted := obj.LastModified
		for k, v := range obj.UserMetadata {
			// MinIO возвращает meta-заголовки с префиксом X-Amz-Meta- или lower-case
			if strings.EqualFold(k, "X-Amz-Meta-Upload-Started-At") || strings.EqualFold(k, "upload-started-at") {
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					uploadStarted = t
				}
				break
			}
		}

		if err := fn(ObjectInfo{
			Key:             obj.Key,
			LastModified:    obj.LastModified,
			UploadStartedAt: uploadStarted,
		}); err != nil {
			return err
		}
	}
	return nil
}
