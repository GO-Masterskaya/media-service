package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
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
}

func NewMinIO(cfg MinIOConfig) (Interface, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client init: %w", err)
	}
	return &minioStorage{client: client, bucket: cfg.Bucket}, nil
}

func (s *minioStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	opts := minio.PutObjectOptions{ContentType: contentType}
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, size, opts)
	if err != nil {
		return fmt.Errorf("put object %q: %w", key, err)
	}
	return nil
}

func (s *minioStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object %q: %w", key, err)
	}
	// GetObject ленивый — проверяем Stat, чтобы отловить NoSuchKey сразу.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, fmt.Errorf("stat object %q: %w", key, err)
	}
	return obj, nil
}

func (s *minioStorage) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*PresignedURL, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("ttl must be positive")
	}
	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, ttl, url.Values{})
	if err != nil {
		return nil, fmt.Errorf("presign object %q: %w", key, err)
	}
	return &PresignedURL{
		URL:       u.String(),
		ExpiresAt: time.Now().Add(ttl),
	}, nil
}

func (s *minioStorage) DeleteObject(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err == nil {
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
	for err := range s.client.RemoveObjects(ctx, s.bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if err.Err != nil {
			removeErrs = append(removeErrs, fmt.Errorf("remove %q: %w", err.ObjectName, err.Err))
		}
	}

	if listErr != nil {
		return fmt.Errorf("list objects for prefix %q: %w", prefix, listErr)
	}
	if len(removeErrs) > 0 {
		return fmt.Errorf("delete prefix %q: %d errors, first: %w", prefix, len(removeErrs), removeErrs[0])
	}
	return nil
}

func (s *minioStorage) Close() error { return nil }
