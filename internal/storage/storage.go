package storage

import (
	"context"
	"io"
	"time"
)

// PresignedURL — подписанная ссылка с временем протухания.
type PresignedURL struct {
	URL       string
	ExpiresAt time.Time
}

// Interface — адаптер объектного хранилища.
type Interface interface {
	// PutObject загружает reader по ключу. size=-1 если размер неизвестен (streaming).
	PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error

	// GetObject возвращает reader объекта.
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)

	// PresignGetObject выдаёт presigned GET-ссылку с TTL.
	PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*PresignedURL, error)

	// DeleteObject удаляет объект. Идемпотентно (нет ошибки если ключ отсутствует).
	DeleteObject(ctx context.Context, key string) error

	// DeletePrefix удаляет все объекты по префиксу (включая «папку» owner/media).
	DeletePrefix(ctx context.Context, prefix string) error

	Close() error
}
