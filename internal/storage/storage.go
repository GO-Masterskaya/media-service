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

// ObjectInfo — метаданные объекта из хранилища.
type ObjectInfo struct {
	Key             string
	LastModified    time.Time
	UploadStartedAt time.Time // +++ ADDED: время начала загрузки из user-metadata
}

// Interface — адаптер объектного хранилища.
type Interface interface {
	// PutObject загружает reader по ключу.
	// Реализация НЕ ДОЛЖНА загружать файл целиком в память.
	// size=-1 означает неизвестный размер (streaming); reader должен поддерживать
	// последовательное чтение без Seek.
	PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error

	// GetObject возвращает reader объекта. Ошибка вылезет при первом Read,
	// если ключ отсутствует (ленивое поведение SDK).
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)

	// PresignGetObject выдаёт presigned GET-ссылку с заданным TTL.
	PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*PresignedURL, error)

	// DeleteObject удаляет объект. Идемпотентно (нет ошибки если ключ отсутствует).
	DeleteObject(ctx context.Context, key string) error

	// DeletePrefix удаляет все объекты по префиксу. Пустой префикс отклоняется.
	DeletePrefix(ctx context.Context, prefix string) error

	// ForEachObject обходит объекты по префиксу, вызывая fn для каждого.
	// Потоковая обработка: не загружает весь бакет в память.
	// Если fn возвращает ошибку — обход прерывается.
	ForEachObject(ctx context.Context, prefix string, fn func(ObjectInfo) error) error

	Close() error
}
