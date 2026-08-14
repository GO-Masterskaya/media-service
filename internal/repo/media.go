// Файл — ЗАГЛУШКА для компиляции задачи #12.
// Полная реализация доменных моделей и SQL-методов ожидается в задаче #10.
// TODO(#10): заменить заглушки на реальные структуры и запросы.
package repo

import (
	"context"
	"errors"
)

// ErrNotFound возвращается когда запись отсутствует в БД.
var ErrNotFound = errors.New("not found")

// Media — минимальная модель таблицы media (SPEC §6).
// Поля будут дополнены в #10.
type Media struct {
	ID         string
	OwnerID    string
	Status     string // deliting, failed, processing, ready ...
	StorageKey string
}

// Derivative — минимальная модель таблицы media_derivative (SPEC §6).
// Поля будут дополнены в #10.
type Derivative struct {
	MediaID    string
	Variant    string
	Status     string
	StorageKey string
}

// MediaRepository — интерфейс чтения медиа и производных. 
// Сигнатуры можно переделать в соответствии с ТЗ, повторюсь, этот файл - заглушка.
// Реализация SQL-запросов ожидается в следующих задачах.
type MediaRepository interface {
	GetMedia(ctx context.Context, id string) (*Media, error)
	GetDerivative(ctx context.Context, mediaID, variant string) (*Derivative, error)
}