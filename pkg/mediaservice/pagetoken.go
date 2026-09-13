package mediaservice

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"mediaservice/internal/repo"
)

// pageTokenVersion - версия формата токена. Пишется внутрь и проверяется при
// разборе, чтобы токен, выданный будущей версией библиотеки, был отвергнут
// явной ошибкой, а не разобран наполовину.
const pageTokenVersion = 1

// pageToken - содержимое непрозрачного токена страницы.
//
// Ядро листает по составному курсору (created_at, id): по одному только
// времени страницы не строятся, потому что две записи могут быть созданы
// в одну микросекунду. Оба поля обязаны доехать до следующего запроса,
// а публичный контракт разрешает передать между запросами только строку -
// отсюда кодирование.
//
// OwnerID кладётся в токен не ради курсора, а ради проверки: токен,
// выданный при листинге одного владельца, не должен продолжать листинг
// другого. Без этой привязки чужой токен молча сдвинул бы выборку
// на позицию из чужой ленты.
type pageToken struct {
	Version   int       `json:"v"`
	OwnerID   uuid.UUID `json:"owner_id"`
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

// encodePageToken собирает токен для продолжения выборки после записи last.
//
// base64 в варианте RawURL: токен переживает попадание в query-параметр
// и в JSON без дополнительного экранирования, а отсутствие padding убирает
// из строки знаки '=', которые часть клиентов режет.
func encodePageToken(ownerID uuid.UUID, createdAt time.Time, id uuid.UUID) (string, error) {
	payload, err := json.Marshal(pageToken{
		Version:   pageTokenVersion,
		OwnerID:   ownerID,
		CreatedAt: createdAt,
		ID:        id,
	})
	if err != nil {
		return "", fmt.Errorf("marshal page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// decodePageToken разбирает токен и проверяет, что он выдан для ownerID.
//
// Пустая строка означает начало выборки и даёт nil-курсор без ошибки:
// первый запрос страницы токена ещё не имеет.
//
// Любая проблема разбора превращается в ErrInvalidArgument без подробностей.
// Токен непрозрачен: вызывающий его не конструирует, поэтому объяснять,
// какое именно поле не сошлось, бессмысленно, а для подбора чужих токенов
// подробный ответ был бы подсказкой.
func decodePageToken(token string, ownerID uuid.UUID) (*repo.MediaCursor, error) {
	if token == "" {
		return nil, nil
	}

	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed page token", ErrInvalidArgument)
	}

	var t pageToken
	if err := json.Unmarshal(payload, &t); err != nil {
		return nil, fmt.Errorf("%w: malformed page token", ErrInvalidArgument)
	}
	if t.Version != pageTokenVersion || t.ID == uuid.Nil || t.CreatedAt.IsZero() {
		return nil, fmt.Errorf("%w: malformed page token", ErrInvalidArgument)
	}
	if t.OwnerID != ownerID {
		return nil, fmt.Errorf("%w: page token belongs to another owner", ErrInvalidArgument)
	}

	return &repo.MediaCursor{CreatedAt: t.CreatedAt, ID: t.ID}, nil
}
