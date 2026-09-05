package mediaservice

import (
	"encoding/json"
	"fmt"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// toInternal преобразует публичный Variant во внутренний storage.Variant,
// отвергая неизвестные значения.
//
// Проверка нужна потому, что Variant это строковый тип, и Go позволяет
// создать любое значение через приведение: Variant("thumbnale") скомпилируется.
// Без проверки такое значение ушло бы в построение пути объекта, и вызывающий
// получил бы «объект не найден» вместо понятной ошибки.
//
// Пустая строка отображается в VariantOriginal. Подстановка живёт здесь,
// потому что toInternal - единственная точка конверсии в библиотеке: правило
// описано один раз и не может разойтись между GetDownloadURL и DownloadStream.
// Такое же поведение у gRPC-хендлера для пустого поля variant, то есть два
// публичных API сервиса отвечают на пустой вход одинаково.
// Возвращается явная константа storage.VariantOriginal, а не storage.Variant(v):
// приведение отдало бы во внутренний слой пустую строку.
//
// ВАЖНО: набор принимаемых значений связан со storage.Variant вручную,
// автоматической связи между типами нет - при добавлении варианта нужно
// править оба места. Пустая строка в этот набор не входит: она не значение
// storage.Variant, а вход, который в него отображается.
func (v Variant) toInternal() (storage.Variant, error) {
	switch v {
	case VariantOriginal, VariantThumb, VariantPreview, VariantR720, VariantR360:
		return storage.Variant(v), nil
	case "":
		return storage.VariantOriginal, nil
	default:
		return "", fmt.Errorf("unknown variant %q: %w", v, ErrInvalidArgument)
	}
}

// toPublicMedia преобразует внутреннюю модель repo.Media в публичную Media.
//
// Наружу отдаётся не всё. StorageKey остаётся внутренней деталью раскладки
// хранилища. IdempotencyKey, BodyFingerprint и ParamsFingerprint влияют
// только на запись и сам объект не описывают. OrigFilename и ExpiresAt
// не отдаются, потому что их нет в message Media: два публичных API одного
// сервиса не должны расходиться по составу полей.
//
// Derivatives остаются пустыми: repo.Media их не содержит, производные лежат
// отдельной таблицей, а media.Service.GetMedia их пока не подтягивает.
//
// Ошибку возвращает только на невалидных данных из ядра: nil-запись,
// битый JSON в metadata, отрицательный size_bytes.
func toPublicMedia(m *repo.Media) (*Media, error) {
	// Отсутствие записи ядро уже превращает в NotFound, поэтому nil здесь
	// означает баг внутри. Выдавать его за ErrNotFound нельзя: пользователь
	// получит правдоподобный ответ и никто не заметит поломку.
	if m == nil {
		return nil, fmt.Errorf("%w: nil media from core", ErrInternal)
	}

	// Metadata во внутренней модели - сырой JSON от ffprobe. Разбираем в map,
	// а не в структуру, потому что набор ключей зависит от типа файла.
	//
	// repo.scanMedia подставляет "{}" вместо пустого поля, так что из БД
	// пустой срез не придёт. Проверка страхует repo.Media, собранные мимо
	// scanMedia - моки в тестах, нулевое значение структуры: на пустом срезе
	// json.Unmarshal вернул бы "unexpected end of JSON input".
	var metadata map[string]any
	if len(m.Metadata) > 0 {
		if err := json.Unmarshal(m.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("%w: parse media metadata: %v", ErrInternal, err)
		}
	}

	// Внутри int64 (bigint в схеме), снаружи uint64 по контракту.
	// Отрицательных размеров быть не должно, но на уровне БД это ничем
	// не закреплено, а конверсия молча превратила бы -1 в 1.8e19.
	if m.SizeBytes < 0 {
		return nil, fmt.Errorf("%w: negative size_bytes: %d", ErrInternal, m.SizeBytes)
	}

	// Значения MediaKind и MediaStatus совпадают с публичными дословно,
	// поэтому достаточно приведения типа. Порядок полей повторяет
	// объявление Media - так видно, что ничего не пропущено.
	return &Media{
		ID:          m.ID,
		OwnerID:     m.OwnerID,
		Kind:        Kind(m.Kind),
		MIMEType:    m.Mime,
		SizeBytes:   uint64(m.SizeBytes),
		Status:      Status(m.Status),
		Metadata:    metadata,
		Derivatives: nil,
		Error:       m.Error,
		CreatedAt:   m.CreatedAt,
	}, nil
}

// toPublicPresignedURL переводит внутреннюю ссылку в публичную.
//
// Отдельная функция, а не сборка структуры по месту: маппинг внутренних
// типов в публичные собран в одном файле, и ядро при желании может
// добавить в свой тип поля, не задев публичный контракт.
func toPublicPresignedURL(p *storage.PresignedURL) (PresignedURL, error) {
	if p == nil {
		return PresignedURL{}, fmt.Errorf("%w: nil presigned url from core", ErrInternal)
	}
	return PresignedURL{
		URL:       p.URL,
		ExpiresAt: p.ExpiresAt,
	}, nil
}
