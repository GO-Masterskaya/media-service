package mediaservice

import (
	"encoding/json"
	"fmt"

	"mediaservice/internal/media"
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

// toPublicMedia преобразует внутреннюю модель media.MediaItem в публичную Media.
//
// На вход принимается item целиком, а не одна repo.Media: производные лежат
// отдельной таблицей и приезжают из ядра рядом с записью. Собирать их
// в разных местах значило бы дать двум вызывающим шанс собрать разный Media.
//
// Наружу отдаётся не всё. StorageKey остаётся внутренней деталью раскладки
// хранилища. IdempotencyKey, BodyFingerprint и ParamsFingerprint влияют
// только на запись и сам объект не описывают. ExpiresAt не отдаётся,
// потому что его нет в message Media: два публичных API одного сервиса
// не должны расходиться по составу полей.
//
// Ошибку возвращает только на невалидных данных из ядра: nil-запись,
// битый JSON в metadata, отрицательный size_bytes.
func toPublicMedia(item *media.MediaItem) (*Media, error) {
	if item == nil {
		return nil, fmt.Errorf("%w: nil media item from core", ErrInternal)
	}
	m := item.Media

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

	derivatives, err := toPublicDerivatives(item.Derivatives)
	if err != nil {
		return nil, err
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
		Derivatives: derivatives,
		Error:       m.Error,
		CreatedAt:   m.CreatedAt,
		Filename:    m.OrigFilename,
	}, nil
}

// toPublicDerivatives переводит производные файлы во внешние типы.
//
// Всегда возвращает непустой срез (возможно, нулевой длины), а не nil:
// вызывающий, сериализующий Media в JSON, получит [] в обоих случаях,
// и отсутствие производных не будет выглядеть по-разному в зависимости
// от того, пришла запись из GetMedia или из ListByOwner.
//
// Variant переносится приведением без проверки по известному набору.
// Это данные, а не вход: значение уже записано конвейером обработки,
// и отвергать его сейчас означало бы сделать чтение невозможным из-за
// варианта, который библиотека просто не знает.
func toPublicDerivatives(in []*repo.Derivative) ([]Derivative, error) {
	out := make([]Derivative, 0, len(in))
	for _, d := range in {
		if d == nil {
			return nil, fmt.Errorf("%w: nil derivative from core", ErrInternal)
		}
		// Та же причина, что и для size_bytes у самой записи: внутри int64,
		// снаружи uint64, и отрицательное значение превратилось бы
		// в гигантское положительное.
		if d.SizeBytes < 0 {
			return nil, fmt.Errorf("%w: negative derivative size_bytes: %d", ErrInternal, d.SizeBytes)
		}
		out = append(out, Derivative{
			Variant:   Variant(d.Variant),
			MIMEType:  d.Mime,
			SizeBytes: uint64(d.SizeBytes),
		})
	}
	return out, nil
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
