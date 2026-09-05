package mediaservice

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
)

// Upload сохраняет медиафайл в хранилище и привязывает его к владельцу
// из params.
//
// Байты читаются из reader потоком, файл не поднимается в память целиком.
// Reader вычитывается один раз и до конца, повторно использовать его нельзя;
// закрыть его, если он закрываемый, обязан вызывающий.
//
// Флаги в params.Processing выполняются только если клиент создан с опцией
// WithProcessing. Без неё задачи обработки не создаются, объект остаётся
// в StatusStored, и это видно по полю Status в результате - опрашивать
// GetMedia в ожидании StatusReady бессмысленно.
//
// params.IdempotencyKey делает повтор безопасным: вызов с тем же ключом
// и тем же содержимым вернёт существующий объект вместо создания нового.
// Тот же ключ с другим файлом или другими параметрами - нарушение контракта
// идемпотентности, оно даёт ErrAlreadyExists.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrAlreadyExists, ErrInternal,
// ErrNotImplemented.
func (c *Client) Upload(ctx context.Context, params UploadParams, reader io.Reader) (UploadResult, error) {
	if err := c.checkOpen(); err != nil {
		return UploadResult{}, err
	}
	return UploadResult{}, ErrNotImplemented // TODO жду мерж #9
}

// GetMedia возвращает метаданные медиаобъекта.
//
// Доступ имеет только владелец: для чужого объекта вернётся ErrAccessDenied.
// В отличие от gRPC-контракта, где проверку владельца можно отключить
// настройкой, библиотека применяет её всегда - встраивающее приложение
// знает, от чьего имени работает, и анонимный режим ему не нужен.
//
// Поле Derivatives всегда пустое: производные лежат в отдельной таблице,
// и ядро не отдаёт их этим методом. Пустое значение не означает, что
// производных нет - доступность конкретной производной проверяется
// вызовом GetDownloadURL или DownloadStream.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrNotFound, ErrAccessDenied, ErrInternal.
func (c *Client) GetMedia(ctx context.Context, ownerID, mediaID uuid.UUID) (*Media, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	if ownerID == uuid.Nil {
		return nil, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if mediaID == uuid.Nil {
		return nil, fmt.Errorf("%w: media_id is required", ErrInvalidArgument)
	}

	m, err := c.core.GetMedia(ctx, ownerID, mediaID)
	if err != nil {
		return nil, mapCoreError(err)
	}
	return toPublicMedia(m)
}

// ListByOwner возвращает страницу медиаобъектов владельца params.OwnerID.
//
// Правила пагинации:
//   - Пустой PageToken означает запрос первой страницы.
//   - Пустой NextPageToken в ответе означает, что страниц больше нет.
//   - PageSize == 0 означает, что размер страницы выбирает сервер.
//     Значения выше допустимого потолка срезаются до него. Конкретные числа
//     появятся вместе с реализацией и будут указаны здесь.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrInternal, ErrNotImplemented.
func (c *Client) ListByOwner(ctx context.Context, params ListParams) (ListResult, error) {
	if err := c.checkOpen(); err != nil {
		return ListResult{}, err
	}
	return ListResult{}, ErrNotImplemented // TODO жду мерж #10
}

// GetDownloadURL возвращает временную ссылку на скачивание медиаобъекта.
//
// Срок жизни ссылки задаётся при создании клиента опцией WithPresignTTL
// и одинаков для всех вызовов.
//
// Пустой variant означает VariantOriginal. В текущей версии сервис отдаёт
// ссылки только на VariantOriginal, VariantThumb и VariantR720;
// VariantPreview и VariantR360 зарезервированы и вернут ErrInvalidArgument.
//
// Для оригинала ссылка выдаётся в любом статусе, кроме Failed и Deleting.
// Для производных объект должен быть в статусе Ready, иначе вернётся
// ErrNotReady; если производная не создавалась - ErrNotFound.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrNotFound, ErrAccessDenied,
// ErrNotReady, ErrInternal.
func (c *Client) GetDownloadURL(ctx context.Context, ownerID, mediaID uuid.UUID, variant Variant) (PresignedURL, error) {
	if err := c.checkOpen(); err != nil {
		return PresignedURL{}, err
	}

	// Проверять обязательно: ядро пропускает сверку владельца, если callerID
	// нулевой, поэтому нулевое значение открыло бы доступ к чужим объектам.
	if ownerID == uuid.Nil {
		return PresignedURL{}, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if mediaID == uuid.Nil {
		return PresignedURL{}, fmt.Errorf("%w: media_id is required", ErrInvalidArgument)
	}

	v, err := variant.toInternal()
	if err != nil {
		return PresignedURL{}, err
	}

	presigned, err := c.core.GetDownloadURL(ctx, ownerID, mediaID, v)
	if err != nil {
		return PresignedURL{}, mapCoreError(err)
	}
	return toPublicPresignedURL(presigned)
}

// DownloadStream открывает содержимое медиаобъекта для чтения.
//
// Закрыть возвращённый поток обязан вызывающий. Проверки - существование
// объекта, права владельца, доступность варианта - выполняются до возврата,
// поэтому поток без ошибки означает, что доступ подтверждён, а сбой при
// чтении относится к сети или хранилищу.
//
// Пустой variant означает VariantOriginal. В отличие от GetDownloadURL, этот
// метод принимает все объявленные варианты, но для VariantPreview и
// VariantR360 производные в текущей версии не создаются, и вернётся
// ErrNotFound.
//
// Поток читает из хранилища напрямую и переживает Close клиента, однако
// полагаться на это не стоит: закрывайте поток до закрытия клиента.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrNotFound, ErrAccessDenied,
// ErrNotReady, ErrInternal.
func (c *Client) DownloadStream(ctx context.Context, ownerID, mediaID uuid.UUID, variant Variant) (io.ReadCloser, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	// Проверять обязательно: ядро пропускает сверку владельца при нулевом
	// callerID, и нулевое значение открыло бы доступ к чужим объектам.
	if ownerID == uuid.Nil {
		return nil, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if mediaID == uuid.Nil {
		return nil, fmt.Errorf("%w: media_id is required", ErrInvalidArgument)
	}

	v, err := variant.toInternal()
	if err != nil {
		return nil, err
	}

	// Ядро принимает вариант строкой, а не storage.Variant, - расхождение
	// с GetDownloadURL осталось с ранних версий.
	rc, err := c.core.OpenMedia(ctx, ownerID, mediaID, string(v))
	if err != nil {
		return nil, mapCoreError(err)
	}
	return rc, nil
}

// Delete снимает привязку медиаобъекта к владельцу ownerID.
//
// Если после снятия привязки объектом никто не владеет, файлы в хранилище
// и запись в БД удаляются физически. Если объект используется другими
// владельцами, снимаются только права ownerID, данные остаются на месте,
// и метод возвращает nil. Успешный вызов не означает, что файла больше нет.
//
// Удалить объект в статусе Processing нельзя: обработчик в этот момент
// работает с файлом. Дождитесь перехода в Ready или Failed.
//
// Операция не идемпотентна: повторный вызов вернёт ErrNotFound, потому что
// привязки уже нет. Чужой объект обычно даёт ту же ошибку, поэтому
// ErrAccessDenied этот метод не возвращает. Исключение - чужой объект
// в статусе Processing: запрет на удаление проверяется раньше прав,
// и вызывающий получит ErrNotReady, то есть узнает о существовании объекта.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrNotFound, ErrNotReady, ErrInternal.
func (c *Client) Delete(ctx context.Context, ownerID, mediaID uuid.UUID) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	if ownerID == uuid.Nil {
		return fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if mediaID == uuid.Nil {
		return fmt.Errorf("%w: media_id is required", ErrInvalidArgument)
	}

	if err := c.core.DeleteMedia(ctx, ownerID, mediaID); err != nil {
		return mapCoreError(err)
	}
	return nil
}

// DeleteByOwner безвозвратно удаляет все медиаобъекты, созданные владельцем ownerID.
//
// Семантика отличается от Delete, и разница существенна. Delete снимает
// привязку и щадит объект, которым владеет кто-то ещё. DeleteByOwner удаляет
// безусловно: файлы в хранилище и записи в БД стираются, даже если на объект
// есть привязки других владельцев. Метод предназначен для сценариев вроде
// удаления учётной записи целиком, а не для массовой отвязки.
//
// Возвращает число фактически удалённых записей в этом вызове. Повторный
// вызов для того же владельца вернёт меньше или ноль: удаление идемпотентно
// по эффекту, но не по возвращаемому числу.
//
// Число значимо и при ошибке: часть объектов может быть уже удалена. Сбой на
// отдельном объекте не останавливает остальные - такая запись остаётся
// помеченной на удаление и подхватывается фоновой сверкой.
//
// Отмена контекста прерывает обработку между объектами: возвращается
// накопленное число и ошибка контекста, а не ErrInternal.
//
// В gRPC-контракте одноимённый метод временно отключён: там ownerID приходит
// из непроверенных метаданных, и массовое необратимое удаление по чужому
// идентификатору неприемлемо. В библиотеке этот риск отсутствует - ownerID
// задаёт встраивающее приложение из собственного доверенного контекста.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrInternal, а также context.Canceled
// и context.DeadlineExceeded.
func (c *Client) DeleteByOwner(ctx context.Context, ownerID uuid.UUID) (int, error) {
	if err := c.checkOpen(); err != nil {
		return 0, err
	}

	if ownerID == uuid.Nil {
		return 0, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}

	/*

			// Ноль означает "размер батча выбирает ядро". Это настройка
			// производительности, а не часть публичного контракта, и в сигнатуру
			// метода она не выносится: обосновать выбор числа вызывающему нечем.
			deleted, err := c.core.DeleteByOwner(ctx, ownerID, 0)
			if err != nil {
				return deleted, mapCoreError(err)
			}
			return deleted, nil
		}

	*/

	// Реализация ждёт #13 (PR #54), где объявлен media.Service.DeleteByOwner.
	return 0, ErrNotImplemented
}
