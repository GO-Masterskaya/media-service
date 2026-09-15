package mediaservice

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/google/uuid"

	"mediaservice/internal/media"
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
// Требует ffprobe в PATH: тип и метаданные файла определяются анализом
// содержимого, а не заявленным MIME. Если ffprobe не найден, метод вернёт
// ErrInternal, не начав читать reader.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrAlreadyExists, ErrQuotaExceeded,
// ErrStorageFull, ErrInternal.
func (c *Client) Upload(ctx context.Context, params UploadParams, reader io.Reader) (UploadResult, error) {
	release, err := c.acquire()
	if err != nil {
		return UploadResult{}, err
	}
	defer release()

	if params.OwnerID == uuid.Nil {
		return UploadResult{}, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if params.IdempotencyKey == "" {
		return UploadResult{}, fmt.Errorf("%w: idempotency_key is required", ErrInvalidArgument)
	}
	if reader == nil {
		return UploadResult{}, fmt.Errorf("%w: reader is required", ErrInvalidArgument)
	}
	if params.TTL < 0 {
		return UploadResult{}, fmt.Errorf("%w: ttl must not be negative", ErrInvalidArgument)
	}

	// Ядро запускает ffprobe на каждом файле, а неудачу запуска трактует как
	// повреждённое содержимое. Без этой проверки отсутствие ffmpeg выглядело
	// бы как ErrInvalidArgument на исправном файле, и чинили бы не то.
	if _, err := exec.LookPath("ffprobe"); err != nil {
		return UploadResult{}, fmt.Errorf(
			"%w: ffprobe not found in PATH, Upload requires ffmpeg", ErrInternal)
	}

	// Наружу срок жизни задаётся длительностью, внутрь уходит момент времени.
	// Перевод делается здесь, потому что точка отсчёта - момент вызова,
	// и вычислять её глубже значило бы считать от неизвестного момента.
	var expiresAt *time.Time
	if params.TTL > 0 {
		t := time.Now().Add(params.TTL)
		expiresAt = &t
	}

	in := media.UploadRequestParams{
		OwnerID: params.OwnerID,
		// Библиотека всегда работает от имени владельца: анонимного режима,
		// который есть у gRPC, здесь нет.
		CallerID:       params.OwnerID,
		Filename:       params.Filename,
		MIME:           params.MIMEType,
		ExpectedSize:   params.ExpectedSize,
		IdempotencyKey: params.IdempotencyKey,
		// Флаги гасятся, если движок обработки не поднят. Пропустить их
		// дальше нельзя: ядро создало бы задачи и перевело объект
		// в Processing, а выполнять их было бы некому. Такой объект
		// остаётся в этом статусе навсегда и вдобавок перестаёт удаляться
		// через Delete.
		MakeThumbnail: params.Processing.MakeThumbnail && c.options.withProcessing,
		Transcode:     params.Processing.Transcode && c.options.withProcessing,
		ExpiresAt:     expiresAt,
	}

	res, err := c.core.Upload(ctx, in, readerChunks(reader))
	if err != nil {
		return UploadResult{}, mapCoreError(err)
	}

	return UploadResult{
		ID:     res.MediaID,
		Status: Status(res.Status),
	}, nil
}

// GetMedia возвращает метаданные медиаобъекта.
//
// Доступ имеет только владелец: для чужого объекта вернётся ErrAccessDenied.
// В отличие от gRPC-контракта, где проверку владельца можно отключить
// настройкой, библиотека применяет её всегда - встраивающее приложение
// знает, от чьего имени работает, и анонимный режим ему не нужен.
//
// Поле Derivatives содержит созданные производные файлы. Пустой список
// означает, что их действительно нет: клиент создан без WithProcessing,
// обработка не запрашивалась, ещё не закончилась или закончилась ошибкой.
// Причину в последнем случае показывает Status вместе с Error.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrNotFound, ErrAccessDenied, ErrInternal.
func (c *Client) GetMedia(ctx context.Context, ownerID, mediaID uuid.UUID) (*Media, error) {
	release, err := c.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	if ownerID == uuid.Nil {
		return nil, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if mediaID == uuid.Nil {
		return nil, fmt.Errorf("%w: media_id is required", ErrInvalidArgument)
	}

	item, err := c.core.GetMediaWithDerivatives(ctx, ownerID, mediaID)
	if err != nil {
		return nil, mapCoreError(err)
	}
	return toPublicMedia(item)
}

// ListByOwner возвращает страницу медиаобъектов владельца params.OwnerID.
//
// Правила пагинации:
//   - Пустой PageToken означает запрос первой страницы.
//   - Пустой NextPageToken в ответе означает, что страниц больше нет.
//   - PageSize == 0 означает DefaultPageSize. Значение больше MaxPageSize
//     даёт ErrInvalidArgument: молча отдать меньше запрошенного значит
//     оставить вызывающего в уверенности, что он увидел всё.
//
// Записи идут от новых к старым, по паре (CreatedAt, ID). Пара, а не одно
// время: две записи могут быть созданы в одну микросекунду, и по времени
// граница страницы получилась бы неоднозначной.
//
// Токен непрозрачен и привязан к владельцу: разбирать его снаружи не нужно,
// а попытка продолжить им выборку другого владельца даёт ErrInvalidArgument.
// С токеном gRPC-контракта он не взаимозаменяем.
//
// Курсорная пагинация устойчива к изменениям между запросами: запись,
// добавленная во время обхода, не сдвинет границу и не приведёт к пропуску
// или повтору соседей, как это происходит с OFFSET.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrInternal.
func (c *Client) ListByOwner(ctx context.Context, params ListParams) (ListResult, error) {
	release, err := c.acquire()
	if err != nil {
		return ListResult{}, err
	}
	defer release()

	if params.OwnerID == uuid.Nil {
		return ListResult{}, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	if params.PageSize > MaxPageSize {
		return ListResult{}, fmt.Errorf("%w: page_size must be <= %d", ErrInvalidArgument, MaxPageSize)
	}

	cursor, err := decodePageToken(params.PageToken, params.OwnerID)
	if err != nil {
		return ListResult{}, err
	}

	// Оба идентификатора одинаковые: библиотека всегда листает от имени
	// владельца, чужие ленты ей запрашивать не для кого.
	page, err := c.core.ListMediaByOwner(ctx, params.OwnerID, params.OwnerID, int(params.PageSize), cursor)
	if err != nil {
		return ListResult{}, mapCoreError(err)
	}

	items := make([]Media, 0, len(page.Items))
	for _, item := range page.Items {
		m, err := toPublicMedia(item)
		if err != nil {
			return ListResult{}, err
		}
		items = append(items, *m)
	}

	// Токен строится из последней отданной записи, а не из первой следующей:
	// следующей у нас нет, ядро сообщает только сам факт её существования.
	var next string
	if page.HasMore && len(page.Items) > 0 {
		last := page.Items[len(page.Items)-1].Media
		next, err = encodePageToken(params.OwnerID, last.CreatedAt, last.ID)
		if err != nil {
			return ListResult{}, fmt.Errorf("%w: %v", ErrInternal, err)
		}
	}

	return ListResult{Items: items, NextPageToken: next}, nil
}

// GetDownloadURL возвращает временную ссылку на скачивание медиаобъекта.
//
// Срок жизни ссылки задаётся при создании клиента опцией WithPresignTTL
// и одинаков для всех вызовов.
//
// Пустой variant означает VariantOriginal. Принимаются VariantOriginal,
// VariantThumb, VariantPreview и VariantR720. VariantR360 конвейером
// не создаётся и даёт ErrInvalidArgument, а не вечный ErrNotFound.
//
// Для оригинала ссылка выдаётся в любом статусе, кроме Failed и Deleting.
// Для производных объект должен быть в статусе Ready, иначе вернётся
// ErrNotReady; если производная не создавалась - ErrNotFound.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrNotFound, ErrAccessDenied,
// ErrNotReady, ErrInternal.
func (c *Client) GetDownloadURL(ctx context.Context, ownerID, mediaID uuid.UUID, variant Variant) (PresignedURL, error) {
	release, err := c.acquire()
	if err != nil {
		return PresignedURL{}, err
	}
	defer release()

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
// Пустой variant означает VariantOriginal. Набор принимаемых вариантов
// тот же, что у GetDownloadURL: VariantR360 даёт ErrInvalidArgument,
// а отсутствующая производная - ErrNotFound.
//
// Поток привязан к клиенту: после Close чтение возвращает ErrClosed.
// Закрыть сам поток всё равно нужно - иначе останется незакрытым
// соединение с хранилищем.
//
// Ошибки: ErrClosed, ErrInvalidArgument, ErrNotFound, ErrAccessDenied,
// ErrNotReady, ErrInternal.
func (c *Client) DownloadStream(ctx context.Context, ownerID, mediaID uuid.UUID, variant Variant) (io.ReadCloser, error) {
	release, err := c.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

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
	return &clientBoundStream{rc: rc, client: c}, nil
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
	release, err := c.acquire()
	if err != nil {
		return err
	}
	defer release()

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
// помеченной на удаление.
//
// Дочистить её во встроенном режиме некому: реконсилятор, который подбирает
// зависшие записи, поднимается только запущенным сервисом. Повторный вызов
// метода их тоже не заберёт - выборка отсекает записи в состоянии удаления.
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
	release, err := c.acquire()
	if err != nil {
		return 0, err
	}
	defer release()

	if ownerID == uuid.Nil {
		return 0, fmt.Errorf("%w: owner_id is required", ErrInvalidArgument)
	}
	// Ноль означает "размер батча выбирает ядро". Это настройка
	// производительности, а не часть публичного контракта: обосновать
	// выбор числа вызывающему нечем.
	deleted, err := c.core.DeleteByOwner(ctx, ownerID, 0)
	if err != nil {
		return deleted, mapCoreError(err)
	}
	return deleted, nil
}
