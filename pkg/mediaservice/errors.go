package mediaservice

import (
	"context"
	"errors"
	"fmt"
	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/upload"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Публичные ошибки библиотеки. Пригодны для errors.Is и errors.As.
// Внутренние типы (repo, media, gRPC status) наружу не протекают -
// ядро возвращает свои ошибки, библиотека маппит их в эти значения.

// ErrNotFound возвращается, когда медиа или запрошенный вариант не найдены.
var ErrNotFound = errors.New("mediaservice: not found")

// ErrInvalidArgument возвращается при некорректных входных данных:
// пустой UUID, неизвестный variant, некорректный размер страницы.
var ErrInvalidArgument = errors.New("mediaservice: invalid argument")

// ErrNotReady возвращается, когда операция невозможна в текущем статусе объекта.
var ErrNotReady = errors.New("mediaservice: media not ready")

// ErrAccessDenied возвращается, когда caller не является владельцем объекта.
var ErrAccessDenied = errors.New("mediaservice: access denied")

// ErrClosed возвращается, когда метод вызван после Close().
var ErrClosed = errors.New("mediaservice: client is closed")

// ErrInternal возвращается при внутренней ошибке сервиса: сбой БД,
// хранилища или нарушение инварианта в ядре. Детали в тексте ошибки,
// но полагаться на них не стоит - они не часть контракта.
var ErrInternal = errors.New("mediaservice: internal error")

// ErrAlreadyExists возвращается, когда ключ идемпотентности переиспользован
// с другим содержимым или другими параметрами загрузки. Повтор с тем же
// файлом ошибки не даёт - вернётся существующий объект.
var ErrAlreadyExists = errors.New("mediaservice: already exists")

// ErrQuotaExceeded возвращается, когда загрузка не помещается в квоту
// владельца. Квота берётся из таблицы storage_quotas, а при её отсутствии -
// из значения, заданного опцией WithStorageQuota.
//
// Ошибка вызывающего и в его власти: освободить место удалением или
// поднять квоту. Проверка выполняется дважды - по заявленному размеру
// до приёма байтов и по фактическому после.
var ErrQuotaExceeded = errors.New("mediaservice: storage quota exceeded")

// ErrStorageFull возвращается, когда на диске под временные файлы не осталось
// места. В отличие от ErrQuotaExceeded, это состояние машины, а не владельца,
// и вызывающий не может его исправить - но может повторить попытку позже.
//
// Отделена от ErrInternal намеренно: ErrInternal означает "что-то сломано,
// повтор не поможет", и повесить на него временную нехватку места значило бы
// сказать вызывающему неправду.
var ErrStorageFull = errors.New("mediaservice: temporary storage is full")

// mapCoreError переводит ошибку ядра в публичную ошибку библиотеки.
//
// Ядро отдаёт ошибки тремя способами: сентинелами пакета media (download,
// persist), gRPC-статусами (service.go) и обычными обёртками fmt.Errorf
// (delete). К ним добавляется отмена контекста, которую нельзя смешивать
// ни с одним из трёх, - отсюда четыре ветки.
//
// Порядок важен: сентинелы разбираются до статусов, потому что
// status.FromError не распознаёт обёрнутый сентинел и вернёт ok == false,
// отправив осмысленную ошибку в ErrInternal.
//
// Всё неопознанное становится ErrInternal с текстом исходной ошибки.
// Текст добавляется через %v, а не %w: обёртка не должна давать вызывающему
// возможность сравнивать результат с внутренними сентинелами ядра.
func mapCoreError(err error) error {
	if err == nil {
		return nil
	}

	// Отмену и таймаут отдаём как стандартные значения, а не как ошибку ядра:
	// вызывающему важно отличать собственную отмену от аварии сервиса, но
	// внутренняя обёртка ему для этого не нужна и наружу выходить не должна.
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	}

	switch {
	case errors.Is(err, media.ErrNotFound), errors.Is(err, repo.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, media.ErrAccessDenied):
		return ErrAccessDenied
	case errors.Is(err, media.ErrFailedPrecondition):
		return ErrNotReady
	case errors.Is(err, media.ErrInvalidArgument):
		return ErrInvalidArgument
	case errors.Is(err, media.ErrAlreadyExists):
		return ErrAlreadyExists
	case errors.Is(err, media.ErrStorageQuotaExceeded):
		return ErrQuotaExceeded
	case errors.Is(err, upload.ErrDiskFull):
		return ErrStorageFull
	}

	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			return ErrNotFound
		case codes.PermissionDenied:
			return ErrAccessDenied
		case codes.FailedPrecondition:
			return ErrNotReady
		case codes.InvalidArgument:
			return ErrInvalidArgument
		case codes.AlreadyExists:
			return ErrAlreadyExists
		case codes.ResourceExhausted:
			// Ядро этот код сейчас не отдаёт: квота и нехватка места
			// приезжают сентинелами и разбираются выше. Ветка стоит на
			// случай, если какой-то путь ядра начнёт отдавать статус, -
			// иначе ResourceExhausted молча уехал бы в ErrInternal.
			return ErrQuotaExceeded
		}
	}
	return fmt.Errorf("%w: %v", ErrInternal, err)
}
