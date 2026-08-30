package mediaservice

import "errors"

// Публичные ошибки библиотеки. Пригодны для errors.Is и errors.As.
// Внутренние типы (repo, media, gRPC status) наружу не протекают -
// ядро возвращает свои ошибки, библиотека маппит их в эти значения.

// ErrNotFound возвращается, когда медиа или запрошенный вариант не найдены.
var ErrNotFound = errors.New("mediaservice: not found")

// ErrInvalidArgument возвращается при некорректных входных данных:
// пустой UUID, неизвестный variant, некорректный размер страницы.
var ErrInvalidArgument = errors.New("mediaservice: invalid argument")

// ErrNotReady возвращается, когда объект существует, но ещё не готов
// к выдаче - например, запрошен thumbnail, а обработка не завершена.
var ErrNotReady = errors.New("mediaservice: media not ready")

// ErrAccessDenied возвращается, когда caller не является владельцем объекта.
var ErrAccessDenied = errors.New("mediaservice: access denied")

// ErrClosed возвращается, когда метод вызван после Close().
var ErrClosed = errors.New("mediaservice: client is closed")

// ErrNotImplemented возвращается, когда метод объявлен в контракте, но
// соответствующий сценарий ядра ещё не реализован. Контракт библиотеки
// зафиксирован заранее (#29), реализация доезжает по мере готовности #9–#13.
var ErrNotImplemented = errors.New("mediaservice: not implemented yet")
