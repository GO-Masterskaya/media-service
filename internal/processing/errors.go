package processing

import (
	"context"
	"errors"
)

// PermanentError — ошибка handler, после которой retry бесполезен.
type PermanentError struct {
	Err error
}

func (p PermanentError) Error() string { return p.Err.Error() }
func (p PermanentError) Unwrap() error { return p.Err }

// RetryableError — временная ошибка, имеет смысл повторить.
type RetryableError struct {
	Err error
}

func (r RetryableError) Error() string { return r.Err.Error() }
func (r RetryableError) Unwrap() error { return r.Err }

// IsPermanent возвращает true, если ошибка помечена как permanent.
func IsPermanent(err error) bool {
	var pe PermanentError
	return errors.As(err, &pe)
}

// ClassifyHandlerError определяет, retryable ли ошибка handler.
// context.Canceled при shutdown обрабатывается отдельно в engine.
func ClassifyHandlerError(err error) error {
	if err == nil {
		return nil
	}
	var pe PermanentError
	if errors.As(err, &pe) {
		return pe
	}
	var re RetryableError
	if errors.As(err, &re) {
		return re
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RetryableError{Err: err}
	}
	// Неизвестные ошибки считаем retryable (консервативно).
	return RetryableError{Err: err}
}
