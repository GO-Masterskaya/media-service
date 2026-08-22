package events

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PermanentError — ошибка, после которой retry бесполезен (DLQ).
type PermanentError struct{ error }

func (p PermanentError) Unwrap() error { return p.error }

// RetryableError — ошибка, после которой стоит повторить (сетевые, БД-локи).
type RetryableError struct{ error }

func (r RetryableError) Unwrap() error { return r.error }

// ClassifyError определяет, retryable ли ошибка.
func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	// Уже классифицирована
	var pe PermanentError
	if errors.As(err, &pe) {
		return pe
	}
	var re RetryableError
	if errors.As(err, &re) {
		return re
	}

	// gRPC статусы
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied, codes.Unauthenticated:
			return PermanentError{err}
		}
	}

	// По умолчанию — retryable (сеть, таймауты, БД)
	return RetryableError{err}
}

// IsPermanent true, если ошибка неисправима.
func IsPermanent(err error) bool {
	var pe PermanentError
	return errors.As(err, &pe)
}

// IsRetryable true, если ошибка временная.
func IsRetryable(err error) bool {
	var re RetryableError
	return errors.As(err, &re)
}
