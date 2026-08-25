package events

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PermanentError — ошибка, после которой retry бесполезен.
// Событие уходит в DLQ (или reject) сразу.
type PermanentError struct{ error }

func (p PermanentError) Unwrap() error { return p.error }

// RetryableError — ошибка, после которой стоит повторить
// (сетевые сбои, временная недоступность БД/Kafka).
type RetryableError struct{ error }

func (r RetryableError) Unwrap() error { return r.error }

// ClassifyError определяет, retryable ли ошибка.
//
// Приоритет:
//  1. Уже обёрнутая PermanentError / RetryableError — возвращаем как есть.
//  2. context.Canceled — permanent (retry не отменит cancellation).
//  3. gRPC статус — мапим по коду.
//  4. Всё остальное — retryable (консервативно).
func ClassifyError(err error) error {
	if err == nil {
		return nil
	}

	// Уже классифицирована — не переклассифицируем.
	var pe PermanentError
	if errors.As(err, &pe) {
		return pe
	}
	var re RetryableError
	if errors.As(err, &re) {
		return re
	}

	// Контекст отменён навсегда — retry не отменит cancellation.
	if errors.Is(err, context.Canceled) {
		return PermanentError{err}
	}

	// gRPC статус — мапим по коду.
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		// Permanent: бизнес-ошибки и нарушения контракта.
		// Retry бесполезен — состояние не изменится.
		case codes.InvalidArgument,
			codes.NotFound,
			codes.AlreadyExists,
			codes.PermissionDenied,
			codes.Unauthenticated,
			codes.FailedPrecondition,
			codes.OutOfRange,
			codes.Unimplemented:
			return PermanentError{err}

		// Retryable: инфраструктурные проблемы.
		// Может восстановиться при повторе.
		case codes.DeadlineExceeded,
			codes.ResourceExhausted,
			codes.Aborted,
			codes.Unavailable,
			codes.Internal,
			codes.Unknown,
			codes.DataLoss:
			return RetryableError{err}
		}
	}

	// По умолчанию — retryable (консервативно).
	// Неизвестная ошибка считается временной.
	return RetryableError{err}
}

func IsPermanent(err error) bool {
	var pe PermanentError
	return errors.As(err, &pe)
}

func IsRetryable(err error) bool {
	var re RetryableError
	return errors.As(err, &re)
}
