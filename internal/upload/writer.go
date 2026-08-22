// Package upload управляет временным хранилищем файлов при загрузке.
// Обеспечивает безопасное создание temp-файлов, лимитирование размера,
// обработку ENOSPC и cleanup при рестарте сервиса.
package upload

import (
	"errors"
	"io"
)

// Ошибки, возвращаемые LimitedWriter.
var (
	// ErrSizeLimitExceeded — записанный объём превысил hard limit.
	ErrSizeLimitExceeded = errors.New("upload: size limit exceeded")

	// ErrDiskFull — файловая система вернула ENOSPC.
	ErrDiskFull = errors.New("upload: disk full (ENOSPC)")
)

// LimitedWriter оборачивает io.Writer, считает записанные байты и
// прекращает запись при превышении maxBytes.
// При получении ENOSPC от нижележащего writer — оборачивает в ErrDiskFull.
type LimitedWriter struct {
	w       io.Writer
	max     int64
	written int64
}

// NewLimitedWriter создаёт LimitedWriter с заданным лимитом.
// maxBytes <= 0 означает отсутствие лимита.
func NewLimitedWriter(w io.Writer, maxBytes int64) *LimitedWriter {
	return &LimitedWriter{
		w:   w,
		max: maxBytes,
	}
}

// Write записывает p в нижележащий writer.
// Если суммарный объём записи превысит max — возвращает ErrSizeLimitExceeded
// без частичной записи (весь чанк отклоняется целиком, даже если часть
// поместилась бы — это сделано намеренно, чтобы упростить обработку ошибок
// и избежать partial writes, которые сложнее откатить).
// Если нижележащий writer вернёт ENOSPC — оборачивает в ErrDiskFull.
func (lw *LimitedWriter) Write(p []byte) (int, error) {
	if lw.max > 0 && lw.written+int64(len(p)) > lw.max {
		return 0, ErrSizeLimitExceeded
	}

	n, err := lw.w.Write(p)
	lw.written += int64(n)

	if err != nil && isENOSPC(err) {
		return n, ErrDiskFull
	}

	return n, err
}

// Written возвращает количество успешно записанных байт.
func (lw *LimitedWriter) Written() int64 {
	return lw.written
}
