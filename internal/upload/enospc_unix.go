//go:build unix

package upload

import (
	"errors"
	"syscall"
)

// isENOSPC проверяет, является ли ошибка ENOSPC (нет места на устройстве).
func isENOSPC(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}
