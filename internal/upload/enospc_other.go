//go:build !unix

package upload

// isENOSPC на не-Unix платформах всегда возвращает false.
// ENOSPC — специфичная для Unix ошибка.
func isENOSPC(_ error) bool {
	return false
}
