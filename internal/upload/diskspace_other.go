//go:build !unix

package upload

import "fmt"

// availableSpace на не-Unix платформах не поддерживается.
// Возвращает ошибку — TempStore.Create пропустит проверку и попробует создать файл,
// при нехватке места получит ENOSPC от ОС.
func availableSpace(path string) (int64, error) {
	return 0, fmt.Errorf("disk space check not supported on this platform")
}
