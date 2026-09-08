//go:build unix

package upload

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// availableSpace возвращает доступное место на файловой системе, содержащей path.
func availableSpace(path string) (int64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %q: %w", path, err)
	}
	// Bavail — блоки, доступные непривилегированным пользователям.
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}
