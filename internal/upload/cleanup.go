package upload

// cleanup.go реализует удаление stale temp-файлов при запуске сервиса.
// Stale — файлы, оставшиеся после предыдущего падения/рестарта,
// определяемые по mtime старше grace period.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// cleanupStale удаляет stale temp-файлы из директории.
// Stale = файл с нашим префиксом/суффиксом, чей mtime старше staleGrace,
// и которого нет в activeFiles.
//
// Возвращает количество удалённых файлов и первую встреченную ошибку.
func (s *TempStore) cleanupStale() (int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if s.metrics != nil {
			s.metrics.CleanupTotal.WithLabelValues("error").Inc()
		}
		return 0, fmt.Errorf("upload cleanup: read dir %q: %w", s.dir, err)
	}

	now := time.Now()
	removed := 0
	var firstErr error

	s.mu.Lock()
	activeSnapshot := make(map[string]struct{}, len(s.activeFiles))
	for name := range s.activeFiles {
		activeSnapshot[name] = struct{}{}
	}
	s.mu.Unlock()

	for _, entry := range entries {
		name := entry.Name()

		// Пропускаем не-наши файлы.
		if !strings.HasPrefix(name, filePrefix) || !strings.HasSuffix(name, fileSuffix) {
			continue
		}

		// Пропускаем активные файлы.
		if _, active := activeSnapshot[name]; active {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("upload cleanup: stat %q: %w", name, err)
			}
			continue
		}

		// Пропускаем свежие файлы (могут принадлежать другому процессу, который ещё жив).
		age := now.Sub(info.ModTime())
		if age < s.staleGrace {
			continue
		}

		path := filepath.Join(s.dir, name)
		if err := os.Remove(path); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("upload cleanup: remove %q: %w", name, err)
			}
			s.logger.Warn("failed to remove stale temp file",
				"path", path, "age", age.Round(time.Second), "error", err)
			continue
		}

		removed++
		s.logger.Info("removed stale temp file",
			"path", path, "age", age.Round(time.Second))
	}

	if s.metrics != nil {
		if firstErr != nil {
			s.metrics.CleanupTotal.WithLabelValues("error").Inc()
		} else {
			s.metrics.CleanupTotal.WithLabelValues("ok").Inc()
		}
		s.metrics.CleanupFilesTotal.Add(float64(removed))
	}

	return removed, firstErr
}
