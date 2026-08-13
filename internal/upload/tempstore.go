package upload

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const (
	// filePrefix — префикс temp-файлов; позволяет отличить наши файлы от чужих.
	filePrefix = "media-upload-"
	// fileSuffix — расширение temp-файлов.
	fileSuffix = ".tmp"
)

// Config содержит параметры TempStore.
type Config struct {
	// Dir — директория для temp-файлов.
	Dir string

	// MaxFileSize — hard limit на размер одного файла (байт).
	// Обычно берётся из config.MaxUploadBytes.
	MaxFileSize int64

	// ReserveBytes — минимум свободного места на диске.
	// Если свободного места меньше — новые upload'ы отклоняются.
	ReserveBytes int64

	// StaleGrace — файлы старше этого считаются stale при startup cleanup.
	StaleGrace time.Duration
}

// TempStore управляет жизненным циклом временных файлов при upload.
//
// Гарантии:
//   - Каждый файл получает уникальное имя (UUID), исключая коллизии.
//   - Файлы создаются с правами 0600 (только владелец).
//   - При создании проверяется свободное место на диске.
//   - Startup cleanup удаляет stale файлы, не трогая активные.
//   - Все операции потокобезопасны.
type TempStore struct {
	dir          string
	maxFileSize  int64
	reserveBytes int64
	staleGrace   time.Duration

	mu          sync.Mutex
	activeFiles map[string]int64 // filename → size in bytes

	metrics *Metrics
	logger  *slog.Logger
}

// New создаёт TempStore, создаёт директорию (если нет) и запускает startup cleanup.
func New(cfg Config, metrics *Metrics, logger *slog.Logger) (*TempStore, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("upload: temp dir is required")
	}
	if cfg.MaxFileSize <= 0 {
		return nil, fmt.Errorf("upload: max file size must be > 0")
	}
	if cfg.StaleGrace <= 0 {
		cfg.StaleGrace = time.Hour
	}
	if logger == nil {
		logger = slog.Default()
	}

	// Создаём директорию с правами 0700 (только владелец rwx).
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, fmt.Errorf("upload: create temp dir %q: %w", cfg.Dir, err)
	}

	s := &TempStore{
		dir:          cfg.Dir,
		maxFileSize:  cfg.MaxFileSize,
		reserveBytes: cfg.ReserveBytes,
		staleGrace:   cfg.StaleGrace,
		activeFiles:  make(map[string]int64),
		metrics:      metrics,
		logger:       logger,
	}

	// Startup cleanup.
	removed, err := s.cleanupStale()
	if err != nil {
		logger.Warn("startup cleanup completed with errors",
			"removed", removed, "error", err)
	} else if removed > 0 {
		logger.Info("startup cleanup completed",
			"removed_files", removed)
	}

	return s, nil
}

// TempFile — временный файл с отслеживанием записи.
// Вызывающий обязан вызвать defer tf.Remove() для гарантированного cleanup.
// Remove() сам вызовет Close() если файл ещё открыт.
//
// TempFile встраивает *os.File, но запись данных должна идти только
// через tf.Writer().Write() — он контролирует size limit.
// Прямой вызов tf.File.Write() обойдёт лимит.
type TempFile struct {
	*os.File                // нижележащий файл на диске
	store    *TempStore     // ссылка на хранилище для cleanup и метрик
	name     string         // basename файла (media-upload-{uuid}.tmp)
	writer   *LimitedWriter // обёртка записи с контролем лимита
	closed   bool           // файл закрыт; повторный Close() — noop
	removed  bool           // файл удалён; повторный Remove() — noop
}

// Writer возвращает LimitedWriter для записи данных в файл.
// Автоматически контролирует size limit.
func (tf *TempFile) Writer() *LimitedWriter {
	return tf.writer
}

// Written возвращает количество записанных байт.
func (tf *TempFile) Written() int64 {
	return tf.writer.Written()
}

// WriteChunk записывает данные в файл через LimitedWriter.
// При ErrSizeLimitExceeded и ErrDiskFull автоматически обновляет метрики.
// Рекомендуется использовать вместо прямого tf.Writer().Write().
func (tf *TempFile) WriteChunk(p []byte) (int, error) {
	n, err := tf.writer.Write(p)

	if err != nil && tf.store.metrics != nil {
		switch {
		case errors.Is(err, ErrSizeLimitExceeded):
			tf.store.metrics.SizeLimitExceededTotal.Inc()
		case errors.Is(err, ErrDiskFull):
			tf.store.metrics.DiskFullTotal.Inc()
		}
	}

	return n, err
}

// Close закрывает файл.
func (tf *TempFile) Close() error {
	if tf.closed {
		return nil
	}
	tf.closed = true

	// Обновляем размер в activeFiles.
	tf.store.mu.Lock()
	tf.store.activeFiles[tf.name] = tf.writer.Written()
	tf.store.mu.Unlock()

	return tf.File.Close()
}

// Remove удаляет файл с диска и снимает с учёта.
// Безопасно вызывать несколько раз.
func (tf *TempFile) Remove() {
	if tf.removed {
		return
	}
	tf.removed = true

	// Закрываем, если ещё не закрыт.
	if !tf.closed {
		_ = tf.Close()
	}

	path := filepath.Join(tf.store.dir, tf.name)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		tf.store.logger.Error("failed to remove temp file",
			"path", path, "error", err)
	}

	written := tf.writer.Written()
	tf.store.mu.Lock()
	delete(tf.store.activeFiles, tf.name)
	tf.store.mu.Unlock()

	if tf.store.metrics != nil {
		tf.store.metrics.TempFilesActive.Dec()
		tf.store.metrics.TempBytesActive.Sub(float64(written))
	}
}

// Create создаёт новый temp файл. Перед созданием проверяет свободное место на диске.
//
// Вызывающий ОБЯЗАН вызвать defer tf.Remove() для гарантированного cleanup.
// Запись данных — через tf.Writer().Write(data).
func (s *TempStore) Create(ctx context.Context) (*TempFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("upload: context cancelled: %w", err)
	}

	// Проверяем свободное место на диске.
	if s.reserveBytes > 0 {
		avail, err := availableSpace(s.dir)
		if err != nil {
			s.logger.Warn("failed to check disk space", "error", err)
			// Не блокируем upload, если не смогли проверить — лучше попробовать и получить ENOSPC.
		} else if avail < s.reserveBytes {
			if s.metrics != nil {
				s.metrics.DiskFullTotal.Inc()
			}
			return nil, fmt.Errorf("upload: insufficient disk space: %d bytes available, %d required: %w",
				avail, s.reserveBytes, ErrDiskFull)
		}
	}

	// Генерируем уникальное имя.
	name := filePrefix + uuid.New().String() + fileSuffix
	path := filepath.Join(s.dir, name)

	// O_CREATE|O_EXCL — атомарное создание: если файл уже существует, вернётся ошибка.
	// O_WRONLY — файл только для записи. Права 0600 — доступ только владельцу.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if isENOSPC(err) {
			if s.metrics != nil {
				s.metrics.DiskFullTotal.Inc()
			}
			return nil, fmt.Errorf("upload: create temp file: %w", ErrDiskFull)
		}
		return nil, fmt.Errorf("upload: create temp file: %w", err)
	}

	// Регистрируем.
	s.mu.Lock()
	s.activeFiles[name] = 0
	s.mu.Unlock()

	if s.metrics != nil {
		s.metrics.TempFilesActive.Inc()
	}

	lw := NewLimitedWriter(f, s.maxFileSize)

	tf := &TempFile{
		File:   f,
		store:  s,
		name:   name,
		writer: lw,
	}

	return tf, nil
}

// ActiveFiles возвращает количество активных temp файлов.
func (s *TempStore) ActiveFiles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.activeFiles)
}

// Dir возвращает директорию temp-файлов.
func (s *TempStore) Dir() string {
	return s.dir
}

// availableSpace возвращает доступное место на файловой системе, содержащей path.
func availableSpace(path string) (int64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %q: %w", path, err)
	}
	// Bavail — блоки, доступные непривилегированным пользователям.
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}
