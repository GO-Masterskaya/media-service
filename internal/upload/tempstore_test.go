//go:build unix

package upload

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStore создаёт TempStore для тестов с изолированной директорией и registry.
func newTestStore(t *testing.T, opts ...func(*Config)) *TempStore {
	t.Helper()

	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := Config{
		Dir:             dir,
		MaxFileSize:     1024 * 1024, // 1 MB
		ReserveBytes:    0,           // без проверки места в тестах
		StaleGrace:      time.Minute,
		CleanupInterval: time.Hour, // большой интервал чтобы тикер не срабатывал в обычных тестах
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	store, err := New(cfg, metrics, logger)
	require.NoError(t, err)

	t.Cleanup(func() {
		store.Stop()
	})

	return store
}

// gaugeValue извлекает текущее значение Prometheus Gauge.
func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m io_prometheus.Metric
	require.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}

func TestTempStore_CreateAndRemove(t *testing.T) {
	store := newTestStore(t)

	ctx := context.Background()
	tf, err := store.Create(ctx)
	require.NoError(t, err)
	require.NotNil(t, tf)

	// Файл создан на диске.
	path := filepath.Join(store.Dir(), tf.name)
	_, err = os.Stat(path)
	require.NoError(t, err, "temp file should exist on disk")

	// ActiveFiles = 1.
	assert.Equal(t, 1, store.ActiveFiles())

	// Запись данных.
	data := []byte("hello upload")
	n, err := tf.Writer().Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, int64(len(data)), tf.Written())

	// Удаляем.
	tf.Remove()

	// Файл удалён с диска.
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "temp file should be removed from disk")

	// ActiveFiles = 0.
	assert.Equal(t, 0, store.ActiveFiles())
}

func TestTempStore_SizeLimitExceeded(t *testing.T) {
	store := newTestStore(t, func(cfg *Config) {
		cfg.MaxFileSize = 10 // 10 байт
	})

	ctx := context.Background()
	tf, err := store.Create(ctx)
	require.NoError(t, err)
	defer tf.Remove()

	// Пишем 10 байт — ок.
	_, err = tf.Writer().Write([]byte("0123456789"))
	require.NoError(t, err)

	// Ещё один байт — ошибка.
	_, err = tf.Writer().Write([]byte("x"))
	assert.ErrorIs(t, err, ErrSizeLimitExceeded)
}

func TestTempStore_ContextCancelled(t *testing.T) {
	store := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменяем сразу

	tf, err := store.Create(ctx)
	assert.Error(t, err)
	assert.Nil(t, tf)
}

func TestTempStore_ConcurrentCreation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup
	files := make([]*TempFile, n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tf, err := store.Create(ctx)
			files[idx] = tf
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i], "file %d", i)
		require.NotNil(t, files[i], "file %d", i)
	}

	assert.Equal(t, n, store.ActiveFiles())

	// Удаляем все.
	for i := 0; i < n; i++ {
		files[i].Remove()
	}
	assert.Equal(t, 0, store.ActiveFiles())
}

func TestTempStore_RemoveIdempotent(t *testing.T) {
	store := newTestStore(t)

	ctx := context.Background()
	tf, err := store.Create(ctx)
	require.NoError(t, err)

	// Двойное Remove не паникует.
	tf.Remove()
	tf.Remove()

	assert.Equal(t, 0, store.ActiveFiles())
}

func TestTempStore_CloseIdempotent(t *testing.T) {
	store := newTestStore(t)

	ctx := context.Background()
	tf, err := store.Create(ctx)
	require.NoError(t, err)
	defer tf.Remove()

	// Двойной Close не паникует.
	require.NoError(t, tf.Close())
	require.NoError(t, tf.Close())
}

func TestCleanupStale(t *testing.T) {
	dir := t.TempDir()

	// Создаём "stale" файл (наш префикс/суффикс, mtime в прошлом).
	staleName := filePrefix + "stale-uuid" + fileSuffix
	stalePath := filepath.Join(dir, staleName)
	require.NoError(t, os.WriteFile(stalePath, []byte("stale data"), 0600))
	// Устанавливаем mtime 2 часа назад.
	past := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(stalePath, past, past))

	// Создаём "свежий" файл (наш префикс/суффикс, mtime сейчас).
	freshName := filePrefix + "fresh-uuid" + fileSuffix
	freshPath := filepath.Join(dir, freshName)
	require.NoError(t, os.WriteFile(freshPath, []byte("fresh data"), 0600))

	// Создаём "чужой" файл (другой префикс).
	foreignPath := filepath.Join(dir, "other-file.dat")
	require.NoError(t, os.WriteFile(foreignPath, []byte("foreign"), 0600))
	foreignPast := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(foreignPath, foreignPast, foreignPast))

	// Создаём TempStore — cleanup должен сработать.
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store, err := New(Config{
		Dir:             dir,
		MaxFileSize:     1024,
		ReserveBytes:    0,
		StaleGrace:      time.Hour, // файлы старше 1 часа = stale
		CleanupInterval: time.Hour,
	}, metrics, logger)
	require.NoError(t, err)
	defer store.Stop()

	// stale файл удалён.
	_, err = os.Stat(stalePath)
	assert.True(t, os.IsNotExist(err), "stale file should be removed")

	// fresh файл остался.
	_, err = os.Stat(freshPath)
	assert.NoError(t, err, "fresh file should remain")

	// foreign файл остался.
	_, err = os.Stat(foreignPath)
	assert.NoError(t, err, "foreign file should remain")
}

func TestCleanupStale_SkipsActiveFiles(t *testing.T) {
	dir := t.TempDir()

	// Создаём "активный" файл с mtime в прошлом.
	activeName := filePrefix + "active-uuid" + fileSuffix
	activePath := filepath.Join(dir, activeName)
	require.NoError(t, os.WriteFile(activePath, []byte("active data"), 0600))
	past := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(activePath, past, past))

	// Создаём store, но предварительно зарегистрируем файл как активный.
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store := &TempStore{
		dir:          dir,
		maxFileSize:  1024,
		reserveBytes: 0,
		staleGrace:   time.Hour,
		activeFiles:  map[string]int64{activeName: 0},
		metrics:      metrics,
		logger:       logger,
		stopCleanup:  make(chan struct{}),
		cleanupDone:  make(chan struct{}),
	}
	// Запускаем горутину чтобы Stop() не зависал; interval большой — не тикнет.
	go store.periodicCleanup(time.Hour)
	defer store.Stop()

	removed, err := store.cleanupStale()
	require.NoError(t, err)
	assert.Equal(t, 0, removed)

	// Файл остался.
	_, err = os.Stat(activePath)
	assert.NoError(t, err, "active file should remain")
}

func TestTempStore_PeriodicCleanup(t *testing.T) {
	dir := t.TempDir()

	// Создаём stale файл.
	staleName := filePrefix + "periodic-stale" + fileSuffix
	stalePath := filepath.Join(dir, staleName)
	require.NoError(t, os.WriteFile(stalePath, []byte("data"), 0600))
	past := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(stalePath, past, past))

	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Короткий интервал — тикер сработает быстро.
	store, err := New(Config{
		Dir:             dir,
		MaxFileSize:     1024,
		ReserveBytes:    0,
		StaleGrace:      time.Hour,
		CleanupInterval: 50 * time.Millisecond,
	}, metrics, logger)
	require.NoError(t, err)
	defer store.Stop()

	// Startup cleanup уже удалил stale файл, создаём новый для periodic.
	staleName2 := filePrefix + "periodic-stale-2" + fileSuffix
	stalePath2 := filepath.Join(dir, staleName2)
	require.NoError(t, os.WriteFile(stalePath2, []byte("more data"), 0600))
	require.NoError(t, os.Chtimes(stalePath2, past, past))

	// Ждём тик periodic cleanup.
	assert.Eventually(t, func() bool {
		_, err := os.Stat(stalePath2)
		return os.IsNotExist(err)
	}, 2*time.Second, 25*time.Millisecond, "periodic cleanup should remove stale file")
}

func TestTempStore_StopIdempotent(t *testing.T) {
	store := newTestStore(t)

	// Двойной Stop не паникует и не блокирует.
	store.Stop()
	store.Stop()
}

func TestTempStore_TempBytesActive(t *testing.T) {
	store := newTestStore(t)

	ctx := context.Background()
	tf, err := store.Create(ctx)
	require.NoError(t, err)

	// До записи — gauge = 0.
	assert.Equal(t, float64(0), gaugeValue(t, store.metrics.TempBytesActive))

	// Пишем через WriteChunk — gauge должен вырасти.
	data := []byte("hello world!") // 12 байт
	n, err := tf.WriteChunk(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, float64(12), gaugeValue(t, store.metrics.TempBytesActive))

	// Ещё запись.
	data2 := []byte("more") // 4 байта
	_, err = tf.WriteChunk(data2)
	require.NoError(t, err)
	assert.Equal(t, float64(16), gaugeValue(t, store.metrics.TempBytesActive))

	// Remove — gauge обратно в 0.
	tf.Remove()
	assert.Equal(t, float64(0), gaugeValue(t, store.metrics.TempBytesActive))
}
