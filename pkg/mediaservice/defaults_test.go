package mediaservice_test

import (
	"os"
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/config"
	"mediaservice/pkg/mediaservice"
)

// TestDefaultsMatchServiceConfig - умолчания библиотеки совпадают
// с умолчаниями сервиса.
//
// Зачем. Один и тот же файл не должен приниматься через gRPC и отвергаться
// встроенным режимом: это два входа в один сервис, и правила приёма у них
// обязаны быть одинаковыми.
//
// Без теста расхождение ничем не проявится - обе стороны продолжат работать,
// просто по-разному, и обнаружится это у потребителя библиотеки.
//
// Полностью убрать дублирование нельзя: значения сервиса живут в тегах
// структуры config.Config, а теги не умеют ссылаться на константы. Читать
// же переменные окружения библиотека не должна - это был бы захват
// конфигурации встраивающего приложения. Остаётся зафиксировать связь
// проверкой.
func TestDefaultsMatchServiceConfig(t *testing.T) {
	// cleanenv читает реальное окружение, поэтому переменные, выставленные
	// на машине разработчика, подменили бы умолчания и тест сравнивал бы
	// не то. Снимаем их на время теста и возвращаем обратно.
	for _, key := range []string{"MAX_UPLOAD_BYTES", "MIME_ALLOWLIST"} {
		old, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		require.NoError(t, os.Unsetenv(key))
		t.Cleanup(func() { _ = os.Setenv(key, old) })
	}

	// ReadEnv, а не config.Load: Load вдобавок валидирует конфигурацию
	// целиком и упал бы на пустых обязательных полях, которые к этой
	// проверке отношения не имеют.
	var cfg config.Config
	require.NoError(t, cleanenv.ReadEnv(&cfg))

	require.Equal(t, cfg.MaxUploadBytes, mediaservice.DefaultMaxUploadBytes,
		"умолчание размера в библиотеке разошлось с MAX_UPLOAD_BYTES")
	require.Equal(t, cfg.MIMEAllowlist, mediaservice.DefaultMIMEAllowlist(),
		"умолчание списка типов в библиотеке разошлось с MIME_ALLOWLIST")
}

// TestProcessingDefaultsMatchServiceConfig - настройки движка обработки
// по умолчанию совпадают с настройками сервиса.
//
// Те же соображения, что и для умолчаний загрузки, только величин больше:
// движок, разбирающий очередь вдвое медленнее сервиса или сдающийся после
// другого числа попыток, вёл бы себя иначе на тех же данных.
//
// TempDir намеренно не сверяется. Сервис пишет в абсолютный /tmp/processing,
// а библиотека встраивается в чужое приложение, которому такой путь
// навязывать нельзя, и берёт подкаталог внутри os.TempDir(). Это осознанное
// расхождение, а не недосмотр.
func TestProcessingDefaultsMatchServiceConfig(t *testing.T) {
	keys := []string{
		"WORKER_CONCURRENCY", "POLL_INTERVAL", "JOB_TIMEOUT", "JOB_LEASE",
		"JOB_MAX_ATTEMPTS", "JOB_BACKOFF_BASE", "JOB_BACKOFF_MAX",
		"JOB_BACKOFF_JITTER", "JOB_REAP_BATCH_SIZE", "THUMB_SECOND",
	}
	for _, key := range keys {
		old, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		require.NoError(t, os.Unsetenv(key))
		t.Cleanup(func() { _ = os.Setenv(key, old) })
	}

	var cfg config.Config
	require.NoError(t, cleanenv.ReadEnv(&cfg))

	got := mediaservice.DefaultProcessingConfig()

	require.Equal(t, cfg.WorkerConcurrency, got.WorkerConcurrency)
	require.Equal(t, cfg.PollInterval, got.PollInterval)
	require.Equal(t, cfg.JobTimeout, got.JobTimeout)
	require.Equal(t, cfg.JobLease, got.JobLease)
	require.Equal(t, cfg.MaxJobAttempts, got.MaxJobAttempts)
	require.Equal(t, cfg.JobBackoffBase, got.BackoffBase)
	require.Equal(t, cfg.JobBackoffMax, got.BackoffMax)
	require.Equal(t, cfg.JobBackoffJitter, got.BackoffJitter)
	require.Equal(t, cfg.JobReapBatchSize, got.ReapBatchSize)
	require.Equal(t, cfg.ThumbSecond, got.ThumbSecond)
}
