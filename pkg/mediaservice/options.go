package mediaservice

import (
	"log/slog"
	"time"
)

// defaultPresignTTL - время жизни presigned-ссылок по умолчанию.
const defaultPresignTTL = 15 * time.Minute

// Config содержит базовые параметры для инициализации клиента через New:
// библиотека сама создаёт пул Postgres и клиент MinIO и сама закрывает их
// в Close().
//
// Если у встраивающего проекта уже есть свои соединения, используйте
// NewWithDeps: переданные туда ресурсы библиотека не закрывает.
type Config struct {
	// PostgresDSN - строка подключения к PostgreSQL
	// (например, "postgres://user:pass@host:5432/db"). Обязателен.
	PostgresDSN string

	// Pool задаёт опциональные параметры настройки пула соединений pgxpool.
	// Нулевые значения означают использование настроек по умолчанию,
	// рекомендованных драйвером pgx.
	Pool PoolConfig

	// MinIO содержит реквизиты для подключения к объектному хранилищу
	// (S3/MinIO). Обязателен.
	MinIO MinIOConfig

	// Запрещает создание структуры unkeyed-литералом вида Config{a, b, c}.
	// Благодаря этому добавление новых полей не сломает код потребителей.
	_ struct{}
}

// MinIOConfig содержит реквизиты для подключения к объектному хранилищу.
type MinIOConfig struct {
	Endpoint  string // Адрес хранилища вида host:port, без схемы (например, "localhost:9000").
	AccessKey string // Ключ доступа (Access Key).
	SecretKey string // Секретный ключ (Secret Key).
	Bucket    string // Имя бакета для хранения медиафайлов.
	UseSSL    bool   // Флаг использования HTTPS/TLS.
	Region    string // Регион хранилища (опционально, например, "us-east-1").

	// Запрещает создание структуры unkeyed-литералом.
	// Благодаря этому добавление новых полей не сломает код потребителей.
	_ struct{}
}

// PoolConfig задаёт тонкие настройки пула соединений с PostgreSQL.
// Все поля необязательны: нулевое значение означает умолчание pgxpool.
type PoolConfig struct {
	ConnectTimeout  time.Duration // Таймаут установления соединения.
	QueryTimeout    time.Duration // Таймаут выполнения запроса (если не переопределён в контексте).
	MaxConns        int32         // Максимальное количество соединений в пуле.
	MinConns        int32         // Минимальное количество поддерживаемых соединений.
	MaxConnLifetime time.Duration // Максимальное время жизни соединения перед пересозданием.
	MaxConnIdleTime time.Duration // Максимальное время простоя соединения перед закрытием.

	// Запрещает создание структуры unkeyed-литералом.
	// Благодаря этому добавление новых полей не сломает код потребителей.
	_ struct{}
}

// clientOptions хранит внутренние настройки клиента.
//
// Не экспортируется намеренно: заполняется только через опции Option, чтобы
// каждое значение проходило проверку и нельзя было собрать некорректную
// конфигурацию в обход конструктора.
type clientOptions struct {
	log            *slog.Logger
	presignTTL     time.Duration
	autoMigrate    bool
	withProcessing bool
}

// Option реализует паттерн функциональных опций для гибкой настройки клиента.
// Применим к обоим конструкторам. Добавление новых опций не ломает
// существующий код: список опций имеет переменную длину.
type Option func(*clientOptions)

// defaultOptions возвращает набор настроек клиента по умолчанию, поверх
// которых конструктор применяет переданные опции.
func defaultOptions() *clientOptions {
	return &clientOptions{
		log:            slog.Default(),
		presignTTL:     defaultPresignTTL,
		autoMigrate:    false,
		withProcessing: false,
	}
}

// WithLogger устанавливает пользовательский структурированный логгер.
// Если передан nil, опция игнорируется и остаётся slog.Default().
func WithLogger(log *slog.Logger) Option {
	return func(o *clientOptions) {
		if log != nil {
			o.log = log
		}
	}
}

// WithPresignTTL задаёт время жизни (TTL) для presigned URL, генерируемых
// в GetDownloadURL. По умолчанию 15 минут. Значения <= 0 игнорируются.
func WithPresignTTL(ttl time.Duration) Option {
	return func(o *clientOptions) {
		if ttl > 0 {
			o.presignTTL = ttl
		}
	}
}

// WithAutoMigrate включает автоматическое применение миграций схемы при
// инициализации клиента. Полезно для тестовых сред и изолированных
// развёртываний, где библиотека управляет схемой самостоятельно.
//
// По умолчанию выключено: встраивающий проект может применять миграции своим
// мигратором, и самовольное изменение схемы при запуске стало бы для него
// неожиданностью. В этом случае используйте Migrations, чтобы получить файлы
// миграций и применить их самостоятельно.
func WithAutoMigrate() Option {
	return func(o *clientOptions) {
		o.autoMigrate = true
	}
}

// WithProcessing включает фоновый движок асинхронной обработки
// (воркеры ffmpeg для генерации превью и транскодирования).
//
// По умолчанию выключено: без обработки библиотека остаётся полностью
// рабочей. Это позволяет встраивать её в проекты, которым нужны только
// загрузка и отдача файлов, без требования наличия ffmpeg в окружении
// и без накладных расходов на фоновые горутины.
//
// Без этой опции медиа доходит до статуса Stored, производные не создаются,
// а флаги в UploadParams.Processing игнорируются.
func WithProcessing() Option {
	return func(o *clientOptions) {
		o.withProcessing = true
	}
}
