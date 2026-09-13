package mediaservice

import (
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// defaultPresignTTL - время жизни presigned-ссылок по умолчанию.
const defaultPresignTTL = 15 * time.Minute

// DefaultMaxUploadBytes - потолок размера одного файла по умолчанию, 500 МиБ.
//
// Совпадает с умолчанием MAX_UPLOAD_BYTES в сервисе: один и тот же файл
// не должен приниматься через gRPC и отвергаться встроенным режимом.
// Совпадение закреплено тестом, а не договорённостью.
//
// Экспортируется, чтобы вызывающий мог оттолкнуться от умолчания,
// а не подбирать его опытным путём.
const DefaultMaxUploadBytes int64 = 500 << 20

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

	// Настройки приёма файлов. Живут в опциях, а не в Config, потому что
	// нужны обоим конструкторам, а Config принимает только New.
	maxUploadBytes int64
	mimeAllowlist  []string
	uploadTempDir  string
	storageQuota   int64
	metricsReg     prometheus.Registerer

	// processing хранит настройки движка обработки. Читается только
	// при withProcessing == true.
	processing ProcessingConfig
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

		maxUploadBytes: DefaultMaxUploadBytes,
		mimeAllowlist:  DefaultMIMEAllowlist(),
		uploadTempDir:  filepath.Join(os.TempDir(), "mediaservice-upload"),
		storageQuota:   0,

		// Изолированный реестр, а не prometheus.DefaultRegisterer.
		// Внутренние счётчики регистрируются через MustRegister, а он
		// паникует на повторной регистрации той же метрики: второй клиент
		// в том же процессе уронил бы приложение на конструкторе.
		// Приложению, которому счётчики нужны, их отдаёт
		// WithMetricsRegisterer.
		metricsReg: prometheus.NewRegistry(),
	}
}

// DefaultMIMEAllowlist возвращает список разрешённых типов по умолчанию.
// Совпадает с умолчанием MIME_ALLOWLIST в сервисе.
//
// Функция, а не переменная пакета: срез изменяем, и общая переменная
// позволила бы одному вызывающему незаметно испортить список другому.
// Каждый вызов отдаёт свежую копию, портить её безопасно.
func DefaultMIMEAllowlist() []string {
	return []string{"image/*", "video/*", "audio/*"}
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

// WithProcessing включает фоновый движок асинхронной обработки: воркеры,
// которые снимают задачи из очереди и запускают ffmpeg для генерации
// миниатюр и транскодирования.
//
// Требует ffmpeg и ffprobe в PATH. Если их нет, конструктор вернёт
// ErrInternal с указанием недостающего бинарника - лучше узнать это сразу,
// чем через несколько минут по трём неудачным попыткам каждой задачи.
//
// Движок работает до Close(), который останавливает воркеров и ждёт
// завершения начатых задач.
//
// Без этой опции медиа доходит до статуса Stored и там остаётся: флаги
// в UploadParams.Processing игнорируются, производные не создаются,
// Media.Derivatives всегда пуст. Такой режим полностью рабочий и не требует
// ffmpeg для тех, кому нужны только загрузка и отдача оригиналов.
//
// Настройки берутся из DefaultProcessingConfig. Чтобы изменить их,
// используйте WithProcessingConfig вместо этой опции.
func WithProcessing() Option {
	return func(o *clientOptions) {
		o.withProcessing = true
	}
}

// WithProcessingConfig включает движок обработки с явными настройками.
//
// Включает сам по себе: передавать WithProcessing дополнительно не нужно.
// Сделано так намеренно - разделение на "включить" и "настроить" давало бы
// возможность настроить выключенный движок и не понять, почему ничего
// не происходит.
//
// Нулевые поля структуры заменяются умолчаниями, поэтому заполнять её
// целиком не требуется:
//
//	client, err := mediaservice.New(ctx, cfg,
//		mediaservice.WithProcessingConfig(mediaservice.ProcessingConfig{
//			WorkerConcurrency: 8,
//		}),
//	)
func WithProcessingConfig(cfg ProcessingConfig) Option {
	return func(o *clientOptions) {
		o.withProcessing = true
		o.processing = cfg
	}
}

// WithMaxUploadBytes задаёт потолок размера одного загружаемого файла.
// По умолчанию 500 МиБ. Значения <= 0 игнорируются.
//
// Лимит проверяется дважды: по заявленному ExpectedSize до приёма байтов
// и по фактически принятому объёму. Первая проверка экономит место на диске
// и время, вторая ловит клиента, заявившего меньше, чем прислал.
func WithMaxUploadBytes(limit int64) Option {
	return func(o *clientOptions) {
		if limit > 0 {
			o.maxUploadBytes = limit
		}
	}
}

// WithMIMEAllowlist задаёт список разрешённых MIME-типов.
// По умолчанию image/*, video/*, audio/*.
//
// Поддерживаются точные типы ("image/png"), маски класса ("image/*")
// и "*/*". Пустой список игнорируется: он запретил бы загрузку чего угодно,
// и метод Upload перестал бы работать целиком - как настройка это почти
// наверняка не то, чего хотел вызывающий.
//
// Список проверяется против типа, заявленного в UploadParams.MIMEType.
// Фактический тип дополнительно сверяется с содержимым файла, поэтому
// разрешить "image/*" и прислать под этим видом видео не получится.
func WithMIMEAllowlist(mimeTypes ...string) Option {
	return func(o *clientOptions) {
		if len(mimeTypes) > 0 {
			o.mimeAllowlist = mimeTypes
		}
	}
}

// WithUploadTempDir задаёт каталог для временных файлов загрузки.
// По умолчанию подкаталог mediaservice-upload внутри os.TempDir().
//
// Каталог создаётся конструктором с правами 0700, если его нет. Принятые
// байты пишутся туда до момента, когда файл проверен и переложен в хранилище,
// поэтому свободного места нужно не меньше, чем размер самого большого
// ожидаемого файла.
//
// Каталог не должен быть общим с другим процессом, использующим библиотеку:
// фоновая уборка удаляет чужие файлы старше часа, считая их брошенными.
func WithUploadTempDir(dir string) Option {
	return func(o *clientOptions) {
		if dir != "" {
			o.uploadTempDir = dir
		}
	}
}

// WithStorageQuota задаёт квоту на суммарный объём хранения одного владельца
// в байтах. По умолчанию 0 - без ограничения.
//
// Это значение по умолчанию: индивидуальная квота владельца, заданная
// в таблице storage_quotas, имеет приоритет. Превышение даёт
// ErrQuotaExceeded ещё до приёма байтов, если известен ExpectedSize,
// и повторно после приёма - по фактическому размеру.
func WithStorageQuota(bytes int64) Option {
	return func(o *clientOptions) {
		if bytes >= 0 {
			o.storageQuota = bytes
		}
	}
}

// WithMetricsRegisterer подключает внутренние счётчики библиотеки
// к реестру Prometheus встраивающего приложения.
//
// По умолчанию используется изолированный реестр: счётчики создаются,
// но никуда не попадают. Такой выбор сделан ради безопасности конструктора -
// регистрация в общем реестре паникует при повторе, а два клиента в одном
// процессе это нормальный сценарий.
//
// Передавайте сюда собственный реестр, если хотите видеть метрики временного
// хранилища в своём /metrics. Один и тот же реестр двум клиентам передавать
// нельзя: второй вызов упадёт паникой из client_golang.
//
// nil игнорируется.
func WithMetricsRegisterer(reg prometheus.Registerer) Option {
	return func(o *clientOptions) {
		if reg != nil {
			o.metricsReg = reg
		}
	}
}
