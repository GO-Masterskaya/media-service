package mediaservice

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"

	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// Client - основной клиент mediaservice, предоставляющий публичный API библиотеки.
// Инкапсулирует доменную логику, работу с БД и объектным хранилищем.
//
// Клиент потокобезопасен: все публичные методы могут вызываться из нескольких
// горутин одновременно.
//
// Жизненный цикл:
//   - Создаётся через New() или NewWithDeps()
//   - Используется для вызова методов API (Upload, GetMedia, Delete и т.д.)
//   - Освобождается через Close(), который закрывает только те ресурсы,
//     которые были созданы самим клиентом (см. поля ownsPool, ownsStorage)
type Client struct {
	// core - ядро домена. Библиотека не дублирует его логику, а вызывает
	// те же функции, что и gRPC-хендлеры, транслируя типы в публичные.
	core *media.Service

	// pool - пул соединений с PostgreSQL.
	// Может быть создан клиентом (ownsPool=true) или передан извне (ownsPool=false).
	pool *pgxpool.Pool

	// storage - адаптер к объектному хранилищу MinIO/S3.
	// Хранится в том числе ради Close(): освобождение ресурса зависит
	// от того, кто его создал.
	storage storage.Interface

	// ownsPool указывает, должен ли клиент закрыть пул соединений при вызове Close().
	// true - если пул был создан внутри New(), false - если передан через NewWithDeps().
	ownsPool bool

	// ownsStorage указывает, должен ли клиент закрыть адаптер хранилища при Close().
	// true - если адаптер был создан внутри New(), false - если передан извне.
	ownsStorage bool

	// options - настройки клиента, применённые через функциональные опции
	// (WithLogger, WithPresignTTL, WithProcessing и т.д.).
	options *clientOptions

	// mutex защищает closed от гонки и обеспечивает потокобезопасность методов.
	// RWMutex, а не Mutex, потому что читателей много (все методы проверяют
	// признак закрытия), а писатель один и редкий - это Close().
	mutex sync.RWMutex

	// closed - флаг, указывающий, был ли уже вызван Close().
	// Защищает от повторного закрытия ресурсов и от вызовов после закрытия.
	closed bool
}

// Deps содержит внешние зависимости для инициализации клиента через NewWithDeps().
// Используется, когда встраивающий проект хочет переиспользовать свои собственные
// подключения к БД и хранилищу, а не создавать новые.
//
// Пример использования:
//
//	pool, _ := pgxpool.New(ctx, dsn)
//	minioClient, _ := minio.New(endpoint, &minio.Options{...})
//
//	client, err := mediaservice.NewWithDeps(ctx, mediaservice.Deps{
//	    Pool:   pool,
//	    MinIO:  minioClient,
//	    Bucket: "media",
//	})
//
// В этом случае client.Close() НЕ закроет ни pool, ни minioClient: они останутся
// рабочими для использования в других частях приложения.
//
// Типы полей принадлежат внешним библиотекам (pgx, minio-go) намеренно:
// вызывающий передаёт свои объекты, значит эти зависимости у него уже есть.
// Внутренние типы самого медиа-сервиса наружу по-прежнему не протекают.
type Deps struct {
	// Pool - готовый пул соединений с PostgreSQL.
	// Клиент будет использовать его для всех операций с БД,
	// но НЕ закроет его при вызове Close().
	Pool *pgxpool.Pool

	// MinIO - готовый клиент для работы с объектным хранилищем.
	// Клиент будет использовать его для хранения файлов,
	// но НЕ закроет его при вызове Close().
	MinIO *minio.Client

	// Bucket - имя бакета для хранения медиафайлов.
	// Должен быть создан заранее: библиотека бакеты не создаёт.
	Bucket string
}

// newClient - внутренняя фабрика для создания экземпляра Client.
// Инициализирует репозитории и ядро доменной логики, связывая его
// с переданными зависимостями, и проставляет флаги владения.
//
// Общая часть обоих публичных конструкторов. Вынесена, чтобы добавление
// нового поля в Client требовало правки в одном месте, а не в двух.
func newClient(pool *pgxpool.Pool, st storage.Interface, o *clientOptions, ownsPool bool, ownsStorage bool) *Client {
	mediaRepo := repo.NewPgMediaRepo(pool)
	deriveRepo := repo.NewPgDerivativeRepo(pool)

	// Ядро не знает, кто владеет пулом и хранилищем: оно просто использует
	// переданные интерфейсы. Владение - забота клиента, см. Close().
	core := media.NewService(mediaRepo, deriveRepo, st, o.presignTTL, o.log)

	return &Client{
		core:        core,
		pool:        pool,
		storage:     st,
		ownsPool:    ownsPool,
		ownsStorage: ownsStorage,
		options:     o,
		// mutex и closed остаются нулевыми значениями: незаблокированный
		// мьютекс и false - корректное начальное состояние.
	}
}

// New создаёт клиент по параметрам подключения: библиотека сама поднимает пул
// Postgres и клиент MinIO и сама закрывает их в Close().
//
// Обязательны PostgresDSN, MinIO.Endpoint и MinIO.Bucket. Опции применяются
// поверх значений по умолчанию, см. WithLogger, WithPresignTTL, WithAutoMigrate,
// WithProcessing.
//
// Порядок инициализации:
//   - валидация обязательных полей конфигурации
//   - применение миграций, если включена опция WithAutoMigrate
//   - создание пула соединений с параметрами из cfg.Pool
//   - создание адаптера объектного хранилища
//   - сборка клиента
//
// Семантика владения: конструктор сам создаёт пул и адаптер хранилища,
// поэтому Close() у возвращённого клиента их закроет.
//
// Если у встраивающего проекта уже есть пул и клиент MinIO, используйте
// NewWithDeps, чтобы не создавать вторые подключения к тем же серверам.
//
// Возвращает ошибку, если конфигурация неполная, не удалось применить
// миграции или не удалось установить соединения на старте.
func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	// Применяем функциональные опции поверх настроек по умолчанию.
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	// Валидация до создания ресурсов: незачем поднимать соединения,
	// если конфигурация заведомо неполная.
	if cfg.PostgresDSN == "" {
		return nil, fmt.Errorf("postgres DSN is required: %w", ErrInvalidArgument)
	}
	if cfg.MinIO.Endpoint == "" {
		return nil, fmt.Errorf("minio endpoint is required: %w", ErrInvalidArgument)
	}
	if cfg.MinIO.Bucket == "" {
		return nil, fmt.Errorf("minio bucket is required: %w", ErrInvalidArgument)
	}

	// Миграции применяются до создания пула: мигратор открывает собственное
	// соединение и от пула не зависит. Если схема не накатилась, дальше
	// идти бессмысленно.
	if o.autoMigrate {
		if err := Migrate(cfg.PostgresDSN); err != nil {
			return nil, fmt.Errorf("apply migrations: %w", err)
		}
	}

	poolCfg := repo.PoolConfig{
		DSN:             cfg.PostgresDSN,
		ConnectTimeout:  cfg.Pool.ConnectTimeout,
		QueryTimeout:    cfg.Pool.QueryTimeout,
		MaxConns:        cfg.Pool.MaxConns,
		MinConns:        cfg.Pool.MinConns,
		MaxConnLifetime: cfg.Pool.MaxConnLifetime,
		MaxConnIdleTime: cfg.Pool.MaxConnIdleTime,
	}

	pool, err := repo.NewPool(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	minioCfg := storage.MinIOConfig{
		Endpoint:  cfg.MinIO.Endpoint,
		AccessKey: cfg.MinIO.AccessKey,
		SecretKey: cfg.MinIO.SecretKey,
		Bucket:    cfg.MinIO.Bucket,
		UseSSL:    cfg.MinIO.UseSSL,
		Region:    cfg.MinIO.Region,
	}
	st, err := storage.NewMinIO(minioCfg, o.log)
	if err != nil {
		// Пул уже создан и держит соединения. Если просто вернуть ошибку,
		// ссылка на него будет потеряна и закрыть его станет некому.
		pool.Close()
		return nil, fmt.Errorf("create minio storage: %w", err)
	}

	// Оба флага в true: ресурсы созданы здесь, значит Close() их закроет.
	return newClient(pool, st, o, true, true), nil
}

// NewWithDeps создаёт клиент поверх готовых соединений встраивающего проекта.
//
// Позволяет переиспользовать уже настроенные пул Postgres и клиент MinIO
// вместо создания вторых подключений к тем же серверам.
//
// Семантика владения: Close() эти ресурсы не трогает, закрывать их должен
// тот, кто создал. Адаптер хранилища создаётся здесь, но своих ресурсов
// не имеет и работает через переданный клиент, поэтому тоже не закрывается.
//
// Опция WithAutoMigrate здесь не действует: у конструктора нет строки
// подключения, а мигратору нужна именно она. Применяйте миграции заранее
// через Migrate или Migrations.
func NewWithDeps(_ context.Context, deps Deps, opts ...Option) (*Client, error) {
	// Применяем функциональные опции поверх настроек по умолчанию.
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if deps.Pool == nil {
		return nil, fmt.Errorf("pool is required: %w", ErrInvalidArgument)
	}

	if deps.MinIO == nil {
		return nil, fmt.Errorf("minio client is required: %w", ErrInvalidArgument)
	}

	if deps.Bucket == "" {
		return nil, fmt.Errorf("bucket is required: %w", ErrInvalidArgument)
	}

	st := storage.NewMinIOFromClient(deps.MinIO, deps.Bucket, o.log)

	return newClient(deps.Pool, st, o, false, false), nil
}

// Close освобождает ресурсы, созданные самой библиотекой.
//
// Соединения, переданные через NewWithDeps, не закрываются: владение остаётся
// за вызывающей стороной, и их закрытие сломало бы приложение, которое ими
// пользуется.
//
// Повторный вызов безопасен и возвращает nil: типичный defer client.Close()
// рядом с явным закрытием не должен приводить к ошибке.
//
// После Close все методы клиента возвращают ErrClosed.
func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Пул закрывается отложенно, чтобы это произошло в том числе при выходе
	// с ошибкой от хранилища: неудача с одним ресурсом не повод оставить
	// второй открытым. Close() у пула ошибок не возвращает.
	if c.ownsPool {
		defer c.pool.Close()
	}

	if c.ownsStorage {
		if err := c.storage.Close(); err != nil {
			return fmt.Errorf("mediaservice.Close(): %w", err)
		}
	}
	return nil
}

// checkOpen возвращает ErrClosed, если клиент уже закрыт.
//
// Вызывается в начале каждого публичного метода: без этой проверки обращение
// к закрытому клиенту дало бы невнятную ошибку из глубины драйвера или панику.
//
// Блокировка на чтение, а не на запись: метод только смотрит на флаг, и
// параллельные вызовы не должны выстраиваться в очередь друг за другом.
//
// Внутри Close вызывать нельзя: RWMutex не рекурсивный, и попытка взять
// блокировку повторно приведёт к вечному ожиданию.
func (c *Client) checkOpen() error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return ErrClosed
	}
	return nil
}
