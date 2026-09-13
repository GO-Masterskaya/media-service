package mediaservice

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"mediaservice/internal/config"
	"mediaservice/internal/processing"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

// engineShutdownTimeout - сколько Close() ждёт завершения задач, начатых
// движком обработки.
//
// Ждать бесконечно нельзя: Close() не принимает контекст, и подвисшая задача
// подвесила бы завершение всего приложения. Ждать нисколько тоже нельзя:
// прерванная на середине задача оставит мусор во временном каталоге
// и вернётся в очередь по протухшему lease.
//
// Тридцать секунд - компромисс: типичная задача короче, а длинная
// транскодировка всё равно переживёт перезапуск через механизм lease.
const engineShutdownTimeout = 30 * time.Second

// ProcessingConfig настраивает движок асинхронной обработки.
//
// Нулевые поля заменяются значениями из DefaultProcessingConfig, поэтому
// задавать структуру целиком не нужно: достаточно перечислить то, что
// отличается от умолчания.
//
// Числовые умолчания совпадают с переменными окружения сервиса, чтобы
// встроенный режим вёл себя так же, как запущенный рядом сервис.
// Совпадение закреплено тестом.
type ProcessingConfig struct {
	// WorkerConcurrency - сколько задач выполняется одновременно.
	// Каждый воркер забирает задачу сам, очереди в памяти нет.
	WorkerConcurrency int

	// PollInterval - пауза воркера, когда задач в очереди не нашлось.
	PollInterval time.Duration

	// JobTimeout - потолок времени на одну задачу. Транскодирование
	// длинного видео - самая долгая операция, на неё и рассчитано умолчание.
	JobTimeout time.Duration

	// JobLease - срок аренды задачи. Если воркер не продлил аренду
	// (упал, завис, потерял связь с базой), задачу заберёт другой.
	JobLease time.Duration

	// MaxJobAttempts - сколько раз задача повторяется до перевода
	// в состояние окончательной неудачи.
	MaxJobAttempts int

	// BackoffBase, BackoffMax, BackoffJitter - отсрочка перед повтором.
	// Задержка растёт экспоненциально от BackoffBase до BackoffMax,
	// Jitter в долях единицы размазывает повторы во времени, чтобы
	// задачи, упавшие одновременно, не повторились одновременно.
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	BackoffJitter float64

	// ReapBatchSize - сколько задач с протухшей арендой подбирается
	// за один проход.
	ReapBatchSize int

	// TempDir - каталог для промежуточных файлов ffmpeg. Отличается
	// от каталога загрузки: там лежат принимаемые байты, здесь - результат
	// перекодирования до выгрузки в хранилище.
	TempDir string

	// ThumbSecond - с какой секунды видео берётся кадр для миниатюры.
	ThumbSecond int
}

// DefaultProcessingConfig возвращает настройки движка по умолчанию.
//
// Все числовые значения повторяют env-умолчания сервиса
// (WORKER_CONCURRENCY, POLL_INTERVAL, JOB_TIMEOUT и далее). Исключение -
// TempDir: сервис по умолчанию пишет в /tmp/processing, а библиотека
// встраивается в чужое приложение, которому абсолютный путь навязывать
// нельзя, и берёт подкаталог внутри os.TempDir().
func DefaultProcessingConfig() ProcessingConfig {
	return ProcessingConfig{
		WorkerConcurrency: 2,
		PollInterval:      time.Second,
		JobTimeout:        12 * time.Minute,
		JobLease:          30 * time.Second,
		MaxJobAttempts:    3,
		BackoffBase:       30 * time.Second,
		BackoffMax:        10 * time.Minute,
		BackoffJitter:     0.2,
		ReapBatchSize:     100,
		TempDir:           filepath.Join(os.TempDir(), "mediaservice-processing"),
		ThumbSecond:       1,
	}
}

// withDefaults подставляет умолчания вместо нулевых полей.
//
// Нулевое значение трактуется как "не задано", а не как осмысленный ноль.
// Для всех полей это верно: воркеров не бывает ноль, таймаут ноль означал бы
// мгновенную отмену, пустой каталог - запись в текущий рабочий.
func (p ProcessingConfig) withDefaults() ProcessingConfig {
	d := DefaultProcessingConfig()

	if p.WorkerConcurrency <= 0 {
		p.WorkerConcurrency = d.WorkerConcurrency
	}
	if p.PollInterval <= 0 {
		p.PollInterval = d.PollInterval
	}
	if p.JobTimeout <= 0 {
		p.JobTimeout = d.JobTimeout
	}
	if p.JobLease <= 0 {
		p.JobLease = d.JobLease
	}
	if p.MaxJobAttempts <= 0 {
		p.MaxJobAttempts = d.MaxJobAttempts
	}
	if p.BackoffBase <= 0 {
		p.BackoffBase = d.BackoffBase
	}
	if p.BackoffMax <= 0 {
		p.BackoffMax = d.BackoffMax
	}
	if p.BackoffJitter <= 0 {
		p.BackoffJitter = d.BackoffJitter
	}
	if p.ReapBatchSize <= 0 {
		p.ReapBatchSize = d.ReapBatchSize
	}
	if p.TempDir == "" {
		p.TempDir = d.TempDir
	}
	if p.ThumbSecond <= 0 {
		p.ThumbSecond = d.ThumbSecond
	}

	return p
}

// startProcessing поднимает движок обработки и возвращает его вместе
// с функцией отмены собственного контекста.
//
// Контекст берётся от context.Background(), а не от контекста конструктора.
// Причина существенная: контекст конструктора живёт до возврата из New,
// а движок должен работать до Close(). Передав его в Start, мы получили бы
// движок, умирающий сразу после создания клиента, - причём молча,
// без единой ошибки.
func startProcessing(
	pool *pgxpool.Pool,
	st storage.Interface,
	mediaRepo *repo.PgMediaRepo,
	derivRepo *repo.PgDerivativeRepo,
	o *clientOptions,
) (*processing.Engine, context.CancelFunc, error) {
	cfg := o.processing.withDefaults()

	// Обработчики запускают внешние бинарники. Без этой проверки движок
	// поднялся бы, разобрал очередь и пометил каждую задачу неудачной
	// после трёх попыток - на это ушло бы несколько минут, а причина
	// осталась бы в логах воркера, куда вызывающий не смотрит.
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			return nil, nil, fmt.Errorf(
				"%w: %s not found in PATH, WithProcessing requires ffmpeg", ErrInternal, bin)
		}
	}

	// Каталог создаётся заранее: os.MkdirTemp внутри обработчика откажет,
	// если родителя нет, и ошибка вылезет на первой же задаче вместо
	// конструктора.
	if err := os.MkdirAll(cfg.TempDir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("%w: create processing temp dir: %v", ErrInternal, err)
	}

	// Обработчики принимают *config.Config целиком, хотя используют из него
	// два поля: ProcessingTempDir и ThumbSecond. Конфигурация сервиса
	// собирается из переменных окружения, которых у встраивающего
	// приложения нет, поэтому структура заполняется вручную.
	//
	// Это временная неловкость: правильнее сузить сигнатуру конструкторов
	// обработчиков до маленькой структуры настроек. Отдельная задача,
	// трогающая internal/processing и main.
	handlerCfg := &config.Config{
		ProcessingTempDir: cfg.TempDir,
		ThumbSecond:       cfg.ThumbSecond,
	}

	thumbnailHandler := processing.NewThumbnailHandler(st, derivRepo, handlerCfg, o.log)
	transcodeHandler := processing.NewTranscodeHandler(st, derivRepo, handlerCfg, o.log)

	registry := processing.NewRegistry()

	// Задача несёт только идентификатор медиа, поэтому запись догружается
	// обработчиком. Так же устроено в сервисе: очередь остаётся тонкой,
	// а данные берутся на момент выполнения, а не на момент постановки.
	registry.Register("thumbnail", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		rec, err := loadMediaRecord(ctx, mediaRepo, job.MediaID)
		if err != nil {
			return fmt.Errorf("fetch media for thumbnail: %w", err)
		}
		_, err = thumbnailHandler.ProcessThumbnail(ctx, rec)
		return err
	}))
	registry.Register("transcode", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		rec, err := loadMediaRecord(ctx, mediaRepo, job.MediaID)
		if err != nil {
			return fmt.Errorf("fetch media for transcode: %w", err)
		}
		_, err = transcodeHandler.ProcessTranscode(ctx, rec)
		return err
	}))

	// Идентификатор экземпляра. Под ним берётся аренда задач, и он обязан
	// быть разным у разных процессов: одинаковый позволил бы двум движкам
	// считать чужую аренду своей.
	instanceID := uuid.NewString()

	adapter := processing.NewRepoAdapter(
		repo.NewPgJobRepo(pool),
		instanceID,
		cfg.JobLease,
		cfg.MaxJobAttempts,
		processing.BackoffConfig{
			Base:   cfg.BackoffBase,
			Max:    cfg.BackoffMax,
			Jitter: cfg.BackoffJitter,
		},
		cfg.ReapBatchSize,
	)

	engine := processing.NewEngine(processing.Config{
		WorkerConcurrency: cfg.WorkerConcurrency,
		PollInterval:      cfg.PollInterval,
		JobTimeout:        cfg.JobTimeout,
		LeaseDuration:     cfg.JobLease,
		MaxAttempts:       cfg.MaxJobAttempts,
	}, adapter, registry, processing.NewMetrics(o.metricsReg))

	ctx, cancel := context.WithCancel(context.Background())
	if err := engine.Start(ctx); err != nil {
		cancel()
		return nil, nil, fmt.Errorf("%w: start processing engine: %v", ErrInternal, err)
	}

	o.log.Info("mediaservice: processing engine started",
		"instance", instanceID,
		"concurrency", cfg.WorkerConcurrency,
		"temp_dir", cfg.TempDir)

	return engine, cancel, nil
}

// loadMediaRecord достаёт запись и переводит её в вид, который ждут
// обработчики.
//
// Отдельная функция, а не копия в каждом замыкании: обработчиков два,
// а запрос один, и расходиться они не должны.
func loadMediaRecord(ctx context.Context, mediaRepo *repo.PgMediaRepo, id uuid.UUID) (processing.MediaRecord, error) {
	m, err := mediaRepo.GetByID(ctx, id)
	if err != nil {
		return processing.MediaRecord{}, err
	}
	return processing.MediaRecord{
		ID:        m.ID,
		OwnerID:   m.OwnerID,
		Kind:      processing.Kind(m.Kind),
		SourceKey: m.StorageKey,
	}, nil
}
