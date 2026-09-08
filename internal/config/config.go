// Package config загружает и валидирует env-конфигурацию сервиса через cleanenv.
// Все параметры имеют дефолты из SPEC §7 (тег env-default).
// Если пользователь переопределяет параметр через env, значение валидируется;
// ошибка всегда содержит имя переменной окружения.
package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

// Config содержит все параметры сервиса.
// Дефолты заданы через тег env-default; env переопределяет их.
type Config struct {
	// gRPC
	GRPCAddr      string `env:"GRPC_ADDR"             env-default:":9090"`
	GRPCAuthToken string `env:"GRPC_AUTH_TOKEN"       env-default:"change-me"`

	// Upload
	MaxUploadBytes int64    `env:"MAX_UPLOAD_BYTES"      env-default:"524288000"` // 500MB
	MIMEAllowlist  []string `env:"MIME_ALLOWLIST"        env-default:"image/*,video/*,audio/*" env-separator:","`

	// Processing
	WorkerConcurrency int           `env:"WORKER_CONCURRENCY"    env-default:"2"`
	QueueBuffer       int           `env:"QUEUE_BUFFER"          env-default:"64"`
	JobTimeout        time.Duration `env:"JOB_TIMEOUT"           env-default:"12m"`
	JobLease          time.Duration `env:"JOB_LEASE"             env-default:"30s"`
	PollInterval      time.Duration `env:"POLL_INTERVAL"         env-default:"1s"`
	MaxJobAttempts    int           `env:"JOB_MAX_ATTEMPTS"      env-default:"3"`
	JobBackoffBase    time.Duration `env:"JOB_BACKOFF_BASE"      env-default:"30s"`
	JobBackoffMax     time.Duration `env:"JOB_BACKOFF_MAX"       env-default:"10m"`
	JobBackoffJitter  float64       `env:"JOB_BACKOFF_JITTER"    env-default:"0.2"`
	// JobReapBatchSize — LIMIT для ReapExpiredLeases / RecoverStaleJobs за один тик.
	JobReapBatchSize  int           `env:"JOB_REAP_BATCH_SIZE" env-default:"100"`
	FFMPEGTimeout     time.Duration `env:"FFMPEG_TIMEOUT"        env-default:"10m"`
	ShutdownTimeout   time.Duration `env:"SHUTDOWN_TIMEOUT"      env-default:"30s"`
	Rendition         int           `env:"RENDITION"             env-default:"720"`
	ThumbSecond       int           `env:"THUMB_SECOND"          env-default:"1"`
	ProcessingTempDir string        `env:"PROCESSING_TEMP_DIR" env-default:"/tmp/processing"`

	// Storage / TTL
	PresignTTL       time.Duration `env:"PRESIGN_TTL"           env-default:"15m"`
	TTLReapInterval  time.Duration `env:"TTL_REAP_INTERVAL"     env-default:"1m"`
	TTLReapBatchSize int           `env:"TTL_REAP_BATCH_SIZE"   env-default:"100"`
	// TTLReapDryRun: только считать/логировать "would delete", ничего не
	// удалять — рекомендуется на первый выкат reaper'а (ревью PR #13/#17).
	TTLReapDryRun bool `env:"TTL_REAP_DRY_RUN"      env-default:"false"`

	// Upload temp storage
	UploadTempDir         string        `env:"UPLOAD_TEMP_DIR"       env-default:"/tmp/media-uploads"`
	UploadReserveBytes    int64         `env:"UPLOAD_RESERVE_BYTES"  env-default:"104857600"` // 100MB
	UploadStaleGrace      time.Duration `env:"UPLOAD_STALE_GRACE"       env-default:"1h"`
	UploadCleanupInterval time.Duration `env:"UPLOAD_CLEANUP_INTERVAL"  env-default:"10m"`

	// Limits
	RateLimitRPS         int `env:"RATE_LIMIT_RPS"        env-default:"50"`
	MaxConcurrentStreams int `env:"MAX_CONCURRENT_STREAMS" env-default:"8"`

	// Postgres
	PostgresDSN            string        `env:"POSTGRES_DSN"              env-default:"postgres://media:media@postgres:5432/media?sslmode=disable"`
	PostgresConnectTimeout time.Duration `env:"POSTGRES_CONNECT_TIMEOUT"  env-default:"5s"`
	PostgresQueryTimeout   time.Duration `env:"POSTGRES_QUERY_TIMEOUT"    env-default:"30s"`

	// MinIO
	MinIOEndpoint  string `env:"MINIO_ENDPOINT"        env-default:"minio:9000"`
	MinIOAccessKey string `env:"MINIO_ACCESS_KEY"      env-default:"minioadmin"`
	MinIOSecretKey string `env:"MINIO_SECRET_KEY"      env-default:"minioadmin"`
	MinIOBucket    string `env:"MINIO_BUCKET"          env-default:"media"`
	MinIOUseSSL    bool   `env:"MINIO_USE_SSL"         env-default:"false"`

	// Kafka
	KafkaEnabled  bool     `env:"KAFKA_ENABLED"         env-default:"false"`
	KafkaBrokers  []string `env:"KAFKA_BROKERS"         env-default:"kafka:9092" env-separator:","`
	KafkaTopic    string   `env:"KAFKA_TOPIC"           env-default:"media.events"`
	KafkaDLQTopic string   `env:"KAFKA_DLQ_TOPIC"       env-default:"media.events.dlq"`
	KafkaGroup    string   `env:"KAFKA_GROUP"           env-default:"media-service"`

	// StrictOwnerCheck включает строгую проверку владельца.
	// При true требуется валидный auth interceptor (TODO #5).
	// Пока используется как feature-flag для deploy-модели за gateway.
	StrictOwnerCheck bool `env:"STRICT_OWNER_CHECK" env-default:"false"`

	ReconcilerInterval    time.Duration `env:"RECONCILER_INTERVAL"     env-default:"5m"`
	ReconcilerGracePeriod time.Duration `env:"RECONCILER_GRACE_PERIOD" env-default:"1h"` // 1h для orphan safety
	ReconcilerBatchSize   int           `env:"RECONCILER_BATCH_SIZE"   env-default:"100"`
	ReconcilerDryRun      bool          `env:"RECONCILER_DRY_RUN"      env-default:"false"`

	RetentionInterval  time.Duration `env:"RETENTION_INTERVAL" env-default:"1h"`
	RetentionOlderThan time.Duration `env:"RETENTION_OLDER_THAN" env-default:"720h"` // 30 days
	RetentionBatchSize int           `env:"RETENTION_BATCH_SIZE" env-default:"1000"`
}

// Load читает .env (если есть), накладывает переменные окружения на дефолтные значения и валидирует.
func Load() (*Config, error) {
	var cfg Config

	// .env файл опционален
	_ = godotenv.Load()

	// Загрузка конфигурации из переменных окружения
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return nil, fmt.Errorf("failed to read env: %w", err)
	}
	// Валидация полей
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// String возвращает безопасное строковое представление конфига.
// Секреты удалены из итоговой строки.
func (c *Config) String() string {
	var b strings.Builder
	b.WriteString("Config{")
	fmt.Fprintf(&b, "GRPCAddr:%q, ", c.GRPCAddr)
	fmt.Fprintf(&b, "MaxUploadBytes:%d, ", c.MaxUploadBytes)
	fmt.Fprintf(&b, "MIMEAllowlist:%v, ", c.MIMEAllowlist)
	fmt.Fprintf(&b, "WorkerConcurrency:%d, ", c.WorkerConcurrency)
	fmt.Fprintf(&b, "QueueBuffer:%d, ", c.QueueBuffer)
	fmt.Fprintf(&b, "JobTimeout:%s, ", c.JobTimeout)
	fmt.Fprintf(&b, "JobLease:%s, ", c.JobLease)
	fmt.Fprintf(&b, "PollInterval:%s, ", c.PollInterval)
	fmt.Fprintf(&b, "MaxJobAttempts:%d, ", c.MaxJobAttempts)
	fmt.Fprintf(&b, "JobBackoffBase:%s, ", c.JobBackoffBase)
	fmt.Fprintf(&b, "JobBackoffMax:%s, ", c.JobBackoffMax)
	fmt.Fprintf(&b, "JobBackoffJitter:%v, ", c.JobBackoffJitter)
	fmt.Fprintf(&b, "JobReapBatchSize:%d, ", c.JobReapBatchSize)
	fmt.Fprintf(&b, "FFMPEGTimeout:%s, ", c.FFMPEGTimeout)
	fmt.Fprintf(&b, "ShutdownTimeout:%s, ", c.ShutdownTimeout)
	fmt.Fprintf(&b, "Rendition:%d, ", c.Rendition)
	fmt.Fprintf(&b, "ThumbSecond:%d, ", c.ThumbSecond)
	fmt.Fprintf(&b, "PresignTTL:%s, ", c.PresignTTL)
	fmt.Fprintf(&b, "TTLReapInterval:%s, ", c.TTLReapInterval)
	fmt.Fprintf(&b, "TTLReapBatchSize:%d, ", c.TTLReapBatchSize)
	fmt.Fprintf(&b, "TTLReapDryRun:%t, ", c.TTLReapDryRun)
	fmt.Fprintf(&b, "UploadTempDir:%q, ", c.UploadTempDir)
	fmt.Fprintf(&b, "UploadReserveBytes:%d, ", c.UploadReserveBytes)
	fmt.Fprintf(&b, "UploadStaleGrace:%s, ", c.UploadStaleGrace)
	fmt.Fprintf(&b, "UploadCleanupInterval:%s, ", c.UploadCleanupInterval)
	fmt.Fprintf(&b, "PostgresDSN:%q, ", maskDSN(c.PostgresDSN))
	fmt.Fprintf(&b, "PostgresConnectTimeout:%s, ", c.PostgresConnectTimeout)
	fmt.Fprintf(&b, "PostgresQueryTimeout:%s, ", c.PostgresQueryTimeout)
	fmt.Fprintf(&b, "RateLimitRPS:%d, ", c.RateLimitRPS)
	fmt.Fprintf(&b, "MaxConcurrentStreams:%d, ", c.MaxConcurrentStreams)
	fmt.Fprintf(&b, "MinIOEndpoint:%q, ", c.MinIOEndpoint)
	fmt.Fprintf(&b, "MinIOBucket:%q, ", c.MinIOBucket)
	fmt.Fprintf(&b, "MinIOUseSSL:%v, ", c.MinIOUseSSL)
	fmt.Fprintf(&b, "KafkaEnabled:%v, ", c.KafkaEnabled)
	fmt.Fprintf(&b, "KafkaBrokers:%v, ", c.KafkaBrokers)
	fmt.Fprintf(&b, "KafkaTopic:%q, ", c.KafkaTopic)
	fmt.Fprintf(&b, "KafkaDLQTopic:%q, ", c.KafkaDLQTopic)
	fmt.Fprintf(&b, "KafkaGroup:%q, ", c.KafkaGroup)
	fmt.Fprintf(&b, "RetentionInterval:%s, ", c.RetentionInterval)
	fmt.Fprintf(&b, "RetentionOlderThan:%s, ", c.RetentionOlderThan)
	fmt.Fprintf(&b, "RetentionBatchSize:%d, ", c.RetentionBatchSize)
	b.WriteString("}")
	return b.String()
}

// LogValue позволяет slog использовать String() вместо рефлексии.
// Так при slog.Info("cfg", config) в лог попадёт маскированная строка.
func (c Config) LogValue() slog.Value {
	return slog.StringValue(c.String())
}

// maskDSN оставляет только host:port из DSN, убирая credentials.
func maskDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid>"
	}
	return u.Host
}
