package config

import (
	"fmt"
)

// validate проверяет, что все параметры корректны.
// Ошибка всегда содержит имя переменной окружения.
func (c *Config) validate() error {
	if c.GRPCAddr == "" {
		return fmt.Errorf("GRPC_ADDR is required")
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if c.MaxUploadBytes <= 0 {
		return fmt.Errorf("MAX_UPLOAD_BYTES must be > 0, got %d", c.MaxUploadBytes)
	}
	if len(c.MIMEAllowlist) == 0 {
		return fmt.Errorf("MIME_ALLOWLIST is required")
	}
	if c.WorkerConcurrency <= 0 {
		return fmt.Errorf("WORKER_CONCURRENCY must be > 0, got %d", c.WorkerConcurrency)
	}
	if c.QueueBuffer <= 0 {
		return fmt.Errorf("QUEUE_BUFFER must be > 0, got %d", c.QueueBuffer)
	}
	if c.JobTimeout <= 0 {
		return fmt.Errorf("JOB_TIMEOUT must be > 0, got %s", c.JobTimeout)
	}
	if c.JobLease <= 0 {
		return fmt.Errorf("JOB_LEASE must be > 0, got %s", c.JobLease)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be > 0, got %s", c.PollInterval)
	}
	if c.MaxJobAttempts <= 0 {
		return fmt.Errorf("JOB_MAX_ATTEMPTS must be > 0, got %d", c.MaxJobAttempts)
	}
	if c.FFMPEGTimeout <= 0 {
		return fmt.Errorf("FFMPEG_TIMEOUT must be > 0, got %s", c.FFMPEGTimeout)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0, got %s", c.ShutdownTimeout)
	}
	if c.Rendition <= 0 {
		return fmt.Errorf("RENDITION must be > 0, got %d", c.Rendition)
	}
	if c.ThumbSecond < 0 {
		return fmt.Errorf("THUMB_SECOND must be >= 0, got %d", c.ThumbSecond)
	}
	if c.PresignTTL <= 0 {
		return fmt.Errorf("PRESIGN_TTL must be > 0, got %s", c.PresignTTL)
	}
	if c.TTLReapInterval <= 0 {
		return fmt.Errorf("TTL_REAP_INTERVAL must be > 0, got %s", c.TTLReapInterval)
	}
	if c.RateLimitRPS <= 0 {
		return fmt.Errorf("RATE_LIMIT_RPS must be > 0, got %d", c.RateLimitRPS)
	}
	if c.MaxConcurrentStreams <= 0 {
		return fmt.Errorf("MAX_CONCURRENT_STREAMS must be > 0, got %d", c.MaxConcurrentStreams)
	}
	if c.UploadTempDir == "" {
		return fmt.Errorf("UPLOAD_TEMP_DIR is required")
	}
	if c.UploadReserveBytes < 0 {
		return fmt.Errorf("UPLOAD_RESERVE_BYTES must be >= 0, got %d", c.UploadReserveBytes)
	}
	if c.UploadStaleGrace <= 0 {
		return fmt.Errorf("UPLOAD_STALE_GRACE must be > 0, got %s", c.UploadStaleGrace)
	}
	if c.UploadCleanupInterval <= 0 {
		return fmt.Errorf("UPLOAD_CLEANUP_INTERVAL must be > 0, got %s", c.UploadCleanupInterval)
	}
	if c.PostgresDSN == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}
	if c.PostgresConnectTimeout <= 0 {
		return fmt.Errorf("POSTGRES_CONNECT_TIMEOUT must be > 0, got %s", c.PostgresConnectTimeout)
	}
	if c.PostgresQueryTimeout <= 0 {
		return fmt.Errorf("POSTGRES_QUERY_TIMEOUT must be > 0, got %s", c.PostgresQueryTimeout)
	}
	if c.MinIOEndpoint == "" {
		return fmt.Errorf("MINIO_ENDPOINT is required")
	}
	if c.MinIOAccessKey == "" {
		return fmt.Errorf("MINIO_ACCESS_KEY is required")
	}
	if c.MinIOSecretKey == "" {
		return fmt.Errorf("MINIO_SECRET_KEY is required")
	}
	if c.MinIOBucket == "" {
		return fmt.Errorf("MINIO_BUCKET is required")
	}
	if c.KafkaEnabled {
		if len(c.KafkaBrokers) == 0 || (len(c.KafkaBrokers) == 1 && c.KafkaBrokers[0] == "") {
			return fmt.Errorf("KAFKA_BROKERS is required when KAFKA_ENABLED=true")
		}
		if c.KafkaTopic == "" {
			return fmt.Errorf("KAFKA_TOPIC is required when KAFKA_ENABLED=true")
		}
		if c.KafkaDLQTopic == "" {
			return fmt.Errorf("KAFKA_DLQ_TOPIC is required when KAFKA_ENABLED=true")
		}
		if c.KafkaGroup == "" {
			return fmt.Errorf("KAFKA_GROUP is required when KAFKA_ENABLED=true")
		}
	}
	return nil
}
