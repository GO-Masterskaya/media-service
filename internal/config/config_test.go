package config

import (
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// withCleanEnv очищает окружение на время теста и восстанавливает его после.
// Тест, использующий эту функцию, не должен вызывать t.Parallel().
func withCleanEnv(t *testing.T) {
	t.Helper()
	oldEnv := os.Environ()
	os.Clearenv()
	t.Cleanup(func() {
		os.Clearenv()
		for _, e := range oldEnv {
			if i := strings.IndexByte(e, '='); i >= 0 {
				if err := os.Setenv(e[:i], e[i+1:]); err != nil {
					t.Fatalf("restore env: %v", err)
				}
			}
		}
	})
}

// configFromDefaults читает Config через cleanenv env-default теги в изолированном окружении.
// Новые обязательные параметры из validate() не роняют чужие литералы.
// Токен подставляется явно: GRPC_AUTH_ENABLED=true по умолчанию, а у токена нет env-default.
func configFromDefaults(t *testing.T) Config {
	t.Helper()
	withCleanEnv(t)
	var cfg Config
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv defaults: %v", err)
	}
	if cfg.GRPCAuthEnabled && cfg.GRPCAuthToken == "" {
		cfg.GRPCAuthToken = "test-token"
	}
	return cfg
}

func TestLoadDefaults(t *testing.T) {
	// Auth enabled by default requires an explicit token (no env-default).
	t.Setenv("GRPC_AUTH_TOKEN", "test-token")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GRPCAddr != ":9090" {
		t.Errorf("GRPCAddr: want :9090, got %s", cfg.GRPCAddr)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr: want :8080, got %s", cfg.HTTPAddr)
	}
	if !cfg.GRPCAuthEnabled {
		t.Error("GRPCAuthEnabled: want true by default")
	}
	if cfg.GRPCAuthToken != "test-token" {
		t.Errorf("GRPCAuthToken: want test-token, got %q", cfg.GRPCAuthToken)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout: want 30s, got %s", cfg.ShutdownTimeout)
	}
	if cfg.PostgresConnectTimeout != 5*time.Second {
		t.Errorf("PostgresConnectTimeout: want 5s, got %s", cfg.PostgresConnectTimeout)
	}
	if cfg.PostgresQueryTimeout != 30*time.Second {
		t.Errorf("PostgresQueryTimeout: want 30s, got %s", cfg.PostgresQueryTimeout)
	}
	if cfg.WorkerConcurrency != 2 {
		t.Errorf("WorkerConcurrency: want 2, got %d", cfg.WorkerConcurrency)
	}
	if cfg.JobReapBatchSize != 100 {
		t.Errorf("JobReapBatchSize: want 100, got %d", cfg.JobReapBatchSize)
	}
}

func TestLoadAuthTokenRequiredWhenEnabled(t *testing.T) {
	t.Setenv("GRPC_AUTH_ENABLED", "true")
	t.Setenv("GRPC_AUTH_TOKEN", "")
	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error for empty GRPC_AUTH_TOKEN")
	}
	if !strings.Contains(err.Error(), "GRPC_AUTH_TOKEN") {
		t.Errorf("error should mention GRPC_AUTH_TOKEN, got: %v", err)
	}
}

func TestLoadOverride(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("GRPC_ADDR", ":9999")
	t.Setenv("GRPC_AUTH_TOKEN", "test-token")
	t.Setenv("SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("POSTGRES_CONNECT_TIMEOUT", "2s")
	t.Setenv("POSTGRES_QUERY_TIMEOUT", "10s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GRPCAddr != ":9999" {
		t.Errorf("want :9999, got %s", cfg.GRPCAddr)
	}
	if cfg.ShutdownTimeout != 5*time.Second {
		t.Errorf("want 5s, got %s", cfg.ShutdownTimeout)
	}
	if cfg.PostgresConnectTimeout != 2*time.Second {
		t.Errorf("want 2s, got %s", cfg.PostgresConnectTimeout)
	}
	if cfg.PostgresQueryTimeout != 10*time.Second {
		t.Errorf("want 10s, got %s", cfg.PostgresQueryTimeout)
	}
}

func TestLoadValidationError(t *testing.T) {
	t.Setenv("GRPC_AUTH_TOKEN", "test-token")
	t.Setenv("MAX_UPLOAD_BYTES", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "MAX_UPLOAD_BYTES") {
		t.Errorf("error should contain field name, got: %v", err)
	}
}

func TestLoadProcessingValidationErrors(t *testing.T) {
	tests := []struct {
		envKey   string
		envVal   string
		errMatch string
	}{
		{"JOB_TIMEOUT", "0s", "JOB_TIMEOUT"},
		{"JOB_LEASE", "0s", "JOB_LEASE"},
		{"POLL_INTERVAL", "0s", "POLL_INTERVAL"},
		{"JOB_MAX_ATTEMPTS", "0", "JOB_MAX_ATTEMPTS"},
		{"JOB_REAP_BATCH_SIZE", "0", "JOB_REAP_BATCH_SIZE"},
	}

	for _, tt := range tests {
		t.Run(tt.envKey, func(t *testing.T) {
			t.Setenv("GRPC_AUTH_TOKEN", "test-token")
			t.Setenv(tt.envKey, tt.envVal)
			_, err := Load()
			if err == nil {
				t.Fatalf("expected validation error for %s=%s", tt.envKey, tt.envVal)
			}
			if !strings.Contains(err.Error(), tt.errMatch) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errMatch)
			}
		})
	}
}

func TestConfigStringRedactsSecrets(t *testing.T) {
	cfg := Config{
		GRPCAuthToken:  "super-secret-token",
		PostgresDSN:    "postgres://user:password@dbhost:5432/media",
		MinIOAccessKey: "access-key",
		MinIOSecretKey: "secret-key",
		MinIOEndpoint:  "minio:9000",
		MinIOBucket:    "media",
	}

	s := cfg.String()

	if strings.Contains(s, "super-secret-token") {
		t.Error("String() must not contain GRPCAuthToken")
	}
	if strings.Contains(s, "password") {
		t.Error("String() must not contain Postgres password")
	}
	if strings.Contains(s, "access-key") {
		t.Error("String() must not contain MinIOAccessKey")
	}
	if strings.Contains(s, "secret-key") {
		t.Error("String() must not contain MinIOSecretKey")
	}
	if !strings.Contains(s, "dbhost:5432") {
		t.Error("String() should contain masked DSN host")
	}
}

func TestConfigLogValue(t *testing.T) {
	cfg := Config{GRPCAddr: ":9090", MinIOEndpoint: "minio:9000"}
	val := cfg.LogValue()
	if val.Kind() != slog.KindString {
		t.Fatalf("expected KindString, got %v", val.Kind())
	}
	if !strings.HasPrefix(val.String(), "Config{") {
		t.Errorf("unexpected value: %s", val.String())
	}
}

// TestValidate_RetentionKafkaEnabled мутирует глобальное окружение процесса.
// Не использовать t.Parallel() в этом тесте и его subtests.
func TestValidate_RetentionKafkaEnabled(t *testing.T) {
	base := configFromDefaults(t)
	base.KafkaEnabled = true
	base.KafkaBrokers = []string{"kafka:9092"}
	base.KafkaTopic = "t"
	base.KafkaDLQTopic = "d"
	base.KafkaGroup = "g"
	base.RetentionInterval = time.Hour
	base.RetentionOlderThan = 168 * time.Hour
	base.RetentionBatchSize = 1000

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:   "valid retention",
			mutate: func(c *Config) {},
		},
		{
			name: "older_than too small",
			mutate: func(c *Config) {
				c.RetentionOlderThan = time.Second
			},
			wantErr: "RETENTION_OLDER_THAN must be >= 24h",
		},
		{
			name: "interval zero",
			mutate: func(c *Config) {
				c.RetentionInterval = 0
			},
			wantErr: "RETENTION_INTERVAL must be > 0",
		},
		{
			name: "batch zero",
			mutate: func(c *Config) {
				c.RetentionBatchSize = 0
			},
			wantErr: "RETENTION_BATCH_SIZE must be > 0",
		},
		{
			name: "negative storage quota bytes",
			mutate: func(c *Config) {
				c.StorageQuotaBytes = -1
			},
			wantErr: "STORAGE_QUOTA_BYTES must be >= 0",
		},
		{
			name: "zero upload idle timeout",
			mutate: func(c *Config) {
				c.UploadIdleTimeout = 0
			},
			wantErr: "UPLOAD_IDLE_TIMEOUT must be > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := cfg.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

// TestValidate_JobReapBatchSize мутирует глобальное окружение процесса.
// Не использовать t.Parallel().
func TestValidate_JobReapBatchSize(t *testing.T) {
	cfg := configFromDefaults(t)
	if err := cfg.validate(); err != nil {
		t.Fatalf("defaults must pass validate: %v", err)
	}

	cfg.JobReapBatchSize = 0
	err := cfg.validate()
	if err == nil {
		t.Fatal("expected validation error for JobReapBatchSize=0")
	}
	if !strings.Contains(err.Error(), "JOB_REAP_BATCH_SIZE") {
		t.Errorf("error should mention JOB_REAP_BATCH_SIZE, got: %v", err)
	}
}

// TestValidate_KafkaSecurity мутирует глобальное окружение процесса.
// Не использовать t.Parallel() в этом тесте и его subtests.
func TestValidate_KafkaSecurity(t *testing.T) {
	base := configFromDefaults(t)
	base.KafkaEnabled = true

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:   "дефолты: брокер без авторизации",
			mutate: func(c *Config) {},
		},
		{
			name: "полный набор кредов с TLS",
			mutate: func(c *Config) {
				c.KafkaUsername = "media"
				c.KafkaPassword = "secret"
				c.KafkaTLS = true
			},
		},
		{
			name: "только username",
			mutate: func(c *Config) {
				c.KafkaUsername = "media"
				c.KafkaTLS = true
			},
			wantErr: "KAFKA_USERNAME and KAFKA_PASSWORD must be set together",
		},
		{
			name: "только password",
			mutate: func(c *Config) {
				c.KafkaPassword = "secret"
				c.KafkaTLS = true
			},
			wantErr: "KAFKA_USERNAME and KAFKA_PASSWORD must be set together",
		},
		{
			name: "креды без TLS",
			mutate: func(c *Config) {
				c.KafkaUsername = "media"
				c.KafkaPassword = "secret"
			},
			wantErr: "KAFKA_TLS must be true",
		},
		{
			name: "poll timeout ноль",
			mutate: func(c *Config) {
				c.KafkaPollTimeout = 0
			},
			wantErr: "KAFKA_POLL_TIMEOUT must be > 0",
		},
		{
			name: "reconnect backoff ноль",
			mutate: func(c *Config) {
				c.KafkaReconnectMaxBackoff = 0
			},
			wantErr: "KAFKA_RECONNECT_MAX_BACKOFF must be > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := cfg.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

// TestValidate_KafkaSecurityIgnoredWhenDisabled: при выключенной Kafka
// сервис не создаёт клиента, поэтому кривая пара кредов не должна
// мешать старту.
func TestValidate_KafkaSecurityIgnoredWhenDisabled(t *testing.T) {
	cfg := configFromDefaults(t)
	cfg.KafkaEnabled = false
	cfg.KafkaUsername = "media"
	cfg.KafkaPassword = ""

	if err := cfg.validate(); err != nil {
		t.Fatalf("при KAFKA_ENABLED=false конфиг обязан проходить валидацию: %v", err)
	}
}

func TestConfigStringHidesKafkaCredentials(t *testing.T) {
	cfg := Config{
		KafkaUsername: "kafka-user",
		KafkaPassword: "kafka-secret",
		KafkaTLS:      true,
		PostgresDSN:   "postgres://user:pass@dbhost:5432/media",
	}

	s := cfg.String()

	if strings.Contains(s, "kafka-secret") {
		t.Error("String() must not contain KafkaPassword")
	}
	if strings.Contains(s, "kafka-user") {
		t.Error("String() must not contain KafkaUsername")
	}
	if !strings.Contains(s, "KafkaSASL:true") {
		t.Error("String() should report that SASL is configured")
	}
}
