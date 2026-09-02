package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GRPCAddr != ":9090" {
		t.Errorf("GRPCAddr: want :9090, got %s", cfg.GRPCAddr)
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
}

func TestLoadOverride(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("GRPC_ADDR", ":9999")
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
	}

	for _, tt := range tests {
		t.Run(tt.envKey, func(t *testing.T) {
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
	// Хост из DSN должен присутствовать (маскированный)
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
func TestValidate_RetentionKafkaEnabled(t *testing.T) {
	base := Config{
		GRPCAddr:               ":9090",
		MaxUploadBytes:         1,
		MIMEAllowlist:          []string{"image/*"},
		WorkerConcurrency:      1,
		QueueBuffer:            1,
		JobTimeout:             time.Second,
		JobLease:               time.Second,
		PollInterval:           time.Second,
		MaxJobAttempts:         1,
		FFMPEGTimeout:          time.Second,
		ShutdownTimeout:        time.Second,
		Rendition:              720,
		ThumbSecond:            1,
		PresignTTL:             time.Second,
		TTLReapInterval:        time.Second,
		RateLimitRPS:           50,
		MaxConcurrentStreams:   8,
		UploadTempDir:          "/tmp",
		UploadReserveBytes:     0,
		UploadStaleGrace:       time.Hour,
		UploadCleanupInterval:  time.Hour,
		PostgresDSN:            "postgres://u:p@h:5432/db",
		PostgresConnectTimeout: time.Second,
		PostgresQueryTimeout:   time.Second,
		MinIOEndpoint:          "minio:9000",
		MinIOAccessKey:         "k",
		MinIOSecretKey:         "s",
		MinIOBucket:            "b",
		KafkaEnabled:           true,
		KafkaBrokers:           []string{"kafka:9092"},
		KafkaTopic:             "t",
		KafkaDLQTopic:          "d",
		KafkaGroup:             "g",
		RetentionInterval:      time.Hour,
		RetentionOlderThan:     168 * time.Hour,
		RetentionBatchSize:     1000,
	}

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
