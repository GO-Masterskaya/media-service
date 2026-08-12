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
	if cfg.KafkaPollTimeout != time.Second || cfg.KafkaReconnectMaxBackoff != 10*time.Second {
		t.Errorf("unexpected Kafka timeouts: poll=%s backoff=%s", cfg.KafkaPollTimeout, cfg.KafkaReconnectMaxBackoff)
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

func TestConfigStringRedactsSecrets(t *testing.T) {
	cfg := Config{
		GRPCAuthToken:  "super-secret-token",
		PostgresDSN:    "postgres://user:password@dbhost:5432/media",
		MinIOAccessKey: "access-key",
		MinIOSecretKey: "secret-key",
		MinIOEndpoint:  "minio:9000",
		MinIOBucket:    "media",
		KafkaUsername:  "kafka-user",
		KafkaPassword:  "kafka-password",
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
	if strings.Contains(s, "kafka-user") || strings.Contains(s, "kafka-password") {
		t.Error("String() must not contain Kafka credentials")
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
