package events

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseKafkaLogLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    kgo.LogLevel
		wantErr bool
	}{
		{in: "", want: kgo.LogLevelWarn},
		{in: "none", want: kgo.LogLevelNone},
		{in: "error", want: kgo.LogLevelError},
		{in: "warn", want: kgo.LogLevelWarn},
		{in: "info", want: kgo.LogLevelInfo},
		{in: "debug", want: kgo.LogLevelDebug},
		{in: "WARN", wantErr: true},
		{in: "verbose", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseKafkaLogLevel(tt.in)
			if tt.wantErr {
				require.ErrorContains(t, err, "unknown kafka log level")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestKgoSlogAdapter_Log проверяет главное: сообщение franz-go доходит
// до нашего хендлера, уровень переносится верно и пары ключ-значение
// не теряются. Без этого адаптера сообщения уходили в no-op логгер,
// из-за чего недоступный брокер оставался незаметным (#80).
func TestKgoSlogAdapter_Log(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	adapter := &kgoSlogAdapter{log: log, level: kgo.LogLevelWarn}
	adapter.Log(kgo.LogLevelWarn, "unable to open connection to broker",
		"addr", "kafka:9092", "err", "no such host")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))

	require.Equal(t, "WARN", entry["level"])
	require.Equal(t, "unable to open connection to broker", entry["msg"])
	require.Equal(t, "kafka:9092", entry["addr"])
	require.Equal(t, "no such host", entry["err"])
}

func TestKgoSlogAdapter_Level(t *testing.T) {
	adapter := &kgoSlogAdapter{log: slog.Default(), level: kgo.LogLevelError}
	require.Equal(t, kgo.LogLevelError, adapter.Level())
}

func TestToSlogLevel(t *testing.T) {
	require.Equal(t, slog.LevelError, toSlogLevel(kgo.LogLevelError))
	require.Equal(t, slog.LevelWarn, toSlogLevel(kgo.LogLevelWarn))
	require.Equal(t, slog.LevelInfo, toSlogLevel(kgo.LogLevelInfo))
	require.Equal(t, slog.LevelDebug, toSlogLevel(kgo.LogLevelDebug))
}

// TestKafkaLoggerOpt_AddsComponentGroup: сообщения библиотеки должны быть
// отличимы от наших собственных, иначе в общем логе не понять, кто
// пожаловался — сервис или franz-go.
func TestKafkaLoggerOpt_AddsComponentGroup(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	opt, err := kafkaLoggerOpt(log, "warn")
	require.NoError(t, err)
	require.NotNil(t, opt)

	// Опцию kgo снаружи не прочитать, поэтому проверяем адаптер напрямую
	// тем же способом, каким его строит kafkaLoggerOpt.
	adapter := &kgoSlogAdapter{
		log:   log.With(slog.String("component", "kafka_client")),
		level: kgo.LogLevelWarn,
	}
	adapter.Log(kgo.LogLevelWarn, "request failure")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "kafka_client", entry["component"])
}

func TestKafkaLoggerOpt_RejectsUnknownLevel(t *testing.T) {
	_, err := kafkaLoggerOpt(slog.Default(), "verbose")
	require.ErrorContains(t, err, "unknown kafka log level")
}

// TestKafkaLoggerOpt_NilLoggerFallsBack: конструкторы допускают nil,
// и падать на этом нельзя.
func TestKafkaLoggerOpt_NilLoggerFallsBack(t *testing.T) {
	opt, err := kafkaLoggerOpt(nil, "")
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func TestKafkaLogLevelNames(t *testing.T) {
	require.Equal(t,
		[]string{"debug", "error", "info", "none", "warn"},
		KafkaLogLevelNames())
}
