package events

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultKafkaLogLevel — уровень по умолчанию для клиентов Kafka.
//
// Warn выбран осознанно. На Info franz-go пишет каждый запрос к брокеру,
// включая heartbeat раз в три секунды: лог утонет в шуме, а полезного
// в нём почти ничего. На Warn приходит именно то, ради чего заводился
// адаптер: недоступность брокера, отказы запросов, проблемы ребаланса.
const DefaultKafkaLogLevel = "warn"

// kafkaLogLevels — разбор строки из env в уровень franz-go.
// Используется и валидатором конфига, поэтому вынесен в отдельную карту.
var kafkaLogLevels = map[string]kgo.LogLevel{
	"none":  kgo.LogLevelNone,
	"error": kgo.LogLevelError,
	"warn":  kgo.LogLevelWarn,
	"info":  kgo.LogLevelInfo,
	"debug": kgo.LogLevelDebug,
}

// ParseKafkaLogLevel переводит значение KAFKA_LOG_LEVEL в уровень franz-go.
// Пустая строка означает уровень по умолчанию: так конфиг, собранный
// литералом в тесте, не обязан заполнять поле.
func ParseKafkaLogLevel(s string) (kgo.LogLevel, error) {
	if s == "" {
		s = DefaultKafkaLogLevel
	}
	level, ok := kafkaLogLevels[s]
	if !ok {
		return kgo.LogLevelNone, fmt.Errorf("unknown kafka log level %q", s)
	}
	return level, nil
}

// KafkaLogLevelNames возвращает допустимые значения KAFKA_LOG_LEVEL
// в отсортированном виде.
//
// Существует ради теста в internal/config: там набор продублирован,
// чтобы config не тянул за собой franz-go, и этот тест не даёт дубликату
// разъехаться молча.
func KafkaLogLevelNames() []string {
	names := make([]string, 0, len(kafkaLogLevels))
	for name := range kafkaLogLevels {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// kgoSlogAdapter реализует kgo.Logger поверх slog.
//
// Без него сообщения franz-go пропадают: библиотека пишет их в свой
// логгер, а логгер по умолчанию — no-op. Именно поэтому сервис
// с недоступным брокером молчал (см. #80).
type kgoSlogAdapter struct {
	log   *slog.Logger
	level kgo.LogLevel
}

// Level вызывается franz-go перед каждым сообщением, чтобы не тратить
// время на форматирование того, что всё равно отбросят. Интерфейс
// требует безопасности при конкурентном вызове: поле после создания
// не меняется, поэтому гонки нет.
func (a *kgoSlogAdapter) Level() kgo.LogLevel { return a.level }

// Log переносит сообщение в slog. franz-go гарантирует, что keyvals —
// чередование ключ-значение и ключи всегда строки, то есть формат
// полностью совпадает с вариадическими аргументами slog.
func (a *kgoSlogAdapter) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	// context.Background(), а не nil: slog.Logger.Log разыменовывает
	// контекст при передаче в Handler.
	a.log.Log(context.Background(), toSlogLevel(level), msg, keyvals...)
}

// toSlogLevel — соответствие уровней. LogLevelNone сюда не доходит:
// при нём franz-go не вызывает Log вовсе, но default оставлен, чтобы
// новый уровень в будущей версии библиотеки не потерялся молча.
func toSlogLevel(level kgo.LogLevel) slog.Level {
	switch level {
	case kgo.LogLevelError:
		return slog.LevelError
	case kgo.LogLevelWarn:
		return slog.LevelWarn
	case kgo.LogLevelInfo:
		return slog.LevelInfo
	case kgo.LogLevelDebug:
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

// kafkaLoggerOpt собирает опцию kgo.WithLogger для клиента.
//
// Общая точка для консьюмера и DLQ-продюсера: если подключить логгер
// только к одному, второй останется немым — ровно та ошибка, которую
// уже ловили с TLS и SASL в #72.
func kafkaLoggerOpt(log *slog.Logger, levelName string) (kgo.Opt, error) {
	level, err := ParseKafkaLogLevel(levelName)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return kgo.WithLogger(&kgoSlogAdapter{
		// Группа "kafka_client" отделяет сообщения библиотеки от наших:
		// иначе в логе не отличить "unable to open connection to broker"
		// от собственных ошибок сервиса.
		log:   log.With(slog.String("component", "kafka_client")),
		level: level,
	}), nil
}
