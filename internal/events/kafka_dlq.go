package events

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaDLQPublisher отправляет события в dead-letter topic Kafka.
type KafkaDLQPublisher struct {
	client *kgo.Client
	topic  string
}

// KafkaDLQConfig — параметры продюсера DLQ.
//
// Раньше конструктор принимал brokers и topic по отдельности, и добавить
// к ним ещё три параметра значило бы получить функцию на пять аргументов
// одного типа. Структура заодно делает невозможным вызов, в котором про
// Security просто забыли: поле видно в литерале.
type KafkaDLQConfig struct {
	Brokers  []string
	Topic    string
	Security KafkaSecurity
	// Log — куда писать сообщения самого franz-go. nil → slog.Default().
	Log *slog.Logger
	// LogLevel — none/error/warn/info/debug. Пустая строка →
	// DefaultKafkaLogLevel.
	LogLevel string
}

func NewKafkaDLQPublisher(cfg KafkaDLQConfig) (*KafkaDLQPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("dlq topic required")
	}
	if err := cfg.Security.Validate(); err != nil {
		return nil, err
	}

	// Тот же набор опций, что и у консьюмера. Продюсер DLQ — полноценный
	// клиент Kafka: если TLS и SASL применить только к консьюмеру,
	// исходные payload'ы событий будут уходить в брокер открытым текстом.
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
	}
	opts = append(opts, cfg.Security.clientOpts()...)

	// Продюсер DLQ — такой же клиент Kafka, и немым он быть не должен:
	// молчащий продюсер означает потерю событий без единой строки в логе.
	logOpt, err := kafkaLoggerOpt(cfg.Log, cfg.LogLevel)
	if err != nil {
		return nil, err
	}
	opts = append(opts, logOpt)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &KafkaDLQPublisher{
		client: client,
		topic:  cfg.Topic,
	}, nil
}

func (p *KafkaDLQPublisher) Publish(ctx context.Context, original []byte, eventID uuid.UUID, reason string) error {
	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(eventID.String()),
		Value: original,
		Headers: []kgo.RecordHeader{
			{Key: "dlq_reason", Value: []byte(reason)},
			{Key: "dlq_timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce to dlq topic %s: %w", p.topic, err)
	}
	return nil
}

func (p *KafkaDLQPublisher) Close() error {
	p.client.Close()
	return nil
}
