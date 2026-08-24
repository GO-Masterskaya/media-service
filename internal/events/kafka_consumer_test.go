package events

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewKafkaConsumer_Validation(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	handler := func(ctx context.Context, raw []byte) Result { return Result{} }

	_, err := NewKafkaConsumer(KafkaConsumerConfig{}, handler, log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "brokers required")

	_, err = NewKafkaConsumer(KafkaConsumerConfig{Brokers: []string{"localhost:9092"}}, handler, log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "topic required")

	_, err = NewKafkaConsumer(KafkaConsumerConfig{Brokers: []string{"localhost:9092"}, Topic: "test"}, handler, log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "group id required")

	_, err = NewKafkaConsumer(KafkaConsumerConfig{Brokers: []string{"localhost:9092"}, Topic: "test", GroupID: "g"}, nil, log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler required")
}
