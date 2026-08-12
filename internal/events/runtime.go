// Package events owns Kafka runtime lifecycle. Event decoding and business
// handlers deliberately live outside this package.
package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var ErrHandlerNotConfigured = errors.New("kafka event handler is not configured")

type Config struct {
	Enabled             bool
	Brokers             []string
	Topic               string
	DLQTopic            string
	Group               string
	Username            string
	Password            string
	TLS                 bool
	PollTimeout         time.Duration
	ReconnectMaxBackoff time.Duration
}

type Record struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
}

type Result struct {
	ack       bool
	dlqReason string
}

func Ack() Result { return Result{ack: true} }

func DLQ(reason string) Result { return Result{dlqReason: reason} }

func (r Result) valid() bool { return r.ack != (r.dlqReason != "") }

type Handler interface {
	Handle(context.Context, Record) (Result, error)
}

type HandlerFunc func(context.Context, Record) (Result, error)

func (f HandlerFunc) Handle(ctx context.Context, record Record) (Result, error) { return f(ctx, record) }

// RejectingHandler preserves events in DLQ until a domain handler is wired.
func RejectingHandler(context.Context, Record) (Result, error) { return DLQ(ErrHandlerNotConfigured.Error()), nil }

type client interface {
	Poll(context.Context) ([]Record, error)
	Commit(context.Context, Record) error
	ProduceDLQ(context.Context, Record, string) error
	Close()
}

type clientFactory func(Config) (client, error)

type Runtime struct {
	cfg     Config
	handler Handler
	newClient clientFactory
	logger  *slog.Logger

	mu     sync.Mutex
	client client
	cancel context.CancelFunc
	done   chan struct{}
}

func Start(ctx context.Context, cfg Config, handler Handler, logger *slog.Logger) (*Runtime, error) {
	return start(ctx, cfg, handler, logger, newKafkaClient)
}

func start(parent context.Context, cfg Config, handler Handler, logger *slog.Logger, factory clientFactory) (*Runtime, error) {
	if logger == nil { logger = slog.Default() }
	r := &Runtime{cfg: cfg, handler: handler, newClient: factory, logger: logger}
	if !cfg.Enabled { return r, nil }
	if handler == nil { return nil, errors.New("kafka handler is required when Kafka is enabled") }

	c, err := factory(cfg)
	if err != nil { return nil, fmt.Errorf("create kafka client: %w", err) }
	ctx, cancel := context.WithCancel(parent)
	r.client, r.cancel, r.done = c, cancel, make(chan struct{})
	go r.run(ctx)
	return r, nil
}

func (r *Runtime) run(ctx context.Context) {
	defer close(r.done)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, r.cfg.PollTimeout)
		records, err := r.client.Poll(pollCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil { return }
			// A per-poll deadline is normal when the topic is idle; it is not a
			// connectivity failure and must not trigger reconnect backoff.
			if errors.Is(err, context.DeadlineExceeded) { continue }
			r.logger.Warn("kafka poll failed; retrying", "error", err, "backoff", backoff)
			if !wait(ctx, backoff) { return }
			backoff *= 2
			if backoff > r.cfg.ReconnectMaxBackoff { backoff = r.cfg.ReconnectMaxBackoff }
			continue
		}
		backoff = 100 * time.Millisecond
		for _, record := range records {
			if ctx.Err() != nil { return }
			// Never process a later offset in this batch after an uncommitted
			// record, otherwise its commit could skip the failed record.
			if !r.handle(ctx, record) { break }
		}
	}
}

// handle returns true only when the record was committed. Returning false
// stops the current batch and lets Kafka redeliver the record later.
func (r *Runtime) handle(ctx context.Context, record Record) bool {
	result, err := r.handler.Handle(ctx, record)
	if err != nil {
		result = DLQ(err.Error())
	}
	if !result.valid() {
		result = DLQ("kafka event handler returned invalid result")
	}
	if result.dlqReason != "" {
		if err := r.client.ProduceDLQ(ctx, record, result.dlqReason); err != nil {
			r.logger.Error("publish kafka event to DLQ", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
			return false
		}
	}
	if err := r.client.Commit(ctx, record); err != nil {
		r.logger.Error("commit kafka offset", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
		return false
	}
	return true
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.cancel == nil { r.mu.Unlock(); return nil }
	r.cancel()
	done, c := r.done, r.client
	r.cancel = nil
	r.mu.Unlock()
	select {
	case <-done:
		c.Close()
		return nil
	case <-ctx.Done():
		c.Close() // unblocks a client implementation that ignores cancelled poll contexts.
		return ctx.Err()
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select { case <-ctx.Done(): return false; case <-timer.C: return true }
}
