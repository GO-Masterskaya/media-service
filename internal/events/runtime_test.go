package events

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClient struct {
	mu sync.Mutex
	records [][]Record
	commits []Record
	dlqs []string
	dlqErr error
	closed bool
	pollStarted chan struct{}
}

func (c *fakeClient) Poll(ctx context.Context) ([]Record, error) {
	if c.pollStarted != nil { select { case <-c.pollStarted: default: close(c.pollStarted) } }
	c.mu.Lock()
	if len(c.records) > 0 { records := c.records[0]; c.records = c.records[1:]; c.mu.Unlock(); return records, nil }
	c.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *fakeClient) Commit(_ context.Context, r Record) error { c.mu.Lock(); defer c.mu.Unlock(); c.commits = append(c.commits, r); return nil }
func (c *fakeClient) ProduceDLQ(_ context.Context, _ Record, reason string) error { c.mu.Lock(); defer c.mu.Unlock(); c.dlqs = append(c.dlqs, reason); return c.dlqErr }
func (c *fakeClient) Close() { c.mu.Lock(); defer c.mu.Unlock(); c.closed = true }

func testConfig() Config { return Config{Enabled:true, Topic:"input", DLQTopic:"dlq", Group:"group", PollTimeout:time.Second, ReconnectMaxBackoff:time.Second} }

func TestDisabledDoesNotCreateClient(t *testing.T) {
	called := false
	r, err := start(context.Background(), Config{}, HandlerFunc(RejectingHandler), nil, func(Config) (client, error) { called = true; return nil, nil })
	if err != nil || r == nil { t.Fatalf("start disabled runtime: %v", err) }
	if called { t.Fatal("client must not be created when Kafka is disabled") }
}

func TestAckCommitsManually(t *testing.T) {
	c := &fakeClient{records:[][]Record{{{Topic:"input", Partition:2, Offset:7}}}, pollStarted:make(chan struct{})}
	r, err := start(context.Background(), testConfig(), HandlerFunc(func(context.Context, Record) (Result, error) { return Ack(), nil }), nil, func(Config) (client, error) { return c, nil })
	if err != nil { t.Fatal(err) }
	<-c.pollStarted
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.commits) == 1 })
	if err := r.Shutdown(context.Background()); err != nil { t.Fatal(err) }
}

func TestDLQPublishesBeforeCommit(t *testing.T) {
	c := &fakeClient{records:[][]Record{{{Topic:"input", Offset:9}}}, pollStarted:make(chan struct{})}
	r, err := start(context.Background(), testConfig(), HandlerFunc(func(context.Context, Record) (Result, error) { return DLQ("invalid payload"), nil }), nil, func(Config) (client, error) { return c, nil })
	if err != nil { t.Fatal(err) }
	<-c.pollStarted
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.dlqs) == 1 && len(c.commits) == 1 })
	c.mu.Lock(); if c.dlqs[0] != "invalid payload" { t.Fatalf("DLQ reason = %q", c.dlqs[0]) }; c.mu.Unlock()
	if err := r.Shutdown(context.Background()); err != nil { t.Fatal(err) }
}

func TestHandlerErrorPublishesDLQThenCommits(t *testing.T) {
	c := &fakeClient{records:[][]Record{{{Topic:"input", Offset:3}}}, pollStarted:make(chan struct{})}
	r, err := start(context.Background(), testConfig(), HandlerFunc(func(context.Context, Record) (Result, error) { return Result{}, errors.New("temporary failure") }), nil, func(Config) (client, error) { return c, nil })
	if err != nil { t.Fatal(err) }
	<-c.pollStarted
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.dlqs) == 1 && len(c.commits) == 1 })
	c.mu.Lock(); reason := c.dlqs[0]; c.mu.Unlock()
	if reason != "temporary failure" { t.Fatalf("DLQ reason = %q", reason) }
	if err := r.Shutdown(context.Background()); err != nil { t.Fatal(err) }
}

func TestDLQFailureDoesNotSkipLaterOffset(t *testing.T) {
	c := &fakeClient{records:[][]Record{{{Topic:"input", Offset:5}, {Topic:"input", Offset:6}}}, dlqErr:errors.New("DLQ unavailable"), pollStarted:make(chan struct{})}
	r, err := start(context.Background(), testConfig(), HandlerFunc(func(context.Context, Record) (Result, error) { return DLQ("invalid"), nil }), nil, func(Config) (client, error) { return c, nil })
	if err != nil { t.Fatal(err) }
	<-c.pollStarted
	time.Sleep(20*time.Millisecond)
	c.mu.Lock(); commits, dlqs := len(c.commits), len(c.dlqs); c.mu.Unlock()
	if commits != 0 { t.Fatalf("committed %d records after DLQ failure", commits) }
	if dlqs != 1 { t.Fatalf("attempted DLQ %d times for one batch", dlqs) }
	if err := r.Shutdown(context.Background()); err != nil { t.Fatal(err) }
}

func TestShutdownWaitsForInFlightHandler(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	c := &fakeClient{records:[][]Record{{{Topic:"input"}}}, pollStarted:make(chan struct{})}
	r, err := start(context.Background(), testConfig(), HandlerFunc(func(context.Context, Record) (Result, error) { close(started); <-release; return Ack(), nil }), nil, func(Config) (client, error) { return c, nil })
	if err != nil { t.Fatal(err) }
	<-started
	done := make(chan error, 1); go func() { done <- r.Shutdown(context.Background()) }()
	select { case <-done: t.Fatal("shutdown returned before handler finished"); case <-time.After(20*time.Millisecond): }
	close(release)
	select { case err := <-done: if err != nil { t.Fatal(err) }; case <-time.After(time.Second): t.Fatal("shutdown did not complete") }
	c.mu.Lock(); closed := c.closed; c.mu.Unlock(); if !closed { t.Fatal("client was not closed") }
}

func eventually(t *testing.T, predicate func() bool) {
	t.Helper(); deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) { if predicate() { return }; time.Sleep(time.Millisecond) }
	t.Fatal("condition was not met")
}
