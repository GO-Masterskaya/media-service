package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestAwaitEngineWorkersClosesInfraOnWaitTimeout(t *testing.T) {
	var closed atomic.Bool
	wait := func(ctx context.Context) error {
		<-ctx.Done()
		return context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	awaitEngineWorkers(ctx, wait, func() { closed.Store(true) })
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("awaitEngineWorkers blocked too long on wait timeout")
	}
	if !closed.Load() {
		t.Fatal("closeInfra must run after wait timeout")
	}
}

func TestAwaitEngineWorkersClosesInfraOnHappyPath(t *testing.T) {
	var closed atomic.Bool
	wait := func(ctx context.Context) error {
		return nil
	}
	awaitEngineWorkers(context.Background(), wait, func() { closed.Store(true) })
	if !closed.Load() {
		t.Fatal("closeInfra must run when wait succeeds")
	}
}

func TestAwaitEngineWorkersDoesNotCloseBeforeWaitReturns(t *testing.T) {
	var closed atomic.Bool
	released := make(chan struct{})
	wait := func(ctx context.Context) error {
		select {
		case <-released:
			if closed.Load() {
				return errors.New("closeInfra ran before wait returned")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan struct{})
	go func() {
		awaitEngineWorkers(context.Background(), wait, func() { closed.Store(true) })
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	if closed.Load() {
		t.Fatal("closeInfra must not run while wait is in progress")
	}
	close(released)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("awaitEngineWorkers did not finish")
	}
	if !closed.Load() {
		t.Fatal("closeInfra must run after wait")
	}
}

func TestAwaitEngineWorkersUsesParentDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	var sawParentDeadline atomic.Bool
	wait := func(ctx context.Context) error {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			sawParentDeadline.Store(true)
		}
		return ctx.Err()
	}
	var closed atomic.Bool
	awaitEngineWorkers(parent, wait, func() { closed.Store(true) })
	if !sawParentDeadline.Load() {
		t.Fatal("wait must see parent overallCtx deadline, not a fresh Background timer")
	}
	if !closed.Load() {
		t.Fatal("closeInfra must still run")
	}
}

func TestMainGracefulShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Process.Signal is not supported on Windows; covered on Linux CI")
	}

	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; shutdown test needs a reachable Postgres")
	}

	// Собираем бинарь сервиса во временную директорию.
	tmpDir := t.TempDir()
	bin := filepath.Join(tmpDir, "mediaservice")

	build := exec.Command("go", "build", "-o", bin, ".")
	out, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}

	// Запускаем бинарь с тестовым окружением.
	// Dir = tmpDir, чтобы случайный .env из репозитория не подтянулся.
	run := exec.Command(bin)
	run.Dir = tmpDir
	run.Env = append(os.Environ(),
		"POSTGRES_DSN="+dsn,
		"MINIO_ENDPOINT=minio:9000",
		"MINIO_ACCESS_KEY=minioadmin",
		"MINIO_SECRET_KEY=minioadmin",
		"MINIO_BUCKET=media",
		"SHUTDOWN_TIMEOUT=2s",
	)

	var stdout safeBuffer
	run.Stdout = &stdout
	run.Stderr = &stdout

	if err := run.Start(); err != nil {
		t.Fatalf("start process: %v", err)
	}

	exitDone := make(chan error, 1)
	go func() { exitDone <- run.Wait() }()

	// Ждём, пока main зарегистрирует NotifyContext и залогирует готовность.
	// Фиксированный sleep гоняет гонку: SIGTERM до NotifyContext → exit -1.
	const readyMarker = "all components started successfully"
	readyDeadline := time.After(15 * time.Second)
waitReady:
	for {
		select {
		case err := <-exitDone:
			t.Fatalf("process exited before ready: %v\noutput:\n%s", err, stdout.String())
		case <-readyDeadline:
			_ = run.Process.Kill()
			t.Fatalf("timeout waiting for %q\noutput:\n%s", readyMarker, stdout.String())
		default:
			if strings.Contains(stdout.String(), readyMarker) {
				break waitReady
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Посылаем SIGTERM.
	if err := run.Process.Signal(syscall.SIGTERM); err != nil {
		_ = run.Process.Kill()
		t.Fatalf("send SIGTERM: %v", err)
	}

	select {
	case err := <-exitDone:
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				t.Fatalf("process exited with code %d\noutput:\n%s", exitErr.ExitCode(), stdout.String())
			}
			t.Fatalf("wait error: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = run.Process.Kill()
		t.Fatal("process did not exit after SIGTERM within timeout — possible goroutine leak")
	}

	outStr := stdout.String()
	if !strings.Contains(outStr, "media service stopped gracefully") {
		t.Errorf("expected graceful shutdown message in output:\n%s", outStr)
	}
}

// safeBuffer — io.Writer для concurrent записи процесса и чтения из теста.
type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
