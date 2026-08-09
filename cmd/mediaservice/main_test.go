package main

import (
	"bytes"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestMainGracefulShutdown(t *testing.T) {
	// Собираем бинарь сервиса во временную директорию.
	tmpDir := t.TempDir()
	bin := tmpDir + "/mediaservice"

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
		"POSTGRES_DSN=postgres://u:p@localhost/db",
		"MINIO_ENDPOINT=minio:9000",
		"MINIO_ACCESS_KEY=minioadmin",
		"MINIO_SECRET_KEY=minioadmin",
		"MINIO_BUCKET=media",
		"SHUTDOWN_TIMEOUT=2s",
	)

	var stdout bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stdout

	if err := run.Start(); err != nil {
		t.Fatalf("start process: %v", err)
	}

	// Даём процессу время на инициализацию.
	time.Sleep(200 * time.Millisecond)

	// Посылаем SIGTERM.
	if err := run.Process.Signal(syscall.SIGTERM); err != nil {
		run.Process.Kill()
		t.Fatalf("send SIGTERM: %v", err)
	}

	// Ждём завершения процесса.
	exitDone := make(chan error, 1)
	go func() { exitDone <- run.Wait() }()

	select {
	case err := <-exitDone:
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				t.Fatalf("process exited with code %d\noutput:\n%s", exitErr.ExitCode(), stdout.String())
			}
			t.Fatalf("wait error: %v", err)
		}
	case <-time.After(5 * time.Second):
		run.Process.Kill()
		t.Fatal("process did not exit after SIGTERM within timeout — possible goroutine leak")
	}

	// Проверяем, что в логе есть сообщение о graceful shutdown.
	outStr := stdout.String()
	if !bytes.Contains([]byte(outStr), []byte("media service stopped gracefully")) {
		t.Errorf("expected graceful shutdown message in output:\n%s", outStr)
	}
}