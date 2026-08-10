package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"mediaservice/internal/config"
	"mediaservice/internal/repo"
)

func main() {
	// 1. Загружаем конфиг.
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// 2. Настраиваем логгер (при выводе cfg секреты пропускаются).
	slog.SetDefault(config.NewLogger())
	slog.Info("starting media service", "config", cfg)

	// 3. Миграции до пула: standalone не поднимается на устаревшей схеме.
	if err := repo.RunMigrations(cfg.PostgresDSN); err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}

	// 4. Контекст жизни сервиса. Отменяется по сигналу ОС.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	pool, err := repo.NewPool(ctx, repo.PoolConfig{
		DSN:            cfg.PostgresDSN,
		ConnectTimeout: cfg.PostgresConnectTimeout,
		QueryTimeout:   cfg.PostgresQueryTimeout,
	})
	if err != nil {
		slog.Error("create pool", "error", err)
		os.Exit(1)
	}

	// 5. Запуск компонентов
	//
	// Думаю пока можем реализовать запуск-остановку просто списком,  но в идеале хотелось бы в отдельный менеджер вынести.
	slog.Info("all components started successfully")

	// 6. Ждём сигнала завершения.
	<-ctx.Done()
	slog.Info("shutdown signal received")

	// 7. Graceful shutdown с дедлайном из config.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// 8. Останавливаем компоненты, передаем им shutdownCtx
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		// 8.1 Перестаём принимать новые соединения.
		// закрываем gRPC сервер

		// 8.2 Внутренние компоненты останавливаем параллельно:
		// так даже если один зависнет при остановке, другой всё равно получит сигнал на остановку.
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			// engine.Shutdown(shutdownCtx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			// kafka.Shutdown(shutdownCtx)
		}()

		// Дождаться завершения стримов upload/download.
		wg.Add(1)
		go func() {
			defer wg.Done()
			//код Wait()'a стримов
		}()

		wg.Wait()

		// 8.3 Инфраструктура — быстрые закрытия соединений.
		pool.Close()
		// minioClient.Close()
		_ = shutdownCtx
	}()

	// 9. Дожидаемся остановки компонентов или истечения контекста.
	select {
	case <-shutdownDone:
		slog.Info("media service stopped gracefully")
	case <-shutdownCtx.Done():
		if shutdownCtx.Err() == context.DeadlineExceeded {
			slog.Error("timeout exceeded, components shutdown forcibly", "error", shutdownCtx.Err())
		}
		os.Exit(1)
	}
}
