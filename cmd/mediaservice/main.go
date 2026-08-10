package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"mediaservice/internal/config"
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

	// 3. Контекст жизни сервиса. Отменяется по сигналу ОС.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	// 4. Запуск компонентов
	//
	// Думаю пока можем реализовать запуск-остановку просто списком,  но в идеале хотелось бы в отдельный менеджер вынести.
	slog.Info("all components started successfully")

	// 5. Ждём сигнала завершения.
	<-ctx.Done()
	slog.Info("shutdown signal received")

	// 6. Graceful shutdown с дедлайном из config.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// 7. Останавливаем компоненты, передаем им shutdownCtx
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		// 7.1 Перестаём принимать новые соединения.
		// закрываем gRPC сервер

		// 7.2 Внутренние компоненты останавливаем параллельно:
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

		// 7.3 Инфраструктура — быстрые закрытия соединений.
		// pgPool.Close()
		// minioClient.Close()
	}()

	// 8. Дожидаемся остановки компонентов или истечения контекста.
	select {
	case <-shutdownCtx.Done():
		if shutdownCtx.Err() == context.DeadlineExceeded {
			slog.Error("timeout exceeded, components shutdown forcibly", "error", shutdownCtx.Err())
		}
		os.Exit(1)
	case <-shutdownDone:
		slog.Info("media service stopped gracefully")
	}
}
