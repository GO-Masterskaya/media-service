package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"mediaservice/internal/api"
	"mediaservice/internal/config"
	"mediaservice/internal/media"
	"mediaservice/internal/processing"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	"mediaservice/internal/upload"
	mediav1 "mediaservice/proto/media/v1"
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
	if err = os.MkdirAll(cfg.ProcessingTempDir, 0750); err != nil {
		slog.Error("failed to create processing temp dir", "dir", cfg.ProcessingTempDir, "error", err)
		os.Exit(1)
	}

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

	// +++ ADDED: 4.5 Storage (MinIO)
	sto, err := storage.NewMinIO(storage.MinIOConfig{
		Endpoint:  cfg.MinIOEndpoint,
		AccessKey: cfg.MinIOAccessKey,
		SecretKey: cfg.MinIOSecretKey,
		Bucket:    cfg.MinIOBucket,
		UseSSL:    cfg.MinIOUseSSL,
	}, slog.Default())
	if err != nil {
		slog.Error("storage init failed", "error", err)
		os.Exit(1)
	}

	// +++ ADDED: 4.6 Repos + Service
	mediaRepo := repo.NewPgMediaRepo(pool)
	derivRepo := repo.NewPgDerivativeRepo(pool)
	transcodeHandler := processing.NewTranscodeHandler(sto, derivRepo, cfg, slog.Default())
	thumbnailHandler := processing.NewThumbnailHandler(sto, derivRepo, cfg, slog.Default())

	mediaSvc := media.NewService(mediaRepo, derivRepo, sto, cfg.PresignTTL, slog.Default())

	reconcilerCfg := media.ReconcilerConfig{
		Interval:    cfg.ReconcilerInterval,
		GracePeriod: cfg.ReconcilerGracePeriod,
		BatchSize:   cfg.ReconcilerBatchSize,
		DryRun:      cfg.ReconcilerDryRun,
	}
	rec := media.NewReconciler(mediaRepo, sto, reconcilerCfg, slog.Default())

	// В горутине:
	go rec.Run(ctx)

	// +++ ADDED: 4.7 gRPC server + registration
	grpcServer := grpc.NewServer()
	mediav1.RegisterMediaServiceServer(grpcServer, api.NewMediaServer(mediaSvc, cfg.StrictOwnerCheck))

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("grpc listen", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("grpc server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcLis); err != nil && err != grpc.ErrServerStopped {
			slog.Error("grpc serve", "error", err)
		}
	}()

	// 5. Запуск компонентов

	uploadMetrics := upload.NewMetrics(nil)
	uploadStore, err := upload.New(upload.Config{
		Dir:             cfg.UploadTempDir,
		MaxFileSize:     cfg.MaxUploadBytes,
		ReserveBytes:    cfg.UploadReserveBytes,
		StaleGrace:      cfg.UploadStaleGrace,
		CleanupInterval: cfg.UploadCleanupInterval,
	}, uploadMetrics, slog.Default())
	if err != nil {
		slog.Error("create upload temp store", "error", err)
		os.Exit(1)
	}

	// 5.1 Processing Engine
	jobRepo := repo.NewPgJobRepo(pool)
	ownerID := uuid.NewString() // уникальный ID этого экземпляра сервиса
	repoAdapter := processing.NewRepoAdapter(jobRepo, ownerID, cfg.JobLease, cfg.MaxJobAttempts)

	procRegistry := processing.NewRegistry()
	// Регистрация обработчиков: адаптируем ProcessThumbnail/ProcessTranscode к Handler.Handle(ctx, Job).
	// Загружаем MediaRecord из БД по job.MediaID для получения актуального SourceKey, OwnerID и Kind.
	procRegistry.Register("thumbnail", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		m, err := mediaRepo.GetByID(ctx, job.MediaID)
		if err != nil {
			return fmt.Errorf("fetch media for thumbnail: %w", err)
		}
		_, err = thumbnailHandler.ProcessThumbnail(ctx, processing.MediaRecord{
			ID:        m.ID,
			OwnerID:   m.OwnerID,
			Kind:      processing.Kind(m.Kind),
			SourceKey: m.StorageKey,
		})
		return err
	}))
	procRegistry.Register("transcode", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		m, err := mediaRepo.GetByID(ctx, job.MediaID)
		if err != nil {
			return fmt.Errorf("fetch media for transcode: %w", err)
		}
		_, err = transcodeHandler.ProcessTranscode(ctx, processing.MediaRecord{
			ID:        m.ID,
			OwnerID:   m.OwnerID,
			Kind:      processing.Kind(m.Kind),
			SourceKey: m.StorageKey,
		})
		return err
	}))

	procMetrics := processing.NewMetrics(prometheus.DefaultRegisterer)

	engine := processing.NewEngine(processing.Config{
		WorkerConcurrency: cfg.WorkerConcurrency,
		PollInterval:      cfg.PollInterval,
		JobTimeout:        cfg.JobTimeout,
		LeaseDuration:     cfg.JobLease,
		MaxAttempts:       cfg.MaxJobAttempts,
	}, repoAdapter, procRegistry, procMetrics)

	if err := engine.Start(ctx); err != nil {
		slog.Error("start processing engine", "error", err)
		os.Exit(1)
	}

	slog.Info("all components started successfully",
		"engine_owner", ownerID,
	)

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
		grpcServer.GracefulStop()

		// 8.2 Внутренние компоненты останавливаем параллельно:
		// так даже если один зависнет при остановке, другой всё равно получит сигнал на остановку.
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rec.Shutdown(shutdownCtx); err != nil {
				slog.Error("reconciler shutdown", "error", err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			uploadStore.Stop()
			slog.Info("upload temp store stopped")
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.Stop()
			slog.Info("processing engine stopped")
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
