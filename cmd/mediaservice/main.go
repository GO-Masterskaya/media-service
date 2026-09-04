package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"mediaservice/internal/api"
	"mediaservice/internal/config"
	"mediaservice/internal/events"
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

	// 3. Создаем валидатор
	validator, err := protovalidate.New()
	if err != nil {
		slog.Error("failed to create validator", "error", err)
		os.Exit(1)
	}

	slog.Info("starting media service", "config", cfg)
	if err = os.MkdirAll(cfg.ProcessingTempDir, 0750); err != nil {
		slog.Error("failed to create processing temp dir", "dir", cfg.ProcessingTempDir, "error", err)
		os.Exit(1)
	}

	// 4. Миграции до пула: standalone не поднимается на устаревшей схеме.
	if err := repo.RunMigrations(cfg.PostgresDSN); err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}

	// 5. Контекст жизни сервиса. Отменяется по сигналу ОС.
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

	// 6. Storage (MinIO)
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

	// 7. Repos + Service
	mediaRepo := repo.NewPgMediaRepo(pool)
	derivRepo := repo.NewPgDerivativeRepo(pool)
	eventRepo := repo.NewPgProcessedEventRepo(pool)

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

	go rec.Run(ctx)

	// 8. Kafka consumer + DLQ + retention cleaner (#18, #27, #39)
	var (
		cleaner       events.ProcessedEventCleaner
		kafkaConsumer *events.KafkaConsumer
		dlqPublisher  events.DLQPublisher
	)
	if cfg.KafkaEnabled {
		dlqPublisher, err = events.NewKafkaDLQPublisher(cfg.KafkaBrokers, cfg.KafkaDLQTopic)
		if err != nil {
			slog.Error("dlq publisher init failed", "error", err)
			os.Exit(1)
		}

		host, _ := os.Hostname()
		consumerID := fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])

		handler, err := events.NewHandler(
			mediaSvc,
			eventRepo,
			dlqPublisher,
			consumerID,
			slog.Default(),
		)
		if err != nil {
			slog.Error("event handler init failed", "error", err)
			os.Exit(1)
		}

		kafkaConsumer, err = events.NewKafkaConsumer(
			events.KafkaConsumerConfig{
				Brokers: cfg.KafkaBrokers,
				Topic:   cfg.KafkaTopic,
				GroupID: cfg.KafkaGroup,
			},
			handler.Handle,
			slog.Default(),
		)
		if err != nil {
			slog.Error("kafka consumer init failed", "error", err)
			os.Exit(1)
		}
		go func() {
			if err := kafkaConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("kafka consumer run error", slog.Any("error", err))
			}
		}()

		cleaner = events.NewProcessedEventCleaner(
			eventRepo,
			events.RetentionConfig{
				Interval:   cfg.RetentionInterval,
				OlderThan:  cfg.RetentionOlderThan,
				BatchLimit: cfg.RetentionBatchSize,
			},
			slog.Default(),
		)
		go cleaner.Start(ctx)

		slog.Info("kafka components started",
			"topic", cfg.KafkaTopic,
			"dlq_topic", cfg.KafkaDLQTopic,
			"group", cfg.KafkaGroup,
		)
	}

	// 9. gRPC server с цепочкой interceptors.
	// MaxRecvMsgSize — лимит одного protobuf-сообщения (чанк), не всего upload.
	const maxRecvMsgSize = 16 << 20 // 16 MiB
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Minute,
			Time:              5 * time.Minute,
			Timeout:           20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(
			api.RecoveryInterceptor(),
			api.CorrelationIDInterceptor(),
			api.TokenInterceptor(cfg.GRPCAuthEnabled, cfg.GRPCAuthToken),
			api.ValidationInterceptor(validator),
		),
		grpc.ChainStreamInterceptor(
			api.RecoveryStreamInterceptor(),
			api.CorrelationIDStreamInterceptor(),
			api.TokenStreamInterceptor(cfg.GRPCAuthEnabled, cfg.GRPCAuthToken),
			api.ValidationStreamInterceptor(validator),
		),
	)
	healthServer := api.NewHealthServer(pool)
	mediav1.RegisterMediaServiceServer(grpcServer, api.NewMediaServer(mediaSvc, cfg.StrictOwnerCheck))
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("grpc listen", "error", err)
		os.Exit(1)
	}

	// Ошибки Serve/ListenAndServe не вызывают os.Exit из горутин —
	// отменяем root ctx и идём в единый shutdown path.
	fatalErr := make(chan error, 2)

	go func() {
		slog.Info("grpc server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			fatalErr <- fmt.Errorf("grpc serve: %w", err)
		}
	}()

	// HTTP health server (readyz разделяет drain-флаг с gRPC health).
	healthMux := api.HTTPHealthHandlers(pool, healthServer)
	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           healthMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		slog.Info("http health server listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatalErr <- fmt.Errorf("http health server: %w", err)
		}
	}()

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

	// 10. Processing Engine
	jobRepo := repo.NewPgJobRepo(pool)
	ownerID := uuid.NewString()
	repoAdapter := processing.NewRepoAdapter(jobRepo, ownerID, cfg.JobLease, cfg.MaxJobAttempts, processing.BackoffConfig{
		Base:   cfg.JobBackoffBase,
		Max:    cfg.JobBackoffMax,
		Jitter: cfg.JobBackoffJitter,
	})

	procRegistry := processing.NewRegistry()
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

	slog.Info("all components started successfully", "engine_owner", ownerID)

	// 11. Ждём сигнала завершения или фатальной ошибки Serve.
	var serveFatal error
	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	case serveFatal = <-fatalErr:
		slog.Error("server failed", "error", serveFatal)
		stop()
	}

	// 12. Graceful shutdown с дедлайном из config.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)

		// Сначала NOT_SERVING (gRPC + /readyz), затем окно для LB, потом drain.
		healthServer.SetServingStatus("media.v1.MediaService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		drainWindow := 2 * time.Second
		if cfg.ShutdownTimeout < 4*time.Second {
			drainWindow = cfg.ShutdownTimeout / 2
		}
		timer := time.NewTimer(drainWindow)
		select {
		case <-timer.C:
		case <-shutdownCtx.Done():
			timer.Stop()
		}

		grpcStopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(grpcStopped)
		}()

		select {
		case <-grpcStopped:
			slog.Info("grpc server stopped gracefully")
		case <-shutdownCtx.Done():
			slog.Warn("grpc graceful stop timeout, forcing stop")
			grpcServer.Stop()
		}

		// HTTP гасим после gRPC drain, чтобы /readyz оставался доступен в окне LB.
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown", "error", err)
		}

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rec.Shutdown(shutdownCtx); err != nil {
				slog.Error("reconciler shutdown", "error", err)
			}
		}()

		if kafkaConsumer != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := kafkaConsumer.Shutdown(shutdownCtx); err != nil {
					slog.Error("kafka consumer shutdown", "error", err)
				}
			}()
		}

		if cleaner != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := cleaner.Shutdown(shutdownCtx); err != nil {
					slog.Error("processed event cleaner shutdown", "error", err)
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			uploadStore.Stop()
			slog.Info("upload temp store stopped")
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := engine.Shutdown(shutdownCtx); err != nil {
				slog.Error("processing engine shutdown", "error", err)
			} else {
				slog.Info("processing engine stopped")
			}
		}()

		wg.Wait()

		// После timeout Shutdown воркеры могут ещё работать — ждём их
		// до закрытия pool, иначе пул закроется под живыми горутинами.
		engine.Wait()

		if dlqPublisher != nil {
			if err := dlqPublisher.Close(); err != nil {
				slog.Error("dlq publisher close", "error", err)
			}
		}

		pool.Close()
		if err := sto.Close(); err != nil {
			slog.Error("storage close", "error", err)
		}
	}()

	select {
	case <-shutdownDone:
		if serveFatal != nil {
			slog.Error("media service stopped after server failure", "error", serveFatal)
			os.Exit(1)
		}
		slog.Info("media service stopped gracefully")
	case <-shutdownCtx.Done():
		if shutdownCtx.Err() == context.DeadlineExceeded {
			slog.Error("timeout exceeded, components shutdown forcibly", "error", shutdownCtx.Err())
		}
		os.Exit(1)
	}
}
