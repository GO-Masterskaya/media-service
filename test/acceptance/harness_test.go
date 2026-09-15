package acceptance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/GO-Masterskaya/media-service/internal/api"
	"github.com/GO-Masterskaya/media-service/internal/api/interceptors"
	"github.com/GO-Masterskaya/media-service/internal/config"
	"github.com/GO-Masterskaya/media-service/internal/events"
	"github.com/GO-Masterskaya/media-service/internal/media"
	"github.com/GO-Masterskaya/media-service/internal/metrics"
	"github.com/GO-Masterskaya/media-service/internal/processing"
	"github.com/GO-Masterskaya/media-service/internal/repo"
	"github.com/GO-Masterskaya/media-service/internal/storage"
	"github.com/GO-Masterskaya/media-service/internal/upload"
	mediav1 "github.com/GO-Masterskaya/media-service/proto/media/v1"
)

const (
	testBucket = "media-acceptance"
	minioUser  = "minioadmin"
	minioPass  = "minioadmin"
	authToken  = "acceptance-test-token"
	bufSize    = 1 << 20

	kafkaTopic    = "media.events"
	kafkaDLQTopic = "media.events.dlq"
	kafkaGroup    = "media-acceptance"
	redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.3.6"
)

// AcceptanceSuite поднимает Postgres + MinIO + Redpanda + in-process gRPC
// (как main при KAFKA_ENABLED=true), плюс HTTP /metrics (#21).
type AcceptanceSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	pgContainer       testcontainers.Container
	minioContainer    testcontainers.Container
	redpandaContainer *redpanda.Container

	dsn           string
	minioEndpoint string
	kafkaBroker   string
	tempDir       string

	pool        *pgxpool.Pool
	mediaSvc    *media.Service
	mediaRepo   repo.MediaRepo
	jobRepo     repo.JobRepo
	sto         storage.Interface
	grpcServer  *grpc.Server
	httpServer  *http.Server
	health      *api.HealthServer
	client      mediav1.MediaServiceClient
	conn        *grpc.ClientConn
	lis         *bufconn.Listener
	engine      *processing.Engine
	reaper      *media.Reaper
	reconciler  *media.Reconciler
	promReg     *prometheus.Registry
	metricsURL  string
	httpBaseURL string

	kafkaConsumer *events.KafkaConsumer
	dlqPublisher  events.DLQPublisher
	kafkaProducer *kgo.Client

	ownerID uuid.UUID
}

func TestAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance tests in short mode")
	}
	if !dockerAvailable() {
		t.Skip("docker is not available, skipping acceptance tests")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not available, skipping acceptance tests")
	}
	suite.Run(t, new(AcceptanceSuite))
}

func (s *AcceptanceSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.ownerID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

	s.dsn = s.startPostgres()
	s.minioEndpoint = s.startMinIO()
	s.createBucket()
	s.kafkaBroker = s.startRedpanda()

	require.NoError(s.T(), repo.RunMigrations(s.dsn))

	dir, err := os.MkdirTemp("", "media-acceptance-*")
	require.NoError(s.T(), err)
	s.tempDir = dir

	s.startServer()
}

func (s *AcceptanceSuite) TearDownSuite() {
	if s.kafkaConsumer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = s.kafkaConsumer.Shutdown(shutdownCtx)
		cancel()
	}
	if s.dlqPublisher != nil {
		_ = s.dlqPublisher.Close()
	}
	if s.kafkaProducer != nil {
		s.kafkaProducer.Close()
	}
	if s.reaper != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.reaper.Shutdown(shutdownCtx)
		cancel()
	}
	if s.reconciler != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.reconciler.Shutdown(shutdownCtx)
		cancel()
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.engine != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = s.engine.Shutdown(shutdownCtx)
		cancel()
	}
	if s.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.httpServer.Shutdown(shutdownCtx)
		cancel()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.lis != nil {
		_ = s.lis.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
	}
	termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if s.pgContainer != nil {
		_ = s.pgContainer.Terminate(termCtx)
	}
	if s.minioContainer != nil {
		_ = s.minioContainer.Terminate(termCtx)
	}
	if s.redpandaContainer != nil {
		_ = s.redpandaContainer.Terminate(termCtx)
	}
}

func (s *AcceptanceSuite) authCtx() context.Context {
	return s.authCtxWithCaller("")
}

func (s *AcceptanceSuite) authCtxWithCaller(callerID string) context.Context {
	pairs := []string{
		"authorization", "Bearer " + authToken,
		"x-owner-id", s.ownerID.String(),
	}
	if callerID != "" {
		pairs = append(pairs, "x-caller-id", callerID)
	}
	return metadata.NewOutgoingContext(s.ctx, metadata.Pairs(pairs...))
}

// limitedClientOpts — отдельный gRPC-сервер с жёсткими лимитами (#21),
// чтобы suite-wide лимиты оставались высокими и не флакали остальные кейсы.
type limitedClientOpts struct {
	rps        rate.Limit
	burst      int
	maxStreams int
	allowlist  []string
}

func (s *AcceptanceSuite) newLimitedClient(t *testing.T, opts limitedClientOpts) mediav1.MediaServiceClient {
	t.Helper()
	require.NotNil(t, s.mediaSvc)

	if opts.burst <= 0 {
		opts.burst = 1
	}
	if opts.maxStreams <= 0 {
		opts.maxStreams = 8
	}
	if len(opts.allowlist) == 0 {
		opts.allowlist = []string{"caller-a", "caller-b"}
	}

	validator, err := protovalidate.New()
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	grpcMetrics := metrics.NewGRPCMetrics(reg)
	allowlist := interceptors.NewCallerAllowlist(opts.allowlist)
	rateLimiter := interceptors.NewRateLimiter(opts.rps, opts.burst)
	streamLimiter := interceptors.NewStreamLimiter(opts.maxStreams)

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(16<<20),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, allowlist,
			)...,
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, streamLimiter, allowlist,
			)...,
		),
	)
	mediav1.RegisterMediaServiceServer(srv, api.NewMediaServer(s.mediaSvc, false, 30*time.Second))

	lis := bufconn.Listen(bufSize)
	go func() { _ = srv.Serve(lis) }()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet-limited",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		srv.Stop()
		_ = conn.Close()
		_ = lis.Close()
	})

	return mediav1.NewMediaServiceClient(conn)
}

// newQuotaLimitedClient — отдельный MediaService с крошечной квотой (#22/#19 ENOSPC/quota).
func (s *AcceptanceSuite) newQuotaLimitedClient(t *testing.T, quotaBytes int64) mediav1.MediaServiceClient {
	t.Helper()
	require.NotNil(t, s.mediaRepo)
	require.NotNil(t, s.sto)

	svc := media.NewService(s.mediaRepo, repo.NewPgDerivativeRepo(s.pool), s.sto, 15*time.Minute, slog.Default())
	uploadMetrics := upload.NewMetrics(prometheus.NewRegistry())
	uploadStore, err := upload.New(upload.Config{
		Dir:             filepath.Join(s.tempDir, "uploads-quota"),
		MaxFileSize:     50 << 20,
		ReserveBytes:    1 << 20,
		StaleGrace:      time.Hour,
		CleanupInterval: time.Hour,
	}, uploadMetrics, slog.Default())
	require.NoError(t, err)
	svc.SetUploadConfig(uploadStore, media.DefaultProber{}, 50<<20,
		[]string{"image/*", "video/*", "audio/*"}, quotaBytes)

	validator, err := protovalidate.New()
	require.NoError(t, err)
	reg := prometheus.NewRegistry()
	grpcMetrics := metrics.NewGRPCMetrics(reg)
	allowlist := interceptors.NewCallerAllowlist([]string{"caller-a"})
	rateLimiter := interceptors.NewRateLimiter(rate.Limit(1000), 1000)
	streamLimiter := interceptors.NewStreamLimiter(32)

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(16<<20),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, allowlist,
			)...,
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, streamLimiter, allowlist,
			)...,
		),
	)
	mediav1.RegisterMediaServiceServer(srv, api.NewMediaServer(svc, false, 30*time.Second))

	lis := bufconn.Listen(bufSize)
	go func() { _ = srv.Serve(lis) }()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet-quota",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		srv.Stop()
		_ = conn.Close()
		_ = lis.Close()
	})

	return mediav1.NewMediaServiceClient(conn)
}

func (s *AcceptanceSuite) startServer() {
	t := s.T()

	pool, err := repo.NewPool(s.ctx, repo.PoolConfig{
		DSN:            s.dsn,
		ConnectTimeout: 10 * time.Second,
		QueryTimeout:   30 * time.Second,
	})
	require.NoError(t, err)
	s.pool = pool

	sto, err := storage.NewMinIO(storage.MinIOConfig{
		Endpoint:  s.minioEndpoint,
		AccessKey: minioUser,
		SecretKey: minioPass,
		Bucket:    testBucket,
		UseSSL:    false,
	}, slog.Default())
	require.NoError(t, err)

	mediaRepo := repo.NewPgMediaRepo(pool)
	derivRepo := repo.NewPgDerivativeRepo(pool)
	eventRepo := repo.NewPgProcessedEventRepo(pool)
	s.mediaRepo = mediaRepo
	s.sto = sto
	s.mediaSvc = media.NewService(mediaRepo, derivRepo, sto, 15*time.Minute, slog.Default())
	mediaSvc := s.mediaSvc

	s.promReg = prometheus.NewRegistry()
	reg := s.promReg
	uploadMetrics := upload.NewMetrics(reg)
	uploadStore, err := upload.New(upload.Config{
		Dir:             filepath.Join(s.tempDir, "uploads"),
		MaxFileSize:     50 << 20,
		ReserveBytes:    1 << 20,
		StaleGrace:      time.Hour,
		CleanupInterval: time.Hour,
	}, uploadMetrics, slog.Default())
	require.NoError(t, err)
	mediaSvc.SetUploadConfig(uploadStore, media.DefaultProber{}, 50<<20,
		[]string{"image/*", "video/*", "audio/*"}, 0)

	procCfg := &config.Config{
		ProcessingTempDir: filepath.Join(s.tempDir, "processing"),
		// 0 — первый кадр; на коротких тестовых роликах ThumbSecond=1 даёт EOF.
		ThumbSecond:   0,
		FFMPEGTimeout: 2 * time.Minute,
		Rendition:     720,
	}
	require.NoError(t, os.MkdirAll(procCfg.ProcessingTempDir, 0o750))

	engineCfg := processing.Config{
		WorkerConcurrency: 2,
		PollInterval:      200 * time.Millisecond,
		JobTimeout:        2 * time.Minute,
		LeaseDuration:     30 * time.Second,
		MaxAttempts:       3,
	}
	jobRepo := repo.NewPgJobRepo(pool)
	s.jobRepo = jobRepo
	engineOwner := uuid.NewString()
	repoAdapter := processing.NewRepoAdapter(jobRepo, engineOwner, engineCfg.LeaseDuration, engineCfg.MaxAttempts, processing.BackoffConfig{
		Base:   time.Second,
		Max:    30 * time.Second,
		Jitter: 0.1,
	}, 50)

	thumbnailHandler := processing.NewThumbnailHandler(sto, derivRepo, procCfg, slog.Default())
	transcodeHandler := processing.NewTranscodeHandler(sto, derivRepo, procCfg, slog.Default())

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

	procMetrics := processing.NewMetrics(reg)
	s.engine = processing.NewEngine(engineCfg, repoAdapter, procRegistry, procMetrics)
	require.NoError(t, s.engine.Start(s.ctx))

	// TTL reaper с коротким интервалом для acceptance (§7).
	s.reaper = media.NewReaperWithConfig(mediaSvc, media.ReaperConfig{
		Interval:  200 * time.Millisecond,
		BatchSize: 50,
		DryRun:    false,
	}, slog.Default(), reg)
	go s.reaper.Run(s.ctx)

	// Delete reconciler / orphans (#24) — короткий grace для acceptance.
	s.reconciler = media.NewReconciler(mediaRepo, sto, media.ReconcilerConfig{
		Interval:    200 * time.Millisecond,
		GracePeriod: time.Second,
		BatchSize:   50,
		DryRun:      false,
	}, slog.Default())
	go s.reconciler.Run(s.ctx)

	// Kafka path (KAFKA_ENABLED=true).
	dlq, err := events.NewKafkaDLQPublisher(events.KafkaDLQConfig{
		Brokers:  []string{s.kafkaBroker},
		Topic:    kafkaDLQTopic,
		Log:      slog.Default(),
		LogLevel: "warn",
	})
	require.NoError(t, err)
	s.dlqPublisher = dlq

	handler, err := events.NewHandlerWithConfig(
		mediaSvc,
		eventRepo,
		dlq,
		"acceptance-consumer",
		events.HandlerConfig{LeaseDuration: 30 * time.Second, MaxAttempts: 3},
		slog.Default(),
	)
	require.NoError(t, err)

	s.kafkaConsumer, err = events.NewKafkaConsumer(
		events.KafkaConsumerConfig{
			Brokers:             []string{s.kafkaBroker},
			Topic:               kafkaTopic,
			GroupID:             kafkaGroup,
			PollTimeout:         500 * time.Millisecond,
			ReconnectMaxBackoff: 2 * time.Second,
			LogLevel:            "warn",
		},
		handler.Handle,
		slog.Default(),
	)
	require.NoError(t, err)
	go func() {
		_ = s.kafkaConsumer.Run(s.ctx)
	}()

	s.kafkaProducer, err = kgo.NewClient(
		kgo.SeedBrokers(s.kafkaBroker),
		kgo.AllowAutoTopicCreation(),
	)
	require.NoError(t, err)

	validator, err := protovalidate.New()
	require.NoError(t, err)

	// Высокие лимиты — чтобы suite не упирался в #21 на poll/GetMedia.
	// Жёсткие лимиты проверяются отдельным newLimitedClient.
	callerAllowlist := interceptors.NewCallerAllowlist([]string{"caller-a", "caller-b"})
	grpcMetrics := metrics.NewGRPCMetrics(reg)
	rateLimiter := interceptors.NewRateLimiter(rate.Limit(1000), 1000)
	go interceptors.StartRateLimiterCleanup(s.ctx, rateLimiter, time.Minute, 10*time.Minute)
	streamLimiter := interceptors.NewStreamLimiter(32)

	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(16<<20),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, callerAllowlist,
			)...,
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, streamLimiter, callerAllowlist,
			)...,
		),
	)
	s.health = api.NewHealthServer(pool)
	mediav1.RegisterMediaServiceServer(s.grpcServer, api.NewMediaServer(mediaSvc, false, 30*time.Second))

	s.lis = bufconn.Listen(bufSize)
	go func() {
		_ = s.grpcServer.Serve(s.lis)
	}()

	healthMux := api.HTTPHealthHandlers(pool, s.health)
	healthMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s.httpServer = &http.Server{
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.httpBaseURL = "http://" + httpLis.Addr().String()
	s.metricsURL = s.httpBaseURL + "/metrics"
	go func() {
		_ = s.httpServer.Serve(httpLis)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return s.lis.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	s.conn = conn
	s.client = mediav1.NewMediaServiceClient(conn)
}

func dockerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "info").Run() == nil
}

func (s *AcceptanceSuite) startPostgres() string {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "media",
			"POSTGRES_PASSWORD": "media",
			"POSTGRES_DB":       "media",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	var err error
	s.pgContainer, err = testcontainers.GenericContainer(s.ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(s.T(), err)

	host, err := s.pgContainer.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := s.pgContainer.MappedPort(s.ctx, "5432")
	require.NoError(s.T(), err)
	return fmt.Sprintf("postgres://media:media@%s:%s/media?sslmode=disable", host, port.Port())
}

func (s *AcceptanceSuite) startMinIO() string {
	req := testcontainers.ContainerRequest{
		Image:        "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z",
		ExposedPorts: []string{"9000/tcp"},
		Cmd:          []string{"server", "/data"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     minioUser,
			"MINIO_ROOT_PASSWORD": minioPass,
		},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000").WithStartupTimeout(60 * time.Second),
	}
	var err error
	s.minioContainer, err = testcontainers.GenericContainer(s.ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(s.T(), err)

	host, err := s.minioContainer.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := s.minioContainer.MappedPort(s.ctx, "9000")
	require.NoError(s.T(), err)
	return fmt.Sprintf("%s:%s", host, port.Port())
}

func (s *AcceptanceSuite) startRedpanda() string {
	ctr, err := redpanda.Run(s.ctx, redpandaImage, redpanda.WithAutoCreateTopics())
	require.NoError(s.T(), err)
	s.redpandaContainer = ctr

	broker, err := ctr.KafkaSeedBroker(s.ctx)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), broker)
	s.ensureKafkaTopics(broker)
	return broker
}

func (s *AcceptanceSuite) ensureKafkaTopics(broker string) {
	t := s.T()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer cl.Close()

	admin := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	res, err := admin.CreateTopics(ctx, 1, 1, nil, kafkaTopic, kafkaDLQTopic)
	require.NoError(t, err)
	for topic, tr := range res {
		if tr.Err != nil && !errors.Is(tr.Err, kerr.TopicAlreadyExists) {
			require.NoError(t, tr.Err, "create topic %s", topic)
		}
	}
}

func (s *AcceptanceSuite) createBucket() {
	mc, err := minio.New(s.minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUser, minioPass, ""),
		Secure: false,
	})
	require.NoError(s.T(), err)
	require.NoError(s.T(), mc.MakeBucket(s.ctx, testBucket, minio.MakeBucketOptions{}))
}

func httpGetBytes(t *testing.T, url string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "presigned GET status")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return body
}

func (s *AcceptanceSuite) produceKafka(t *testing.T, topic string, key, value []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res := s.kafkaProducer.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	})
	require.NoError(t, res.FirstErr())
}

func (s *AcceptanceSuite) produceDetach(t *testing.T, eventID, mediaID, ownerID uuid.UUID) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]string{
		"media_id": mediaID.String(),
		"owner_id": ownerID.String(),
	})
	require.NoError(t, err)
	env := map[string]any{
		"event_id":   eventID.String(),
		"event_type": "media.detach",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"payload":    json.RawMessage(payload),
	}
	raw, err := json.Marshal(env)
	require.NoError(t, err)
	s.produceKafka(t, kafkaTopic, []byte(eventID.String()), raw)
	return raw
}

func (s *AcceptanceSuite) waitMediaGone(t *testing.T, mediaID string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		_, err := s.client.GetMedia(s.authCtx(), &mediav1.GetMediaRequest{MediaId: mediaID})
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for media %s to be deleted via kafka detach", mediaID)
}

func (s *AcceptanceSuite) waitDLQContains(t *testing.T, wantSubstring string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(s.kafkaBroker),
		kgo.ConsumerGroup("acceptance-dlq-reader-"+uuid.NewString()),
		kgo.ConsumeTopics(kafkaDLQTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)
	defer cl.Close()

	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if fe.Err != nil && ctx.Err() == nil {
					t.Logf("dlq poll error: %v", fe.Err)
				}
			}
		}
		var found bool
		fetches.EachRecord(func(r *kgo.Record) {
			if wantSubstring == "" ||
				string(r.Value) == wantSubstring ||
				bytes.Contains(r.Value, []byte(wantSubstring)) {
				found = true
			}
		})
		if found {
			return
		}
	}
	t.Fatalf("timeout waiting for DLQ message containing %q", wantSubstring)
}
