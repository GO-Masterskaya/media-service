package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// HealthServer реализует gRPC health checking.
type HealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	pool     *pgxpool.Pool
	mu       sync.RWMutex
	statuses map[string]grpc_health_v1.HealthCheckResponse_ServingStatus
	notify   chan struct{}
}

func NewHealthServer(pool *pgxpool.Pool) *HealthServer {
	return &HealthServer{
		pool: pool,
		statuses: map[string]grpc_health_v1.HealthCheckResponse_ServingStatus{
			"":                      grpc_health_v1.HealthCheckResponse_SERVING,
			"media.v1.MediaService": grpc_health_v1.HealthCheckResponse_SERVING,
		},
		notify: make(chan struct{}, 1),
	}
}

// SetServingStatus переключает readiness probe для сервиса.
func (s *HealthServer) SetServingStatus(service string, status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	s.mu.Lock()
	s.statuses[service] = status
	s.mu.Unlock()
	s.signalWatchers()
}

func (s *HealthServer) signalWatchers() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *HealthServer) servingStatus(service string) grpc_health_v1.HealthCheckResponse_ServingStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if st, ok := s.statuses[service]; ok {
		return st
	}
	if st, ok := s.statuses[""]; ok {
		return st
	}
	return grpc_health_v1.HealthCheckResponse_SERVING
}

// Ready возвращает true, если процесс принимает трафик (общий drain-флаг).
func (s *HealthServer) Ready() bool {
	return s.servingStatus("") == grpc_health_v1.HealthCheckResponse_SERVING
}

// Check отвечает на gRPC health probe.
func (s *HealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if req.Service != "" && req.Service != "media.v1.MediaService" {
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_UNKNOWN}, nil
	}
	if st := s.servingStatus(req.Service); st != grpc_health_v1.HealthCheckResponse_SERVING {
		return &grpc_health_v1.HealthCheckResponse{Status: st}, nil
	}
	if s.pool != nil {
		if err := s.pool.Ping(ctx); err != nil {
			slog.Error("health check failed", "error", err)
			return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, nil
		}
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch стримит изменения serving status (grpc_health_probe / xDS).
func (s *HealthServer) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	service := req.GetService()
	if service != "" && service != "media.v1.MediaService" {
		return status.Error(codes.NotFound, "unknown service")
	}

	last := grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN
	send := func() error {
		st := s.servingStatus(service)
		if st == last {
			return nil
		}
		last = st
		return stream.Send(&grpc_health_v1.HealthCheckResponse{Status: st})
	}

	if err := send(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-s.notify:
			if err := send(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := send(); err != nil {
				return err
			}
		}
	}
}

// HTTPHealthHandlers возвращает *http.ServeMux с /livez и /readyz.
// /readyz учитывает общий drain-флаг HealthServer (как gRPC Check).
func HTTPHealthHandlers(pool *pgxpool.Pool, health *HealthServer) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if health != nil && !health.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		if pool != nil {
			if err := pool.Ping(r.Context()); err != nil {
				slog.Error("readyz failed", "error", err)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"not_ready"}`))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	return mux
}
