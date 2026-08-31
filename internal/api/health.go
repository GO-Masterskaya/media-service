package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// HealthServer стандартный реализует gRPC health checking.
type HealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	pool     *pgxpool.Pool
	mu       sync.RWMutex
	statuses map[string]grpc_health_v1.HealthCheckResponse_ServingStatus
}

func NewHealthServer(pool *pgxpool.Pool) *HealthServer {
	return &HealthServer{
		pool: pool,
		statuses: map[string]grpc_health_v1.HealthCheckResponse_ServingStatus{
			"":                      grpc_health_v1.HealthCheckResponse_SERVING,
			"media.v1.MediaService": grpc_health_v1.HealthCheckResponse_SERVING,
		},
	}
}

// SetServingStatus переключает readiness probe для сервиса.
func (s *HealthServer) SetServingStatus(service string, status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[service] = status
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

// Check отвечает на gRPC health probe.
// Пустой Service или "media.v1.MediaService" → readiness сервиса (проверяет Postgres).
func (s *HealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if req.Service != "" && req.Service != "media.v1.MediaService" {
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_UNKNOWN}, nil
	}
	if st := s.servingStatus(req.Service); st != grpc_health_v1.HealthCheckResponse_SERVING {
		return &grpc_health_v1.HealthCheckResponse{Status: st}, nil
	}
	if err := s.pool.Ping(ctx); err != nil {
		slog.Error("health check failed", "error", err)
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, nil
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch пока не реализован в v1, сейчас как заглушка.
func (s *HealthServer) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "watch is not implemented")
}

// HTTPHealthHandlers возвращает *http.ServeMux с /livez и /readyz.
func HTTPHealthHandlers(pool *pgxpool.Pool) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := pool.Ping(r.Context()); err != nil {
			slog.Error("readyz failed", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	return mux
}
