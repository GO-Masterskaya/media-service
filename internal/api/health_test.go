package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestHealthServer_SetServingStatus(t *testing.T) {
	t.Parallel()

	s := NewHealthServer(nil)
	s.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	resp, err := s.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("got %v, want NOT_SERVING", resp.Status)
	}
}

func TestHTTPHealthHandlers_Livez(t *testing.T) {
	t.Parallel()

	mux := HTTPHealthHandlers(nil)
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != `{"status":"ok"}` {
		t.Fatalf("got body %q", body)
	}
}
