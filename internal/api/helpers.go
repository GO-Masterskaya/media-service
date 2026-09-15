package api

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GO-Masterskaya/media-service/internal/api/interceptors"
	"github.com/GO-Masterskaya/media-service/internal/storage"
)

// LBDrainWait — сколько ждать после NOT_SERVING перед GracefulStop.
// Счётчик смотрим только после not-ready, иначе окно для LB схлопывается зря.
func LBDrainWait(window time.Duration, inFlight int64) time.Duration {
	if inFlight == 0 {
		return 0
	}

	return window
}

// parseVariant нормализует variant и отвергает неизвестные до обращения к БД.
// r_360 не производится pipeline'ом — InvalidArgument, а не вечный NotFound.
func parseVariant(raw string) (storage.Variant, error) {
	if raw == "" {
		return storage.VariantOriginal, nil
	}

	v := storage.Variant(raw)

	switch v {
	case storage.VariantOriginal,
		storage.VariantThumb,
		storage.VariantPreview,
		storage.VariantR720:
		return v, nil

	default:
		return "", status.Errorf(
			codes.InvalidArgument,
			"unsupported variant: %s",
			raw,
		)
	}
}

// logAttrs добавляет correlation_id к slog-атрибутам,
// если он есть в context.
func logAttrs(ctx context.Context, attrs ...any) []any {
	if cid := interceptors.CorrelationIDFromContext(ctx); cid != "" {
		attrs = append(
			[]any{"correlation_id", cid},
			attrs...,
		)
	}

	return attrs
}
