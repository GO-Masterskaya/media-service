// Package api — тонкий gRPC-адаптер (SPEC §3).
// Полный сервер и регистрация в gRPC runtime — задача #5.
// Здесь реализован только DownloadStream (#12).
package api

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/media"
	mediav1 "mediaservice/proto/media/v1"
)

// DownloadStream отдаёт файл клиенту чанками (server-streaming).
// Per-caller stream limit и rate limit обеспечиваются interceptor'ами задачи #21.
func (s *MediaServer) DownloadStream(req *mediav1.DownloadStreamRequest, stream mediav1.MediaService_DownloadStreamServer) error {
	ctx := stream.Context()

	id, err := uuid.Parse(req.MediaId)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid media_id")
	}

	// Проверяем отмену до начала работы.
	if ctx.Err() != nil {
		return status.Error(codes.Canceled, "request canceled")
	}

	slog.Info("download started", slog.String("mediaID", req.MediaId), slog.String("variant", req.Variant))
	// счетчик отображающий размер отправленных байт для логирования
	var bytesSent int64
	defer slog.Info("download finished", slog.String("mediaID", req.MediaId), slog.Int64("bytesSent", bytesSent))

	err = s.svc.DownloadStream(ctx, id, req.Variant, func(chunk []byte) error {
		bytesSent += int64(len(chunk))
		return stream.Send(&mediav1.DownloadChunk{Data: chunk})
	})

	if err != nil {
		// гарантия получения клиентом Canceled/DeadlineExceeded, при мертвом контексте, независимо от ошибки из DownloadStream
		if ctx.Err() != nil {
			switch ctx.Err() {
			case context.DeadlineExceeded:
				return status.Error(codes.DeadlineExceeded, ctx.Err().Error())
			default:
				return status.Error(codes.Canceled, ctx.Err().Error())
			}
		}
		return mapDownloadError(err)
	}
	return nil
}

// mapDownloadError возвращает представление доменной ошибки в gRPC статусе
func mapDownloadError(err error) error {
	switch {
	case errors.Is(err, media.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, media.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, media.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
