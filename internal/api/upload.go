package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/upload"
	mediav1 "mediaservice/proto/media/v1"
)

// Upload реализует client-streaming RPC для загрузки медиафайлов.
// Первым сообщением обязательно должен быть UploadInit, последующими — чанки данных.
func (s *MediaServer) Upload(stream mediav1.MediaService_UploadServer) error {
	ctx := stream.Context()
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, ctx.Err().Error())
		}
		return status.Error(codes.Canceled, ctx.Err().Error())
	}

	callerID, err := s.resolveCaller(ctx)
	if err != nil {
		return err
	}

	idleTimeout := s.idleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}

	type recvResult struct {
		req *mediav1.UploadRequest
		err error
	}

	recvCh := make(chan recvResult, 1)
	recvCtx, cancelRecv := context.WithCancel(ctx)
	defer cancelRecv()

	go func() {
		for {
			req, err := stream.Recv()
			select {
			case recvCh <- recvResult{req: req, err: err}:
				if err != nil {
					return
				}
			case <-recvCtx.Done():
				return
			}
		}
	}()

	recvWithTimeout := func() (*mediav1.UploadRequest, error) {
		timer := time.NewTimer(idleTimeout)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
			}
			return nil, status.Error(codes.Canceled, ctx.Err().Error())
		case <-timer.C:
			return nil, status.Errorf(codes.DeadlineExceeded, "stream idle timeout: no data received within %s", idleTimeout)
		case res := <-recvCh:
			return res.req, res.err
		}
	}

	// 1. Получаем первое сообщение: строго UploadInit
	firstReq, err := recvWithTimeout()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "stream is empty: expected UploadInit")
		}
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "recv error: %v", err)
	}

	init := firstReq.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "first message must be UploadInit")
	}

	ownerID, err := uuid.Parse(init.OwnerId)
	if err != nil || ownerID == uuid.Nil {
		return status.Error(codes.InvalidArgument, "invalid owner_id UUID")
	}

	var expiresAt *time.Time
	if init.Ttl != nil && init.Ttl.IsValid() {
		d := init.Ttl.AsDuration()
		if d <= 0 {
			return status.Error(codes.InvalidArgument, "ttl duration must be positive")
		}
		t := time.Now().Add(d)
		expiresAt = &t
	}

	var makeThumbnail, transcode bool
	if proc := init.GetProcessing(); proc != nil {
		makeThumbnail = proc.GetMakeThumbnail()
		transcode = proc.GetTranscode()
	}

	params := media.UploadRequestParams{
		OwnerID:        ownerID,
		CallerID:       callerID,
		Filename:       init.Filename,
		MIME:           init.Mime,
		ExpectedSize:   init.ExpectedSize,
		IdempotencyKey: init.IdempotencyKey,
		MakeThumbnail:  makeThumbnail,
		Transcode:      transcode,
		ExpiresAt:      expiresAt,
	}

	// 2. Функция получения чанков из стрима с idle-timeout защитой (#78)
	chunkRecv := func() ([]byte, error) {
		req, err := recvWithTimeout()
		if err != nil {
			return nil, err // io.EOF или таймаут/сетевая ошибка
		}
		if req.GetInit() != nil {
			return nil, fmt.Errorf("%w: duplicate UploadInit message in stream", media.ErrInvalidArgument)
		}
		return req.GetChunk(), nil
	}

	// 3. Вызов доменной логики загрузки
	res, err := s.svc.Upload(ctx, params, chunkRecv)
	if err != nil {
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return status.Error(codes.DeadlineExceeded, ctx.Err().Error())
			}
			return status.Error(codes.Canceled, ctx.Err().Error())
		}
		if errors.Is(err, upload.ErrDiskFull) || errors.Is(err, media.ErrStorageQuotaExceeded) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return mapMediaError(err)
	}

	// 4. Отправляем ответ и закрываем стрим
	return stream.SendAndClose(&mediav1.UploadResponse{
		MediaId: res.MediaID.String(),
		Status:  mapRepoStatusToProto(res.Status),
	})
}

func mapRepoStatusToProto(st repo.MediaStatus) mediav1.MediaStatus {
	switch st {
	case repo.MediaStatusStored:
		return mediav1.MediaStatus_STORED
	case repo.MediaStatusProcessing:
		return mediav1.MediaStatus_PROCESSING
	case repo.MediaStatusReady:
		return mediav1.MediaStatus_READY
	case repo.MediaStatusFailed:
		return mediav1.MediaStatus_FAILED
	case repo.MediaStatusDeleting:
		return mediav1.MediaStatus_DELETING
	default:
		return mediav1.MediaStatus_STATUS_UNSPECIFIED
	}
}
