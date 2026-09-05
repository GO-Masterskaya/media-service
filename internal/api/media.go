package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	mediav1 "mediaservice/proto/media/v1"
)

type mediaPageToken struct {
	Version   int       `json:"v"`
	OwnerID   uuid.UUID `json:"owner_id"`
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

type MediaServer struct {
	mediav1.UnimplementedMediaServiceServer
	svc              *media.Service
	strictOwnerCheck bool
}

func NewMediaServer(svc *media.Service, strictOwnerCheck bool) *MediaServer {
	return &MediaServer{
		svc:              svc,
		strictOwnerCheck: strictOwnerCheck,
	}
}

// callerIDFromMetadata извлекает owner_id из gRPC metadata.
// При отсутствии metadata или x-owner-id возвращает uuid.Nil, nil —
// проверка обязательности лежит на вызывающем (strictOwnerCheck).
//
// По ТЗ v1 сервис не валидирует токен — он доверяет вызывающему (gateway/mesh).
// x-owner-id должен инжектиться доверенным посредником; прямой доступ клиента
// к gRPC media-service исключён архитектурно. Полноценная валидация auth —
// TODO (#5, флаг GRPC_AUTH_VALIDATE).
func callerIDFromMetadata(ctx context.Context) (uuid.UUID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.Nil, nil
	}
	vals := md.Get("x-owner-id")
	if len(vals) == 0 {
		return uuid.Nil, nil
	}
	id, err := uuid.Parse(vals[0])
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid owner_id")
	}
	return id, nil
}

func encodeMediaPageToken(ownerID uuid.UUID, cursor *repo.MediaCursor) (string, error) {
	if cursor == nil {
		return "", nil
	}
	payload, err := json.Marshal(mediaPageToken{
		Version:   1,
		OwnerID:   ownerID,
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeMediaPageToken(token string, ownerID uuid.UUID) (*repo.MediaCursor, error) {
	if token == "" {
		return nil, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid page token")
	}
	var t mediaPageToken
	if err := json.Unmarshal(payload, &t); err != nil || t.Version != 1 || t.OwnerID != ownerID || t.ID == uuid.Nil || t.CreatedAt.IsZero() {
		return nil, fmt.Errorf("invalid page token")
	}
	return &repo.MediaCursor{CreatedAt: t.CreatedAt, ID: t.ID}, nil
}

func toProtoMedia(item *media.MediaItem) (*mediav1.Media, error) {
	m := item.Media
	var metadata *structpb.Struct
	if len(m.Metadata) > 0 {
		var obj map[string]any
		if err := json.Unmarshal(m.Metadata, &obj); err != nil {
			return nil, fmt.Errorf("invalid media metadata: %w", err)
		}
		var err error
		metadata, err = structpb.NewStruct(obj)
		if err != nil {
			return nil, fmt.Errorf("convert media metadata: %w", err)
		}
	} else {
		metadata, _ = structpb.NewStruct(map[string]any{})
	}

	out := &mediav1.Media{
		Id:        m.ID.String(),
		OwnerId:   m.OwnerID.String(),
		Kind:      mediaKindToProto(m.Kind),
		Mime:      m.Mime,
		SizeBytes: uint64(m.SizeBytes),
		Status:    mediaStatusToProto(m.Status),
		Metadata:  metadata,
		Error:     m.Error,
		CreatedAt: timestamppb.New(m.CreatedAt),
	}
	out.Derivatives = make([]*mediav1.Derivative, 0, len(item.Derivatives))
	for _, d := range item.Derivatives {
		out.Derivatives = append(out.Derivatives, &mediav1.Derivative{
			Variant:   d.Variant,
			Mime:      d.Mime,
			SizeBytes: uint64(d.SizeBytes),
		})
	}
	return out, nil
}

func mediaKindToProto(kind repo.MediaKind) mediav1.MediaKind {
	switch kind {
	case repo.MediaKindImage:
		return mediav1.MediaKind_IMAGE
	case repo.MediaKindVideo:
		return mediav1.MediaKind_VIDEO
	case repo.MediaKindAudio:
		return mediav1.MediaKind_AUDIO
	default:
		return mediav1.MediaKind_KIND_UNSPECIFIED
	}
}

func mediaStatusToProto(st repo.MediaStatus) mediav1.MediaStatus {
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

func (s *MediaServer) GetMedia(ctx context.Context, req *mediav1.GetMediaRequest) (*mediav1.Media, error) {
	if req == nil || req.MediaId == "" {
		return nil, status.Error(codes.InvalidArgument, "media_id is required")
	}
	mediaID, err := uuid.Parse(req.MediaId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid media_id")
	}

	item, err := s.svc.GetMediaWithDerivatives(ctx, mediaID)
	if err != nil {
		return nil, mapMediaError(err)
	}
	if s.strictOwnerCheck {
		callerID, err := s.resolveCaller(ctx)
		if err != nil {
			return nil, err
		}
		if callerID != item.Media.OwnerID {
			return nil, status.Error(codes.PermissionDenied, media.ErrAccessDenied.Error())
		}
	}
	out, err := toProtoMedia(item)
	if err != nil {
		return nil, status.Error(codes.Internal, "invalid media metadata")
	}
	return out, nil
}

func (s *MediaServer) ListMediaByOwner(ctx context.Context, req *mediav1.ListMediaByOwnerRequest) (*mediav1.ListMediaByOwnerResponse, error) {
	if req == nil || req.OwnerId == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_id is required")
	}
	ownerID, err := uuid.Parse(req.OwnerId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid owner_id")
	}
	if req.PageSize > 1000 {
		return nil, status.Error(codes.InvalidArgument, "page_size must be <= 1000")
	}
	if s.strictOwnerCheck {
		callerID, err := s.resolveCaller(ctx)
		if err != nil {
			return nil, err
		}
		if callerID != ownerID {
			return nil, status.Error(codes.PermissionDenied, media.ErrAccessDenied.Error())
		}
	}

	cursor, err := decodeMediaPageToken(req.PageToken, ownerID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}

	page, err := s.svc.ListMediaByOwner(ctx, ownerID, int(req.PageSize), cursor)
	if err != nil {
		return nil, mapMediaError(err)
	}

	resp := &mediav1.ListMediaByOwnerResponse{Items: make([]*mediav1.Media, 0, len(page.Items))}
	for _, item := range page.Items {
		m, err := toProtoMedia(item)
		if err != nil {
			return nil, status.Error(codes.Internal, "invalid media metadata")
		}
		resp.Items = append(resp.Items, m)
	}

	if page.HasMore && len(page.Items) > 0 {
		last := page.Items[len(page.Items)-1].Media
		token, err := encodeMediaPageToken(ownerID, &repo.MediaCursor{CreatedAt: last.CreatedAt, ID: last.ID})
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to encode page token")
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (s *MediaServer) GetDownloadURL(ctx context.Context, req *mediav1.GetDownloadURLRequest) (*mediav1.GetDownloadURLResponse, error) {
	if req.MediaId == "" {
		return nil, status.Error(codes.InvalidArgument, "media_id is required")
	}

	mediaID, err := uuid.Parse(req.MediaId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid media_id")
	}

	variant := storage.Variant(req.Variant)
	if variant == "" {
		variant = storage.VariantOriginal
	}

	switch variant {
	case storage.VariantOriginal, storage.VariantThumb, storage.VariantR720:
		// ok
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid variant: %s", req.Variant)
	}

	callerID, err := s.resolveCaller(ctx)
	if err != nil {
		return nil, err
	}

	url, err := s.svc.GetDownloadURL(ctx, callerID, mediaID, variant)
	if err != nil {
		return nil, mapMediaError(err)
	}

	return &mediav1.GetDownloadURLResponse{
		Url:       url.URL,
		ExpiresAt: timestamppb.New(url.ExpiresAt),
	}, nil
}

// mapMediaError — доменные ошибки media.* → gRPC status (Upload/Download/…).
func mapMediaError(err error) error {
	switch {
	case errors.Is(err, media.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, media.ErrAccessDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, media.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, media.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, media.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		// GetDownloadURL уже может вернуть готовый status.Error.
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, err.Error())
	}
}

// resolveCaller — единая точка решения strict/non-strict.
func (s *MediaServer) resolveCaller(ctx context.Context) (uuid.UUID, error) {
	callerID, err := callerIDFromMetadata(ctx)
	if err != nil {
		return uuid.Nil, err // только InvalidArgument
	}
	if callerID == uuid.Nil && s.strictOwnerCheck {
		return uuid.Nil, status.Error(codes.Unauthenticated, "strict owner check enabled: missing trusted caller identity")
	}
	return callerID, nil
}
