package events

import (
	"context"

	"mediaservice/internal/repo"

	"github.com/google/uuid"
)

// MediaService — подмножество media.Service для event handler'а.
type MediaService interface {
	GetMedia(ctx context.Context, mediaID uuid.UUID) (*repo.Media, error)
	AttachMedia(ctx context.Context, mediaID uuid.UUID, ownerID uuid.UUID) error
	DeleteMedia(ctx context.Context, callerID, mediaID uuid.UUID) error
}
