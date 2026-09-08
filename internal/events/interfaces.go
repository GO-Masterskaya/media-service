package events

import (
	"context"

	"github.com/google/uuid"
)

// MediaService — подмножество media.Service для event handler'а.
type MediaService interface {
	AttachMedia(ctx context.Context, mediaID uuid.UUID, ownerID uuid.UUID) error
	DeleteMedia(ctx context.Context, callerID, mediaID uuid.UUID) error
}
