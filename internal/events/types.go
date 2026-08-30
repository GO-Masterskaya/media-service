package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope — обёртка Kafka-сообщения.
type Envelope struct {
	EventID   uuid.UUID       `json:"event_id"`
	EventType string          `json:"event_type"` // media.attach | media.detach
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// AttachPayload — media прикрепляется к owner.
type AttachPayload struct {
	MediaID uuid.UUID `json:"media_id"`
	OwnerID uuid.UUID `json:"owner_id"`
}

// DetachPayload — media открепляется (удаляется).
type DetachPayload struct {
	MediaID uuid.UUID `json:"media_id"`
	OwnerID uuid.UUID `json:"owner_id"` // для проверки прав
}

func (e *Envelope) Validate() error {
	if e.EventID == uuid.Nil {
		return fmt.Errorf("event_id required")
	}
	if e.EventType == "" {
		return fmt.Errorf("event_type required")
	}
	switch e.EventType {
	case "media.attach", "media.detach":
		// ok
	default:
		return fmt.Errorf("unknown event_type: %s", e.EventType)
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("timestamp required")
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("payload required")
	}
	return nil
}
