package events

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// DecodeEnvelope парсит и валидирует JSON-конверт.
func DecodeEnvelope(data []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", PermanentError{err})
	}
	if err := env.Validate(); err != nil {
		return nil, fmt.Errorf("validation: %w", PermanentError{err})
	}
	return &env, nil
}

// DecodeAttach извлекает payload attach.
func DecodeAttach(payload json.RawMessage) (*AttachPayload, error) {
	var p AttachPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("attach payload: %w", PermanentError{err})
	}
	if p.MediaID == uuid.Nil || p.OwnerID == uuid.Nil {
		return nil, PermanentError{fmt.Errorf("media_id and owner_id required")}
	}
	return &p, nil
}

// DecodeDetach извлекает payload detach.
func DecodeDetach(payload json.RawMessage) (*DetachPayload, error) {
	var p DetachPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("detach payload: %w", PermanentError{err})
	}
	if p.MediaID == uuid.Nil {
		return nil, PermanentError{fmt.Errorf("media_id required")}
	}
	if p.OwnerID == uuid.Nil {
		return nil, PermanentError{fmt.Errorf("owner_id required")}
	}
	return &p, nil
}
