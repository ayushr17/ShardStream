package event

import (
	"errors"
	"time"
)

var (
	// ErrMissingSourceID is returned when an event has no source_id.
	ErrMissingSourceID = errors.New("source_id is required")
	// ErrMissingEventType is returned when an event has no event_type.
	ErrMissingEventType = errors.New("event_type is required")
)

// Event is a single generic "user did X" event. Kept intentionally generic
// (source_id = "who sent this", event_type = "what kind of thing happened")
// so the pipeline works for page analytics, IoT telemetry, game events, etc.
// without changing this layer.
type Event struct {
	SourceID  string            `json:"source_id"`
	EventType string            `json:"event_type"` // e.g. "view", "click", "submit"
	Timestamp time.Time         `json:"timestamp"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Validate checks that the required fields are present.
func (e Event) Validate() error {
	if e.SourceID == "" {
		return ErrMissingSourceID
	}
	if e.EventType == "" {
		return ErrMissingEventType
	}
	return nil
}
