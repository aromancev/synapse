package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	streamTypeRe = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)
	eventTypeRe  = regexp.MustCompile(`^[a-z0-9]+(?:[._][a-z0-9]+)*$`)
)

// Event is a single immutable domain event.
type Event struct {
	GlobalPosition int64  `json:"global_position"`
	ID             string `json:"id"`
	StreamID       string `json:"stream_id"`
	StreamType     string `json:"stream_type"`
	StreamVersion  int64  `json:"stream_version"`
	EventType      string `json:"event_type"`
	EventVersion   int64  `json:"event_version"`
	OccurredAt     int64  `json:"occurred_at"`
	RecordedAt     int64  `json:"recorded_at"`
	Payload        string `json:"payload"`
	Meta           string `json:"meta"`
}

// Normalized returns a normalized copy of the event.
func (e Event) Normalized() Event {
	e.ID = strings.TrimSpace(e.ID)
	e.StreamID = strings.TrimSpace(e.StreamID)
	e.StreamType = strings.TrimSpace(e.StreamType)
	e.EventType = strings.TrimSpace(e.EventType)
	e.Payload = strings.TrimSpace(e.Payload)
	e.Meta = strings.TrimSpace(e.Meta)

	var payload bytes.Buffer
	if err := json.Compact(&payload, []byte(e.Payload)); err == nil {
		e.Payload = payload.String()
	}

	var meta bytes.Buffer
	if err := json.Compact(&meta, []byte(e.Meta)); err == nil {
		e.Meta = meta.String()
	}

	return e
}

// Validate checks whether Event is valid.
func (e Event) Validate() []error {
	var errs []error

	if e.ID == "" {
		errs = append(errs, errors.New("id is required"))
	} else if _, err := uuid.Parse(e.ID); err != nil {
		errs = append(errs, errors.New("id must be a valid UUID"))
	}

	if e.StreamID == "" {
		errs = append(errs, errors.New("stream_id is required"))
	} else if len(e.StreamID) > 191 {
		errs = append(errs, errors.New("stream_id must not exceed 191 characters"))
	}

	if e.StreamType == "" {
		errs = append(errs, errors.New("stream_type is required"))
	} else {
		if len(e.StreamType) > 64 {
			errs = append(errs, errors.New("stream_type must not exceed 64 characters"))
		}
		if !streamTypeRe.MatchString(e.StreamType) {
			errs = append(errs, errors.New("stream_type must be snake_case with lowercase latin letters and numbers only"))
		}
	}

	if e.StreamVersion <= 0 {
		errs = append(errs, errors.New("stream_version is required and must be > 0"))
	}

	if e.EventType == "" {
		errs = append(errs, errors.New("event_type is required"))
	} else {
		if len(e.EventType) > 128 {
			errs = append(errs, errors.New("event_type must not exceed 128 characters"))
		}
		if !eventTypeRe.MatchString(e.EventType) {
			errs = append(errs, errors.New("event_type must be lowercase and use dots/underscores as separators"))
		}
	}

	if e.EventVersion <= 0 {
		errs = append(errs, errors.New("event_version is required and must be > 0"))
	}

	if e.OccurredAt <= 0 {
		errs = append(errs, errors.New("occurred_at is required"))
	} else if e.OccurredAt > time.Now().Add(24*time.Hour).Unix() {
		errs = append(errs, errors.New("occurred_at cannot be in the far future"))
	}

	if e.RecordedAt <= 0 {
		errs = append(errs, errors.New("recorded_at is required"))
	} else if e.RecordedAt > time.Now().Add(24*time.Hour).Unix() {
		errs = append(errs, errors.New("recorded_at cannot be in the far future"))
	}

	if e.Payload == "" {
		errs = append(errs, errors.New("payload is required"))
	} else {
		if len(e.Payload) > 256*1024 {
			errs = append(errs, errors.New("payload must not exceed 256KB"))
		}
		var doc any
		if err := json.Unmarshal([]byte(e.Payload), &doc); err != nil {
			errs = append(errs, errors.New("payload must be valid JSON"))
		}
	}

	if e.Meta == "" {
		errs = append(errs, errors.New("meta is required"))
	} else {
		if len(e.Meta) > 64*1024 {
			errs = append(errs, errors.New("meta must not exceed 64KB"))
		}
		var doc any
		if err := json.Unmarshal([]byte(e.Meta), &doc); err != nil {
			errs = append(errs, errors.New("meta must be valid JSON"))
		}
	}

	return errs
}
