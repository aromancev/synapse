package events

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const eventIDPrefix = "event_"

var (
	streamTypeRe = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)
	eventTypeRe  = regexp.MustCompile(`^[a-z0-9]+(?:[._][a-z0-9]+)*$`)
)

// EventID is a prefixed ULID-backed identifier for events.
type EventID ulid.ULID

// NewEventID generates a new event ID.
func NewEventID() (EventID, error) {
	id, err := ulid.New(ulid.Now(), ulid.DefaultEntropy())
	if err != nil {
		return EventID{}, fmt.Errorf("generate ulid: %w", err)
	}
	return EventID(id), nil
}

// String returns the prefixed event ID string.
func (id EventID) String() string {
	return eventIDPrefix + ulid.ULID(id).String()
}

// Bytes returns the raw ULID bytes.
func (id EventID) Bytes() []byte {
	value := ulid.ULID(id)
	bytes := make([]byte, len(value))
	copy(bytes, value[:])
	return bytes
}

// ParseEventID parses a prefixed ULID string into an EventID.
func ParseEventID(s string) (EventID, error) {
	if strings.TrimSpace(s) == "" {
		return EventID{}, errors.New("id is required")
	}
	if !strings.HasPrefix(s, eventIDPrefix) {
		return EventID{}, fmt.Errorf("id must have prefix %q", eventIDPrefix)
	}

	id, err := ulid.ParseStrict(strings.TrimPrefix(s, eventIDPrefix))
	if err != nil {
		return EventID{}, fmt.Errorf("parse ulid: %w", err)
	}
	return EventID(id), nil
}

// Scan implements sql.Scanner for database reads.
func (id *EventID) Scan(src any) error {
	v, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into EventID", src)
	}
	if len(v) != len(ulid.ULID{}) {
		return fmt.Errorf("invalid byte length for EventID: %d", len(v))
	}
	var parsed ulid.ULID
	copy(parsed[:], v)
	*id = EventID(parsed)
	return nil
}

// Value implements driver.Valuer for database writes.
func (id EventID) Value() (driver.Value, error) {
	value := ulid.ULID(id)
	return value[:], nil
}

// MarshalJSON implements json.Marshaler.
func (id EventID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + id.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (id *EventID) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)
	parsed, err := ParseEventID(str)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// EventType identifies a domain event kind.
type EventType string

func (t EventType) String() string { return string(t) }
func (t EventType) Normalized() EventType {
	return EventType(strings.TrimSpace(string(t)))
}
func (t *EventType) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		*t = EventType(v)
		return nil
	case string:
		*t = EventType(v)
		return nil
	default:
		return fmt.Errorf("cannot scan %T into EventType", src)
	}
}
func (t EventType) Value() (driver.Value, error) { return t.String(), nil }
func (t EventType) Validate() error {
	value := t.Normalized().String()
	if value == "" {
		return errors.New("event_type is required")
	}
	if len(value) > 128 {
		return errors.New("event_type must not exceed 128 characters")
	}
	if !eventTypeRe.MatchString(value) {
		return errors.New("event_type must be lowercase and use dots/underscores as separators")
	}
	return nil
}

// Request is a lightweight event request without operational fields.
// The caller provides semantics; Stream.Record fills in operational fields.
type Request struct {
	EventType    EventType `json:"event_type"`
	EventVersion int64     `json:"event_version"`
	OccurredAt   int64     `json:"occurred_at"`
	Payload      any       `json:"payload"`
	Meta         any       `json:"meta"`
}

// Validate checks whether Request is valid.
func (r Request) Validate() error {
	if err := r.EventType.Validate(); err != nil {
		return err
	}
	if r.EventVersion <= 0 {
		return errors.New("event_version must be > 0")
	}
	if r.OccurredAt <= 0 {
		return errors.New("occurred_at is required")
	}
	if r.Payload == nil {
		return errors.New("payload is required")
	}
	return nil
}

// Event is a single immutable domain event.
type Event struct {
	GlobalPosition int64           `json:"global_position"`
	ID             EventID         `json:"id"`
	StreamID       StreamID        `json:"stream_id"`
	StreamType     StreamType      `json:"stream_type"`
	StreamVersion  int64           `json:"stream_version"`
	EventType      EventType       `json:"event_type"`
	EventVersion   int64           `json:"event_version"`
	OccurredAt     int64           `json:"occurred_at"`
	RecordedAt     int64           `json:"recorded_at"`
	Payload        json.RawMessage `json:"payload"`
	Meta           json.RawMessage `json:"meta"`
}

// Normalized returns a normalized copy of the event.
func (e Event) Normalized() Event {
	e.StreamID = e.StreamID.Normalized()
	e.StreamType = e.StreamType.Normalized()
	e.EventType = e.EventType.Normalized()
	e.Payload = compactJSON(e.Payload)
	e.Meta = compactJSON(e.Meta)
	return e
}

// Validate checks whether Event is valid.
func (e Event) Validate() []error {
	var errs []error

	var emptyID EventID
	if e.ID == emptyID {
		errs = append(errs, errors.New("id is required"))
	}
	if err := e.StreamID.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := e.StreamType.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := e.EventType.Validate(); err != nil {
		errs = append(errs, err)
	}
	if e.StreamVersion <= 0 {
		errs = append(errs, errors.New("stream_version is required and must be > 0"))
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
	if len(e.Payload) == 0 {
		errs = append(errs, errors.New("payload is required"))
	} else {
		if len(e.Payload) > 256*1024 {
			errs = append(errs, errors.New("payload must not exceed 256KB"))
		}
		var doc any
		if err := json.Unmarshal(e.Payload, &doc); err != nil {
			errs = append(errs, errors.New("payload must be valid JSON"))
		}
	}
	if len(e.Meta) == 0 {
		errs = append(errs, errors.New("meta is required"))
	} else {
		if len(e.Meta) > 64*1024 {
			errs = append(errs, errors.New("meta must not exceed 64KB"))
		}
		var doc any
		if err := json.Unmarshal(e.Meta, &doc); err != nil {
			errs = append(errs, errors.New("meta must be valid JSON"))
		}
	}

	return errs
}

func compactJSON(value json.RawMessage) json.RawMessage {
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err == nil {
		return json.RawMessage(compact.Bytes())
	}
	return value
}
