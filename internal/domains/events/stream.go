package events

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// StreamID identifies an event stream.
type StreamID string

func (id StreamID) String() string { return string(id) }
func (id StreamID) Normalized() StreamID {
	return StreamID(strings.TrimSpace(string(id)))
}
func (id *StreamID) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		*id = StreamID(v)
		return nil
	case string:
		*id = StreamID(v)
		return nil
	default:
		return fmt.Errorf("cannot scan %T into StreamID", src)
	}
}
func (id StreamID) Value() (driver.Value, error) { return id.String(), nil }
func (id StreamID) Validate() error {
	if id.Normalized() == "" {
		return errors.New("stream_id is required")
	}
	return nil
}

// StreamType identifies a category of streams.
type StreamType string

func (t StreamType) String() string { return string(t) }
func (t StreamType) Normalized() StreamType {
	return StreamType(strings.TrimSpace(string(t)))
}
func (t *StreamType) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		*t = StreamType(v)
		return nil
	case string:
		*t = StreamType(v)
		return nil
	default:
		return fmt.Errorf("cannot scan %T into StreamType", src)
	}
}
func (t StreamType) Value() (driver.Value, error) { return t.String(), nil }
func (t StreamType) Validate() error {
	value := t.Normalized().String()
	if value == "" {
		return errors.New("stream_type is required")
	}
	if len(value) > 64 {
		return errors.New("stream_type must not exceed 64 characters")
	}
	if !streamTypeRe.MatchString(value) {
		return errors.New("stream_type must be snake_case with lowercase latin letters and numbers only")
	}
	return nil
}

// Aggregate consumes ordered events from a stream.
type Aggregate interface {
	Apply(ctx context.Context, event Event) error
}

// Stream is a handle for aggregate stream events.
type Stream struct {
	streamID       StreamID
	streamType     StreamType
	appliedEvents  []Event
	nextVersion    int64
	recordedEvents []Event
}

func NewStream(streamID StreamID, streamType StreamType, events []Event) *Stream {
	copied := append([]Event(nil), events...)
	var nextVersion int64 = 1
	for _, e := range copied {
		if e.StreamVersion >= nextVersion {
			nextVersion = e.StreamVersion + 1
		}
	}
	return &Stream{
		streamID:      streamID.Normalized(),
		streamType:    streamType.Normalized(),
		appliedEvents: copied,
		nextVersion:   nextVersion,
	}
}

// Init replays all stream events in order into the aggregate.
func (s *Stream) Init(ctx context.Context, aggregate Aggregate) error {
	for _, e := range s.appliedEvents {
		if err := aggregate.Apply(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// Record creates a new Event from the request, assigns operational fields,
// and stores it in the recordedEvents buffer.
func (s *Stream) Record(req Request) error {
	if err := req.Validate(); err != nil {
		return err
	}
	payloadJSON, err := json.Marshal(req.Payload)
	if err != nil {
		return err
	}
	metaJSON, err := json.Marshal(req.Meta)
	if err != nil {
		return err
	}
	eventID, err := NewEventID()
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	e := Event{
		ID:            eventID,
		StreamID:      s.streamID,
		StreamType:    s.streamType,
		StreamVersion: s.nextVersion,
		EventType:     req.EventType.Normalized(),
		EventVersion:  req.EventVersion,
		OccurredAt:    req.OccurredAt,
		RecordedAt:    now,
		Payload:       payloadJSON,
		Meta:          metaJSON,
	}
	s.recordedEvents = append(s.recordedEvents, e)
	s.nextVersion++
	return nil
}

// RecordedEvents returns the events recorded since the stream was created.
func (s *Stream) RecordedEvents() []Event {
	copied := append([]Event(nil), s.recordedEvents...)
	return copied
}
