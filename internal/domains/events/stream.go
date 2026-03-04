package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Aggregate consumes ordered events from a stream.
type Aggregate interface {
	Apply(event Event) error
}

// Stream is a handle for aggregate stream events.
type Stream struct {
	streamID       string
	streamType     string
	appliedEvents  []Event
	nextVersion    int64
	recordedEvents []Event
}

func NewStream(streamID, streamType string, events []Event) *Stream {
	copied := append([]Event(nil), events...)
	var nextVersion int64 = 1
	for _, e := range copied {
		if e.StreamVersion >= nextVersion {
			nextVersion = e.StreamVersion + 1
		}
	}
	return &Stream{
		streamID:      streamID,
		streamType:    streamType,
		appliedEvents: copied,
		nextVersion:   nextVersion,
	}
}

// Init replays all stream events in order into the aggregate.
func (s *Stream) Init(aggregate Aggregate) error {
	for _, e := range s.appliedEvents {
		if err := aggregate.Apply(e); err != nil {
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

	now := time.Now().Unix()
	e := Event{
		ID:            uuid.NewString(),
		StreamID:      s.streamID,
		StreamType:    s.streamType,
		StreamVersion: s.nextVersion,
		EventType:     req.EventType,
		EventVersion:  req.EventVersion,
		OccurredAt:    req.OccurredAt,
		RecordedAt:    now,
		Payload:       string(payloadJSON),
		Meta:          string(metaJSON),
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
