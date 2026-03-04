package schemas

import (
	"github.com/aromancev/synapse/internal/domains/events"
)

const (
	EventTypeSchemaAdded = "schema.added"
)

// Aggregate is a placeholder event-sourced aggregate for schemas.
type Aggregate struct{}

func (a *Aggregate) Apply(event events.Event) error {
	_ = event
	return nil
}

// Add records a schema creation event into the stream at the given occurred time.
func (a *Aggregate) Add(stream *events.Stream, schema Schema, occurredAt int64) error {
	req := events.Request{
		EventType:    EventTypeSchemaAdded,
		EventVersion: 1,
		OccurredAt:   occurredAt,
		Payload:      schema,
		Meta:         map[string]any{},
	}

	return stream.Record(req)
}
