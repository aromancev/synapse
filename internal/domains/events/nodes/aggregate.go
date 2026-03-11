package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
)

const (
	StreamTypeNode       events.StreamType = "node"
	EventTypeNodeCreated events.EventType  = "node.created"
)

type createdEvent struct {
	UID      ID              `json:"uid"`
	SchemaID events.StreamID `json:"schema_id"`
	Payload  json.RawMessage `json:"payload"`
}

func (e createdEvent) normalized() createdEvent {
	node := Node{UID: e.UID, SchemaID: e.SchemaID, Payload: e.Payload}.Normalized()
	return createdEvent{UID: node.UID, SchemaID: node.SchemaID, Payload: node.Payload}
}

func (e createdEvent) validate() []error {
	node := Node{UID: e.UID, SchemaID: e.SchemaID, CreatedAt: time.Now().Unix(), Payload: e.Payload}
	return node.Validate()
}

// Aggregate is an event-sourced aggregate for nodes.
type Aggregate struct {
	exists   bool
	uid      ID
	schemaID events.StreamID
	payload  json.RawMessage
}

func (a *Aggregate) Apply(ctx context.Context, event events.Event) error {
	_ = ctx

	switch event.EventType {
	case EventTypeNodeCreated:
		return a.applyCreated(event)
	default:
		return nil
	}
}

func (a *Aggregate) applyCreated(event events.Event) error {
	var payload createdEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	payload = payload.normalized()
	a.exists = true
	a.uid = payload.UID
	a.schemaID = payload.SchemaID
	a.payload = payload.Payload
	return nil
}

func (a *Aggregate) Exists() bool { return a.exists }

func (a *Aggregate) Create(ctx context.Context, stream *events.Stream, payload json.RawMessage, uid ID, schemaID events.StreamID) error {
	_ = ctx

	eventPayload := createdEvent{UID: uid, SchemaID: schemaID, Payload: payload}.normalized()
	if validationErrors := eventPayload.validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	return stream.Record(events.Request{
		EventType:    EventTypeNodeCreated,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      eventPayload,
	})
}
