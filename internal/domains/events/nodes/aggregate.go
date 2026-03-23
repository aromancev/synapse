package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
)

const (
	StreamTypeNode               events.StreamType = "node"
	EventTypeNodeCreated         events.EventType  = "node.created"
	EventTypeNodeUpdated         events.EventType  = "node.updated"
	EventTypeNodeKeywordsUpdated events.EventType  = "node.keywords_updated"
	EventTypeNodeArchived        events.EventType  = "node.archived"
)

type nodeCreatedEvent struct {
	ID       ID              `json:"id"`
	SchemaID events.StreamID `json:"schema_id"`
	Payload  json.RawMessage `json:"payload"`
}

func (e nodeCreatedEvent) normalized() nodeCreatedEvent {
	node := Node{ID: e.ID, SchemaID: e.SchemaID, Payload: e.Payload}.Normalized()
	return nodeCreatedEvent{ID: node.ID, SchemaID: node.SchemaID, Payload: node.Payload}
}

func (e nodeCreatedEvent) validate() []error {
	node := Node{ID: e.ID, SchemaID: e.SchemaID, CreatedAt: time.Now().Unix(), Payload: e.Payload}
	return node.Validate()
}

type nodeUpdatedEvent struct {
	Payload json.RawMessage `json:"payload"`
}

func (e nodeUpdatedEvent) validate() []error {
	var errs []error
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
	return errs
}

type nodeKeywordsUpdatedEvent struct {
	Keywords []string `json:"keywords"`
}

func (e nodeKeywordsUpdatedEvent) normalized() nodeKeywordsUpdatedEvent {
	return nodeKeywordsUpdatedEvent{Keywords: NormalizeKeywords(e.Keywords)}
}

func (e nodeKeywordsUpdatedEvent) validate() []error {
	var errs []error
	if len(e.Keywords) > 64 {
		errs = append(errs, errors.New("keywords must not exceed 64 items"))
	}
	for _, keyword := range e.Keywords {
		if keyword == "" {
			errs = append(errs, errors.New("keywords must not contain empty values"))
			continue
		}
		if len(keyword) > 64 {
			errs = append(errs, errors.New("keyword must not exceed 64 characters"))
		}
	}
	return errs
}

// Aggregate is an event-sourced aggregate for nodes.
type Aggregate struct {
	exists     bool
	archivedAt int64
	id         ID
	schemaID   events.StreamID
	payload    json.RawMessage
	keywords   []string
}

func (a *Aggregate) Apply(ctx context.Context, event events.Event) error {
	_ = ctx

	switch event.EventType {
	case EventTypeNodeCreated:
		return a.applyCreated(event)
	case EventTypeNodeUpdated:
		return a.applyUpdated(event)
	case EventTypeNodeKeywordsUpdated:
		return a.applyKeywordsUpdated(event)
	case EventTypeNodeArchived:
		return a.applyArchived(event)
	default:
		return nil
	}
}

func (a *Aggregate) applyCreated(event events.Event) error {
	var payload nodeCreatedEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	payload = payload.normalized()

	a.exists = true
	a.id = payload.ID
	a.schemaID = payload.SchemaID
	a.payload = payload.Payload
	a.keywords = nil
	return nil
}

func (a *Aggregate) applyUpdated(event events.Event) error {
	var payload nodeUpdatedEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	a.payload = payload.Payload
	return nil
}

func (a *Aggregate) applyKeywordsUpdated(event events.Event) error {
	var payload nodeKeywordsUpdatedEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	a.keywords = payload.Keywords
	return nil
}

func (a *Aggregate) applyArchived(event events.Event) error {
	a.archivedAt = event.OccurredAt
	return nil
}

func (a *Aggregate) Exists() bool              { return a.exists }
func (a *Aggregate) Archived() bool            { return a.archivedAt > 0 }
func (a *Aggregate) ArchivedAt() int64         { return a.archivedAt }
func (a *Aggregate) SchemaID() events.StreamID { return a.schemaID }
func (a *Aggregate) Payload() json.RawMessage  { return a.payload }
func (a *Aggregate) Keywords() []string        { return append([]string(nil), a.keywords...) }

func (a *Aggregate) Create(ctx context.Context, stream *events.Stream, payload json.RawMessage, id ID, schemaID events.StreamID) error {
	_ = ctx

	eventPayload := nodeCreatedEvent{ID: id, SchemaID: schemaID, Payload: payload}.normalized()
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

func (a *Aggregate) Update(ctx context.Context, stream *events.Stream, payload json.RawMessage) error {
	_ = ctx

	if a.Archived() {
		return errors.New("node is archived")
	}

	eventPayload := nodeUpdatedEvent{Payload: payload}
	if validationErrors := eventPayload.validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	return stream.Record(events.Request{
		EventType:    EventTypeNodeUpdated,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      eventPayload,
	})
}

func (a *Aggregate) UpdateKeywords(ctx context.Context, stream *events.Stream, keywords []string) error {
	_ = ctx

	if !a.Exists() {
		return errors.New("node does not exist")
	}
	if a.Archived() {
		return errors.New("node is archived")
	}

	eventPayload := nodeKeywordsUpdatedEvent{Keywords: keywords}.normalized()
	if validationErrors := eventPayload.validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	return stream.Record(events.Request{
		EventType:    EventTypeNodeKeywordsUpdated,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      eventPayload,
	})
}

func (a *Aggregate) Archive(ctx context.Context, stream *events.Stream) error {
	_ = ctx

	if !a.Exists() {
		return errors.New("node does not exist")
	}
	if a.Archived() {
		return errors.New("node is already archived")
	}

	return stream.Record(events.Request{
		EventType:    EventTypeNodeArchived,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      struct{}{},
	})
}
