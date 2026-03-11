package links

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
)

const (
	StreamTypeLink       events.StreamType = "link"
	EventTypeLinkCreated events.EventType  = "link.created"
	EventTypeLinkRemoved events.EventType  = "link.removed"
)

type createdEvent struct {
	From events.StreamID `json:"from"`
	To   events.StreamID `json:"to"`
}

func (e createdEvent) normalized() createdEvent {
	e.From, e.To = normalizePair(e.From, e.To)
	return e
}

func (e createdEvent) validate() []error {
	e = e.normalized()

	var errs []error
	if err := e.From.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := e.To.Validate(); err != nil {
		errs = append(errs, err)
	}
	if e.From == e.To {
		errs = append(errs, errors.New("from and to must be different"))
	}
	return errs
}

// Aggregate is an event-sourced aggregate for an undirected link.
type Aggregate struct {
	exists bool
	from   events.StreamID
	to     events.StreamID
}

func (a *Aggregate) Apply(ctx context.Context, event events.Event) error {
	_ = ctx

	switch event.EventType {
	case EventTypeLinkCreated:
		return a.applyCreated(event)
	case EventTypeLinkRemoved:
		a.exists = false
		a.from = ""
		a.to = ""
		return nil
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
	a.from = payload.From
	a.to = payload.To
	return nil
}

func (a *Aggregate) Exists() bool { return a.exists }

func (a *Aggregate) Create(ctx context.Context, stream *events.Stream, from, to events.StreamID) error {
	_ = ctx

	if a.exists {
		return errors.New("link already exists")
	}

	payload := createdEvent{From: from, To: to}.normalized()
	if validationErrors := payload.validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	return stream.Record(events.Request{
		EventType:    EventTypeLinkCreated,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      payload,
	})
}
