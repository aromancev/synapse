package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
)

const (
	StreamTypeNode     events.StreamType = "node"
	EventTypeNodeAdded events.EventType  = "node.added"
)

// Aggregate is an event-sourced aggregate for nodes.
type Aggregate struct {
	node *Node
}

func (a *Aggregate) Apply(ctx context.Context, event events.Event) error {
	_ = ctx

	switch event.EventType {
	case EventTypeNodeAdded:
		return a.applyNodeAdded(event)
	default:
		return nil
	}
}

func (a *Aggregate) applyNodeAdded(event events.Event) error {
	var node Node
	if err := json.Unmarshal(event.Payload, &node); err != nil {
		return err
	}
	a.node = &node
	return nil
}

// Add records a node creation event into the stream.
func (a *Aggregate) Add(ctx context.Context, stream *events.Stream, node Node) error {
	_ = ctx

	node = node.Normalized()
	if validationErrors := node.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	req := events.Request{
		EventType:    EventTypeNodeAdded,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      node,
		Meta:         map[string]any{},
	}
	return stream.Record(req)
}
