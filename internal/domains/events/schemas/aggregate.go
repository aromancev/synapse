package schemas

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/aromancev/synapse/internal/domains/events"
)

const (
	EventTypeSchemaAdded events.EventType = "schema.added"
)

// Aggregate is an event-sourced aggregate for schemas.
type Aggregate struct {
	compiledSchema *jsonschema.Schema
}

func (a *Aggregate) Apply(ctx context.Context, event events.Event) error {
	_ = ctx

	switch event.EventType {
	case EventTypeSchemaAdded:
		return a.applySchemaAdded(event)
	default:
		return nil
	}
}

func (a *Aggregate) applySchemaAdded(event events.Event) error {
	var schema Schema
	if err := json.Unmarshal(event.Payload, &schema); err != nil {
		return err
	}

	compiler := jsonschema.NewCompiler()
	const resourceURL = "schema.json"

	var doc any
	if err := json.Unmarshal(schema.Schema, &doc); err != nil {
		return err
	}
	if err := compiler.AddResource(resourceURL, doc); err != nil {
		return err
	}
	compiled, err := compiler.Compile(resourceURL)
	if err != nil {
		return err
	}

	a.compiledSchema = compiled
	return nil
}

// Validate validates a JSON payload against the compiled schema.
func (a *Aggregate) Validate(ctx context.Context, payload json.RawMessage) error {
	_ = ctx

	if a.compiledSchema == nil {
		return errors.New("no schema has been applied")
	}
	var doc any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return err
	}
	return a.compiledSchema.Validate(doc)
}

// Add records a schema creation event into the stream.
func (a *Aggregate) Add(ctx context.Context, stream *events.Stream, schema Schema) error {
	_ = ctx

	req := events.Request{
		EventType:    EventTypeSchemaAdded,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      schema,
		Meta:         map[string]any{},
	}
	return stream.Record(req)
}
