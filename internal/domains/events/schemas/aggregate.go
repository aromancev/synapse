package schemas

import (
	"encoding/json"
	"errors"

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

func (a *Aggregate) Apply(event events.Event) error {
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
func (a *Aggregate) Validate(payload json.RawMessage) error {
	if a.compiledSchema == nil {
		return errors.New("no schema has been applied")
	}
	var doc any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return err
	}
	return a.compiledSchema.Validate(doc)
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
