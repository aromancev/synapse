package schemas

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/aromancev/synapse/internal/domains/events"
)

const (
	StreamTypeSchema       events.StreamType = "schema"
	EventTypeSchemaCreated events.EventType  = "schema.created"
)

type createdEvent struct {
	ID     ID              `json:"id"`
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
}

func (e createdEvent) normalized() createdEvent {
	e.Name = strings.TrimSpace(e.Name)
	e.Schema = json.RawMessage(strings.TrimSpace(string(e.Schema)))

	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(e.Schema)); err == nil {
		e.Schema = json.RawMessage(compact.Bytes())
	}

	return e
}

func (e createdEvent) validate() []error {
	var errs []error

	var emptyID ID
	if e.ID == emptyID {
		errs = append(errs, errors.New("id is required"))
	}
	if e.Name == "" {
		errs = append(errs, errors.New("schema name is required"))
	} else {
		if len(e.Name) > 64 {
			errs = append(errs, errors.New("schema name must not exceed 64 characters"))
		}
		if !schemaNameRe.MatchString(e.Name) {
			errs = append(errs, errors.New("schema name must be snake_case with lowercase latin letters and numbers only"))
		}
	}
	if len(e.Schema) == 0 {
		errs = append(errs, errors.New("schema is required"))
	} else {
		if len(e.Schema) > 16*1024 {
			errs = append(errs, errors.New("schema must not exceed 16KB"))
		}
		var doc any
		if err := json.Unmarshal(e.Schema, &doc); err != nil {
			errs = append(errs, errors.New("schema must be valid JSON"))
		} else {
			compiler := jsonschema.NewCompiler()
			const resourceURL = "schema.json"
			if err := compiler.AddResource(resourceURL, doc); err != nil {
				errs = append(errs, err)
			} else if _, err := compiler.Compile(resourceURL); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errs
}

// Aggregate is an event-sourced aggregate for schemas.
type Aggregate struct {
	id             ID
	name           string
	compiledSchema *jsonschema.Schema
}

func (a *Aggregate) Apply(ctx context.Context, event events.Event) error {
	_ = ctx

	switch event.EventType {
	case EventTypeSchemaCreated:
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

	compiler := jsonschema.NewCompiler()
	const resourceURL = "schema.json"

	var doc any
	if err := json.Unmarshal(payload.Schema, &doc); err != nil {
		return err
	}
	if err := compiler.AddResource(resourceURL, doc); err != nil {
		return err
	}
	compiled, err := compiler.Compile(resourceURL)
	if err != nil {
		return err
	}

	a.id = payload.ID
	a.name = payload.Name
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

func (a *Aggregate) Create(ctx context.Context, stream *events.Stream, id ID, name string, schemaJSON json.RawMessage) error {
	_ = ctx

	eventPayload := createdEvent{ID: id, Name: name, Schema: schemaJSON}.normalized()
	if validationErrors := eventPayload.validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	return stream.Record(events.Request{
		EventType:    EventTypeSchemaCreated,
		EventVersion: 1,
		OccurredAt:   time.Now().Unix(),
		Payload:      eventPayload,
	})
}
