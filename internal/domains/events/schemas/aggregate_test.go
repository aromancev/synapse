package schemas

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregate_Create(t *testing.T) {
	t.Run("records schema created event", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		schema := Schema{
			ID:     id,
			Name:   "person",
			Schema: json.RawMessage(`{"type":"object"}`),
		}

		stream := events.NewStream(events.StreamID(id.String()), events.StreamType("schema"), nil)
		aggregate := &Aggregate{}

		err = aggregate.Create(context.Background(), stream, schema.ID, schema.Name, schema.Schema)
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 1)

		e := recorded[0]
		assert.Equal(t, events.StreamType("schema"), e.StreamType)
		assert.Equal(t, EventTypeSchemaCreated, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))

		var payload Schema
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, schema.ID, payload.ID)
		assert.Equal(t, schema.Name, payload.Name)
		assert.JSONEq(t, string(schema.Schema), string(payload.Schema))
	})
}

func TestAggregate_Archive(t *testing.T) {
	t.Run("records schema archived event", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		stream := events.NewStream(events.StreamID(id.String()), events.StreamType("schema"), nil)
		aggregate := &Aggregate{}
		require.NoError(t, aggregate.Create(context.Background(), stream, id, "person", json.RawMessage(`{"type":"object"}`)))
		require.NoError(t, replayRecorded(t, aggregate, stream))

		err = aggregate.Archive(context.Background(), stream)
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 2)
		assert.Equal(t, EventTypeSchemaArchived, recorded[1].EventType)
	})

	t.Run("replay marks schema as archived", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		aggregate := &Aggregate{}
		require.NoError(t, aggregate.Apply(context.Background(), events.Event{
			StreamID:      events.StreamID(id.String()),
			StreamType:    events.StreamType("schema"),
			StreamVersion: 1,
			EventType:     EventTypeSchemaCreated,
			EventVersion:  1,
			Payload:       mustMarshalSchema(t, Schema{ID: id, Name: "person", Schema: json.RawMessage(`{"type":"object"}`)}),
		}))
		require.NoError(t, aggregate.Apply(context.Background(), events.Event{EventType: EventTypeSchemaArchived, OccurredAt: 1700000100, Payload: []byte(`{}`)}))

		assert.True(t, aggregate.Exists())
		assert.True(t, aggregate.Archived())
		assert.Equal(t, int64(1700000100), aggregate.ArchivedAt())
	})

	t.Run("rejects archive when schema does not exist", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		stream := events.NewStream(events.StreamID(id.String()), events.StreamType("schema"), nil)
		aggregate := &Aggregate{}
		err = aggregate.Archive(context.Background(), stream)
		require.ErrorContains(t, err, "schema does not exist")
	})

	t.Run("rejects archive when schema is already archived", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		stream := events.NewStream(events.StreamID(id.String()), events.StreamType("schema"), nil)
		aggregate := &Aggregate{}
		require.NoError(t, aggregate.Create(context.Background(), stream, id, "person", json.RawMessage(`{"type":"object"}`)))
		require.NoError(t, replayRecorded(t, aggregate, stream))
		require.NoError(t, aggregate.Archive(context.Background(), stream))
		require.NoError(t, replayRecorded(t, aggregate, stream))

		err = aggregate.Archive(context.Background(), stream)
		require.ErrorContains(t, err, "schema is already archived")
	})
}

func TestAggregate_ApplyAndValidate(t *testing.T) {
	id, err := NewID()
	require.NoError(t, err)

	schema := Schema{
		ID:     id,
		Name:   "person",
		Schema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`),
	}
	payloadJSON, err := json.Marshal(schema)
	require.NoError(t, err)

	t.Run("applies schema event and compiles schema", func(t *testing.T) {
		aggregate := &Aggregate{}

		err := aggregate.Apply(context.Background(), events.Event{
			StreamID:      events.StreamID(id.String()),
			StreamType:    events.StreamType("schema"),
			StreamVersion: 1,
			EventType:     EventTypeSchemaCreated,
			EventVersion:  1,
			Payload:       payloadJSON,
		})
		require.NoError(t, err)
		assert.NotNil(t, aggregate.compiledSchema)
	})

	t.Run("validates payload against compiled schema", func(t *testing.T) {
		aggregate := &Aggregate{}
		require.NoError(t, aggregate.Apply(context.Background(), events.Event{
			StreamID:      events.StreamID(id.String()),
			StreamType:    events.StreamType("schema"),
			StreamVersion: 1,
			EventType:     EventTypeSchemaCreated,
			EventVersion:  1,
			Payload:       payloadJSON,
		}))

		require.NoError(t, aggregate.Validate(context.Background(), json.RawMessage(`{"name":"Ada"}`)))

		err := aggregate.Validate(context.Background(), json.RawMessage(`{"age":42}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing property 'name'")
	})

	t.Run("fails when no schema has been applied", func(t *testing.T) {
		aggregate := &Aggregate{}

		err := aggregate.Validate(context.Background(), json.RawMessage(`{"name":"Ada"}`))
		require.Error(t, err)
		assert.Equal(t, "no schema has been applied", err.Error())
	})
}

func mustMarshalSchema(t *testing.T, schema Schema) []byte {
	t.Helper()
	payload, err := json.Marshal(schema)
	require.NoError(t, err)
	return payload
}

func replayRecorded(t *testing.T, aggregate *Aggregate, stream *events.Stream) error {
	t.Helper()
	for _, event := range stream.RecordedEvents() {
		if err := aggregate.Apply(context.Background(), event); err != nil {
			return err
		}
	}
	return nil
}
