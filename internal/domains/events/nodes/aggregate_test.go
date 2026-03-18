package nodes

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
	t.Run("records normalized node created event", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		stream := events.NewStream(events.StreamID(id.String()), StreamTypeNode, nil)
		aggregate := &Aggregate{}

		err = aggregate.Create(context.Background(), stream, json.RawMessage(` { "name": "Ada" } `), id, events.StreamID("  schema_01HXYZ  "))
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 1)

		e := recorded[0]
		assert.Equal(t, StreamTypeNode, e.StreamType)
		assert.Equal(t, EventTypeNodeCreated, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))

		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.JSONEq(t, `"`+id.String()+`"`, string(payload["id"]))
		assert.JSONEq(t, `"schema_01HXYZ"`, string(payload["schema_id"]))
		assert.JSONEq(t, `{"name":"Ada"}`, string(payload["payload"]))
	})
}

func TestAggregate_Update(t *testing.T) {
	t.Run("records node updated event with full payload", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		stream := events.NewStream(events.StreamID(id.String()), StreamTypeNode, nil)
		aggregate := &Aggregate{}
		require.NoError(t, aggregate.Create(context.Background(), stream, json.RawMessage(`{"name":"Ada"}`), id, events.StreamID("schema_01HXYZ")))
		require.NoError(t, replayAggregate(t, aggregate, stream.RecordedEvents()[:1]))

		err = aggregate.Update(context.Background(), stream, json.RawMessage(` { "name": "Grace", "active": true } `))
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 2)

		e := recorded[1]
		assert.Equal(t, EventTypeNodeUpdated, e.EventType)
		assert.Equal(t, int64(2), e.StreamVersion)

		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.JSONEq(t, `{"name":"Grace","active":true}`, string(payload["payload"]))
	})

	t.Run("replay replaces payload from update event", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)

		createdPayload, err := json.Marshal(nodeCreatedEvent{
			ID:       id,
			SchemaID: events.StreamID("schema_01HXYZ"),
			Payload:  json.RawMessage(`{"name":"Ada"}`),
		}.normalized())
		require.NoError(t, err)

		updatedPayload, err := json.Marshal(nodeUpdatedEvent{Payload: json.RawMessage(`{"name":"Grace","tags":["one","two"]}`)})
		require.NoError(t, err)

		aggregate := &Aggregate{}
		require.NoError(t, aggregate.Apply(context.Background(), events.Event{EventType: EventTypeNodeCreated, Payload: createdPayload}))
		require.NoError(t, aggregate.Apply(context.Background(), events.Event{EventType: EventTypeNodeUpdated, Payload: updatedPayload}))

		assert.JSONEq(t, `{"name":"Grace","tags":["one","two"]}`, string(aggregate.Payload()))
	})
}

func replayAggregate(t *testing.T, aggregate *Aggregate, recorded []events.Event) error {
	t.Helper()
	for _, event := range recorded {
		if err := aggregate.Apply(context.Background(), event); err != nil {
			return err
		}
	}
	return nil
}
