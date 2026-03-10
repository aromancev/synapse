package synapse

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newTestService(t *testing.T) (*Synapse, *events.Repository) {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	eventsRepo := events.NewRepository(db)
	require.NoError(t, eventsRepo.Init(context.Background()))

	return NewSynapse(db), eventsRepo
}

func TestSynapse_AddSchema(t *testing.T) {
	t.Run("normalizes validates and appends schema event", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)

		err := svc.AddSchema(context.Background(), "  person  ", " { \"type\": \"object\" } ")
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 1)

		e := eventsInStream[0]
		assert.True(t, strings.HasPrefix(e.StreamID.String(), "schema_"))
		assert.Equal(t, SchemaStreamType, e.StreamType)
		assert.Equal(t, EventTypeSchemaAdded, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))

		var payload schemas.Schema
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.True(t, strings.HasPrefix(payload.ID.String(), "schema_"))
		assert.Equal(t, "person", payload.Name)
		assert.Equal(t, json.RawMessage(`{"type":"object"}`), payload.Schema)
	})

	t.Run("fails on invalid schema", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)

		err := svc.AddSchema(context.Background(), "Bad-Name", `{"type":"object"}`)
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 0)
	})
}

func TestSynapse_AddNode(t *testing.T) {
	t.Run("normalizes validates and appends node event", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)

		require.NoError(t, svc.AddSchema(context.Background(), "person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`))
		seedEvents, err := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID := seedEvents[0].StreamID

		before := time.Now().Unix()
		err = svc.AddNode(context.Background(), schemaID, " { \"name\": \"Ada\" } ")
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 2)

		e := eventsInStream[1]
		assert.True(t, strings.HasPrefix(e.StreamID.String(), "node_"))
		assert.Equal(t, NodeStreamType, e.StreamType)
		assert.Equal(t, EventTypeNodeAdded, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		var payload nodes.Node
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.True(t, strings.HasPrefix(payload.UID.String(), "node_"))
		assert.Equal(t, schemaID, payload.SchemaID)
		assert.Equal(t, json.RawMessage(`{"name":"Ada"}`), payload.Payload)
		assert.GreaterOrEqual(t, payload.CreatedAt, before)
		assert.LessOrEqual(t, payload.CreatedAt, after)
	})

	t.Run("fails on invalid node", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)

		err := svc.AddNode(context.Background(), events.StreamID(""), `{"name":"Ada"}`)
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 0)
	})

	t.Run("fails when payload does not match schema", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)

		require.NoError(t, svc.AddSchema(context.Background(), "person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`))
		seedEvents, err := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)

		err = svc.AddNode(context.Background(), seedEvents[0].StreamID, `{"age":42}`)
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 1)
	})
}
