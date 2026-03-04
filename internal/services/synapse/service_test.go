package synapse

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/aromancev/synapse/internal/domains/events"
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

		eventsInStream, err := eventsRepo.GetEventsByStream(context.Background(), "schema:person", 0)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 1)

		e := eventsInStream[0]
		assert.Equal(t, "schema:person", e.StreamID)
		assert.Equal(t, StreamType, e.StreamType)
		assert.Equal(t, EventTypeSchemaAdded, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)

		var payload schemas.Schema
		require.NoError(t, json.Unmarshal([]byte(e.Payload), &payload))
		assert.Equal(t, "person", payload.Name)
		assert.Equal(t, `{"type":"object"}`, payload.Schema)
	})

	t.Run("fails on invalid schema", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)

		err := svc.AddSchema(context.Background(), "Bad-Name", `{"type":"object"}`)
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetEventsByStream(context.Background(), "schema:Bad-Name", 0)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 0)
	})
}
