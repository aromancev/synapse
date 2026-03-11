package synapse

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/links"
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
		assert.Equal(t, schemas.StreamTypeSchema, e.StreamType)
		assert.Equal(t, schemas.EventTypeSchemaCreated, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))

		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Contains(t, string(payload["id"]), "schema_")
		assert.JSONEq(t, `"person"`, string(payload["name"]))
		assert.JSONEq(t, `{"type":"object"}`, string(payload["schema"]))
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
		assert.Equal(t, nodes.StreamTypeNode, e.StreamType)
		assert.Equal(t, nodes.EventTypeNodeCreated, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Contains(t, string(payload["uid"]), "node_")
		assert.JSONEq(t, `"`+schemaID.String()+`"`, string(payload["schema_id"]))
		assert.JSONEq(t, `{"name":"Ada"}`, string(payload["payload"]))
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

func TestSynapse_LinkNodes(t *testing.T) {
	t.Run("appends one undirected link event", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo)

		before := time.Now().Unix()
		err := svc.LinkNodes(context.Background(), toID, fromID)
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 4)

		e := eventsInStream[3]
		assert.Equal(t, links.StreamTypeLink, e.StreamType)
		assert.Equal(t, links.EventTypeLinkCreated, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.Equal(t, links.StreamIDForPair(events.StreamID(fromID.String()), events.StreamID(toID.String())), e.StreamID)
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		var payload map[string]string
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, fromID.String(), payload["from"])
		assert.Equal(t, toID.String(), payload["to"])
	})

	t.Run("fails when either node does not exist", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)
		_, fromID, _ := seedTwoNodes(t, svc, eventsRepo)
		missing, err := nodes.ParseID("node_01ARZ3NDEKTSV4RRFFQ69G5FAX")
		require.NoError(t, err)

		err = svc.LinkNodes(context.Background(), fromID, missing)
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 3)
	})

	t.Run("fails when undirected link already exists", func(t *testing.T) {
		svc, eventsRepo := newTestService(t)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo)

		require.NoError(t, svc.LinkNodes(context.Background(), fromID, toID))
		err := svc.LinkNodes(context.Background(), toID, fromID)
		require.ErrorContains(t, err, "link already exists")

		eventsInStream, streamErr := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 4)
	})
}

func seedTwoNodes(t *testing.T, svc *Synapse, eventsRepo *events.Repository) (schemaID events.StreamID, fromID, toID nodes.ID) {
	t.Helper()

	require.NoError(t, svc.AddSchema(context.Background(), "person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`))
	seedEvents, err := eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
	require.NoError(t, err)
	require.Len(t, seedEvents, 1)
	schemaID = seedEvents[0].StreamID

	require.NoError(t, svc.AddNode(context.Background(), schemaID, `{"name":"Ada"}`))
	require.NoError(t, svc.AddNode(context.Background(), schemaID, `{"name":"Grace"}`))

	seedEvents, err = eventsRepo.GetEventsFromGlobalPosition(context.Background(), 0)
	require.NoError(t, err)
	require.Len(t, seedEvents, 3)

	fromID, err = nodes.ParseID(seedEvents[1].StreamID.String())
	require.NoError(t, err)
	toID, err = nodes.ParseID(seedEvents[2].StreamID.String())
	require.NoError(t, err)
	return schemaID, fromID, toID
}
