package synapse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/links"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/replicators"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newTestService(t *testing.T, reps ...replicators.Replicator) (*Synapse, *events.Repository, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	eventsRepo := events.NewRepository()
	require.NoError(t, eventsRepo.Init(context.Background(), db))
	require.NoError(t, schemas.NewProjectionRepository().Init(context.Background(), db))
	require.NoError(t, nodes.NewProjectionRepository().Init(context.Background(), db))
	require.NoError(t, links.NewProjectionRepository().Init(context.Background(), db))

	return NewSynapse(db, reps...), eventsRepo, db
}

func TestSynapse_AddSchema(t *testing.T) {
	t.Run("normalizes validates and appends schema event", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t)

		err := svc.AddSchema(context.Background(), "  person  ", " { \"type\": \"object\" } ")
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
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
		svc, eventsRepo, db := newTestService(t)

		require.NoError(t, svc.AddSchema(context.Background(), "person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID := seedEvents[0].StreamID

		before := time.Now().Unix()
		err = svc.AddNode(context.Background(), schemaID, " { \"name\": \"Ada\" } ")
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
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
		assert.Contains(t, string(payload["id"]), "node_")
		assert.JSONEq(t, `"`+schemaID.String()+`"`, string(payload["schema_id"]))
		assert.JSONEq(t, `{"name":"Ada"}`, string(payload["payload"]))
	})

	t.Run("fails when payload does not match schema", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t)

		require.NoError(t, svc.AddSchema(context.Background(), "person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)

		err = svc.AddNode(context.Background(), seedEvents[0].StreamID, `{"age":42}`)
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 1)
	})
}

func TestSynapse_LinkNodes(t *testing.T) {
	t.Run("appends one undirected link event", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)

		before := time.Now().Unix()
		err := svc.LinkNodes(context.Background(), toID, fromID)
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
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
		svc, eventsRepo, db := newTestService(t)
		_, fromID, _ := seedTwoNodes(t, svc, eventsRepo, db)
		missing, err := nodes.ParseID("node_01ARZ3NDEKTSV4RRFFQ69G5FAX")
		require.NoError(t, err)

		err = svc.LinkNodes(context.Background(), fromID, missing)
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 3)
	})

	t.Run("fails when undirected link already exists", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)

		require.NoError(t, svc.LinkNodes(context.Background(), fromID, toID))
		err := svc.LinkNodes(context.Background(), toID, fromID)
		require.ErrorContains(t, err, "link already exists")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 4)
	})
}

func TestSynapse_UnlinkNodes(t *testing.T) {
	t.Run("appends one undirected unlink event", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)
		require.NoError(t, svc.LinkNodes(context.Background(), fromID, toID))

		before := time.Now().Unix()
		err := svc.UnlinkNodes(context.Background(), toID, fromID)
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 5)

		e := eventsInStream[4]
		assert.Equal(t, links.StreamTypeLink, e.StreamType)
		assert.Equal(t, links.EventTypeLinkRemoved, e.EventType)
		assert.Equal(t, int64(2), e.StreamVersion)
		assert.Equal(t, links.StreamIDForPair(events.StreamID(fromID.String()), events.StreamID(toID.String())), e.StreamID)
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		var payload map[string]string
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, fromID.String(), payload["from"])
		assert.Equal(t, toID.String(), payload["to"])
	})

	t.Run("fails when link does not exist", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)

		err := svc.UnlinkNodes(context.Background(), fromID, toID)
		require.ErrorContains(t, err, "link does not exist")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 3)
	})
}

func TestSynapse_RunProjections(t *testing.T) {
	t.Run("catches up all projections and persists iterators", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t)
		schemasRepo := schemas.NewProjectionRepository()
		nodesRepo := nodes.NewProjectionRepository()
		linksRepo := links.NewProjectionRepository()

		schemaID, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)
		require.NoError(t, svc.LinkNodes(context.Background(), fromID, toID))
		require.NoError(t, svc.UnlinkNodes(context.Background(), fromID, toID))

		require.NoError(t, svc.RunProjections(context.Background()))

		storedSchemas, err := schemasRepo.GetSchemas(context.Background(), db)
		require.NoError(t, err)
		require.Len(t, storedSchemas, 1)
		assert.Equal(t, schemaID.String(), storedSchemas[0].ID.String())

		storedNodes, err := nodesRepo.GetNodesBySchemaID(context.Background(), db, schemaID, 10)
		require.NoError(t, err)
		require.Len(t, storedNodes, 2)

		storedLinks, err := linksRepo.GetLinksFrom(context.Background(), db, []events.StreamID{events.StreamID(fromID.String()), events.StreamID(toID.String())}, 10)
		require.NoError(t, err)
		require.Empty(t, storedLinks)

		schemaPos, err := eventsRepo.GetProjectionIterator(context.Background(), db, schemas.ProjectionName, schemas.StreamTypeSchema)
		require.NoError(t, err)
		nodePos, err := eventsRepo.GetProjectionIterator(context.Background(), db, nodes.ProjectionName, nodes.StreamTypeNode)
		require.NoError(t, err)
		linkPos, err := eventsRepo.GetProjectionIterator(context.Background(), db, links.ProjectionName, links.StreamTypeLink)
		require.NoError(t, err)
		assert.Greater(t, schemaPos, int64(0))
		assert.Greater(t, nodePos, int64(0))
		assert.Greater(t, linkPos, int64(0))

		require.NoError(t, svc.RunProjections(context.Background()))

		storedLinksAgain, err := linksRepo.GetLinksFrom(context.Background(), db, []events.StreamID{events.StreamID(fromID.String())}, 10)
		require.NoError(t, err)
		require.Empty(t, storedLinksAgain)
	})
}

func TestSynapse_RunReplicators(t *testing.T) {
	t.Run("catches up all events into a single jsonl file and persists iterators", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "replica.jsonl")
		rep := replicators.NewFile("events_jsonl", path)
		svc, eventsRepo, db := newTestService(t, rep)
		schemaID, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)
		require.NoError(t, svc.LinkNodes(context.Background(), fromID, toID))

		require.NoError(t, svc.RunReplicators(context.Background()))

		iterator, err := eventsRepo.GetReplicatorIterator(context.Background(), db, rep.Name())
		require.NoError(t, err)
		assert.Greater(t, iterator, int64(0))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		require.Len(t, lines, 4)

		for i, line := range lines {
			var event events.Event
			require.NoError(t, json.Unmarshal([]byte(line), &event))
			assert.Equal(t, int64(i+1), event.GlobalPosition)
		}

		require.NoError(t, svc.RunReplicators(context.Background()))

		data, err = os.ReadFile(path)
		require.NoError(t, err)
		lines = strings.Split(strings.TrimSpace(string(data)), "\n")
		require.Len(t, lines, 4)
		assert.Equal(t, schemaID.String(), mustReadReplicatedEvent(t, lines[0]).StreamID.String())
	})
}

func TestSynapse_Restore(t *testing.T) {
	t.Run("restores events from named replicator into an empty database", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "replica.jsonl")
		rep := replicators.NewFile("events_jsonl", path)
		sourceSvc, sourceEventsRepo, sourceDB := newTestService(t, rep)
		schemaID, fromID, toID := seedTwoNodes(t, sourceSvc, sourceEventsRepo, sourceDB)
		require.NoError(t, sourceSvc.LinkNodes(context.Background(), fromID, toID))
		require.NoError(t, sourceSvc.RunReplicators(context.Background()))

		targetRep := replicators.NewFile("events_jsonl", path)
		targetSvc, targetEventsRepo, targetDB := newTestService(t, targetRep)
		require.NoError(t, targetSvc.Restore(context.Background(), "events_jsonl"))

		sourceEvents, err := sourceEventsRepo.GetStreamEvents(context.Background(), sourceDB, "", 0, 100)
		require.NoError(t, err)
		targetEvents, err := targetEventsRepo.GetStreamEvents(context.Background(), targetDB, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, targetEvents, len(sourceEvents))
		assert.Equal(t, sourceEvents, targetEvents)

		require.NoError(t, targetSvc.RunProjections(context.Background()))
		nodesRepo := nodes.NewProjectionRepository()
		storedNodes, err := nodesRepo.GetNodesBySchemaID(context.Background(), targetDB, schemaID, 10)
		require.NoError(t, err)
		require.Len(t, storedNodes, 2)
	})

	t.Run("fails when event store is not empty", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "replica.jsonl")
		rep := replicators.NewFile("events_jsonl", path)
		sourceSvc, sourceEventsRepo, sourceDB := newTestService(t, rep)
		_, fromID, toID := seedTwoNodes(t, sourceSvc, sourceEventsRepo, sourceDB)
		require.NoError(t, sourceSvc.LinkNodes(context.Background(), fromID, toID))
		require.NoError(t, sourceSvc.RunReplicators(context.Background()))

		targetSvc, _, _ := newTestService(t, replicators.NewFile("events_jsonl", path))
		require.NoError(t, targetSvc.AddSchema(context.Background(), "person", `{"type":"object"}`))

		err := targetSvc.Restore(context.Background(), "events_jsonl")
		require.ErrorContains(t, err, "event store must be empty")
	})

	t.Run("fails when replicator is not found", func(t *testing.T) {
		svc, _, _ := newTestService(t)
		err := svc.Restore(context.Background(), "missing")
		require.ErrorContains(t, err, `replicator "missing" not found`)
	})
}

func mustReadReplicatedEvent(t *testing.T, line string) events.Event {
	t.Helper()

	var event events.Event
	require.NoError(t, json.Unmarshal([]byte(line), &event))
	return event
}

func seedTwoNodes(t *testing.T, svc *Synapse, eventsRepo *events.Repository, db *sql.DB) (schemaID events.StreamID, fromID, toID nodes.ID) {
	t.Helper()

	require.NoError(t, svc.AddSchema(context.Background(), "person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`))
	seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
	require.NoError(t, err)
	require.Len(t, seedEvents, 1)
	schemaID = seedEvents[0].StreamID

	require.NoError(t, svc.AddNode(context.Background(), schemaID, `{"name":"Ada"}`))
	require.NoError(t, svc.AddNode(context.Background(), schemaID, `{"name":"Grace"}`))

	seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
	require.NoError(t, err)
	require.Len(t, seedEvents, 3)

	fromID, err = nodes.ParseID(seedEvents[1].StreamID.String())
	require.NoError(t, err)
	toID, err = nodes.ParseID(seedEvents[2].StreamID.String())
	require.NoError(t, err)
	return schemaID, fromID, toID
}
