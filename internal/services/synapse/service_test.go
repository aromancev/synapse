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

func newTestService(t *testing.T, rep replicators.Replicator) (*Synapse, *events.Repository, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	eventsRepo := events.NewRepository()
	require.NoError(t, eventsRepo.Init(context.Background(), db))
	require.NoError(t, schemas.NewProjectionRepository().Init(context.Background(), db))
	require.NoError(t, nodes.NewProjectionRepository().Init(context.Background(), db))
	require.NoError(t, links.NewProjectionRepository().Init(context.Background(), db))

	return NewSynapse(db, rep), eventsRepo, db
}

func TestSynapse_AddSchema(t *testing.T) {
	t.Run("normalizes validates and appends schema event", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		err := svc.AddSchema(context.Background(), "  person  ", json.RawMessage(" { \"type\": \"object\" } "))
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
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		before := time.Now().Unix()
		err = svc.AddNode(context.Background(), schemaID, json.RawMessage(" { \"name\": \"Ada\" } "))
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
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		err = svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"age":42}`))
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 1)
	})

	t.Run("fails when schema is archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.ArchiveSchema(context.Background(), schemaID))
		err = svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada"}`))
		require.ErrorContains(t, err, "schema is archived")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 2)
	})
}

func TestSynapse_ArchiveSchema(t *testing.T) {
	t.Run("appends schema archived event and projections mark stored schema as archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.RunProjection(context.Background(), schemas.NewProjection()))
		_, err = schemas.NewProjectionRepository().GetSchemaByID(context.Background(), db, schemaID)
		require.NoError(t, err)

		before := time.Now().Unix()
		err = svc.ArchiveSchema(context.Background(), schemaID)
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 2)

		e := eventsInStream[1]
		assert.Equal(t, schemaID.StreamID(), e.StreamID)
		assert.Equal(t, schemas.StreamTypeSchema, e.StreamType)
		assert.Equal(t, schemas.EventTypeSchemaArchived, e.EventType)
		assert.Equal(t, int64(2), e.StreamVersion)
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		require.NoError(t, svc.RunProjection(context.Background(), schemas.NewProjection()))
		stored, err := schemas.NewProjectionRepository().GetSchemaByID(context.Background(), db, schemaID)
		require.NoError(t, err)
		assert.Positive(t, stored.ArchivedAt)

		activeSchemas, err := schemas.NewProjectionRepository().GetSchemas(context.Background(), db)
		require.NoError(t, err)
		assert.Empty(t, activeSchemas)

		archivedSchemas, err := schemas.NewProjectionRepository().GetArchivedSchemas(context.Background(), db)
		require.NoError(t, err)
		require.Len(t, archivedSchemas, 1)
		assert.Equal(t, schemaID, archivedSchemas[0].ID)
	})

	t.Run("fails when schema does not exist", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		missing, err := schemas.ParseID("schema_01ARZ3NDEKTSV4RRFFQ69G5FAX")
		require.NoError(t, err)

		err = svc.ArchiveSchema(context.Background(), missing)
		require.ErrorContains(t, err, "schema does not exist")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 0)
	})

	t.Run("fails when schema is already archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object"}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.ArchiveSchema(context.Background(), schemaID))
		err = svc.ArchiveSchema(context.Background(), schemaID)
		require.ErrorContains(t, err, "schema is already archived")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 2)
	})
}

func TestSynapse_UpdateNode(t *testing.T) {
	t.Run("appends node updated event and projections replace payload", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"},"active":{"type":"boolean"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada"}`)))
		seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 2)
		nodeID, err := nodes.ParseID(seedEvents[1].StreamID.String())
		require.NoError(t, err)

		before := time.Now().Unix()
		err = svc.UpdateNode(context.Background(), nodeID, json.RawMessage(`{"name":"Grace","active":true}`))
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 3)

		e := eventsInStream[2]
		assert.Equal(t, nodeID.StreamID(), e.StreamID)
		assert.Equal(t, nodes.StreamTypeNode, e.StreamType)
		assert.Equal(t, nodes.EventTypeNodeUpdated, e.EventType)
		assert.Equal(t, int64(2), e.StreamVersion)
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.JSONEq(t, `{"name":"Grace","active":true}`, string(payload["payload"]))

		require.NoError(t, svc.RunProjection(context.Background(), nodes.NewProjection()))
		stored, err := nodes.NewProjectionRepository().GetNodeByID(context.Background(), db, nodeID)
		require.NoError(t, err)
		assert.JSONEq(t, `{"name":"Grace","active":true}`, string(stored.Payload))
	})

	t.Run("fails when updated payload does not match schema", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada"}`)))
		seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 2)
		nodeID, err := nodes.ParseID(seedEvents[1].StreamID.String())
		require.NoError(t, err)

		err = svc.UpdateNode(context.Background(), nodeID, json.RawMessage(`42`))
		require.Error(t, err)

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 2)
	})

	t.Run("fails when node is archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada"}`)))
		seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 2)
		nodeID, err := nodes.ParseID(seedEvents[1].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.ArchiveNode(context.Background(), nodeID))
		err = svc.UpdateNode(context.Background(), nodeID, json.RawMessage(`{"name":"Grace"}`))
		require.ErrorContains(t, err, "node is archived")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 3)
	})
}

func TestSynapse_UpdateNodeKeywords(t *testing.T) {
	t.Run("appends node keywords updated event and projections replace keywords", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada"}`)))
		seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 2)
		nodeID, err := nodes.ParseID(seedEvents[1].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.UpdateNodeKeywords(context.Background(), nodeID, []string{"  Math  ", "history", "math"}))

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 3)

		e := eventsInStream[2]
		assert.Equal(t, nodes.EventTypeNodeKeywordsUpdated, e.EventType)
		assert.Equal(t, int64(2), e.StreamVersion)

		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.JSONEq(t, `["math","history"]`, string(payload["keywords"]))

		require.NoError(t, svc.RunProjection(context.Background(), nodes.NewProjection()))
		stored, err := nodes.NewProjectionRepository().GetNodeByID(context.Background(), db, nodeID)
		require.NoError(t, err)
		assert.Equal(t, []string{"math", "history"}, stored.Keywords)
		assert.Equal(t, "name Ada math history", stored.SearchText)
	})
}

func TestSynapse_ArchiveNode(t *testing.T) {
	t.Run("appends node archived event and projections mark stored node as archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada"}`)))
		seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 2)
		nodeID, err := nodes.ParseID(seedEvents[1].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.RunProjection(context.Background(), nodes.NewProjection()))
		_, err = nodes.NewProjectionRepository().GetNodeByID(context.Background(), db, nodeID)
		require.NoError(t, err)

		before := time.Now().Unix()
		err = svc.ArchiveNode(context.Background(), nodeID)
		after := time.Now().Unix()
		require.NoError(t, err)

		eventsInStream, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, eventsInStream, 3)

		e := eventsInStream[2]
		assert.Equal(t, nodeID.StreamID(), e.StreamID)
		assert.Equal(t, nodes.StreamTypeNode, e.StreamType)
		assert.Equal(t, nodes.EventTypeNodeArchived, e.EventType)
		assert.Equal(t, int64(2), e.StreamVersion)
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		require.NoError(t, svc.RunProjection(context.Background(), nodes.NewProjection()))
		stored, err := nodes.NewProjectionRepository().GetNodeByID(context.Background(), db, nodeID)
		require.NoError(t, err)
		assert.Positive(t, stored.ArchivedAt)

		activeNodes, err := nodes.NewProjectionRepository().GetNodesBySchemaID(context.Background(), db, schemaID.StreamID(), 10)
		require.NoError(t, err)
		assert.Empty(t, activeNodes)

		archivedNodes, err := nodes.NewProjectionRepository().GetArchivedNodesBySchemaID(context.Background(), db, schemaID.StreamID(), 10)
		require.NoError(t, err)
		require.Len(t, archivedNodes, 1)
		assert.Equal(t, nodeID, archivedNodes[0].ID)
	})

	t.Run("fails when node does not exist", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		missing, err := nodes.ParseID("node_01ARZ3NDEKTSV4RRFFQ69G5FAX")
		require.NoError(t, err)

		err = svc.ArchiveNode(context.Background(), missing)
		require.ErrorContains(t, err, "node does not exist")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 0)
	})

	t.Run("fails when node is already archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		_, nodeID, _ := seedTwoNodes(t, svc, eventsRepo, db)

		require.NoError(t, svc.ArchiveNode(context.Background(), nodeID))
		err := svc.ArchiveNode(context.Background(), nodeID)
		require.ErrorContains(t, err, "node is already archived")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 4)
	})
}

func TestSynapse_LinkNodes(t *testing.T) {
	t.Run("appends one undirected link event", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
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
		assert.Equal(t, links.StreamIDForPair(fromID.StreamID(), toID.StreamID()), e.StreamID)
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		var payload map[string]string
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, fromID.String(), payload["from"])
		assert.Equal(t, toID.String(), payload["to"])
	})

	t.Run("fails when either node does not exist", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
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
		svc, eventsRepo, db := newTestService(t, nil)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)

		require.NoError(t, svc.LinkNodes(context.Background(), fromID, toID))
		err := svc.LinkNodes(context.Background(), toID, fromID)
		require.ErrorContains(t, err, "link already exists")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 4)
	})

	t.Run("fails when from node is archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)
		require.NoError(t, svc.ArchiveNode(context.Background(), fromID))

		err := svc.LinkNodes(context.Background(), fromID, toID)
		require.ErrorContains(t, err, "from node is archived")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 4)
	})

	t.Run("fails when to node is archived", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)
		require.NoError(t, svc.ArchiveNode(context.Background(), toID))

		err := svc.LinkNodes(context.Background(), fromID, toID)
		require.ErrorContains(t, err, "to node is archived")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 4)
	})
}

func TestSynapse_UnlinkNodes(t *testing.T) {
	t.Run("appends one undirected unlink event", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
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
		assert.Equal(t, links.StreamIDForPair(fromID.StreamID(), toID.StreamID()), e.StreamID)
		assert.GreaterOrEqual(t, e.OccurredAt, before)
		assert.LessOrEqual(t, e.OccurredAt, after)

		var payload map[string]string
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, fromID.String(), payload["from"])
		assert.Equal(t, toID.String(), payload["to"])
	})

	t.Run("fails when link does not exist", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		_, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)

		err := svc.UnlinkNodes(context.Background(), fromID, toID)
		require.ErrorContains(t, err, "link does not exist")

		eventsInStream, streamErr := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, streamErr)
		assert.Len(t, eventsInStream, 3)
	})
}

func TestSynapse_SearchNodes(t *testing.T) {
	t.Run("returns active nodes in fts rank order", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"},"summary":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada Lovelace","summary":"analytical engine pioneer"}`)))
		require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Grace Hopper","summary":"compiler pioneer"}`)))
		require.NoError(t, svc.RunProjections(context.Background()))

		results, err := svc.SearchNodes(context.Background(), "analytical", 10)
		require.NoError(t, err)
		require.Len(t, results, 1)

		stored, err := nodes.NewProjectionRepository().GetNodesByIDs(context.Background(), db, results)
		require.NoError(t, err)
		require.Len(t, stored, 1)
		assert.JSONEq(t, `{"name":"Ada Lovelace","summary":"analytical engine pioneer"}`, string(stored[0].Payload))
		assert.Equal(t, "name Ada Lovelace summary analytical engine pioneer", stored[0].SearchText)
	})

	t.Run("returns nil for blank query", func(t *testing.T) {
		svc, _, _ := newTestService(t, nil)
		results, err := svc.SearchNodes(context.Background(), "   ", 10)
		require.NoError(t, err)
		assert.Nil(t, results)
	})
}

func TestSynapse_GetLinkedNodes(t *testing.T) {
	t.Run("walks the graph bidirectionally with depth breadth and archive filtering", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)

		require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
		seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 1)
		schemaID, err := schemas.ParseID(seedEvents[0].StreamID.String())
		require.NoError(t, err)

		for _, name := range []string{"Ada", "Grace", "Linus", "Margaret", "Archived"} {
			require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(fmt.Sprintf(`{"name":%q}`, name))))
		}

		seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, seedEvents, 6)

		var ids []nodes.ID
		for _, e := range seedEvents[1:] {
			id, err := nodes.ParseID(e.StreamID.String())
			require.NoError(t, err)
			ids = append(ids, id)
		}
		adaID, graceID, linusID, margaretID, archivedID := ids[0], ids[1], ids[2], ids[3], ids[4]

		require.NoError(t, svc.LinkNodes(context.Background(), adaID, graceID))
		require.NoError(t, svc.LinkNodes(context.Background(), graceID, linusID))
		require.NoError(t, svc.LinkNodes(context.Background(), graceID, margaretID))
		require.NoError(t, svc.LinkNodes(context.Background(), graceID, archivedID))
		require.NoError(t, svc.ArchiveNode(context.Background(), archivedID))
		require.NoError(t, svc.RunProjections(context.Background()))

		linked, err := svc.GetLinkedNodes(context.Background(), []nodes.ID{adaID}, 2, 2)
		require.NoError(t, err)
		require.Len(t, linked, 4)

		assert.Equal(t, adaID, linked[0].ID)
		assert.Contains(t, []nodes.ID{linusID, margaretID}, linked[3].ID)

		got := map[nodes.ID]nodes.Node{}
		for _, node := range linked {
			got[node.ID] = node
			assert.Zero(t, node.ArchivedAt)
		}
		assert.Contains(t, got, adaID)
		assert.Contains(t, got, graceID)
		assert.Contains(t, got, linusID)
		assert.Contains(t, got, margaretID)
		assert.NotContains(t, got, archivedID)
	})

	t.Run("depth zero returns only active seed nodes", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
		_, activeID, archivedID := seedTwoNodes(t, svc, eventsRepo, db)
		require.NoError(t, svc.ArchiveNode(context.Background(), archivedID))
		require.NoError(t, svc.RunProjections(context.Background()))

		linked, err := svc.GetLinkedNodes(context.Background(), []nodes.ID{activeID, archivedID, activeID}, 0, 10)
		require.NoError(t, err)
		require.Len(t, linked, 1)
		assert.Equal(t, activeID, linked[0].ID)
	})

	t.Run("rejects invalid breadth", func(t *testing.T) {
		svc, _, _ := newTestService(t, nil)
		linked, err := svc.GetLinkedNodes(context.Background(), nil, 1, 0)
		require.ErrorContains(t, err, "breadth must be positive")
		assert.Nil(t, linked)
	})
}

func TestSynapse_RunProjections(t *testing.T) {
	t.Run("catches up all projections and persists iterators", func(t *testing.T) {
		svc, eventsRepo, db := newTestService(t, nil)
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

		storedNodes, err := nodesRepo.GetNodesBySchemaID(context.Background(), db, schemaID.StreamID(), 10)
		require.NoError(t, err)
		require.Len(t, storedNodes, 2)

		storedLinks, err := linksRepo.GetLinksFrom(context.Background(), db, []events.StreamID{fromID.StreamID(), toID.StreamID()}, 10)
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

		storedLinksAgain, err := linksRepo.GetLinksFrom(context.Background(), db, []events.StreamID{fromID.StreamID()}, 10)
		require.NoError(t, err)
		require.Empty(t, storedLinksAgain)
	})
}

func TestSynapse_RunReplication(t *testing.T) {
	t.Run("catches up all events into a single jsonl file and persists iterators", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "replica.jsonl")
		rep := replicators.NewFile("events_jsonl", path)
		svc, eventsRepo, db := newTestService(t, rep)
		schemaID, fromID, toID := seedTwoNodes(t, svc, eventsRepo, db)
		require.NoError(t, svc.LinkNodes(context.Background(), fromID, toID))

		require.NoError(t, svc.RunReplication(context.Background()))

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

		require.NoError(t, svc.RunReplication(context.Background()))

		data, err = os.ReadFile(path)
		require.NoError(t, err)
		lines = strings.Split(strings.TrimSpace(string(data)), "\n")
		require.Len(t, lines, 4)
		assert.Equal(t, schemaID.String(), mustReadReplicatedEvent(t, lines[0]).StreamID.String())
	})
}

func TestSynapse_Restore(t *testing.T) {
	t.Run("restores events from replicator into an empty database", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "replica.jsonl")
		rep := replicators.NewFile("events_jsonl", path)
		sourceSvc, sourceEventsRepo, sourceDB := newTestService(t, rep)
		schemaID, fromID, toID := seedTwoNodes(t, sourceSvc, sourceEventsRepo, sourceDB)
		require.NoError(t, sourceSvc.LinkNodes(context.Background(), fromID, toID))
		require.NoError(t, sourceSvc.RunReplication(context.Background()))

		targetRep := replicators.NewFile("events_jsonl", path)
		targetSvc, targetEventsRepo, targetDB := newTestService(t, targetRep)
		require.NoError(t, targetSvc.Restore(context.Background()))

		sourceEvents, err := sourceEventsRepo.GetStreamEvents(context.Background(), sourceDB, "", 0, 100)
		require.NoError(t, err)
		targetEvents, err := targetEventsRepo.GetStreamEvents(context.Background(), targetDB, "", 0, 100)
		require.NoError(t, err)
		require.Len(t, targetEvents, len(sourceEvents))
		assert.Equal(t, sourceEvents, targetEvents)

		require.NoError(t, targetSvc.RunProjections(context.Background()))
		nodesRepo := nodes.NewProjectionRepository()
		storedNodes, err := nodesRepo.GetNodesBySchemaID(context.Background(), targetDB, schemaID.StreamID(), 10)
		require.NoError(t, err)
		require.Len(t, storedNodes, 2)
	})

	t.Run("fails when event store is not empty", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "replica.jsonl")
		rep := replicators.NewFile("events_jsonl", path)
		sourceSvc, sourceEventsRepo, sourceDB := newTestService(t, rep)
		_, fromID, toID := seedTwoNodes(t, sourceSvc, sourceEventsRepo, sourceDB)
		require.NoError(t, sourceSvc.LinkNodes(context.Background(), fromID, toID))
		require.NoError(t, sourceSvc.RunReplication(context.Background()))

		targetSvc, _, _ := newTestService(t, replicators.NewFile("events_jsonl", path))
		require.NoError(t, targetSvc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object"}`)))

		err := targetSvc.Restore(context.Background())
		require.ErrorContains(t, err, "event store must be empty")
	})

	t.Run("fails when no replicator configured", func(t *testing.T) {
		svc, _, _ := newTestService(t, nil)
		err := svc.Restore(context.Background())
		require.ErrorContains(t, err, "no replicator configured")
	})
}

func mustReadReplicatedEvent(t *testing.T, line string) events.Event {
	t.Helper()

	var event events.Event
	require.NoError(t, json.Unmarshal([]byte(line), &event))
	return event
}

func seedTwoNodes(t *testing.T, svc *Synapse, eventsRepo *events.Repository, db *sql.DB) (schemaID schemas.ID, fromID, toID nodes.ID) {
	t.Helper()

	require.NoError(t, svc.AddSchema(context.Background(), "person", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)))
	seedEvents, err := eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
	require.NoError(t, err)
	require.Len(t, seedEvents, 1)
	schemaID, err = schemas.ParseID(seedEvents[0].StreamID.String())
	require.NoError(t, err)

	require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Ada"}`)))
	require.NoError(t, svc.AddNode(context.Background(), schemaID, json.RawMessage(`{"name":"Grace"}`)))

	seedEvents, err = eventsRepo.GetStreamEvents(context.Background(), db, "", 0, 100)
	require.NoError(t, err)
	require.Len(t, seedEvents, 3)

	fromID, err = nodes.ParseID(seedEvents[1].StreamID.String())
	require.NoError(t, err)
	toID, err = nodes.ParseID(seedEvents[2].StreamID.String())
	require.NoError(t, err)
	return schemaID, fromID, toID
}
