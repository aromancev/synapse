package nodes

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newTestRepository(t *testing.T) (*ProjectionRepository, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err, "open db")
	t.Cleanup(func() { _ = db.Close() })

	repo := NewProjectionRepository()
	require.NoError(t, repo.Init(context.Background(), db), "init repo")

	return repo, db
}

func TestRepository(t *testing.T) {
	t.Run("UpsertNode and GetNodesBySchemaID", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		schemaID := events.StreamID("schema_01HXYZ")

		id, err := NewID()
		require.NoError(t, err)

		err = repo.UpsertNode(ctx, db, Node{ID: id, SchemaID: schemaID, CreatedAt: 1700000000, Payload: json.RawMessage(` { "name": "Ada" } `)})
		require.NoError(t, err)

		nodes, err := repo.GetNodesBySchemaID(ctx, db, schemaID, 10)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, id, nodes[0].ID)
		assert.Equal(t, schemaID, nodes[0].SchemaID)
		assert.Equal(t, int64(0), nodes[0].ArchivedAt)
		assert.Equal(t, json.RawMessage(`{"name":"Ada"}`), nodes[0].Payload)
		assert.Empty(t, nodes[0].Keywords)
		assert.Equal(t, "name Ada", nodes[0].SearchText)
	})

	t.Run("GetNodeByID returns stored node", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		id, err := NewID()
		require.NoError(t, err)

		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: id, SchemaID: events.StreamID("schema_01HXYZ"), CreatedAt: 1700000000, Payload: json.RawMessage(`{"name":"Ada"}`)}))

		node, err := repo.GetNodeByID(ctx, db, id)
		require.NoError(t, err)
		assert.Equal(t, id, node.ID)
	})

	t.Run("UpsertNode updates existing id", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		id, err := NewID()
		require.NoError(t, err)

		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: id, SchemaID: events.StreamID("schema_01HXYZ"), CreatedAt: 1700000000, Payload: json.RawMessage(`{"name":"Ada"}`)}))
		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: id, SchemaID: events.StreamID("schema_01HXYZ"), CreatedAt: 1700000010, Payload: json.RawMessage(`{"name":"Grace"}`)}))

		node, err := repo.GetNodeByID(ctx, db, id)
		require.NoError(t, err)
		assert.Equal(t, int64(1700000010), node.CreatedAt)
		assert.Equal(t, json.RawMessage(`{"name":"Grace"}`), node.Payload)
	})

	t.Run("UpsertNode fails when created_at is missing", func(t *testing.T) {
		repo, db := newTestRepository(t)
		id, err := NewID()
		require.NoError(t, err)

		err = repo.UpsertNode(context.Background(), db, Node{ID: id, SchemaID: events.StreamID("schema_01HXYZ"), Payload: json.RawMessage(`{"name":"Ada"}`)})
		require.Error(t, err)
	})

	t.Run("UpsertNode fails for missing id", func(t *testing.T) {
		repo, db := newTestRepository(t)
		err := repo.UpsertNode(context.Background(), db, Node{ID: ID{}, SchemaID: events.StreamID("schema_01HXYZ"), CreatedAt: time.Now().Unix(), Payload: json.RawMessage(`{"name":"Ada"}`)})
		require.Error(t, err)
	})

	t.Run("UpsertNode fails for missing schema id", func(t *testing.T) {
		repo, db := newTestRepository(t)
		id, err := NewID()
		require.NoError(t, err)

		err = repo.UpsertNode(context.Background(), db, Node{ID: id, SchemaID: events.StreamID(""), CreatedAt: time.Now().Unix(), Payload: json.RawMessage(`{"name":"Ada"}`)})
		require.Error(t, err)
	})

	t.Run("UpsertNode fails for oversized payload", func(t *testing.T) {
		repo, db := newTestRepository(t)
		id, err := NewID()
		require.NoError(t, err)

		over := `{"x":"` + strings.Repeat("a", 256*1024) + `"}`
		err = repo.UpsertNode(context.Background(), db, Node{ID: id, SchemaID: events.StreamID("schema_01HXYZ"), CreatedAt: time.Now().Unix(), Payload: json.RawMessage(over)})
		require.Error(t, err)
	})

	t.Run("GetNodesBySchemaID excludes archived nodes", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		schemaID := events.StreamID("schema_01HXYZ")
		activeID, err := NewID()
		require.NoError(t, err)
		archivedID, err := NewID()
		require.NoError(t, err)

		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: activeID, SchemaID: schemaID, CreatedAt: 1700000000, Payload: json.RawMessage(`{"name":"Ada"}`)}))
		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: archivedID, SchemaID: schemaID, CreatedAt: 1700000010, ArchivedAt: 1700000020, Payload: json.RawMessage(`{"name":"Grace"}`)}))

		nodes, err := repo.GetNodesBySchemaID(ctx, db, schemaID, 10)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, activeID, nodes[0].ID)
	})

	t.Run("GetArchivedNodesBySchemaID returns archived nodes", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		schemaID := events.StreamID("schema_01HXYZ")
		id, err := NewID()
		require.NoError(t, err)

		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: id, SchemaID: schemaID, CreatedAt: 1700000000, ArchivedAt: 1700000020, Payload: json.RawMessage(`{"name":"Ada"}`)}))

		nodes, err := repo.GetArchivedNodesBySchemaID(ctx, db, schemaID, 10)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, id, nodes[0].ID)
		assert.Equal(t, int64(1700000020), nodes[0].ArchivedAt)
	})

	t.Run("stores and loads normalized keywords", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		id, err := NewID()
		require.NoError(t, err)

		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: id, SchemaID: events.StreamID("schema_01HXYZ"), CreatedAt: 1700000000, Payload: json.RawMessage(`{"name":"Ada"}`), Keywords: []string{"  Math  ", "history science", "math"}}))

		node, err := repo.GetNodeByID(ctx, db, id)
		require.NoError(t, err)
		assert.Equal(t, []string{"math", "history", "science"}, node.Keywords)
		assert.Equal(t, "name Ada math history science", node.SearchText)
	})

	t.Run("SearchNodeIDs returns active matches from fts", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		schemaID := events.StreamID("schema_01HXYZ")
		adaID, err := NewID()
		require.NoError(t, err)
		graceID, err := NewID()
		require.NoError(t, err)

		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: adaID, SchemaID: schemaID, CreatedAt: 1700000000, Payload: json.RawMessage(`{"name":"Ada Lovelace","summary":"analytical engine pioneer"}`)}))
		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: graceID, SchemaID: schemaID, CreatedAt: 1700000010, ArchivedAt: 1700000020, Payload: json.RawMessage(`{"name":"Grace Hopper"}`)}))

		hits, err := repo.SearchNodeIDs(ctx, db, "analytical", 10)
		require.NoError(t, err)
		require.Len(t, hits, 1)
		assert.Equal(t, adaID, hits[0].ID)

		hits, err = repo.SearchNodeIDs(ctx, db, "Grace", 10)
		require.NoError(t, err)
		assert.Empty(t, hits)
	})

	t.Run("SearchNodeIDs supports explicit OR clauses with grouped AND terms", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()
		schemaID := events.StreamID("schema_01HXYZ")
		alphaID, err := NewID()
		require.NoError(t, err)
		betaID, err := NewID()
		require.NoError(t, err)
		gammaID, err := NewID()
		require.NoError(t, err)

		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: alphaID, SchemaID: schemaID, CreatedAt: 1700000000, Payload: json.RawMessage(`{"title":"Alpha"}`), Keywords: []string{"blue", "fox"}}))
		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: betaID, SchemaID: schemaID, CreatedAt: 1700000010, Payload: json.RawMessage(`{"title":"Beta"}`), Keywords: []string{"blue", "moon"}}))
		require.NoError(t, repo.UpsertNode(ctx, db, Node{ID: gammaID, SchemaID: schemaID, CreatedAt: 1700000020, Payload: json.RawMessage(`{"title":"Gamma"}`), Keywords: []string{"green"}}))

		hits, err := repo.SearchNodeIDs(ctx, db, `("blue" "fox") OR "green"`, 10)
		require.NoError(t, err)
		require.Len(t, hits, 2)
		assert.ElementsMatch(t, []ID{alphaID, gammaID}, []ID{hits[0].ID, hits[1].ID})
	})

	t.Run("ID uses prefixed text form", func(t *testing.T) {
		id, err := NewID()
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(id.String(), "node_"))
		parsed, err := ParseID(id.String())
		require.NoError(t, err)
		assert.Equal(t, id, parsed)
	})
}
