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

func newTestRepository(t *testing.T) *Repository {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err, "open db")
	t.Cleanup(func() { _ = db.Close() })

	repo := NewRepository(db)
	require.NoError(t, repo.Init(context.Background()), "init repo")

	return repo
}

func TestRepository(t *testing.T) {
	t.Run("AddNode and GetNodesBySchemaID", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()
		schemaID := events.StreamID("schema_01HXYZ")

		uid, err := NewID()
		require.NoError(t, err)

		err = repo.AddNode(ctx, Node{
			UID:       uid,
			SchemaID:  schemaID,
			CreatedAt: 1700000000,
			Payload:   json.RawMessage(` { "name": "Ada" } `),
		})
		require.NoError(t, err)

		nodes, err := repo.GetNodesBySchemaID(ctx, schemaID, 10)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, uid, nodes[0].UID)
		assert.Equal(t, schemaID, nodes[0].SchemaID)
		assert.Equal(t, json.RawMessage(`{"name":"Ada"}`), nodes[0].Payload)
	})

	t.Run("AddNode fails when created_at is missing", func(t *testing.T) {
		repo := newTestRepository(t)

		uid, err := NewID()
		require.NoError(t, err)

		err = repo.AddNode(context.Background(), Node{
			UID:      uid,
			SchemaID: events.StreamID("schema_01HXYZ"),
			Payload:  json.RawMessage(`{"name":"Ada"}`),
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for missing uid", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddNode(context.Background(), Node{
			UID:       ID{},
			SchemaID:  events.StreamID("schema_01HXYZ"),
			CreatedAt: time.Now().Unix(),
			Payload:   json.RawMessage(`{"name":"Ada"}`),
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for missing schema id", func(t *testing.T) {
		repo := newTestRepository(t)
		uid, err := NewID()
		require.NoError(t, err)

		err = repo.AddNode(context.Background(), Node{
			UID:       uid,
			SchemaID:  events.StreamID(""),
			CreatedAt: time.Now().Unix(),
			Payload:   json.RawMessage(`{"name":"Ada"}`),
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for oversized payload", func(t *testing.T) {
		repo := newTestRepository(t)
		uid, err := NewID()
		require.NoError(t, err)

		over := `{"x":"` + strings.Repeat("a", 256*1024) + `"}`
		err = repo.AddNode(context.Background(), Node{
			UID:       uid,
			SchemaID:  events.StreamID("schema_01HXYZ"),
			CreatedAt: time.Now().Unix(),
			Payload:   json.RawMessage(over),
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for duplicate uid", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()
		uid, err := NewID()
		require.NoError(t, err)

		node := Node{UID: uid, SchemaID: events.StreamID("schema_01HXYZ"), CreatedAt: time.Now().Unix(), Payload: json.RawMessage(`{"name":"Ada"}`)}
		require.NoError(t, repo.AddNode(ctx, node))
		require.Error(t, repo.AddNode(ctx, node))
	})

	t.Run("ID uses prefixed text form", func(t *testing.T) {
		uid, err := NewID()
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(uid.String(), "node_"))
		parsed, err := ParseID(uid.String())
		require.NoError(t, err)
		assert.Equal(t, uid, parsed)
	})
}
