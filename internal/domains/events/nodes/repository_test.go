package nodes

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
	t.Run("AddNode and GetNodesBySchema", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddNode(ctx, Node{
			ExternalID: uuid.NewString(),
			Schema:     "person",
			CreatedAt:  1700000000,
			Payload:    ` { "name": "Ada" } `,
		})
		require.NoError(t, err)

		nodes, err := repo.GetNodesBySchema(ctx, "person", 10)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, `{"name":"Ada"}`, nodes[0].Payload)
	})

	t.Run("AddNode fails when created_at is missing", func(t *testing.T) {
		repo := newTestRepository(t)

		err := repo.AddNode(context.Background(), Node{
			ExternalID: uuid.NewString(),
			Schema:     "person",
			Payload:    `{"name":"Ada"}`,
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for invalid external_id", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddNode(context.Background(), Node{
			ExternalID: "nope",
			Schema:     "person",
			CreatedAt:  time.Now().Unix(),
			Payload:    `{"name":"Ada"}`,
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for invalid schema name", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddNode(context.Background(), Node{
			ExternalID: uuid.NewString(),
			Schema:     "Person",
			CreatedAt:  time.Now().Unix(),
			Payload:    `{"name":"Ada"}`,
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for oversized payload", func(t *testing.T) {
		repo := newTestRepository(t)
		over := `{"x":"` + strings.Repeat("a", 256*1024) + `"}`
		err := repo.AddNode(context.Background(), Node{
			ExternalID: uuid.NewString(),
			Schema:     "person",
			CreatedAt:  time.Now().Unix(),
			Payload:    over,
		})
		require.Error(t, err)
	})

	t.Run("AddNode fails for duplicate external_id", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()
		externalID := uuid.NewString()

		node := Node{ExternalID: externalID, Schema: "person", CreatedAt: time.Now().Unix(), Payload: `{"name":"Ada"}`}
		require.NoError(t, repo.AddNode(ctx, node))
		require.Error(t, repo.AddNode(ctx, node))
	})
}
