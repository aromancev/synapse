package schemas

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newTestRepository(t *testing.T) *ProjectionRepository {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err, "open db")
	t.Cleanup(func() { _ = db.Close() })

	repo := NewProjectionRepository(db)
	require.NoError(t, repo.Init(context.Background()), "init repo")

	return repo
}

func mustNewID(t *testing.T) ID {
	t.Helper()
	id, err := NewID()
	require.NoError(t, err)
	return id
}

func TestRepository(t *testing.T) {
	t.Run("UpsertSchema and GetSchemas", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()
		schemaID := mustNewID(t)

		err := repo.UpsertSchema(ctx, Schema{
			ID:   schemaID,
			Name: "  person  ",
			Schema: json.RawMessage(`
				{
				  "type": "object",
				  "properties": {
				    "name": { "type": "string" }
				  },
				  "required": ["name"]
				}
			`),
		})
		require.NoError(t, err, "add schema")

		schemas, err := repo.GetSchemas(ctx)
		require.NoError(t, err, "get schemas")
		require.Len(t, schemas, 1)
		assert.Equal(t, schemaID, schemas[0].ID)
		assert.Equal(t, "person", schemas[0].Name)
		assert.Equal(t, json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`), schemas[0].Schema)
	})

	t.Run("UpsertSchema fails for empty id", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "person",
			Schema: json.RawMessage(`{"type":"object"}`),
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema fails for empty name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			ID:     mustNewID(t),
			Name:   "   ",
			Schema: json.RawMessage(`{"type":"object"}`),
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema fails for invalid name format", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		for _, name := range []string{"Person", "person-name", "имя"} {
			err := repo.UpsertSchema(ctx, Schema{
				ID:     mustNewID(t),
				Name:   name,
				Schema: json.RawMessage(`{"type":"object"}`),
			})
			require.Error(t, err, "expected error for invalid schema name %q", name)
		}
	})

	t.Run("UpsertSchema allows numbers in name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			ID:     mustNewID(t),
			Name:   "person_1",
			Schema: json.RawMessage(`{"type":"object"}`),
		})
		require.NoError(t, err)
	})

	t.Run("UpsertSchema fails when name exceeds 64 chars", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			ID:     mustNewID(t),
			Name:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Schema: json.RawMessage(`{"type":"object"}`),
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema fails when schema exceeds 16KB", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		tooLarge := `{"x":"` + strings.Repeat("a", 16380) + `"}`
		err := repo.UpsertSchema(ctx, Schema{
			ID:     mustNewID(t),
			Name:   "large_schema",
			Schema: json.RawMessage(tooLarge),
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema fails for invalid json schema", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			ID:     mustNewID(t),
			Name:   "broken",
			Schema: json.RawMessage(`{"type":"not-a-valid-json-schema-type"}`),
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema updates existing schema by name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		firstID := mustNewID(t)
		secondID := mustNewID(t)
		require.NoError(t, repo.UpsertSchema(ctx, Schema{ID: firstID, Name: "entity", Schema: json.RawMessage(`{"type":"object"}`)}))
		require.NoError(t, repo.UpsertSchema(ctx, Schema{ID: secondID, Name: "entity", Schema: json.RawMessage(`{"type":"string"}`)}))

		schemas, err := repo.GetSchemas(ctx)
		require.NoError(t, err)
		require.Len(t, schemas, 1)
		assert.Equal(t, secondID, schemas[0].ID)
		assert.Equal(t, json.RawMessage(`{"type":"string"}`), schemas[0].Schema)
	})
}
