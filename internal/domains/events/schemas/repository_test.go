package schemas

import (
	"context"
	"database/sql"
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

func TestRepository(t *testing.T) {
	t.Run("UpsertSchema and GetSchemas", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name: "  person  ",
			Schema: `
				{
				  "type": "object",
				  "properties": {
				    "name": { "type": "string" }
				  },
				  "required": ["name"]
				}
			`,
		})
		require.NoError(t, err, "add schema")

		schemas, err := repo.GetSchemas(ctx)
		require.NoError(t, err, "get schemas")
		require.Len(t, schemas, 1)
		assert.Equal(t, "person", schemas[0].Name)
		assert.Equal(t, `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`, schemas[0].Schema)
	})

	t.Run("UpsertSchema fails for empty name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "   ",
			Schema: `{"type":"object"}`,
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema fails for invalid name format", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		for _, name := range []string{"Person", "person-name", "имя"} {
			err := repo.UpsertSchema(ctx, Schema{
				Name:   name,
				Schema: `{"type":"object"}`,
			})
			require.Error(t, err, "expected error for invalid schema name %q", name)
		}
	})

	t.Run("UpsertSchema allows numbers in name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "person_1",
			Schema: `{"type":"object"}`,
		})
		require.NoError(t, err)
	})

	t.Run("UpsertSchema fails when name exceeds 64 chars", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Schema: `{"type":"object"}`,
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema fails when schema exceeds 16KB", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		tooLarge := `{"x":"` + strings.Repeat("a", 16380) + `"}`
		err := repo.UpsertSchema(ctx, Schema{
			Name:   "large_schema",
			Schema: tooLarge,
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema fails for invalid json schema", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "broken",
			Schema: `{"type":"not-a-valid-json-schema-type"}`,
		})
		require.Error(t, err)
	})

	t.Run("UpsertSchema updates existing schema by name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		require.NoError(t, repo.UpsertSchema(ctx, Schema{Name: "entity", Schema: `{"type":"object"}`}))
		require.NoError(t, repo.UpsertSchema(ctx, Schema{Name: "entity", Schema: `{"type":"string"}`}))

		schemas, err := repo.GetSchemas(ctx)
		require.NoError(t, err)
		require.Len(t, schemas, 1)
		assert.Equal(t, `{"type":"string"}`, schemas[0].Schema)
	})
}
