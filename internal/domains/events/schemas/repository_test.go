package schemas

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func newTestRepository(t *testing.T) *ProjectionRepository {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	repo := NewProjectionRepository(db)
	if err := repo.Init(context.Background()); err != nil {
		t.Fatalf("init repo: %v", err)
	}

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
		if err != nil {
			t.Fatalf("add schema: %v", err)
		}

		schemas, err := repo.GetSchemas(ctx)
		if err != nil {
			t.Fatalf("get schemas: %v", err)
		}

		if len(schemas) != 1 {
			t.Fatalf("expected 1 schema, got %d", len(schemas))
		}
		if schemas[0].Name != "person" {
			t.Fatalf("expected schema name %q, got %q", "person", schemas[0].Name)
		}
		if schemas[0].Schema != `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}` {
			t.Fatalf("expected normalized one-line schema, got %q", schemas[0].Schema)
		}
	})

	t.Run("UpsertSchema fails for empty name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "   ",
			Schema: `{"type":"object"}`,
		})
		if err == nil {
			t.Fatal("expected error for empty schema name")
		}
	})

	t.Run("UpsertSchema fails for invalid name format", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		for _, name := range []string{"Person", "person-name", "имя"} {
			err := repo.UpsertSchema(ctx, Schema{
				Name:   name,
				Schema: `{"type":"object"}`,
			})
			if err == nil {
				t.Fatalf("expected error for invalid schema name %q", name)
			}
		}
	})

	t.Run("UpsertSchema allows numbers in name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "person_1",
			Schema: `{"type":"object"}`,
		})
		if err != nil {
			t.Fatalf("expected name with numbers to be valid, got: %v", err)
		}
	})

	t.Run("UpsertSchema fails when name exceeds 64 chars", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Schema: `{"type":"object"}`,
		})
		if err == nil {
			t.Fatal("expected error for too long schema name")
		}
	})

	t.Run("UpsertSchema fails when schema exceeds 16KB", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		tooLarge := `{"x":"` + strings.Repeat("a", 16380) + `"}`
		err := repo.UpsertSchema(ctx, Schema{
			Name:   "large_schema",
			Schema: tooLarge,
		})
		if err == nil {
			t.Fatal("expected error for too large schema")
		}
	})

	t.Run("UpsertSchema fails for invalid json schema", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.UpsertSchema(ctx, Schema{
			Name:   "broken",
			Schema: `{"type":"not-a-valid-json-schema-type"}`,
		})
		if err == nil {
			t.Fatal("expected error for invalid json schema")
		}
	})

	t.Run("UpsertSchema updates existing schema by name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		if err := repo.UpsertSchema(ctx, Schema{Name: "entity", Schema: `{"type":"object"}`}); err != nil {
			t.Fatalf("first upsert schema: %v", err)
		}

		if err := repo.UpsertSchema(ctx, Schema{Name: "entity", Schema: `{"type":"string"}`}); err != nil {
			t.Fatalf("second upsert schema: %v", err)
		}

		schemas, err := repo.GetSchemas(ctx)
		if err != nil {
			t.Fatalf("get schemas: %v", err)
		}
		if len(schemas) != 1 {
			t.Fatalf("expected 1 schema, got %d", len(schemas))
		}
		if schemas[0].Schema != `{"type":"string"}` {
			t.Fatalf("expected schema to be updated, got %q", schemas[0].Schema)
		}
	})
}
