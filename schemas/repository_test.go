package schemas

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func newTestRepository(t *testing.T) *Repository {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	repo := NewRepository(db)
	if err := repo.Init(context.Background()); err != nil {
		t.Fatalf("init repo: %v", err)
	}

	return repo
}

func TestRepository(t *testing.T) {
	t.Run("AddSchema and GetSchemas", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddSchema(ctx, Schema{
			Name:   "  person  ",
			Schema: `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`,
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
	})

	t.Run("AddSchema fails for empty name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddSchema(ctx, Schema{
			Name:   "   ",
			Schema: `{"type":"object"}`,
		})
		if err == nil {
			t.Fatal("expected error for empty schema name")
		}
	})

	t.Run("AddSchema fails for invalid json schema", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddSchema(ctx, Schema{
			Name:   "broken",
			Schema: `{"type":"not-a-valid-json-schema-type"}`,
		})
		if err == nil {
			t.Fatal("expected error for invalid json schema")
		}
	})

	t.Run("AddSchema fails for duplicate name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		validSchema := `{"type":"object"}`

		if err := repo.AddSchema(ctx, Schema{Name: "entity", Schema: validSchema}); err != nil {
			t.Fatalf("first add schema: %v", err)
		}

		err := repo.AddSchema(ctx, Schema{Name: "entity", Schema: validSchema})
		if err == nil {
			t.Fatal("expected unique constraint error for duplicate name")
		}
	})
}
