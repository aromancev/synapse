package schemas

import (
	"context"
	"database/sql"
	"strings"
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

	t.Run("AddSchema fails for invalid name format", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		for _, name := range []string{"Person", "person-name", "имя"} {
			err := repo.AddSchema(ctx, Schema{
				Name:   name,
				Schema: `{"type":"object"}`,
			})
			if err == nil {
				t.Fatalf("expected error for invalid schema name %q", name)
			}
		}
	})

	t.Run("AddSchema allows numbers in name", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddSchema(ctx, Schema{
			Name:   "person_1",
			Schema: `{"type":"object"}`,
		})
		if err != nil {
			t.Fatalf("expected name with numbers to be valid, got: %v", err)
		}
	})

	t.Run("AddSchema fails when name exceeds 64 chars", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddSchema(ctx, Schema{
			Name:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Schema: `{"type":"object"}`,
		})
		if err == nil {
			t.Fatal("expected error for too long schema name")
		}
	})

	t.Run("AddSchema fails when schema exceeds 16KB", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		tooLarge := `{"x":"` + strings.Repeat("a", 16380) + `"}`
		err := repo.AddSchema(ctx, Schema{
			Name:   "large_schema",
			Schema: tooLarge,
		})
		if err == nil {
			t.Fatal("expected error for too large schema")
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
