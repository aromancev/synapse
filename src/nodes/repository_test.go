package nodes

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
	t.Run("AddNode and GetNodesBySchema", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddNode(ctx, Node{
			ExternalID: uuid.NewString(),
			Schema:     "person",
			CreatedAt:  1700000000,
			Payload:    ` { "name": "Ada" } `,
		})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}

		nodes, err := repo.GetNodesBySchema(ctx, "person", 10)
		if err != nil {
			t.Fatalf("get nodes: %v", err)
		}
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
		if nodes[0].Payload != `{"name":"Ada"}` {
			t.Fatalf("expected normalized payload, got %q", nodes[0].Payload)
		}
	})

	t.Run("AddNode sets created_at when missing", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		err := repo.AddNode(ctx, Node{
			ExternalID: uuid.NewString(),
			Schema:     "person",
			Payload:    `{"name":"Ada"}`,
		})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}

		nodes, err := repo.GetNodesBySchema(ctx, "person", 1)
		if err != nil {
			t.Fatalf("get nodes: %v", err)
		}
		if len(nodes) != 1 || nodes[0].CreatedAt <= 0 {
			t.Fatalf("expected created_at to be set, got %+v", nodes)
		}
	})

	t.Run("AddNode fails for invalid external_id", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddNode(context.Background(), Node{
			ExternalID: "nope",
			Schema:     "person",
			CreatedAt:  time.Now().Unix(),
			Payload:    `{"name":"Ada"}`,
		})
		if err == nil {
			t.Fatal("expected error for invalid external_id")
		}
	})

	t.Run("AddNode fails for invalid schema name", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddNode(context.Background(), Node{
			ExternalID: uuid.NewString(),
			Schema:     "Person",
			CreatedAt:  time.Now().Unix(),
			Payload:    `{"name":"Ada"}`,
		})
		if err == nil {
			t.Fatal("expected error for invalid schema")
		}
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
		if err == nil {
			t.Fatal("expected error for oversized payload")
		}
	})

	t.Run("AddNode fails for duplicate external_id", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()
		externalID := uuid.NewString()

		node := Node{ExternalID: externalID, Schema: "person", CreatedAt: time.Now().Unix(), Payload: `{"name":"Ada"}`}
		if err := repo.AddNode(ctx, node); err != nil {
			t.Fatalf("first add node: %v", err)
		}
		if err := repo.AddNode(ctx, node); err == nil {
			t.Fatal("expected unique constraint error for duplicate external_id")
		}
	})
}
