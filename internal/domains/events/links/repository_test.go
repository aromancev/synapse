package links

import (
	"context"
	"database/sql"
	"testing"
	"time"

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
	t.Run("AddLink and query both directions", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		if err := repo.AddLink(ctx, Link{From: 1, To: 2, Weight: 0.4, CreatedAt: 1700000000}); err != nil {
			t.Fatalf("add link: %v", err)
		}
		if err := repo.AddLink(ctx, Link{From: 1, To: 3, Weight: 0.9, CreatedAt: 1700000010}); err != nil {
			t.Fatalf("add link: %v", err)
		}

		fromLinks, err := repo.GetLinksFrom(ctx, []int64{1}, 10)
		if err != nil {
			t.Fatalf("get links from: %v", err)
		}
		if len(fromLinks) != 2 {
			t.Fatalf("expected 2 outgoing links, got %d", len(fromLinks))
		}
		if fromLinks[0].To != 3 || fromLinks[0].Weight != 0.9 {
			t.Fatalf("expected highest-weight link first, got %+v", fromLinks[0])
		}

		toLinks, err := repo.GetLinksTo(ctx, []int64{2}, 10)
		if err != nil {
			t.Fatalf("get links to: %v", err)
		}
		if len(toLinks) != 1 || toLinks[0].From != 1 {
			t.Fatalf("unexpected incoming links: %+v", toLinks)
		}

		bulkFrom, err := repo.GetLinksFrom(ctx, []int64{1, 999}, 10)
		if err != nil {
			t.Fatalf("get links from (bulk): %v", err)
		}
		if len(bulkFrom) != 2 {
			t.Fatalf("expected 2 links from bulk, got %d", len(bulkFrom))
		}

		bulkTo, err := repo.GetLinksTo(ctx, []int64{2, 3}, 10)
		if err != nil {
			t.Fatalf("get links to (bulk): %v", err)
		}
		if len(bulkTo) != 2 {
			t.Fatalf("expected 2 links to bulk, got %d", len(bulkTo))
		}
	})

	t.Run("AddLink fails for missing created_at", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddLink(context.Background(), Link{From: 1, To: 2, Weight: 0.2})
		if err == nil {
			t.Fatal("expected error for missing created_at")
		}
	})

	t.Run("AddLink fails for invalid ids", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddLink(context.Background(), Link{From: 0, To: -1, Weight: 0.1, CreatedAt: time.Now().Unix()})
		if err == nil {
			t.Fatal("expected error for invalid ids")
		}
	})

	t.Run("AddLink fails for equal ids", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddLink(context.Background(), Link{From: 2, To: 2, Weight: 0.1, CreatedAt: time.Now().Unix()})
		if err == nil {
			t.Fatal("expected error for equal ids")
		}
	})

	t.Run("AddLink allows from greater than to", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddLink(context.Background(), Link{From: 3, To: 2, Weight: 0.1, CreatedAt: time.Now().Unix()})
		if err != nil {
			t.Fatalf("expected from > to to be allowed, got: %v", err)
		}
	})

	t.Run("AddLink fails for weight out of range", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddLink(context.Background(), Link{From: 1, To: 2, Weight: 1.1, CreatedAt: time.Now().Unix()})
		if err == nil {
			t.Fatal("expected error for weight out of range")
		}
	})

	t.Run("AddLink fails when created_at is in the future", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.AddLink(context.Background(), Link{From: 1, To: 2, Weight: 0.5, CreatedAt: time.Now().Add(time.Second).Unix()})
		if err == nil {
			t.Fatal("expected error for future created_at")
		}
	})
}
