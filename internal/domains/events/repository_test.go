package events

import (
	"context"
	"database/sql"
	"errors"
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

func newEvent(streamID, streamType, eventType string, occurredAt int64) Event {
	return Event{
		ID:           uuid.NewString(),
		StreamID:     streamID,
		StreamType:   streamType,
		EventType:    eventType,
		EventVersion: 1,
		OccurredAt:   occurredAt,
		RecordedAt:   occurredAt,
		Payload:      `{"ok":true}`,
		Meta:         `{}`,
	}
}

func TestRepository(t *testing.T) {
	t.Run("AppendEvent and GetEventsByStream", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		e1 := newEvent("node:1", "node", "node.created", 1700000000)
		if err := repo.AppendEvent(ctx, e1, 0); err != nil {
			t.Fatalf("append e1: %v", err)
		}

		e2 := newEvent("node:1", "node", "node.renamed", 1700000001)
		if err := repo.AppendEvent(ctx, e2, 1); err != nil {
			t.Fatalf("append e2: %v", err)
		}

		events, err := repo.GetEventsByStream(ctx, "node:1", 0)
		if err != nil {
			t.Fatalf("get by stream: %v", err)
		}
		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}
		if events[0].StreamVersion != 1 || events[1].StreamVersion != 2 {
			t.Fatalf("unexpected stream versions: %d, %d", events[0].StreamVersion, events[1].StreamVersion)
		}
	})

	t.Run("AppendEvent fails on concurrency conflict", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		e := newEvent("schema:person", "schema", "schema.registered", time.Now().Unix())
		if err := repo.AppendEvent(ctx, e, 0); err != nil {
			t.Fatalf("append e: %v", err)
		}

		e2 := newEvent("schema:person", "schema", "schema.updated", time.Now().Unix())
		err := repo.AppendEvent(ctx, e2, 0)
		if err == nil {
			t.Fatal("expected concurrency conflict")
		}
		if !errors.Is(err, ErrConcurrencyConflict) {
			t.Fatalf("expected ErrConcurrencyConflict, got: %v", err)
		}
	})

	t.Run("GetEventsFromGlobalPosition", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		if err := repo.AppendEvent(ctx, newEvent("node:1", "node", "node.created", 1700000000), 0); err != nil {
			t.Fatalf("append first event: %v", err)
		}
		if err := repo.AppendEvent(ctx, newEvent("node:2", "node", "node.created", 1700000001), 0); err != nil {
			t.Fatalf("append second event: %v", err)
		}

		events, err := repo.GetEventsFromGlobalPosition(ctx, 1)
		if err != nil {
			t.Fatalf("get from global position: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}
		if events[0].GlobalPosition != 2 {
			t.Fatalf("expected global position 2, got %d", events[0].GlobalPosition)
		}
	})

	t.Run("Projection iterator advance/get/reset", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		pos, err := repo.GetProjectionIterator(ctx, "nodes_projection", "node")
		if err != nil {
			t.Fatalf("get empty iterator: %v", err)
		}
		if pos != 0 {
			t.Fatalf("expected empty iterator to be 0, got %d", pos)
		}

		if err := repo.AdvanceProjectionIterator(ctx, "nodes_projection", "node", 5, 1700000000); err != nil {
			t.Fatalf("advance iterator to 5: %v", err)
		}
		if err := repo.AdvanceProjectionIterator(ctx, "nodes_projection", "node", 3, 1700000001); err != nil {
			t.Fatalf("advance iterator backwards should still succeed: %v", err)
		}

		pos, err = repo.GetProjectionIterator(ctx, "nodes_projection", "node")
		if err != nil {
			t.Fatalf("get iterator after advance: %v", err)
		}
		if pos != 5 {
			t.Fatalf("expected iterator position 5, got %d", pos)
		}

		if err := repo.ResetProjectionIterator(ctx, "nodes_projection", "node", 1700000002); err != nil {
			t.Fatalf("reset iterator: %v", err)
		}
		pos, err = repo.GetProjectionIterator(ctx, "nodes_projection", "node")
		if err != nil {
			t.Fatalf("get iterator after reset: %v", err)
		}
		if pos != 0 {
			t.Fatalf("expected iterator position 0 after reset, got %d", pos)
		}
	})

	t.Run("GetStreamTypeHeadGlobalPosition", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		head, err := repo.GetStreamTypeHeadGlobalPosition(ctx, "node")
		if err != nil {
			t.Fatalf("get empty head: %v", err)
		}
		if head != 0 {
			t.Fatalf("expected empty head to be 0, got %d", head)
		}

		if err := repo.AppendEvent(ctx, newEvent("node:1", "node", "node.created", 1700000000), 0); err != nil {
			t.Fatalf("append node event 1: %v", err)
		}
		if err := repo.AppendEvent(ctx, newEvent("schema:1", "schema", "schema.created", 1700000001), 0); err != nil {
			t.Fatalf("append schema event: %v", err)
		}
		if err := repo.AppendEvent(ctx, newEvent("node:1", "node", "node.updated", 1700000002), 1); err != nil {
			t.Fatalf("append node event 2: %v", err)
		}

		head, err = repo.GetStreamTypeHeadGlobalPosition(ctx, "node")
		if err != nil {
			t.Fatalf("get node head: %v", err)
		}
		if head != 3 {
			t.Fatalf("expected node head global position 3, got %d", head)
		}
	})
}
