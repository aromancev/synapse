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
}
