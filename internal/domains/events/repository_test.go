package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newTestRepository(t *testing.T) (*Repository, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err, "open db")
	t.Cleanup(func() { _ = db.Close() })

	repo := NewRepository()
	require.NoError(t, repo.Init(context.Background(), db), "init repo")

	return repo, db
}

func newEvent(streamID StreamID, streamType StreamType, eventType EventType, occurredAt int64) Event {
	eventID, err := NewEventID()
	if err != nil {
		panic(err)
	}
	return Event{
		ID:           eventID,
		StreamID:     streamID,
		StreamType:   streamType,
		EventType:    eventType,
		EventVersion: 1,
		OccurredAt:   occurredAt,
		RecordedAt:   occurredAt,
		Payload:      json.RawMessage(`{"ok":true}`),
		Meta:         json.RawMessage(`{}`),
	}
}

func TestRepository(t *testing.T) {
	t.Run("AppendEvent and GetEventsByStream", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()

		e1 := newEvent(StreamID("node:1"), StreamType("node"), EventType("node.created"), 1700000000)
		require.NoError(t, repo.AppendEvent(ctx, db, e1, 0))

		e2 := newEvent(StreamID("node:1"), StreamType("node"), EventType("node.renamed"), 1700000001)
		require.NoError(t, repo.AppendEvent(ctx, db, e2, 1))

		events, err := repo.GetEventsByStream(ctx, db, StreamID("node:1"), 0)
		require.NoError(t, err)
		require.Len(t, events, 2)
		assert.Equal(t, int64(1), events[0].StreamVersion)
		assert.Equal(t, int64(2), events[1].StreamVersion)
		assert.True(t, strings.HasPrefix(events[0].ID.String(), "event_"))
	})

	t.Run("AppendEvent fails on concurrency conflict", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()

		e := newEvent(StreamID("schema:person"), StreamType("schema"), EventType("schema.registered"), time.Now().Unix())
		require.NoError(t, repo.AppendEvent(ctx, db, e, 0))

		e2 := newEvent(StreamID("schema:person"), StreamType("schema"), EventType("schema.updated"), time.Now().Unix())
		err := repo.AppendEvent(ctx, db, e2, 0)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrConcurrencyConflict), "expected ErrConcurrencyConflict, got: %v", err)
	})

	t.Run("GetStreamEvents", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()

		require.NoError(t, repo.AppendEvent(ctx, db, newEvent(StreamID("node:1"), StreamType("node"), EventType("node.created"), 1700000000), 0))
		require.NoError(t, repo.AppendEvent(ctx, db, newEvent(StreamID("node:2"), StreamType("node"), EventType("node.created"), 1700000001), 0))

		events, err := repo.GetStreamEvents(ctx, db, "", 1, 100)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, int64(2), events[0].GlobalPosition)
	})

	t.Run("Projection iterator advance/get/reset", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()

		pos, err := repo.GetProjectionIterator(ctx, db, "nodes_projection", StreamType("node"))
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)

		require.NoError(t, repo.AdvanceProjectionIterator(ctx, db, "nodes_projection", StreamType("node"), 5, 1700000000))
		require.NoError(t, repo.AdvanceProjectionIterator(ctx, db, "nodes_projection", StreamType("node"), 3, 1700000001))

		pos, err = repo.GetProjectionIterator(ctx, db, "nodes_projection", StreamType("node"))
		require.NoError(t, err)
		assert.Equal(t, int64(5), pos)

		require.NoError(t, repo.ResetProjectionIterator(ctx, db, "nodes_projection", StreamType("node"), 1700000002))
		pos, err = repo.GetProjectionIterator(ctx, db, "nodes_projection", StreamType("node"))
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)
	})

	t.Run("GetStreamTypeHeadGlobalPosition", func(t *testing.T) {
		repo, db := newTestRepository(t)
		ctx := context.Background()

		head, err := repo.GetStreamTypeHeadGlobalPosition(ctx, db, StreamType("node"))
		require.NoError(t, err)
		assert.Equal(t, int64(0), head)

		require.NoError(t, repo.AppendEvent(ctx, db, newEvent(StreamID("node:1"), StreamType("node"), EventType("node.created"), 1700000000), 0))
		require.NoError(t, repo.AppendEvent(ctx, db, newEvent(StreamID("schema:1"), StreamType("schema"), EventType("schema.created"), 1700000001), 0))
		require.NoError(t, repo.AppendEvent(ctx, db, newEvent(StreamID("node:1"), StreamType("node"), EventType("node.updated"), 1700000002), 1))

		head, err = repo.GetStreamTypeHeadGlobalPosition(ctx, db, StreamType("node"))
		require.NoError(t, err)
		assert.Equal(t, int64(3), head)
	})
}
