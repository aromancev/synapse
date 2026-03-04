package events

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newTestRepository(t *testing.T) *Repository {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err, "open db")
	t.Cleanup(func() { _ = db.Close() })

	repo := NewRepository(db)
	require.NoError(t, repo.Init(context.Background()), "init repo")

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
		require.NoError(t, repo.AppendEvent(ctx, e1, 0))

		e2 := newEvent("node:1", "node", "node.renamed", 1700000001)
		require.NoError(t, repo.AppendEvent(ctx, e2, 1))

		events, err := repo.GetEventsByStream(ctx, "node:1", 0)
		require.NoError(t, err)
		require.Len(t, events, 2)
		assert.Equal(t, int64(1), events[0].StreamVersion)
		assert.Equal(t, int64(2), events[1].StreamVersion)
	})

	t.Run("AppendEvent fails on concurrency conflict", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		e := newEvent("schema:person", "schema", "schema.registered", time.Now().Unix())
		require.NoError(t, repo.AppendEvent(ctx, e, 0))

		e2 := newEvent("schema:person", "schema", "schema.updated", time.Now().Unix())
		err := repo.AppendEvent(ctx, e2, 0)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrConcurrencyConflict), "expected ErrConcurrencyConflict, got: %v", err)
	})

	t.Run("GetEventsFromGlobalPosition", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		require.NoError(t, repo.AppendEvent(ctx, newEvent("node:1", "node", "node.created", 1700000000), 0))
		require.NoError(t, repo.AppendEvent(ctx, newEvent("node:2", "node", "node.created", 1700000001), 0))

		events, err := repo.GetEventsFromGlobalPosition(ctx, 1)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, int64(2), events[0].GlobalPosition)
	})

	t.Run("Projection iterator advance/get/reset", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		pos, err := repo.GetProjectionIterator(ctx, "nodes_projection", "node")
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)

		require.NoError(t, repo.AdvanceProjectionIterator(ctx, "nodes_projection", "node", 5, 1700000000))
		require.NoError(t, repo.AdvanceProjectionIterator(ctx, "nodes_projection", "node", 3, 1700000001))

		pos, err = repo.GetProjectionIterator(ctx, "nodes_projection", "node")
		require.NoError(t, err)
		assert.Equal(t, int64(5), pos)

		require.NoError(t, repo.ResetProjectionIterator(ctx, "nodes_projection", "node", 1700000002))
		pos, err = repo.GetProjectionIterator(ctx, "nodes_projection", "node")
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)
	})

	t.Run("GetStreamTypeHeadGlobalPosition", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		head, err := repo.GetStreamTypeHeadGlobalPosition(ctx, "node")
		require.NoError(t, err)
		assert.Equal(t, int64(0), head)

		require.NoError(t, repo.AppendEvent(ctx, newEvent("node:1", "node", "node.created", 1700000000), 0))
		require.NoError(t, repo.AppendEvent(ctx, newEvent("schema:1", "schema", "schema.created", 1700000001), 0))
		require.NoError(t, repo.AppendEvent(ctx, newEvent("node:1", "node", "node.updated", 1700000002), 1))

		head, err = repo.GetStreamTypeHeadGlobalPosition(ctx, "node")
		require.NoError(t, err)
		assert.Equal(t, int64(3), head)
	})
}
