package links

import (
	"context"
	"database/sql"
	"testing"
	"time"

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
	t.Run("UpsertLink and query both directions", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		require.NoError(t, repo.UpsertLink(ctx, Link{From: 1, To: 2, Weight: 0.4, CreatedAt: 1700000000}))
		require.NoError(t, repo.UpsertLink(ctx, Link{From: 1, To: 3, Weight: 0.9, CreatedAt: 1700000010}))

		fromLinks, err := repo.GetLinksFrom(ctx, []int64{1}, 10)
		require.NoError(t, err)
		require.Len(t, fromLinks, 2)
		assert.Equal(t, int64(3), fromLinks[0].To)
		assert.Equal(t, 0.9, fromLinks[0].Weight)

		toLinks, err := repo.GetLinksTo(ctx, []int64{2}, 10)
		require.NoError(t, err)
		require.Len(t, toLinks, 1)
		assert.Equal(t, int64(1), toLinks[0].From)

		bulkFrom, err := repo.GetLinksFrom(ctx, []int64{1, 999}, 10)
		require.NoError(t, err)
		require.Len(t, bulkFrom, 2)

		bulkTo, err := repo.GetLinksTo(ctx, []int64{2, 3}, 10)
		require.NoError(t, err)
		require.Len(t, bulkTo, 2)
	})

	t.Run("UpsertLink updates existing link", func(t *testing.T) {
		repo := newTestRepository(t)
		ctx := context.Background()

		require.NoError(t, repo.UpsertLink(ctx, Link{From: 1, To: 2, Weight: 0.2, CreatedAt: 1700000000}))
		require.NoError(t, repo.UpsertLink(ctx, Link{From: 1, To: 2, Weight: 0.8, CreatedAt: 1700000010}))

		links, err := repo.GetLinksFrom(ctx, []int64{1}, 10)
		require.NoError(t, err)
		require.Len(t, links, 1)
		assert.Equal(t, 0.8, links[0].Weight)
		assert.Equal(t, int64(1700000010), links[0].CreatedAt)
	})

	t.Run("UpsertLink fails for missing created_at", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.UpsertLink(context.Background(), Link{From: 1, To: 2, Weight: 0.2})
		require.Error(t, err)
	})

	t.Run("UpsertLink fails for invalid ids", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.UpsertLink(context.Background(), Link{From: 0, To: -1, Weight: 0.1, CreatedAt: time.Now().Unix()})
		require.Error(t, err)
	})

	t.Run("UpsertLink fails for equal ids", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.UpsertLink(context.Background(), Link{From: 2, To: 2, Weight: 0.1, CreatedAt: time.Now().Unix()})
		require.Error(t, err)
	})

	t.Run("UpsertLink allows from greater than to", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.UpsertLink(context.Background(), Link{From: 3, To: 2, Weight: 0.1, CreatedAt: time.Now().Unix()})
		require.NoError(t, err)
	})

	t.Run("UpsertLink fails for weight out of range", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.UpsertLink(context.Background(), Link{From: 1, To: 2, Weight: 1.1, CreatedAt: time.Now().Unix()})
		require.Error(t, err)
	})

	t.Run("UpsertLink fails when created_at is in the future", func(t *testing.T) {
		repo := newTestRepository(t)
		err := repo.UpsertLink(context.Background(), Link{From: 1, To: 2, Weight: 0.5, CreatedAt: time.Now().Add(time.Second).Unix()})
		require.Error(t, err)
	})
}
