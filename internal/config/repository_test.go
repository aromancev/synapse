package config

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestRepository(t *testing.T) {
	t.Run("get returns default config when empty", func(t *testing.T) {
		db := openTestDB(t)
		repo := NewRepository(db)

		require.NoError(t, repo.Init(context.Background()))

		cfg, err := repo.Get(context.Background())
		require.NoError(t, err)

		fileCfg, err := cfg.Logger.FileConfig()
		require.NoError(t, err)
		require.Equal(t, LoggerTypeFile, cfg.Logger.Type)
		require.Equal(t, DefaultLogPath, fileCfg.Path)
		require.Empty(t, cfg.Replicators)
	})

	t.Run("upsert normalizes file logger config and replicators", func(t *testing.T) {
		db := openTestDB(t)
		repo := NewRepository(db)

		require.NoError(t, repo.Init(context.Background()))
		require.NoError(t, repo.Upsert(context.Background(), Config{
			Logger: Logger{Type: LoggerTypeFile},
			Replicators: []Replicator{{
				Name: "events_jsonl",
				Type: ReplicatorTypeFile,
			}},
		}))

		cfg, err := repo.Get(context.Background())
		require.NoError(t, err)

		fileCfg, err := cfg.Logger.FileConfig()
		require.NoError(t, err)
		require.Equal(t, DefaultLogPath, fileCfg.Path)
		require.Len(t, cfg.Replicators, 1)

		replicatorCfg, err := cfg.Replicators[0].FileConfig()
		require.NoError(t, err)
		require.Equal(t, DefaultReplicatorFilePath, replicatorCfg.Path)
	})

	t.Run("upsert rejects duplicate replicator names", func(t *testing.T) {
		db := openTestDB(t)
		repo := NewRepository(db)

		require.NoError(t, repo.Init(context.Background()))
		err := repo.Upsert(context.Background(), Config{
			Replicators: []Replicator{
				{Name: "events_jsonl", Type: ReplicatorTypeFile},
				{Name: "events_jsonl", Type: ReplicatorTypeFile},
			},
		})
		require.ErrorContains(t, err, "duplicate replicator name")
	})
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}
