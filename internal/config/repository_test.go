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

		fileCfg, err := cfg.Logging.Logger.FileConfig()
		require.NoError(t, err)
		require.Equal(t, LoggerTypeFile, cfg.Logging.Logger.Type)
		require.Equal(t, DefaultLogPath, fileCfg.Path)
		require.Equal(t, ReplicationModeDisabled, cfg.Replication.Mode)
		require.Nil(t, cfg.Replication.Replicator)
	})

	t.Run("upsert normalizes file logger config and replicators", func(t *testing.T) {
		db := openTestDB(t)
		repo := NewRepository(db)

		require.NoError(t, repo.Init(context.Background()))
		require.NoError(t, repo.Upsert(context.Background(), Config{
			Logging: Logging{Logger: Logger{Type: LoggerTypeFile}},
			Replication: Replication{
				Mode: ReplicationModeAuto,
				Replicator: &Replicator{
					Name: "events_jsonl",
					Type: ReplicatorTypeFile,
				},
			},
		}))

		cfg, err := repo.Get(context.Background())
		require.NoError(t, err)

		fileCfg, err := cfg.Logging.Logger.FileConfig()
		require.NoError(t, err)
		require.Equal(t, DefaultLogPath, fileCfg.Path)
		require.Equal(t, ReplicationModeAuto, cfg.Replication.Mode)
		require.NotNil(t, cfg.Replication.Replicator)

		replicatorCfg, err := cfg.Replication.Replicator.FileConfig()
		require.NoError(t, err)
		require.Equal(t, DefaultReplicatorFilePath, replicatorCfg.Path)
	})

	t.Run("upsert rejects unsupported replication mode", func(t *testing.T) {
		db := openTestDB(t)
		repo := NewRepository(db)

		require.NoError(t, repo.Init(context.Background()))
		err := repo.Upsert(context.Background(), Config{
			Replication: Replication{Mode: ReplicationMode("weird")},
		})
		require.ErrorContains(t, err, "validate config")
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
