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
	})

	t.Run("upsert normalizes file logger config", func(t *testing.T) {
		db := openTestDB(t)
		repo := NewRepository(db)

		require.NoError(t, repo.Init(context.Background()))
		require.NoError(t, repo.Upsert(context.Background(), Config{
			Logger: Logger{Type: LoggerTypeFile},
		}))

		cfg, err := repo.Get(context.Background())
		require.NoError(t, err)

		fileCfg, err := cfg.Logger.FileConfig()
		require.NoError(t, err)
		require.Equal(t, DefaultLogPath, fileCfg.Path)
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
