package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Init(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS settings (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	settings JSON NOT NULL
);
`

	if _, err := r.db.ExecContext(ctx, query); err != nil {
		return err
	}

	return r.Upsert(ctx, newDefault())
}

func (r *Repository) Get(ctx context.Context) (Settings, error) {
	const query = `
SELECT settings
FROM settings
WHERE id = 1;
`

	var raw []byte
	if err := r.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return newDefault(), nil
		}
		return Settings{}, fmt.Errorf("get settings: %w", err)
	}

	var cfg Settings
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Settings{}, fmt.Errorf("decode settings: %w", err)
	}
	if cfg.LogPath == "" {
		cfg.LogPath = DefaultLogPath
	}
	return cfg, nil
}

func (r *Repository) Upsert(ctx context.Context, cfg Settings) error {
	if cfg.LogPath == "" {
		cfg.LogPath = DefaultLogPath
	}

	payload, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode settings: %w", err)
	}

	const query = `
INSERT INTO settings(id, settings)
VALUES(1, ?)
ON CONFLICT(id) DO UPDATE SET settings = excluded.settings;
`

	if _, err := r.db.ExecContext(ctx, query, payload); err != nil {
		return fmt.Errorf("upsert settings: %w", err)
	}
	return nil
}
