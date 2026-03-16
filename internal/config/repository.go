package config

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
CREATE TABLE IF NOT EXISTS config (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	config JSON NOT NULL
);
`

	if _, err := r.db.ExecContext(ctx, query); err != nil {
		return err
	}

	return r.Upsert(ctx, newDefault())
}

func (r *Repository) Get(ctx context.Context) (Config, error) {
	const query = `
SELECT config
FROM config
WHERE id = 1;
`

	var raw []byte
	if err := r.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		// Return default config on any error (not found, table doesn't exist, etc.)
		return newDefault(), nil
	}

	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (r *Repository) Upsert(ctx context.Context, cfg Config) error {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return err
	}

	payload, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}

	const query = `
INSERT INTO config(id, config)
VALUES(1, ?)
ON CONFLICT(id) DO UPDATE SET config = excluded.config;
`

	if _, err := r.db.ExecContext(ctx, query, payload); err != nil {
		return fmt.Errorf("upsert config: %w", err)
	}
	return nil
}
