package settings

import (
	"context"
	"database/sql"

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
);`

	_, err := r.db.ExecContext(ctx, query)
	return err
}
