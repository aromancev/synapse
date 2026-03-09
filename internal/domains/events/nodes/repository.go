package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Repository struct {
	db DB
}

func NewRepository(db DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Init(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS nodes (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	uid BLOB NOT NULL,
	schema_name TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	payload JSON NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS nodes_uid_uq ON nodes(uid);
CREATE INDEX IF NOT EXISTS nodes_schema_name_created_at_desc_idx ON nodes(schema_name, created_at DESC);
`

	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *Repository) AddNode(ctx context.Context, n Node) error {
	n = n.Normalized()

	if validationErrors := n.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	const query = `INSERT INTO nodes(uid, schema_name, created_at, payload) VALUES(?, ?, ?, ?);`
	if _, err := r.db.ExecContext(ctx, query, n.UID, n.SchemaName, n.CreatedAt, n.Payload); err != nil {
		return fmt.Errorf("insert node: %w", err)
	}

	return nil
}

func (r *Repository) GetNodesBySchema(ctx context.Context, schemaName string, limit int) ([]Node, error) {
	if limit <= 0 {
		limit = 100
	}

	const query = `
SELECT id, uid, schema_name, created_at, payload
FROM nodes
WHERE schema_name = ?
ORDER BY created_at DESC
LIMIT ?;
`

	rows, err := r.db.QueryContext(ctx, query, schemaName, limit)
	if err != nil {
		return nil, fmt.Errorf("query nodes by schema: %w", err)
	}
	defer rows.Close()

	var out []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.UID, &n.SchemaName, &n.CreatedAt, &n.Payload); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		out = append(out, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate nodes: %w", err)
	}

	return out, nil
}
