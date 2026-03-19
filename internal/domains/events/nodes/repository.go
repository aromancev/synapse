package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

type ProjectionRepository struct{}

func NewProjectionRepository() *ProjectionRepository {
	return &ProjectionRepository{}
}

func (r *ProjectionRepository) Init(ctx context.Context, db sqlx.DB) error {
	const query = `
CREATE TABLE IF NOT EXISTS nodes (
	id BLOB PRIMARY KEY,
	schema_id TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	archived_at INTEGER NOT NULL DEFAULT 0,
	payload JSON NOT NULL
);
CREATE INDEX IF NOT EXISTS nodes_schema_id_created_at_desc_idx ON nodes(schema_id, created_at DESC);
CREATE INDEX IF NOT EXISTS nodes_schema_id_archived_at_created_at_desc_idx ON nodes(schema_id, archived_at, created_at DESC);
`

	_, err := db.ExecContext(ctx, query)
	return err
}

func (r *ProjectionRepository) UpsertNode(ctx context.Context, db sqlx.DB, n Node) error {
	n = n.Normalized()

	if validationErrors := n.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	const query = `
INSERT INTO nodes(id, schema_id, created_at, archived_at, payload)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	schema_id = excluded.schema_id,
	created_at = excluded.created_at,
	archived_at = excluded.archived_at,
	payload = excluded.payload;
`
	if _, err := db.ExecContext(ctx, query, n.ID, n.SchemaID, n.CreatedAt, n.ArchivedAt, n.Payload); err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}

	return nil
}

func (r *ProjectionRepository) GetNodeByID(ctx context.Context, db sqlx.DB, id ID) (Node, error) {
	const query = `
SELECT id, schema_id, created_at, archived_at, payload
FROM nodes
WHERE id = ?;
`

	var n Node
	if err := db.QueryRowContext(ctx, query, id).Scan(&n.ID, &n.SchemaID, &n.CreatedAt, &n.ArchivedAt, &n.Payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Node{}, fmt.Errorf("node not found: %s", id)
		}
		return Node{}, fmt.Errorf("get node by id: %w", err)
	}

	return n, nil
}

func (r *ProjectionRepository) GetNodesBySchemaID(ctx context.Context, db sqlx.DB, schemaID events.StreamID, limit int) ([]Node, error) {
	if limit <= 0 {
		limit = 100
	}

	const query = `
SELECT id, schema_id, created_at, archived_at, payload
FROM nodes
WHERE schema_id = ? AND archived_at = 0
ORDER BY created_at DESC
LIMIT ?;
`

	rows, err := db.QueryContext(ctx, query, schemaID, limit)
	if err != nil {
		return nil, fmt.Errorf("query nodes by schema_id: %w", err)
	}
	defer rows.Close()

	var out []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.SchemaID, &n.CreatedAt, &n.ArchivedAt, &n.Payload); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		out = append(out, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate nodes: %w", err)
	}

	return out, nil
}

func (r *ProjectionRepository) GetArchivedNodesBySchemaID(ctx context.Context, db sqlx.DB, schemaID events.StreamID, limit int) ([]Node, error) {
	if limit <= 0 {
		limit = 100
	}

	const query = `
SELECT id, schema_id, created_at, archived_at, payload
FROM nodes
WHERE schema_id = ? AND archived_at > 0
ORDER BY archived_at DESC, created_at DESC
LIMIT ?;
`

	rows, err := db.QueryContext(ctx, query, schemaID, limit)
	if err != nil {
		return nil, fmt.Errorf("query archived nodes by schema_id: %w", err)
	}
	defer rows.Close()

	var out []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.SchemaID, &n.CreatedAt, &n.ArchivedAt, &n.Payload); err != nil {
			return nil, fmt.Errorf("scan archived node: %w", err)
		}
		out = append(out, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate archived nodes: %w", err)
	}

	return out, nil
}
