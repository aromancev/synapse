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
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	uid BLOB NOT NULL,
	schema_id TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	payload JSON NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS nodes_uid_uq ON nodes(uid);
CREATE INDEX IF NOT EXISTS nodes_schema_id_created_at_desc_idx ON nodes(schema_id, created_at DESC);
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
INSERT INTO nodes(uid, schema_id, created_at, payload)
VALUES(?, ?, ?, ?)
ON CONFLICT(uid) DO UPDATE SET
	schema_id = excluded.schema_id,
	created_at = excluded.created_at,
	payload = excluded.payload;
`
	if _, err := db.ExecContext(ctx, query, n.UID, n.SchemaID, n.CreatedAt, n.Payload); err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}

	return nil
}

func (r *ProjectionRepository) GetNodeByUID(ctx context.Context, db sqlx.DB, uid ID) (Node, error) {
	const query = `
SELECT id, uid, schema_id, created_at, payload
FROM nodes
WHERE uid = ?;
`

	var n Node
	if err := db.QueryRowContext(ctx, query, uid).Scan(&n.ID, &n.UID, &n.SchemaID, &n.CreatedAt, &n.Payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Node{}, fmt.Errorf("node not found: %s", uid)
		}
		return Node{}, fmt.Errorf("get node by uid: %w", err)
	}

	return n, nil
}

func (r *ProjectionRepository) GetNodesBySchemaID(ctx context.Context, db sqlx.DB, schemaID events.StreamID, limit int) ([]Node, error) {
	if limit <= 0 {
		limit = 100
	}

	const query = `
SELECT id, uid, schema_id, created_at, payload
FROM nodes
WHERE schema_id = ?
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
		if err := rows.Scan(&n.ID, &n.UID, &n.SchemaID, &n.CreatedAt, &n.Payload); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		out = append(out, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate nodes: %w", err)
	}

	return out, nil
}
