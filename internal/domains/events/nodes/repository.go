package nodes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

const nodesFTSTable = "nodes_fts"

type SearchHit struct {
	ID    ID
	Score float64
}

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
	payload JSON NOT NULL,
	keywords JSON NOT NULL DEFAULT '[]',
	search_text TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS nodes_schema_id_created_at_desc_idx ON nodes(schema_id, created_at DESC);
CREATE INDEX IF NOT EXISTS nodes_schema_id_archived_at_created_at_desc_idx ON nodes(schema_id, archived_at, created_at DESC);
CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
	node_id,
	search_text,
	tokenize='unicode61'
);
`

	_, err := db.ExecContext(ctx, query)
	return err
}

func (r *ProjectionRepository) UpsertNode(ctx context.Context, db sqlx.DB, n Node) error {
	n = n.Normalized()

	if validationErrors := n.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	keywordsJSON, err := jsonKeywords(n.Keywords)
	if err != nil {
		return err
	}

	const query = `
INSERT INTO nodes(id, schema_id, created_at, archived_at, payload, keywords, search_text)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	schema_id = excluded.schema_id,
	created_at = excluded.created_at,
	archived_at = excluded.archived_at,
	payload = excluded.payload,
	keywords = excluded.keywords,
	search_text = excluded.search_text;
`
	if _, err := db.ExecContext(ctx, query, n.ID, n.SchemaID, n.CreatedAt, n.ArchivedAt, n.Payload, keywordsJSON, n.SearchText); err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}

	if err := r.updateSearchIndex(ctx, db, n); err != nil {
		return err
	}

	return nil
}

func (r *ProjectionRepository) GetNodeByID(ctx context.Context, db sqlx.DB, id ID) (Node, error) {
	const query = `
SELECT id, schema_id, created_at, archived_at, payload, keywords, search_text
FROM nodes
WHERE id = ?;
`

	var n Node
	var keywordsJSON []byte
	if err := db.QueryRowContext(ctx, query, id).Scan(&n.ID, &n.SchemaID, &n.CreatedAt, &n.ArchivedAt, &n.Payload, &keywordsJSON, &n.SearchText); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Node{}, fmt.Errorf("node not found: %s", id)
		}
		return Node{}, fmt.Errorf("get node by id: %w", err)
	}
	keywords, err := parseKeywords(keywordsJSON)
	if err != nil {
		return Node{}, err
	}
	n.Keywords = keywords

	return n, nil
}

func (r *ProjectionRepository) GetNodesByIDs(ctx context.Context, db sqlx.DB, ids []ID) ([]Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	placeholders := inPlaceholders(len(ids))
	query := fmt.Sprintf(`
SELECT id, schema_id, created_at, archived_at, payload, keywords, search_text
FROM nodes
WHERE id IN (%s)
ORDER BY created_at DESC;
`, placeholders)

	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query nodes by ids: %w", err)
	}
	defer rows.Close()

	var out []Node
	for rows.Next() {
		var n Node
		var keywordsJSON []byte
		if err := rows.Scan(&n.ID, &n.SchemaID, &n.CreatedAt, &n.ArchivedAt, &n.Payload, &keywordsJSON, &n.SearchText); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		keywords, err := parseKeywords(keywordsJSON)
		if err != nil {
			return nil, err
		}
		n.Keywords = keywords
		out = append(out, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate nodes: %w", err)
	}

	return out, nil
}

func (r *ProjectionRepository) GetNodesBySchemaID(ctx context.Context, db sqlx.DB, schemaID events.StreamID, limit int) ([]Node, error) {
	if limit <= 0 {
		limit = 100
	}

	const query = `
SELECT id, schema_id, created_at, archived_at, payload, keywords, search_text
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
		var keywordsJSON []byte
		if err := rows.Scan(&n.ID, &n.SchemaID, &n.CreatedAt, &n.ArchivedAt, &n.Payload, &keywordsJSON, &n.SearchText); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		keywords, err := parseKeywords(keywordsJSON)
		if err != nil {
			return nil, err
		}
		n.Keywords = keywords
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
SELECT id, schema_id, created_at, archived_at, payload, keywords, search_text
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
		var keywordsJSON []byte
		if err := rows.Scan(&n.ID, &n.SchemaID, &n.CreatedAt, &n.ArchivedAt, &n.Payload, &keywordsJSON, &n.SearchText); err != nil {
			return nil, fmt.Errorf("scan archived node: %w", err)
		}
		keywords, err := parseKeywords(keywordsJSON)
		if err != nil {
			return nil, err
		}
		n.Keywords = keywords
		out = append(out, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate archived nodes: %w", err)
	}

	return out, nil
}

func (r *ProjectionRepository) SearchNodeIDs(ctx context.Context, db sqlx.DB, query string, limit int) ([]SearchHit, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}

	const searchQuery = `
SELECT node_id, bm25(nodes_fts) AS score
FROM nodes_fts
WHERE nodes_fts MATCH ?
ORDER BY score
LIMIT ?;
`

	rows, err := db.QueryContext(ctx, searchQuery, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search node ids: %w", err)
	}
	defer rows.Close()

	var hits []SearchHit
	for rows.Next() {
		var nodeID string
		var hit SearchHit
		if err := rows.Scan(&nodeID, &hit.Score); err != nil {
			return nil, fmt.Errorf("scan search hit: %w", err)
		}
		hit.ID, err = ParseID(nodeID)
		if err != nil {
			return nil, fmt.Errorf("parse search hit id: %w", err)
		}
		hits = append(hits, hit)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search hits: %w", err)
	}

	return hits, nil
}

func (r *ProjectionRepository) updateSearchIndex(ctx context.Context, db sqlx.DB, n Node) error {
	const deleteQuery = `DELETE FROM nodes_fts WHERE node_id = ?;`
	if _, err := db.ExecContext(ctx, deleteQuery, n.ID.String()); err != nil {
		return fmt.Errorf("delete node from fts: %w", err)
	}

	if n.ArchivedAt > 0 || strings.TrimSpace(n.SearchText) == "" {
		return nil
	}

	const insertQuery = `INSERT INTO nodes_fts(node_id, search_text) VALUES(?, ?);`
	if _, err := db.ExecContext(ctx, insertQuery, n.ID.String(), n.SearchText); err != nil {
		return fmt.Errorf("insert node into fts: %w", err)
	}

	return nil
}

func parseKeywords(data []byte) ([]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var keywords []string
	if err := json.Unmarshal(data, &keywords); err != nil {
		return nil, fmt.Errorf("unmarshal node keywords: %w", err)
	}

	return NormalizeKeywords(keywords), nil
}

func jsonKeywords(keywords []string) ([]byte, error) {
	data, err := json.Marshal(NormalizeKeywords(keywords))
	if err != nil {
		return nil, fmt.Errorf("marshal node keywords: %w", err)
	}
	return data, nil
}

func inPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ",")
}
