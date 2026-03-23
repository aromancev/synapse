package schemas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/aromancev/synapse/internal/platform/sqlx"
)

type ProjectionRepository struct{}

func NewProjectionRepository() *ProjectionRepository {
	return &ProjectionRepository{}
}

func (r *ProjectionRepository) Init(ctx context.Context, db sqlx.DB) error {
	const query = `
CREATE TABLE IF NOT EXISTS schemas (
	id BLOB NOT NULL,
	name TEXT NOT NULL,
	schema JSON NOT NULL,
	archived_at INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS schemas_id_uq ON schemas(id);
CREATE UNIQUE INDEX IF NOT EXISTS schemas_name_uq ON schemas(name);
CREATE INDEX IF NOT EXISTS schemas_archived_at_name_idx ON schemas(archived_at, name);
`

	_, err := db.ExecContext(ctx, query)
	return err
}

func (r *ProjectionRepository) UpsertSchema(ctx context.Context, db sqlx.DB, s Schema) error {
	s = s.Normalized()

	if validationErrors := s.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	const query = `
INSERT INTO schemas(id, name, schema, archived_at)
VALUES(?, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
	id = excluded.id,
	schema = excluded.schema,
	archived_at = excluded.archived_at;
`
	if _, err := db.ExecContext(ctx, query, s.ID, s.Name, s.Schema, s.ArchivedAt); err != nil {
		return fmt.Errorf("upsert schema: %w", err)
	}

	return nil
}

func (r *ProjectionRepository) GetSchemaByID(ctx context.Context, db sqlx.DB, id ID) (Schema, error) {
	const query = `SELECT id, name, schema, archived_at FROM schemas WHERE id = ?;`

	var s Schema
	if err := db.QueryRowContext(ctx, query, id).Scan(&s.ID, &s.Name, &s.Schema, &s.ArchivedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Schema{}, fmt.Errorf("schema not found: %s", id)
		}
		return Schema{}, fmt.Errorf("get schema by id: %w", err)
	}

	return s, nil
}

func (r *ProjectionRepository) GetSchemas(ctx context.Context, db sqlx.DB) ([]Schema, error) {
	const query = `SELECT id, name, schema, archived_at FROM schemas WHERE archived_at = 0 ORDER BY name;`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []Schema
	for rows.Next() {
		var s Schema
		if err := rows.Scan(&s.ID, &s.Name, &s.Schema, &s.ArchivedAt); err != nil {
			return nil, fmt.Errorf("scan schema: %w", err)
		}
		schemas = append(schemas, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schemas: %w", err)
	}

	return schemas, nil
}

func (r *ProjectionRepository) GetArchivedSchemas(ctx context.Context, db sqlx.DB) ([]Schema, error) {
	const query = `SELECT id, name, schema, archived_at FROM schemas WHERE archived_at > 0 ORDER BY archived_at DESC, name;`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query archived schemas: %w", err)
	}
	defer rows.Close()

	var schemas []Schema
	for rows.Next() {
		var s Schema
		if err := rows.Scan(&s.ID, &s.Name, &s.Schema, &s.ArchivedAt); err != nil {
			return nil, fmt.Errorf("scan archived schema: %w", err)
		}
		schemas = append(schemas, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate archived schemas: %w", err)
	}

	return schemas, nil
}
