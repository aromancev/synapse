package schemas

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

type ProjectionRepository struct {
	db DB
}

func NewProjectionRepository(db DB) *ProjectionRepository {
	return &ProjectionRepository{db: db}
}

func (r *ProjectionRepository) Init(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS schemas (
	id BLOB NOT NULL,
	name TEXT NOT NULL,
	schema JSON NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS schemas_id_uq ON schemas(id);
CREATE UNIQUE INDEX IF NOT EXISTS schemas_name_uq ON schemas(name);
`

	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *ProjectionRepository) UpsertSchema(ctx context.Context, s Schema) error {
	s = s.Normalized()

	if validationErrors := s.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	const query = `
INSERT INTO schemas(id, name, schema)
VALUES(?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
	id = excluded.id,
	schema = excluded.schema;
`
	if _, err := r.db.ExecContext(ctx, query, s.ID, s.Name, s.Schema); err != nil {
		return fmt.Errorf("upsert schema: %w", err)
	}

	return nil
}

func (r *ProjectionRepository) GetSchemas(ctx context.Context) ([]Schema, error) {
	const query = `SELECT id, name, schema FROM schemas ORDER BY name;`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []Schema
	for rows.Next() {
		var s Schema
		if err := rows.Scan(&s.ID, &s.Name, &s.Schema); err != nil {
			return nil, fmt.Errorf("scan schema: %w", err)
		}
		schemas = append(schemas, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schemas: %w", err)
	}

	return schemas, nil
}
