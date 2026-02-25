package schemas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Init(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS schemas (
	name TEXT NOT NULL,
	schema JSON NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS schemas_name_uq ON schemas(name);
`

	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *Repository) AddSchema(ctx context.Context, s Schema) error {
	s = s.Normalized()

	if validationErrors := s.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	const query = `INSERT INTO schemas(name, schema) VALUES(?, ?);`
	if _, err := r.db.ExecContext(ctx, query, s.Name, s.Schema); err != nil {
		return fmt.Errorf("insert schema: %w", err)
	}

	return nil
}

func (r *Repository) GetSchemas(ctx context.Context) ([]Schema, error) {
	const query = `SELECT name, schema FROM schemas ORDER BY name;`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []Schema
	for rows.Next() {
		var s Schema
		if err := rows.Scan(&s.Name, &s.Schema); err != nil {
			return nil, fmt.Errorf("scan schema: %w", err)
		}
		schemas = append(schemas, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schemas: %w", err)
	}

	return schemas, nil
}
