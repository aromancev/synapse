package links

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

type ProjectionRepository struct{}

func NewProjectionRepository() *ProjectionRepository {
	return &ProjectionRepository{}
}

func (r *ProjectionRepository) Init(ctx context.Context, db sqlx.DB) error {
	const query = `
CREATE TABLE IF NOT EXISTS links (
	"from" TEXT NOT NULL,
	"to" TEXT NOT NULL,
	weight REAL NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY ("from", "to")
);
CREATE INDEX IF NOT EXISTS links_to_idx ON links("to");
CREATE INDEX IF NOT EXISTS links_created_at_desc_idx ON links(created_at DESC);
`

	_, err := db.ExecContext(ctx, query)
	return err
}

func (r *ProjectionRepository) UpsertLink(ctx context.Context, db sqlx.DB, l Link) error {
	if validationErrors := l.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	const query = `
INSERT INTO links("from", "to", weight, created_at)
VALUES(?, ?, ?, ?)
ON CONFLICT("from", "to") DO UPDATE SET
	weight = excluded.weight,
	created_at = excluded.created_at;
`
	if _, err := db.ExecContext(ctx, query, l.From, l.To, l.Weight, l.CreatedAt); err != nil {
		return fmt.Errorf("upsert link: %w", err)
	}

	return nil
}

func (r *ProjectionRepository) DeleteLink(ctx context.Context, db sqlx.DB, from, to events.StreamID) error {
	from, to = normalizePair(from, to)
	if err := from.Validate(); err != nil {
		return fmt.Errorf("from: %w", err)
	}
	if err := to.Validate(); err != nil {
		return fmt.Errorf("to: %w", err)
	}
	if from == to {
		return errors.New("from and to must be different")
	}

	const query = `
DELETE FROM links
WHERE "from" = ? AND "to" = ?;
`
	if _, err := db.ExecContext(ctx, query, from, to); err != nil {
		return fmt.Errorf("delete link: %w", err)
	}

	return nil
}

func (r *ProjectionRepository) GetLinksFrom(ctx context.Context, db sqlx.DB, fromIDs []events.StreamID, limit int) ([]Link, error) {
	if len(fromIDs) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}

	placeholders := inPlaceholders(len(fromIDs))
	query := fmt.Sprintf(`
SELECT "from", "to", weight, created_at
FROM links
WHERE "from" IN (%s)
ORDER BY weight DESC
LIMIT ?;
`, placeholders)

	args := make([]any, 0, len(fromIDs)+1)
	for _, id := range fromIDs {
		args = append(args, id)
	}
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query links from: %w", err)
	}
	defer rows.Close()

	var out []Link
	for rows.Next() {
		var l Link
		if err := rows.Scan(&l.From, &l.To, &l.Weight, &l.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		out = append(out, l)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate links from: %w", err)
	}

	return out, nil
}

func (r *ProjectionRepository) GetLinksTo(ctx context.Context, db sqlx.DB, toIDs []events.StreamID, limit int) ([]Link, error) {
	if len(toIDs) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}

	placeholders := inPlaceholders(len(toIDs))
	query := fmt.Sprintf(`
SELECT "from", "to", weight, created_at
FROM links
WHERE "to" IN (%s)
ORDER BY weight DESC
LIMIT ?;
`, placeholders)

	args := make([]any, 0, len(toIDs)+1)
	for _, id := range toIDs {
		args = append(args, id)
	}
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query links to: %w", err)
	}
	defer rows.Close()

	var out []Link
	for rows.Next() {
		var l Link
		if err := rows.Scan(&l.From, &l.To, &l.Weight, &l.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		out = append(out, l)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate links to: %w", err)
	}

	return out, nil
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
