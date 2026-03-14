package events

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/aromancev/synapse/internal/platform/sqlx"
)

var ErrConcurrencyConflict = errors.New("concurrency conflict")

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

func (r *Repository) Init(ctx context.Context, db sqlx.DB) error {
	const query = `
CREATE TABLE IF NOT EXISTS events (
	global_position INTEGER PRIMARY KEY AUTOINCREMENT,
	id BLOB NOT NULL,
	stream_id TEXT NOT NULL,
	stream_type TEXT NOT NULL,
	stream_version INTEGER NOT NULL,
	event_type TEXT NOT NULL,
	event_version INTEGER NOT NULL,
	occurred_at INTEGER NOT NULL,
	recorded_at INTEGER NOT NULL,
	payload JSON NOT NULL,
	meta JSON NOT NULL,
	UNIQUE(id),
	UNIQUE(stream_id, stream_version)
);
CREATE INDEX IF NOT EXISTS events_event_type_idx ON events(event_type);
CREATE INDEX IF NOT EXISTS events_stream_type_global_position_desc_idx ON events(stream_type, global_position DESC);

CREATE TABLE IF NOT EXISTS projection_iterators (
	projection_name TEXT NOT NULL,
	stream_type TEXT NOT NULL,
	last_global_position INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	PRIMARY KEY (projection_name, stream_type)
);

CREATE TABLE IF NOT EXISTS replicator_iterators (
	replicator_name TEXT PRIMARY KEY,
	last_global_position INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
`

	_, err := db.ExecContext(ctx, query)
	return err
}

func (r *Repository) AppendEvent(ctx context.Context, db sqlx.DB, e Event, expectedStreamVersion int64) error {
	e = e.Normalized()
	e.StreamVersion = expectedStreamVersion + 1

	if expectedStreamVersion < 0 {
		return errors.New("expected_stream_version must be >= 0")
	}

	if validationErrors := e.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	const insertQuery = `
INSERT INTO events(
	id, stream_id, stream_type, stream_version, event_type, event_version,
	occurred_at, recorded_at, payload, meta
)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
WHERE (SELECT COALESCE(MAX(stream_version), 0) FROM events WHERE stream_id = ?) = ?;`

	res, err := db.ExecContext(ctx, insertQuery,
		e.ID, e.StreamID.String(), e.StreamType.String(), e.StreamVersion, e.EventType.String(), e.EventVersion,
		e.OccurredAt, e.RecordedAt, e.Payload, e.Meta,
		e.StreamID.String(), expectedStreamVersion,
	)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: stream_id=%q expected=%d", ErrConcurrencyConflict, e.StreamID, expectedStreamVersion)
	}

	return nil
}

func (r *Repository) GetEventsByStream(ctx context.Context, db sqlx.DB, streamID StreamID, fromVersion int64) ([]Event, error) {
	streamID = streamID.Normalized()
	if streamID == "" {
		return nil, errors.New("stream_id is required")
	}
	if fromVersion < 0 {
		fromVersion = 0
	}

	const query = `
SELECT global_position, id, stream_id, stream_type, stream_version, event_type, event_version,
       occurred_at, recorded_at, payload, meta
FROM events
WHERE stream_id = ? AND stream_version > ?
ORDER BY stream_version ASC;
`

	rows, err := db.QueryContext(ctx, query, streamID.String(), fromVersion)
	if err != nil {
		return nil, fmt.Errorf("query events by stream: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.GlobalPosition,
			&e.ID,
			&e.StreamID,
			&e.StreamType,
			&e.StreamVersion,
			&e.EventType,
			&e.EventVersion,
			&e.OccurredAt,
			&e.RecordedAt,
			&e.Payload,
			&e.Meta,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		out = append(out, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events by stream: %w", err)
	}

	return out, nil
}

func (r *Repository) GetStreamEvents(ctx context.Context, db sqlx.DB, streamType StreamType, fromPosition int64, limit int) ([]Event, error) {
	streamType = streamType.Normalized()
	if fromPosition < 0 {
		fromPosition = 0
	}
	if limit <= 0 {
		limit = 100
	}

	query := `
SELECT global_position, id, stream_id, stream_type, stream_version, event_type, event_version,
       occurred_at, recorded_at, payload, meta
FROM events
WHERE global_position > ?`
	args := []any{fromPosition}

	if streamType != "" {
		query += ` AND stream_type = ?`
		args = append(args, streamType.String())
	}

	query += `
ORDER BY global_position ASC
LIMIT ?;
`
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query events from global position: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.GlobalPosition,
			&e.ID,
			&e.StreamID,
			&e.StreamType,
			&e.StreamVersion,
			&e.EventType,
			&e.EventVersion,
			&e.OccurredAt,
			&e.RecordedAt,
			&e.Payload,
			&e.Meta,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		out = append(out, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events from global position: %w", err)
	}

	return out, nil
}

func (r *Repository) GetProjectionIterator(ctx context.Context, db sqlx.DB, projectionName string, streamType StreamType) (int64, error) {
	if projectionName == "" {
		return 0, errors.New("projection_name is required")
	}
	streamType = streamType.Normalized()
	if streamType == "" {
		return 0, errors.New("stream_type is required")
	}

	const query = `
SELECT last_global_position
FROM projection_iterators
WHERE projection_name = ? AND stream_type = ?;
`

	var position int64
	err := db.QueryRowContext(ctx, query, projectionName, streamType.String()).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("get projection iterator: %w", err)
	}

	return position, nil
}

func (r *Repository) AdvanceProjectionIterator(ctx context.Context, db sqlx.DB, projectionName string, streamType StreamType, toGlobalPosition int64, updatedAt int64) error {
	if projectionName == "" {
		return errors.New("projection_name is required")
	}
	streamType = streamType.Normalized()
	if streamType == "" {
		return errors.New("stream_type is required")
	}
	if toGlobalPosition < 0 {
		return errors.New("to_global_position must be >= 0")
	}

	const query = `
INSERT INTO projection_iterators(projection_name, stream_type, last_global_position, updated_at)
VALUES(?, ?, ?, ?)
ON CONFLICT(projection_name, stream_type) DO UPDATE SET
	last_global_position = MAX(projection_iterators.last_global_position, excluded.last_global_position),
	updated_at = excluded.updated_at;
`

	if _, err := db.ExecContext(ctx, query, projectionName, streamType.String(), toGlobalPosition, updatedAt); err != nil {
		return fmt.Errorf("advance projection iterator: %w", err)
	}

	return nil
}

func (r *Repository) ResetProjectionIterator(ctx context.Context, db sqlx.DB, projectionName string, streamType StreamType, updatedAt int64) error {
	if projectionName == "" {
		return errors.New("projection_name is required")
	}
	streamType = streamType.Normalized()
	if streamType == "" {
		return errors.New("stream_type is required")
	}

	const query = `
INSERT INTO projection_iterators(projection_name, stream_type, last_global_position, updated_at)
VALUES(?, ?, 0, ?)
ON CONFLICT(projection_name, stream_type) DO UPDATE SET
	last_global_position = 0,
	updated_at = excluded.updated_at;
`

	if _, err := db.ExecContext(ctx, query, projectionName, streamType.String(), updatedAt); err != nil {
		return fmt.Errorf("reset projection iterator: %w", err)
	}

	return nil
}

func (r *Repository) GetReplicatorIterator(ctx context.Context, db sqlx.DB, replicatorName string) (int64, error) {
	if replicatorName == "" {
		return 0, errors.New("replicator_name is required")
	}

	const query = `
SELECT last_global_position
FROM replicator_iterators
WHERE replicator_name = ?;
`

	var position int64
	err := db.QueryRowContext(ctx, query, replicatorName).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("get replicator iterator: %w", err)
	}

	return position, nil
}

func (r *Repository) AdvanceReplicatorIterator(ctx context.Context, db sqlx.DB, replicatorName string, toGlobalPosition int64, updatedAt int64) error {
	if replicatorName == "" {
		return errors.New("replicator_name is required")
	}
	if toGlobalPosition < 0 {
		return errors.New("to_global_position must be >= 0")
	}

	const query = `
INSERT INTO replicator_iterators(replicator_name, last_global_position, updated_at)
VALUES(?, ?, ?)
ON CONFLICT(replicator_name) DO UPDATE SET
	last_global_position = MAX(replicator_iterators.last_global_position, excluded.last_global_position),
	updated_at = excluded.updated_at;
`

	if _, err := db.ExecContext(ctx, query, replicatorName, toGlobalPosition, updatedAt); err != nil {
		return fmt.Errorf("advance replicator iterator: %w", err)
	}

	return nil
}

func (r *Repository) ResetReplicatorIterator(ctx context.Context, db sqlx.DB, replicatorName string, updatedAt int64) error {
	if replicatorName == "" {
		return errors.New("replicator_name is required")
	}

	const query = `
INSERT INTO replicator_iterators(replicator_name, last_global_position, updated_at)
VALUES(?, 0, ?)
ON CONFLICT(replicator_name) DO UPDATE SET
	last_global_position = 0,
	updated_at = excluded.updated_at;
`

	if _, err := db.ExecContext(ctx, query, replicatorName, updatedAt); err != nil {
		return fmt.Errorf("reset replicator iterator: %w", err)
	}

	return nil
}

func (r *Repository) GetStreamTypeHeadGlobalPosition(ctx context.Context, db sqlx.DB, streamType StreamType) (int64, error) {
	streamType = streamType.Normalized()
	if streamType == "" {
		return 0, errors.New("stream_type is required")
	}

	const query = `SELECT COALESCE(MAX(global_position), 0) FROM events WHERE stream_type = ?;`

	var head int64
	if err := db.QueryRowContext(ctx, query, streamType.String()).Scan(&head); err != nil {
		return 0, fmt.Errorf("get stream type head global position: %w", err)
	}

	return head, nil
}

func (r *Repository) GetHeadGlobalPosition(ctx context.Context, db sqlx.DB) (int64, error) {
	const query = `SELECT COALESCE(MAX(global_position), 0) FROM events;`

	var head int64
	if err := db.QueryRowContext(ctx, query).Scan(&head); err != nil {
		return 0, fmt.Errorf("get head global position: %w", err)
	}

	return head, nil
}
