package events

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrConcurrencyConflict = errors.New("concurrency conflict")

type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Repository struct {
	db DB
}

func NewRepository(db DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Init(ctx context.Context) error {
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
`

	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *Repository) AppendEvent(ctx context.Context, e Event, expectedStreamVersion int64) error {
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

	res, err := r.db.ExecContext(ctx, insertQuery,
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

func (r *Repository) GetEventsByStream(ctx context.Context, streamID StreamID, fromVersion int64) ([]Event, error) {
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

	rows, err := r.db.QueryContext(ctx, query, streamID.String(), fromVersion)
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

func (r *Repository) GetEventsFromGlobalPosition(ctx context.Context, fromPosition int64) ([]Event, error) {
	if fromPosition < 0 {
		fromPosition = 0
	}

	const query = `
SELECT global_position, id, stream_id, stream_type, stream_version, event_type, event_version,
       occurred_at, recorded_at, payload, meta
FROM events
WHERE global_position > ?
ORDER BY global_position ASC;
`

	rows, err := r.db.QueryContext(ctx, query, fromPosition)
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

func (r *Repository) GetProjectionIterator(ctx context.Context, projectionName string, streamType StreamType) (int64, error) {
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
	err := r.db.QueryRowContext(ctx, query, projectionName, streamType.String()).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("get projection iterator: %w", err)
	}

	return position, nil
}

func (r *Repository) AdvanceProjectionIterator(ctx context.Context, projectionName string, streamType StreamType, toGlobalPosition int64, updatedAt int64) error {
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

	if _, err := r.db.ExecContext(ctx, query, projectionName, streamType.String(), toGlobalPosition, updatedAt); err != nil {
		return fmt.Errorf("advance projection iterator: %w", err)
	}

	return nil
}

func (r *Repository) ResetProjectionIterator(ctx context.Context, projectionName string, streamType StreamType, updatedAt int64) error {
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

	if _, err := r.db.ExecContext(ctx, query, projectionName, streamType.String(), updatedAt); err != nil {
		return fmt.Errorf("reset projection iterator: %w", err)
	}

	return nil
}

func (r *Repository) GetStreamTypeHeadGlobalPosition(ctx context.Context, streamType StreamType) (int64, error) {
	streamType = streamType.Normalized()
	if streamType == "" {
		return 0, errors.New("stream_type is required")
	}

	const query = `SELECT COALESCE(MAX(global_position), 0) FROM events WHERE stream_type = ?;`

	var head int64
	if err := r.db.QueryRowContext(ctx, query, streamType.String()).Scan(&head); err != nil {
		return 0, fmt.Errorf("get stream type head global position: %w", err)
	}

	return head, nil
}
