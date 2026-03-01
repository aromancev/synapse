package events

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrConcurrencyConflict = errors.New("concurrency conflict")

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Init(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS events (
	global_position INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
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

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var currentVersion int64
	const currentVersionQuery = `SELECT COALESCE(MAX(stream_version), 0) FROM events WHERE stream_id = ?;`
	if err := tx.QueryRowContext(ctx, currentVersionQuery, e.StreamID).Scan(&currentVersion); err != nil {
		return fmt.Errorf("get current stream version: %w", err)
	}

	if currentVersion != expectedStreamVersion {
		return fmt.Errorf("%w: stream_id=%q expected=%d actual=%d", ErrConcurrencyConflict, e.StreamID, expectedStreamVersion, currentVersion)
	}

	const insertQuery = `
INSERT INTO events(
	id, stream_id, stream_type, stream_version, event_type, event_version,
	occurred_at, recorded_at, payload, meta
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
	if _, err := tx.ExecContext(ctx, insertQuery,
		e.ID, e.StreamID, e.StreamType, e.StreamVersion, e.EventType, e.EventVersion,
		e.OccurredAt, e.RecordedAt, e.Payload, e.Meta,
	); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func (r *Repository) GetEventsByStream(ctx context.Context, streamID string, fromVersion int64) ([]Event, error) {
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

	rows, err := r.db.QueryContext(ctx, query, streamID, fromVersion)
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
