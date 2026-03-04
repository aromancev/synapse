package synapse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
)

const (
	StreamType           = "schema"
	EventTypeSchemaAdded = "schema.added"
)

type Synapse struct {
	db *sql.DB
}

func NewSynapse(db *sql.DB) *Synapse {
	return &Synapse{db: db}
}

func (s *Synapse) AddSchema(ctx context.Context, name, schemaJSON string) error {
	schema := schemas.Schema{Name: name, Schema: schemaJSON}.Normalized()
	if validationErrors := schema.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository(tx)

	streamID := schemaStreamID(schema.Name)
	streamEvents, err := eventsRepo.GetEventsByStream(ctx, streamID, 0)
	if err != nil {
		return fmt.Errorf("get schema stream events: %w", err)
	}

	stream := events.NewStream(streamID, StreamType, streamEvents)
	aggregate := &schemas.Aggregate{}
	if err := stream.Init(aggregate); err != nil {
		return fmt.Errorf("init schema aggregate: %w", err)
	}

	now := time.Now().Unix()
	if err := aggregate.Add(stream, schema, now); err != nil {
		return fmt.Errorf("aggregate add schema: %w", err)
	}

	for _, e := range stream.RecordedEvents() {
		if err := eventsRepo.AppendEvent(ctx, e, e.StreamVersion-1); err != nil {
			return fmt.Errorf("append schema event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func schemaStreamID(schemaName string) string {
	return "schema:" + schemaName
}
