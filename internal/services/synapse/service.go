package synapse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
)

const (
	SchemaStreamType     events.StreamType = "schema"
	NodeStreamType       events.StreamType = "node"
	EventTypeSchemaAdded events.EventType  = schemas.EventTypeSchemaAdded
	EventTypeNodeAdded   events.EventType  = nodes.EventTypeNodeAdded
)

type Synapse struct {
	db *sql.DB
}

func NewSynapse(db *sql.DB) *Synapse {
	return &Synapse{db: db}
}

func (s *Synapse) AddSchema(ctx context.Context, name, schemaJSON string) error {
	schemaID, err := schemas.NewID()
	if err != nil {
		return fmt.Errorf("new schema id: %w", err)
	}

	schema := schemas.Schema{ID: schemaID, Name: name, Schema: json.RawMessage(schemaJSON)}.Normalized()
	if validationErrors := schema.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository(tx)

	streamID := events.StreamID(schema.ID.String())
	aggregate, stream, err := loadAggregate(ctx, eventsRepo, streamID, SchemaStreamType, func() *schemas.Aggregate {
		return &schemas.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load schema aggregate: %w", err)
	}

	if err := aggregate.Add(ctx, stream, schema); err != nil {
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

func (s *Synapse) AddNode(ctx context.Context, schemaID events.StreamID, payloadJSON string) error {
	nodeID, err := nodes.NewID()
	if err != nil {
		return fmt.Errorf("new node id: %w", err)
	}

	now := time.Now().Unix()
	node := nodes.Node{UID: nodeID, SchemaID: schemaID, CreatedAt: now, Payload: json.RawMessage(payloadJSON)}.Normalized()
	if validationErrors := node.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository(tx)

	schemaAggregate, _, err := loadAggregate(ctx, eventsRepo, schemaID, SchemaStreamType, func() *schemas.Aggregate {
		return &schemas.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load schema aggregate: %w", err)
	}
	if err := schemaAggregate.Validate(ctx, node.Payload); err != nil {
		return fmt.Errorf("validate node payload against schema: %w", err)
	}

	streamID := events.StreamID(node.UID.String())
	aggregate, stream, err := loadAggregate(ctx, eventsRepo, streamID, NodeStreamType, func() *nodes.Aggregate {
		return &nodes.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load node aggregate: %w", err)
	}

	if err := aggregate.Add(ctx, stream, node); err != nil {
		return fmt.Errorf("aggregate add node: %w", err)
	}

	for _, e := range stream.RecordedEvents() {
		if err := eventsRepo.AppendEvent(ctx, e, e.StreamVersion-1); err != nil {
			return fmt.Errorf("append node event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func loadAggregate[T events.Aggregate](ctx context.Context, repo *events.Repository, streamID events.StreamID, streamType events.StreamType, newAggregate func() T) (T, *events.Stream, error) {
	aggregate := newAggregate()

	streamEvents, err := repo.GetEventsByStream(ctx, streamID, 0)
	if err != nil {
		return aggregate, nil, err
	}

	stream := events.NewStream(streamID, streamType, streamEvents)
	if err := stream.Init(ctx, aggregate); err != nil {
		return aggregate, nil, err
	}

	return aggregate, stream, nil
}
