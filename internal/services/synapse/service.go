package synapse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/links"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
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
	aggregate, stream, err := loadAggregate(ctx, eventsRepo, streamID, schemas.StreamTypeSchema, func() *schemas.Aggregate {
		return &schemas.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load schema aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, schema.ID, schema.Name, schema.Schema); err != nil {
		return fmt.Errorf("aggregate create schema: %w", err)
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

	node := nodes.Node{UID: nodeID, SchemaID: schemaID, CreatedAt: nowUnix(), Payload: json.RawMessage(payloadJSON)}.Normalized()
	if validationErrors := node.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository(tx)

	schemaAggregate, _, err := loadAggregate(ctx, eventsRepo, schemaID, schemas.StreamTypeSchema, func() *schemas.Aggregate {
		return &schemas.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load schema aggregate: %w", err)
	}
	if err := schemaAggregate.Validate(ctx, node.Payload); err != nil {
		return fmt.Errorf("validate node payload against schema: %w", err)
	}

	streamID := events.StreamID(node.UID.String())
	aggregate, stream, err := loadAggregate(ctx, eventsRepo, streamID, nodes.StreamTypeNode, func() *nodes.Aggregate {
		return &nodes.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load node aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, node.Payload, node.UID, node.SchemaID); err != nil {
		return fmt.Errorf("aggregate create node: %w", err)
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

func (s *Synapse) LinkNodes(ctx context.Context, fromID, toID nodes.ID) error {
	fromStreamID := events.StreamID(fromID.String())
	toStreamID := events.StreamID(toID.String())
	fromStreamID, toStreamID = normalizeLinkPair(fromStreamID, toStreamID)
	if err := fromStreamID.Validate(); err != nil {
		return fmt.Errorf("from: %w", err)
	}
	if err := toStreamID.Validate(); err != nil {
		return fmt.Errorf("to: %w", err)
	}
	if fromStreamID == toStreamID {
		return errors.New("from and to must be different")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository(tx)

	fromAggregate, _, err := loadAggregate(ctx, eventsRepo, fromStreamID, nodes.StreamTypeNode, func() *nodes.Aggregate {
		return &nodes.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load from node aggregate: %w", err)
	}
	if !fromAggregate.Exists() {
		return fmt.Errorf("from node does not exist: %s", fromID)
	}

	toAggregate, _, err := loadAggregate(ctx, eventsRepo, toStreamID, nodes.StreamTypeNode, func() *nodes.Aggregate {
		return &nodes.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load to node aggregate: %w", err)
	}
	if !toAggregate.Exists() {
		return fmt.Errorf("to node does not exist: %s", toID)
	}

	streamID := links.StreamIDForPair(fromStreamID, toStreamID)
	aggregate, stream, err := loadAggregate(ctx, eventsRepo, streamID, links.StreamTypeLink, func() *links.Aggregate {
		return &links.Aggregate{}
	})
	if err != nil {
		return fmt.Errorf("load link aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, fromStreamID, toStreamID); err != nil {
		return fmt.Errorf("aggregate create link: %w", err)
	}

	for _, e := range stream.RecordedEvents() {
		if err := eventsRepo.AppendEvent(ctx, e, e.StreamVersion-1); err != nil {
			return fmt.Errorf("append link event: %w", err)
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

func nowUnix() int64 {
	return time.Now().Unix()
}

func normalizeLinkPair(from, to events.StreamID) (events.StreamID, events.StreamID) {
	from = from.Normalized()
	to = to.Normalized()
	if from.String() > to.String() {
		from, to = to, from
	}
	return from, to
}
