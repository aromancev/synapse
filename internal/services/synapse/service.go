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
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

type Synapse struct {
	db *sql.DB
}

type projection interface {
	Name() string
	StreamType() events.StreamType
	Project(ctx context.Context, db sqlx.DB, event events.Event) error
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

	eventsRepo := events.NewRepository()
	streamID := events.StreamID(schema.ID.String())
	stream, err := loadStream(ctx, eventsRepo, tx, streamID, schemas.StreamTypeSchema)
	if err != nil {
		return fmt.Errorf("load schema stream: %w", err)
	}

	aggregate := &schemas.Aggregate{}
	if err := replayAggregate(ctx, aggregate, stream); err != nil {
		return fmt.Errorf("replay schema aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, schema.ID, schema.Name, schema.Schema); err != nil {
		return fmt.Errorf("aggregate create schema: %w", err)
	}

	for _, e := range stream.RecordedEvents() {
		if err := eventsRepo.AppendEvent(ctx, tx, e, e.StreamVersion-1); err != nil {
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

	node := nodes.Node{ID: nodeID, SchemaID: schemaID, CreatedAt: nowUnix(), Payload: json.RawMessage(payloadJSON)}.Normalized()
	if validationErrors := node.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository()

	schemaStream, err := loadStream(ctx, eventsRepo, tx, schemaID, schemas.StreamTypeSchema)
	if err != nil {
		return fmt.Errorf("load schema stream: %w", err)
	}
	schemaAggregate := &schemas.Aggregate{}
	if err := replayAggregate(ctx, schemaAggregate, schemaStream); err != nil {
		return fmt.Errorf("replay schema aggregate: %w", err)
	}
	if err := schemaAggregate.Validate(ctx, node.Payload); err != nil {
		return fmt.Errorf("validate node payload against schema: %w", err)
	}

	streamID := events.StreamID(node.ID.String())
	stream, err := loadStream(ctx, eventsRepo, tx, streamID, nodes.StreamTypeNode)
	if err != nil {
		return fmt.Errorf("load node stream: %w", err)
	}
	aggregate := &nodes.Aggregate{}
	if err := replayAggregate(ctx, aggregate, stream); err != nil {
		return fmt.Errorf("replay node aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, node.Payload, node.ID, node.SchemaID); err != nil {
		return fmt.Errorf("aggregate create node: %w", err)
	}

	for _, e := range stream.RecordedEvents() {
		if err := eventsRepo.AppendEvent(ctx, tx, e, e.StreamVersion-1); err != nil {
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

	eventsRepo := events.NewRepository()

	fromStream, err := loadStream(ctx, eventsRepo, tx, fromStreamID, nodes.StreamTypeNode)
	if err != nil {
		return fmt.Errorf("load from node stream: %w", err)
	}
	fromAggregate := &nodes.Aggregate{}
	if err := replayAggregate(ctx, fromAggregate, fromStream); err != nil {
		return fmt.Errorf("replay from node aggregate: %w", err)
	}
	if !fromAggregate.Exists() {
		return fmt.Errorf("from node does not exist: %s", fromID)
	}

	toStream, err := loadStream(ctx, eventsRepo, tx, toStreamID, nodes.StreamTypeNode)
	if err != nil {
		return fmt.Errorf("load to node stream: %w", err)
	}
	toAggregate := &nodes.Aggregate{}
	if err := replayAggregate(ctx, toAggregate, toStream); err != nil {
		return fmt.Errorf("replay to node aggregate: %w", err)
	}
	if !toAggregate.Exists() {
		return fmt.Errorf("to node does not exist: %s", toID)
	}

	streamID := links.StreamIDForPair(fromStreamID, toStreamID)
	stream, err := loadStream(ctx, eventsRepo, tx, streamID, links.StreamTypeLink)
	if err != nil {
		return fmt.Errorf("load link stream: %w", err)
	}
	aggregate := &links.Aggregate{}
	if err := replayAggregate(ctx, aggregate, stream); err != nil {
		return fmt.Errorf("replay link aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, fromStreamID, toStreamID); err != nil {
		return fmt.Errorf("aggregate create link: %w", err)
	}

	for _, e := range stream.RecordedEvents() {
		if err := eventsRepo.AppendEvent(ctx, tx, e, e.StreamVersion-1); err != nil {
			return fmt.Errorf("append link event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Synapse) RunProjections(ctx context.Context) error {
	projections := []projection{
		schemas.NewProjection(),
		nodes.NewProjection(),
		links.NewProjection(),
	}

	for _, p := range projections {
		if err := s.RunProjection(ctx, p); err != nil {
			return fmt.Errorf("run projection %s: %w", p.Name(), err)
		}
	}

	return nil
}

func (s *Synapse) RunProjection(ctx context.Context, p projection) error {
	eventsRepo := events.NewRepository()
	for {
		lastPosition, err := eventsRepo.GetProjectionIterator(ctx, s.db, p.Name(), p.StreamType())
		if err != nil {
			return fmt.Errorf("get iterator: %w", err)
		}

		headPosition, err := eventsRepo.GetStreamTypeHeadGlobalPosition(ctx, s.db, p.StreamType())
		if err != nil {
			return fmt.Errorf("get head position: %w", err)
		}
		if lastPosition >= headPosition {
			return nil
		}

		batch, err := eventsRepo.GetStreamEvents(ctx, s.db, p.StreamType(), lastPosition, 100)
		if err != nil {
			return fmt.Errorf("get events batch: %w", err)
		}
		if len(batch) == 0 {
			return nil
		}

		for _, e := range batch {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("begin projection transaction: %w", err)
			}

			if err := p.Project(ctx, tx, e); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("project event at global position %d: %w", e.GlobalPosition, err)
			}

			if err := eventsRepo.AdvanceProjectionIterator(ctx, tx, p.Name(), p.StreamType(), e.GlobalPosition, nowUnix()); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("advance iterator: %w", err)
			}

			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit projection transaction: %w", err)
			}
		}
	}
}

func loadStream(ctx context.Context, repo *events.Repository, db sqlx.DB, streamID events.StreamID, streamType events.StreamType) (*events.Stream, error) {
	streamEvents, err := repo.GetEventsByStream(ctx, db, streamID, 0)
	if err != nil {
		return nil, err
	}

	return events.NewStream(streamID, streamType, streamEvents), nil
}

func replayAggregate(ctx context.Context, aggregate events.Aggregate, stream *events.Stream) error {
	for _, event := range stream.AppliedEvents() {
		if err := aggregate.Apply(ctx, event); err != nil {
			return err
		}
	}
	return nil
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
