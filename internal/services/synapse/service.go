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
	"github.com/aromancev/synapse/internal/domains/events/replicators"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

type Synapse struct {
	db         *sql.DB
	replicator replicators.Replicator
}

type projection interface {
	Name() string
	StreamType() events.StreamType
	Project(ctx context.Context, db sqlx.DB, event events.Event) error
}

func NewSynapse(db *sql.DB, replicator replicators.Replicator) *Synapse {
	return &Synapse{db: db, replicator: replicator}
}

func (s *Synapse) AddSchema(ctx context.Context, name string, schemaJSON json.RawMessage) error {
	schemaID, err := schemas.NewID()
	if err != nil {
		return fmt.Errorf("new schema id: %w", err)
	}

	schema := schemas.Schema{ID: schemaID, Name: name, Schema: schemaJSON}.Normalized()
	if validationErrors := schema.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository()
	stream, err := loadStream(ctx, eventsRepo, tx, schema.ID.StreamID(), schemas.StreamTypeSchema)
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

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return fmt.Errorf("append schema events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Synapse) AddNode(ctx context.Context, schemaID schemas.ID, payloadJSON json.RawMessage) error {
	nodeID, err := nodes.NewID()
	if err != nil {
		return fmt.Errorf("new node id: %w", err)
	}

	node := nodes.Node{ID: nodeID, SchemaID: schemaID.StreamID(), CreatedAt: nowUnix(), Payload: payloadJSON}.Normalized()
	if validationErrors := node.Validate(); len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository()

	schemaAggregate, err := loadSchemaAggregate(ctx, eventsRepo, tx, schemaID)
	if err != nil {
		return err
	}
	if err := schemaAggregate.Validate(ctx, node.Payload); err != nil {
		return fmt.Errorf("validate node payload against schema: %w", err)
	}

	stream, err := loadStream(ctx, eventsRepo, tx, node.ID.StreamID(), nodes.StreamTypeNode)
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

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return fmt.Errorf("append node events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Synapse) UpdateNode(ctx context.Context, nodeID nodes.ID, payloadJSON json.RawMessage) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository()
	aggregate, err := loadExistingNodeAggregate(ctx, eventsRepo, tx, nodeID)
	if err != nil {
		return err
	}

	schemaID, err := schemas.ParseID(aggregate.SchemaID().String())
	if err != nil {
		return fmt.Errorf("parse schema id: %w", err)
	}
	schemaAggregate, err := loadSchemaAggregate(ctx, eventsRepo, tx, schemaID)
	if err != nil {
		return err
	}
	if err := schemaAggregate.Validate(ctx, payloadJSON); err != nil {
		return fmt.Errorf("validate node payload against schema: %w", err)
	}

	stream, err := loadStream(ctx, eventsRepo, tx, nodeID.StreamID(), nodes.StreamTypeNode)
	if err != nil {
		return fmt.Errorf("load node stream: %w", err)
	}
	if err := aggregate.Update(ctx, stream, payloadJSON); err != nil {
		return fmt.Errorf("aggregate update node: %w", err)
	}

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return fmt.Errorf("append node events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Synapse) LinkNodes(ctx context.Context, fromID, toID nodes.ID) error {
	return s.mutateLink(ctx, fromID, toID, func(aggregate *links.Aggregate, stream *events.Stream, fromStreamID, toStreamID events.StreamID) error {
		if err := aggregate.Create(ctx, stream, fromStreamID, toStreamID); err != nil {
			return fmt.Errorf("aggregate create link: %w", err)
		}
		return nil
	})
}

func (s *Synapse) UnlinkNodes(ctx context.Context, fromID, toID nodes.ID) error {
	return s.mutateLink(ctx, fromID, toID, func(aggregate *links.Aggregate, stream *events.Stream, fromStreamID, toStreamID events.StreamID) error {
		if err := aggregate.Remove(ctx, stream, fromStreamID, toStreamID); err != nil {
			return fmt.Errorf("aggregate remove link: %w", err)
		}
		return nil
	})
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

func (s *Synapse) RunReplication(ctx context.Context) error {
	if s.replicator == nil {
		return nil
	}

	eventsRepo := events.NewRepository()
	for {
		lastPosition, err := eventsRepo.GetReplicatorIterator(ctx, s.db, s.replicator.Name())
		if err != nil {
			return fmt.Errorf("get iterator: %w", err)
		}

		headPosition, err := eventsRepo.GetHeadGlobalPosition(ctx, s.db)
		if err != nil {
			return fmt.Errorf("get head position: %w", err)
		}
		if lastPosition >= headPosition {
			return nil
		}

		batch, err := eventsRepo.GetStreamEvents(ctx, s.db, "", lastPosition, 100)
		if err != nil {
			return fmt.Errorf("get events batch: %w", err)
		}
		if len(batch) == 0 {
			return nil
		}

		for _, e := range batch {
			if err := s.replicator.Replicate(ctx, e); err != nil {
				return fmt.Errorf("replicate event at global position %d: %w", e.GlobalPosition, err)
			}

			if err := eventsRepo.AdvanceReplicatorIterator(ctx, s.db, s.replicator.Name(), e.GlobalPosition, nowUnix()); err != nil {
				return fmt.Errorf("advance iterator: %w", err)
			}
		}
	}
}

func (s *Synapse) Restore(ctx context.Context) error {
	eventsRepo := events.NewRepository()
	headPosition, err := eventsRepo.GetHeadGlobalPosition(ctx, s.db)
	if err != nil {
		return fmt.Errorf("get head position: %w", err)
	}
	if headPosition != 0 {
		return errors.New("event store must be empty before restore")
	}

	if s.replicator == nil {
		return errors.New("no replicator configured")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin restore transaction: %w", err)
	}
	defer tx.Rollback()

	if err := s.replicator.Restore(ctx, eventsRepo, tx); err != nil {
		return fmt.Errorf("restore from replicator %s: %w", s.replicator.Name(), err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit restore transaction: %w", err)
	}

	return nil
}

func loadSchemaAggregate(ctx context.Context, repo *events.Repository, db sqlx.DB, id schemas.ID) (*schemas.Aggregate, error) {
	stream, err := loadStream(ctx, repo, db, id.StreamID(), schemas.StreamTypeSchema)
	if err != nil {
		return nil, fmt.Errorf("load schema stream: %w", err)
	}
	aggregate := &schemas.Aggregate{}
	if err := replayAggregate(ctx, aggregate, stream); err != nil {
		return nil, fmt.Errorf("replay schema aggregate: %w", err)
	}
	return aggregate, nil
}

func loadExistingNodeAggregate(ctx context.Context, repo *events.Repository, db sqlx.DB, id nodes.ID) (*nodes.Aggregate, error) {
	stream, err := loadStream(ctx, repo, db, id.StreamID(), nodes.StreamTypeNode)
	if err != nil {
		return nil, fmt.Errorf("load node stream: %w", err)
	}
	aggregate := &nodes.Aggregate{}
	if err := replayAggregate(ctx, aggregate, stream); err != nil {
		return nil, fmt.Errorf("replay node aggregate: %w", err)
	}
	if !aggregate.Exists() {
		return nil, fmt.Errorf("node does not exist: %s", id)
	}
	return aggregate, nil
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

func appendRecordedEvents(ctx context.Context, repo *events.Repository, db sqlx.DB, stream *events.Stream) error {
	for _, event := range stream.RecordedEvents() {
		if err := repo.AppendEvent(ctx, db, event, event.StreamVersion-1); err != nil {
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

func (s *Synapse) mutateLink(ctx context.Context, fromID, toID nodes.ID, mutate func(aggregate *links.Aggregate, stream *events.Stream, fromStreamID, toStreamID events.StreamID) error) error {
	fromStreamID := fromID.StreamID()
	toStreamID := toID.StreamID()
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

	if _, err := loadExistingNodeAggregate(ctx, eventsRepo, tx, fromID); err != nil {
		return fmt.Errorf("from: %w", err)
	}

	if _, err := loadExistingNodeAggregate(ctx, eventsRepo, tx, toID); err != nil {
		return fmt.Errorf("to: %w", err)
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

	if err := mutate(aggregate, stream, fromStreamID, toStreamID); err != nil {
		return err
	}

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return fmt.Errorf("append link events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
