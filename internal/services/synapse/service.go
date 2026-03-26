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

func (s *Synapse) AddSchema(ctx context.Context, name string, schemaJSON json.RawMessage) (schemas.ID, error) {
	schemaID, err := schemas.NewID()
	if err != nil {
		return schemas.ID{}, fmt.Errorf("new schema id: %w", err)
	}

	schema := schemas.Schema{ID: schemaID, Name: name, Schema: schemaJSON}.Normalized()
	if validationErrors := schema.Validate(); len(validationErrors) > 0 {
		return schemas.ID{}, errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return schemas.ID{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository()
	stream, err := loadStream(ctx, eventsRepo, tx, schema.ID.StreamID(), schemas.StreamTypeSchema)
	if err != nil {
		return schemas.ID{}, fmt.Errorf("load schema stream: %w", err)
	}

	aggregate := &schemas.Aggregate{}
	if err := replayAggregate(ctx, aggregate, stream); err != nil {
		return schemas.ID{}, fmt.Errorf("replay schema aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, schema.ID, schema.Name, schema.Schema); err != nil {
		return schemas.ID{}, fmt.Errorf("aggregate create schema: %w", err)
	}

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return schemas.ID{}, fmt.Errorf("append schema events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return schemas.ID{}, fmt.Errorf("commit transaction: %w", err)
	}

	return schema.ID, nil
}

func (s *Synapse) AddNode(ctx context.Context, schemaID schemas.ID, payloadJSON json.RawMessage) (nodes.ID, error) {
	nodeID, err := nodes.NewID()
	if err != nil {
		return nodes.ID{}, fmt.Errorf("new node id: %w", err)
	}

	node := nodes.Node{ID: nodeID, SchemaID: schemaID.StreamID(), CreatedAt: nowUnix(), Payload: payloadJSON}.Normalized()
	if validationErrors := node.Validate(); len(validationErrors) > 0 {
		return nodes.ID{}, errors.Join(validationErrors...)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nodes.ID{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository()

	schemaAggregate, err := loadSchemaAggregate(ctx, eventsRepo, tx, schemaID)
	if err != nil {
		return nodes.ID{}, err
	}
	if schemaAggregate.Archived() {
		return nodes.ID{}, errors.New("schema is archived")
	}
	if err := schemaAggregate.Validate(ctx, node.Payload); err != nil {
		return nodes.ID{}, fmt.Errorf("validate node payload against schema: %w", err)
	}

	stream, err := loadStream(ctx, eventsRepo, tx, node.ID.StreamID(), nodes.StreamTypeNode)
	if err != nil {
		return nodes.ID{}, fmt.Errorf("load node stream: %w", err)
	}
	aggregate := &nodes.Aggregate{}
	if err := replayAggregate(ctx, aggregate, stream); err != nil {
		return nodes.ID{}, fmt.Errorf("replay node aggregate: %w", err)
	}

	if err := aggregate.Create(ctx, stream, node.Payload, node.ID, node.SchemaID); err != nil {
		return nodes.ID{}, fmt.Errorf("aggregate create node: %w", err)
	}

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return nodes.ID{}, fmt.Errorf("append node events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nodes.ID{}, fmt.Errorf("commit transaction: %w", err)
	}

	return node.ID, nil
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
	stream, err := loadStream(ctx, eventsRepo, tx, nodeID.StreamID(), nodes.StreamTypeNode)
	if err != nil {
		return fmt.Errorf("load node stream: %w", err)
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

func (s *Synapse) UpdateNodeKeywords(ctx context.Context, nodeID nodes.ID, keywords []string) error {
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
	stream, err := loadStream(ctx, eventsRepo, tx, nodeID.StreamID(), nodes.StreamTypeNode)
	if err != nil {
		return fmt.Errorf("load node stream: %w", err)
	}

	if err := aggregate.UpdateKeywords(ctx, stream, keywords); err != nil {
		return fmt.Errorf("aggregate update node keywords: %w", err)
	}

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return fmt.Errorf("append node events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Synapse) ArchiveSchema(ctx context.Context, schemaID schemas.ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	eventsRepo := events.NewRepository()
	aggregate, err := loadExistingSchemaAggregate(ctx, eventsRepo, tx, schemaID)
	if err != nil {
		return err
	}
	stream, err := loadStream(ctx, eventsRepo, tx, schemaID.StreamID(), schemas.StreamTypeSchema)
	if err != nil {
		return fmt.Errorf("load schema stream: %w", err)
	}

	if err := aggregate.Archive(ctx, stream); err != nil {
		return fmt.Errorf("aggregate archive schema: %w", err)
	}

	if err := appendRecordedEvents(ctx, eventsRepo, tx, stream); err != nil {
		return fmt.Errorf("append schema events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Synapse) ArchiveNode(ctx context.Context, nodeID nodes.ID) error {
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
	stream, err := loadStream(ctx, eventsRepo, tx, nodeID.StreamID(), nodes.StreamTypeNode)
	if err != nil {
		return fmt.Errorf("load node stream: %w", err)
	}

	if err := aggregate.Archive(ctx, stream); err != nil {
		return fmt.Errorf("aggregate archive node: %w", err)
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

func (s *Synapse) GetSchemas(ctx context.Context) ([]schemas.Schema, error) {
	schemasRepo := schemas.NewProjectionRepository()
	storedSchemas, err := schemasRepo.GetSchemas(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("get schemas: %w", err)
	}

	return storedSchemas, nil
}

func (s *Synapse) GetArchivedSchemas(ctx context.Context) ([]schemas.Schema, error) {
	schemasRepo := schemas.NewProjectionRepository()
	storedSchemas, err := schemasRepo.GetArchivedSchemas(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("get archived schemas: %w", err)
	}

	return storedSchemas, nil
}

func (s *Synapse) GetNodesBySchemaID(ctx context.Context, schemaID schemas.ID, limit int) ([]nodes.Node, error) {
	nodesRepo := nodes.NewProjectionRepository()
	storedNodes, err := nodesRepo.GetNodesBySchemaID(ctx, s.db, schemaID.StreamID(), limit)
	if err != nil {
		return nil, fmt.Errorf("get nodes by schema id: %w", err)
	}

	return storedNodes, nil
}

func (s *Synapse) GetNodeKeywords(ctx context.Context, nodeID nodes.ID) ([]string, error) {
	nodesRepo := nodes.NewProjectionRepository()
	storedNode, err := nodesRepo.GetNodeByID(ctx, s.db, nodeID)
	if err != nil {
		return nil, fmt.Errorf("get node keywords: %w", err)
	}

	return storedNode.Keywords, nil
}

func (s *Synapse) GetArchivedNodesBySchemaID(ctx context.Context, schemaID schemas.ID, limit int) ([]nodes.Node, error) {
	nodesRepo := nodes.NewProjectionRepository()
	storedNodes, err := nodesRepo.GetArchivedNodesBySchemaID(ctx, s.db, schemaID.StreamID(), limit)
	if err != nil {
		return nil, fmt.Errorf("get archived nodes by schema id: %w", err)
	}

	return storedNodes, nil
}

func (s *Synapse) SearchNodes(ctx context.Context, query string, limit int) ([]nodes.ID, error) {
	nodesRepo := nodes.NewProjectionRepository()
	hits, err := nodesRepo.SearchNodeIDs(ctx, s.db, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search node ids: %w", err)
	}
	if len(hits) == 0 {
		return nil, nil
	}

	ids := make([]nodes.ID, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.ID)
	}

	return ids, nil
}

func (s *Synapse) GetLinkedNodes(ctx context.Context, frontier []nodes.ID, depth, breadth int) ([]nodes.Node, error) {
	if depth < 0 {
		return nil, errors.New("depth must be non-negative")
	}
	if breadth <= 0 {
		return nil, errors.New("breadth must be positive")
	}
	if len(frontier) == 0 {
		return nil, nil
	}

	nodesRepo := nodes.NewProjectionRepository()
	linksRepo := links.NewProjectionRepository()

	seen := make(map[nodes.ID]struct{}, len(frontier))
	result := make([]nodes.Node, 0, len(frontier))
	current := dedupeNodeIDs(frontier)

	for level := 0; level <= depth; level++ {
		fetched, err := nodesRepo.GetNodesByIDs(ctx, s.db, current)
		if err != nil {
			return nil, fmt.Errorf("get frontier nodes: %w", err)
		}

		activeFrontier := make([]nodes.Node, 0, len(fetched))
		for _, node := range fetched {
			if node.ArchivedAt > 0 {
				continue
			}
			if _, ok := seen[node.ID]; ok {
				continue
			}
			seen[node.ID] = struct{}{}
			activeFrontier = append(activeFrontier, node)
			result = append(result, node)
		}

		if level == depth || len(activeFrontier) == 0 {
			break
		}

		frontierStreamIDs := make([]events.StreamID, 0, len(activeFrontier))
		for _, node := range activeFrontier {
			frontierStreamIDs = append(frontierStreamIDs, node.ID.StreamID())
		}

		outgoing, err := linksRepo.GetLinksFrom(ctx, s.db, frontierStreamIDs, len(frontierStreamIDs)*breadth)
		if err != nil {
			return nil, fmt.Errorf("get outgoing links: %w", err)
		}
		incoming, err := linksRepo.GetLinksTo(ctx, s.db, frontierStreamIDs, len(frontierStreamIDs)*breadth)
		if err != nil {
			return nil, fmt.Errorf("get incoming links: %w", err)
		}

		candidateIDs := collectLinkedNodeIDs(frontierStreamIDs, outgoing, incoming, seen)
		if len(candidateIDs) == 0 {
			break
		}

		candidateNodes, err := nodesRepo.GetNodesByIDs(ctx, s.db, candidateIDs)
		if err != nil {
			return nil, fmt.Errorf("get candidate nodes: %w", err)
		}

		current = make([]nodes.ID, 0, breadth)
		for _, node := range candidateNodes {
			if node.ArchivedAt > 0 {
				continue
			}
			current = append(current, node.ID)
			if len(current) == breadth {
				break
			}
		}
		if len(current) == 0 {
			break
		}
	}

	return result, nil
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

func loadExistingSchemaAggregate(ctx context.Context, repo *events.Repository, db sqlx.DB, id schemas.ID) (*schemas.Aggregate, error) {
	aggregate, err := loadSchemaAggregate(ctx, repo, db, id)
	if err != nil {
		return nil, err
	}
	if !aggregate.Exists() {
		return nil, fmt.Errorf("schema does not exist: %s", id)
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

func dedupeNodeIDs(ids []nodes.ID) []nodes.ID {
	seen := make(map[nodes.ID]struct{}, len(ids))
	out := make([]nodes.ID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func collectLinkedNodeIDs(frontier []events.StreamID, outgoing, incoming []links.Link, seen map[nodes.ID]struct{}) []nodes.ID {
	frontierSet := make(map[events.StreamID]struct{}, len(frontier))
	for _, id := range frontier {
		frontierSet[id] = struct{}{}
	}

	candidateSet := make(map[nodes.ID]struct{})
	candidates := make([]nodes.ID, 0, len(outgoing)+len(incoming))
	appendCandidate := func(streamID events.StreamID) {
		if _, ok := frontierSet[streamID]; ok {
			return
		}
		nodeID, err := nodes.ParseID(streamID.String())
		if err != nil {
			return
		}
		if _, ok := seen[nodeID]; ok {
			return
		}
		if _, ok := candidateSet[nodeID]; ok {
			return
		}
		candidateSet[nodeID] = struct{}{}
		candidates = append(candidates, nodeID)
	}

	for _, link := range outgoing {
		appendCandidate(link.From)
		appendCandidate(link.To)
	}
	for _, link := range incoming {
		appendCandidate(link.From)
		appendCandidate(link.To)
	}

	return candidates
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

	fromAggregate, err := loadExistingNodeAggregate(ctx, eventsRepo, tx, fromID)
	if err != nil {
		return fmt.Errorf("from: %w", err)
	}
	if fromAggregate.Archived() {
		return errors.New("from node is archived")
	}

	toAggregate, err := loadExistingNodeAggregate(ctx, eventsRepo, tx, toID)
	if err != nil {
		return fmt.Errorf("to: %w", err)
	}
	if toAggregate.Archived() {
		return errors.New("to node is archived")
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
