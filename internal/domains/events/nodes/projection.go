package nodes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

const ProjectionName = "nodes"

type Projector interface {
	Name() string
	StreamType() events.StreamType
	Project(ctx context.Context, db sqlx.DB, event events.Event) error
}

type Projection struct {
	repo *ProjectionRepository
}

func NewProjection() *Projection {
	return &Projection{repo: NewProjectionRepository()}
}

func (p *Projection) Name() string {
	return ProjectionName
}

func (p *Projection) StreamType() events.StreamType {
	return StreamTypeNode
}

func (p *Projection) Project(ctx context.Context, db sqlx.DB, event events.Event) error {
	switch event.EventType {
	case EventTypeNodeCreated:
		var payload nodeCreatedEvent
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("unmarshal node.created payload: %w", err)
		}
		payload = payload.normalized()

		return p.repo.UpsertNode(ctx, db, Node{
			ID:         payload.ID,
			SchemaID:   payload.SchemaID,
			CreatedAt:  event.OccurredAt,
			ArchivedAt: 0,
			Payload:    payload.Payload,
			SearchText: BuildSearchText(payload.Payload, nil),
		})
	case EventTypeNodeUpdated:
		var payload nodeUpdatedEvent
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("unmarshal node.updated payload: %w", err)
		}

		nodeID, err := ParseID(event.StreamID.String())
		if err != nil {
			return fmt.Errorf("parse node id from stream id: %w", err)
		}
		current, err := p.repo.GetNodeByID(ctx, db, nodeID)
		if err != nil {
			return fmt.Errorf("get existing node: %w", err)
		}

		current.Payload = payload.Payload
		current.SearchText = BuildSearchText(current.Payload, current.Keywords)
		return p.repo.UpsertNode(ctx, db, current)
	case EventTypeNodeKeywordsUpdated:
		var payload nodeKeywordsUpdatedEvent
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("unmarshal node.keywords_updated payload: %w", err)
		}
		payload = payload.normalized()

		nodeID, err := ParseID(event.StreamID.String())
		if err != nil {
			return fmt.Errorf("parse node id from stream id: %w", err)
		}
		current, err := p.repo.GetNodeByID(ctx, db, nodeID)
		if err != nil {
			return fmt.Errorf("get existing node: %w", err)
		}

		current.Keywords = payload.Keywords
		current.SearchText = BuildSearchText(current.Payload, current.Keywords)
		return p.repo.UpsertNode(ctx, db, current)
	case EventTypeNodeArchived:
		nodeID, err := ParseID(event.StreamID.String())
		if err != nil {
			return fmt.Errorf("parse node id from stream id: %w", err)
		}
		current, err := p.repo.GetNodeByID(ctx, db, nodeID)
		if err != nil {
			return fmt.Errorf("get existing node: %w", err)
		}

		current.ArchivedAt = event.OccurredAt
		return p.repo.UpsertNode(ctx, db, current)
	default:
		return nil
	}
}
