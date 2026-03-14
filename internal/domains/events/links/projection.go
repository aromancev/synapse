package links

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

const ProjectionName = "links"

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
	return StreamTypeLink
}

func (p *Projection) Project(ctx context.Context, db sqlx.DB, event events.Event) error {
	switch event.EventType {
	case EventTypeLinkCreated:
		var payload createdEvent
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("unmarshal link.created payload: %w", err)
		}
		payload = payload.normalized()

		return p.repo.UpsertLink(ctx, db, Link{
			From:      payload.From,
			To:        payload.To,
			Weight:    1,
			CreatedAt: event.OccurredAt,
		})
	default:
		return nil
	}
}
