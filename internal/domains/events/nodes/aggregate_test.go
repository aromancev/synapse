package nodes

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregate_Add(t *testing.T) {
	t.Run("records normalized node added event", func(t *testing.T) {
		uid, err := NewID()
		require.NoError(t, err)

		stream := events.NewStream(events.StreamID(uid.String()), StreamTypeNode, nil)
		aggregate := &Aggregate{}
		require.NoError(t, stream.Init(context.Background(), aggregate))

		now := time.Now().Unix()
		err = aggregate.Add(context.Background(), stream, Node{
			UID:       uid,
			SchemaID:  events.StreamID("  schema_01HXYZ  "),
			CreatedAt: now,
			Payload:   json.RawMessage(` { "name": "Ada" } `),
		})
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 1)

		e := recorded[0]
		assert.Equal(t, StreamTypeNode, e.StreamType)
		assert.Equal(t, EventTypeNodeAdded, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))

		var payload Node
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, uid, payload.UID)
		assert.Equal(t, events.StreamID("schema_01HXYZ"), payload.SchemaID)
		assert.Equal(t, json.RawMessage(`{"name":"Ada"}`), payload.Payload)
	})
}
