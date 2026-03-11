package nodes

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregate_Create(t *testing.T) {
	t.Run("records normalized node created event", func(t *testing.T) {
		uid, err := NewID()
		require.NoError(t, err)

		stream := events.NewStream(events.StreamID(uid.String()), StreamTypeNode, nil)
		aggregate := &Aggregate{}
		require.NoError(t, stream.Init(context.Background(), aggregate))

		err = aggregate.Create(context.Background(), stream, json.RawMessage(` { "name": "Ada" } `), uid, events.StreamID("  schema_01HXYZ  "))
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 1)

		e := recorded[0]
		assert.Equal(t, StreamTypeNode, e.StreamType)
		assert.Equal(t, EventTypeNodeCreated, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))

		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.JSONEq(t, `"`+uid.String()+`"`, string(payload["uid"]))
		assert.JSONEq(t, `"schema_01HXYZ"`, string(payload["schema_id"]))
		assert.JSONEq(t, `{"name":"Ada"}`, string(payload["payload"]))
	})
}
