package links

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func replayAggregate(t *testing.T, aggregate *Aggregate, stream *events.Stream) {
	t.Helper()
	for _, event := range stream.AppliedEvents() {
		require.NoError(t, aggregate.Apply(context.Background(), event))
	}
}

func TestAggregate_Create(t *testing.T) {
	t.Run("records normalized link created event", func(t *testing.T) {
		from := events.StreamID("node_01ARZ3NDEKTSV4RRFFQ69G5FAV")
		to := events.StreamID("node_01ARZ3NDEKTSV4RRFFQ69G5FAW")

		stream := events.NewStream(StreamIDForPair(to, from), StreamTypeLink, nil)
		aggregate := &Aggregate{}

		err := aggregate.Create(context.Background(), stream, to, from)
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 1)

		e := recorded[0]
		assert.True(t, strings.HasPrefix(e.StreamID.String(), "link_node_01ARZ3NDEKTSV4RRFFQ69G5FAV_node_01ARZ3NDEKTSV4RRFFQ69G5FAW"))
		assert.Equal(t, StreamTypeLink, e.StreamType)
		assert.Equal(t, EventTypeLinkCreated, e.EventType)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))

		var payload map[string]string
		require.NoError(t, json.Unmarshal(e.Payload, &payload))
		assert.Equal(t, from.String(), payload["from"])
		assert.Equal(t, to.String(), payload["to"])
	})

	t.Run("refuses duplicate existing link", func(t *testing.T) {
		from := events.StreamID("node_01ARZ3NDEKTSV4RRFFQ69G5FAV")
		to := events.StreamID("node_01ARZ3NDEKTSV4RRFFQ69G5FAW")

		stream := events.NewStream(StreamIDForPair(from, to), StreamTypeLink, nil)
		aggregate := &Aggregate{}
		require.NoError(t, aggregate.Create(context.Background(), stream, from, to))

		replayed := events.NewStream(stream.RecordedEvents()[0].StreamID, StreamTypeLink, stream.RecordedEvents())
		aggregate = &Aggregate{}
		replayAggregate(t, aggregate, replayed)
		require.True(t, aggregate.Exists())
		err := aggregate.Create(context.Background(), replayed, to, from)
		require.EqualError(t, err, "link already exists")
	})
}
