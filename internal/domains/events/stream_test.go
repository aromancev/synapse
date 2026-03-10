package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	t.Run("Record creates event with marshaled payload", func(t *testing.T) {
		stream := NewStream(StreamID("stream-1"), StreamType("test"), nil)

		req := Request{
			EventType:    EventType("test.event"),
			EventVersion: 1,
			OccurredAt:   1234567890,
			Payload:      map[string]string{"name": "test"},
			Meta:         map[string]string{"user": "admin"},
		}

		err := stream.Record(req)
		require.NoError(t, err)

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 1)

		e := recorded[0]
		assert.Equal(t, StreamID("stream-1"), e.StreamID)
		assert.Equal(t, StreamType("test"), e.StreamType)
		assert.Equal(t, EventType("test.event"), e.EventType)
		assert.Equal(t, int64(1), e.EventVersion)
		assert.Equal(t, int64(1), e.StreamVersion)
		assert.Equal(t, int64(1234567890), e.OccurredAt)
		assert.NotZero(t, e.RecordedAt)
		assert.Equal(t, json.RawMessage(`{"name":"test"}`), e.Payload)
		assert.Contains(t, string(e.Meta), "user")
		assert.True(t, strings.HasPrefix(e.ID.String(), "event_"))
	})

	t.Run("Record increments stream version", func(t *testing.T) {
		stream := NewStream(StreamID("stream-2"), StreamType("test"), nil)
		_ = stream.Record(Request{EventType: EventType("a"), EventVersion: 1, OccurredAt: 1, Payload: "x", Meta: "y"})
		_ = stream.Record(Request{EventType: EventType("b"), EventVersion: 1, OccurredAt: 2, Payload: "x", Meta: "y"})
		_ = stream.Record(Request{EventType: EventType("c"), EventVersion: 1, OccurredAt: 3, Payload: "x", Meta: "y"})

		recorded := stream.RecordedEvents()
		require.Len(t, recorded, 3)
		assert.Equal(t, int64(1), recorded[0].StreamVersion)
		assert.Equal(t, int64(2), recorded[1].StreamVersion)
		assert.Equal(t, int64(3), recorded[2].StreamVersion)
	})

	t.Run("Init replays events into aggregate", func(t *testing.T) {
		eventID1, err := NewEventID()
		require.NoError(t, err)
		eventID2, err := NewEventID()
		require.NoError(t, err)

		existing := []Event{
			{ID: eventID1, StreamID: StreamID("s1"), StreamType: StreamType("test"), StreamVersion: 1, EventType: EventType("init"), EventVersion: 1, OccurredAt: 1, RecordedAt: 1, Payload: json.RawMessage(`{"v":1}`), Meta: json.RawMessage(`{}`)},
			{ID: eventID2, StreamID: StreamID("s1"), StreamType: StreamType("test"), StreamVersion: 2, EventType: EventType("update"), EventVersion: 1, OccurredAt: 2, RecordedAt: 2, Payload: json.RawMessage(`{"v":2}`), Meta: json.RawMessage(`{}`)},
		}
		stream := NewStream(StreamID("s1"), StreamType("test"), existing)

		var received []Event
		agg := &testAggregate{received: &received}

		err = stream.Init(context.Background(), agg)
		require.NoError(t, err)
		require.Len(t, received, 2)
		assert.Equal(t, EventType("init"), received[0].EventType)
		assert.Equal(t, EventType("update"), received[1].EventType)
	})

	t.Run("Init returns apply error", func(t *testing.T) {
		eventID, err := NewEventID()
		require.NoError(t, err)
		stream := NewStream(StreamID("s1"), StreamType("test"), []Event{{ID: eventID, StreamID: StreamID("s1"), StreamType: StreamType("test"), StreamVersion: 1, EventType: EventType("init"), EventVersion: 1, OccurredAt: 1, RecordedAt: 1, Payload: json.RawMessage(`{"v":1}`), Meta: json.RawMessage(`{}`)}})
		err = stream.Init(context.Background(), failingAggregate{})
		require.EqualError(t, err, "boom")
	})
}

type testAggregate struct{ received *[]Event }

func (a *testAggregate) Apply(ctx context.Context, e Event) error {
	_ = ctx
	*a.received = append(*a.received, e)
	return nil
}

type failingAggregate struct{}

func (failingAggregate) Apply(context.Context, Event) error { return errors.New("boom") }
