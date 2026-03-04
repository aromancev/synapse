package events

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testAggregate struct {
	versions []int64
}

func (a *testAggregate) Apply(event Event) error {
	a.versions = append(a.versions, event.StreamVersion)
	return nil
}

func TestStream_Init_ReplaysEventsInOrder(t *testing.T) {
	s := NewStream("schema:person", "schema", []Event{
		{StreamVersion: 1, GlobalPosition: 10},
		{StreamVersion: 2, GlobalPosition: 20},
		{StreamVersion: 3, GlobalPosition: 30},
	})

	agg := &testAggregate{}
	err := s.Init(agg)

	assert.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, agg.versions)
}

type failingAggregate struct{}

func (a *failingAggregate) Apply(event Event) error {
	_ = event
	return errors.New("boom")
}

func TestStream_Init_ReturnsApplyError(t *testing.T) {
	s := NewStream("schema:person", "schema", []Event{{StreamVersion: 1}})

	err := s.Init(&failingAggregate{})

	assert.EqualError(t, err, "boom")
}

func TestStream_Record_CreatesEventWithMarshaledPayload(t *testing.T) {
	s := NewStream("schema:person", "schema", nil)

	req := Request{
		EventType:    "test.event",
		EventVersion: 1,
		OccurredAt:   1234567890,
		Payload:      map[string]string{"name": "test"},
		Meta:         map[string]any{"user": "admin"},
	}

	err := s.Record(req)
	assert.NoError(t, err)

	recorded := s.RecordedEvents()
	assert.Len(t, recorded, 1)
	assert.Equal(t, "schema:person", recorded[0].StreamID)
	assert.Equal(t, "schema", recorded[0].StreamType)
	assert.Equal(t, int64(1), recorded[0].StreamVersion)
	assert.Equal(t, "test.event", recorded[0].EventType)
	assert.Equal(t, `{"name":"test"}`, recorded[0].Payload)
	assert.Contains(t, recorded[0].Meta, "user")
}

func TestRequest_Validate_RequiresFields(t *testing.T) {
	tests := []struct {
		name    string
		req     Request
		wantErr string
	}{
		{
			name:    "missing event_type",
			req:     Request{EventVersion: 1, OccurredAt: 1, Payload: "x", Meta: "y"},
			wantErr: "event_type is required",
		},
		{
			name:    "missing event_version",
			req:     Request{EventType: "test", OccurredAt: 1, Payload: "x", Meta: "y"},
			wantErr: "event_version must be > 0",
		},
		{
			name:    "missing occurred_at",
			req:     Request{EventType: "test", EventVersion: 1, Payload: "x", Meta: "y"},
			wantErr: "occurred_at is required",
		},
		{
			name:    "missing payload",
			req:     Request{EventType: "test", EventVersion: 1, OccurredAt: 1, Meta: "y"},
			wantErr: "payload is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}
