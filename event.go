package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const AnyVersion int64 = -1

type Event struct {
	Sequence      int64
	Stream        string
	StreamVersion int64
	Type          string
	Payload       json.RawMessage
	OccurredAt    time.Time
}

type Payload interface {
	EventType() string
}

type TimedPayload interface {
	Payload
	OccurredAt() time.Time
}

type Codec struct {
	types map[string]func() any
}

func NewCodec() *Codec {
	return &Codec{types: make(map[string]func() any)}
}

func (c *Codec) Register(eventType string, factory func() any) {
	c.types[eventType] = factory
}

func (c *Codec) Encode(payload Payload) (string, json.RawMessage, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", nil, fmt.Errorf("encode event %q: %w", payload.EventType(), err)
	}
	return payload.EventType(), data, nil
}

func (c *Codec) Decode(event Event) (any, error) {
	factory, ok := c.types[event.Type]
	if !ok {
		return nil, fmt.Errorf("unknown event type %q", event.Type)
	}
	payload := factory()
	if err := json.Unmarshal(event.Payload, payload); err != nil {
		return nil, fmt.Errorf("decode event %q at sequence %d: %w", event.Type, event.Sequence, err)
	}
	return payload, nil
}

type EventStore interface {
	Append(ctx context.Context, stream string, expectedVersion int64, events ...EnvelopeInput) ([]Event, error)
	ReadStream(ctx context.Context, stream string) ([]Event, error)
	ReadFrom(ctx context.Context, afterSequence int64, limit int) ([]Event, error)
	LastSequence(ctx context.Context) (int64, error)
	Watch(ctx context.Context, afterSequence int64) <-chan Event
}

type EventReader interface {
	ReadStream(ctx context.Context, stream string) ([]Event, error)
	ReadFrom(ctx context.Context, afterSequence int64, limit int) ([]Event, error)
	LastSequence(ctx context.Context) (int64, error)
	Watch(ctx context.Context, afterSequence int64) <-chan Event
}

type EnvelopeInput struct {
	Type       string
	Payload    json.RawMessage
	OccurredAt time.Time
}

type ConcurrencyError struct {
	Stream        string
	Expected      int64
	ActualVersion int64
}

func (e ConcurrencyError) Error() string {
	return fmt.Sprintf("concurrency conflict on stream %q: expected version %d, actual %d", e.Stream, e.Expected, e.ActualVersion)
}

func IsConcurrencyError(err error) bool {
	var target ConcurrencyError
	return errors.As(err, &target)
}
