package replay

import (
	"context"
	"time"
)

type TypedEventStore struct {
	EventStore
	codec *Codec
}

func NewTypedEventStore(store EventStore, codec *Codec) *TypedEventStore {
	return &TypedEventStore{EventStore: store, codec: codec}
}

func (s *TypedEventStore) Append(ctx context.Context, stream string, expectedVersion int64, payloads ...Payload) ([]Event, error) {
	inputs := make([]EnvelopeInput, 0, len(payloads))
	for _, payload := range payloads {
		eventType, data, err := s.codec.Encode(payload)
		if err != nil {
			return nil, err
		}
		var occurredAt time.Time
		if timed, ok := payload.(TimedPayload); ok {
			occurredAt = timed.OccurredAt()
		}
		inputs = append(inputs, EnvelopeInput{Type: eventType, Payload: data, OccurredAt: occurredAt})
	}
	return s.EventStore.Append(ctx, stream, expectedVersion, inputs...)
}
