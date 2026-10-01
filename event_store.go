package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const NoStreamVersion int64 = 0

type UnsavedEvent struct {
	Type string
	Data any
}

type RecordedEvent struct {
	ID         int64           `json:"id"`
	StreamID   string          `json:"stream_id"`
	Version    int64           `json:"version"`
	Type       string          `json:"type"`
	Data       json.RawMessage `json:"data"`
	OccurredAt time.Time       `json:"occurred_at"`
}

type EventStore interface {
	Append(ctx context.Context, streamID string, expectedVersion int64, events ...UnsavedEvent) ([]RecordedEvent, error)
	ReadStream(ctx context.Context, streamID string) ([]RecordedEvent, error)
	ReadStreamAfter(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, error)
	ReadTail(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, int64, error)
	StreamVersion(ctx context.Context, streamID string) (int64, error)
}

// AppendLocker serializes appends against projection cut-over. While fn
// runs, no append to any stream of the store can be committed, so a
// projection can replay its tail and flip the current pointer without losing
// events that arrive in between. The EventStore passed to fn performs its
// operations under the held lock and must be used (instead of the outer
// store) for any reads or appends inside fn.
type AppendLocker interface {
	WithAppendLock(fn func(locked EventStore) error) error
}

type ConcurrencyError struct {
	StreamID        string
	ExpectedVersion int64
	ActualVersion   int64
}

func (e ConcurrencyError) Error() string {
	return fmt.Sprintf("replay: expected stream %q version %d, actual version %d", e.StreamID, e.ExpectedVersion, e.ActualVersion)
}

type MemoryEventStore struct {
	mu      sync.Mutex
	streams map[string][]RecordedEvent
	nextID  atomic.Int64
	now     func() time.Time
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		streams: make(map[string][]RecordedEvent),
		now:     time.Now,
	}
}

func (s *MemoryEventStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...UnsavedEvent) ([]RecordedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(streamID, expectedVersion, events...)
}

func (s *MemoryEventStore) appendLocked(streamID string, expectedVersion int64, events ...UnsavedEvent) ([]RecordedEvent, error) {
	history := s.streams[streamID]
	actualVersion := int64(len(history))
	if actualVersion != expectedVersion {
		return nil, ConcurrencyError{StreamID: streamID, ExpectedVersion: expectedVersion, ActualVersion: actualVersion}
	}

	recorded := make([]RecordedEvent, 0, len(events))
	for i, event := range events {
		if event.Type == "" {
			return nil, fmt.Errorf("replay: event at position %d has an empty type", i)
		}
		payload, err := json.Marshal(event.Data)
		if err != nil {
			return nil, fmt.Errorf("replay: encode event %q: %w", event.Type, err)
		}
		eventVersion := actualVersion + int64(i) + 1
		recordedEvent := RecordedEvent{
			ID:         s.nextID.Add(1),
			StreamID:   streamID,
			Version:    eventVersion,
			Type:       event.Type,
			Data:       payload,
			OccurredAt: s.now().UTC(),
		}
		recorded = append(recorded, recordedEvent)
	}

	s.streams[streamID] = append(history, recorded...)
	return recorded, nil
}

func (s *MemoryEventStore) ReadStream(ctx context.Context, streamID string) ([]RecordedEvent, error) {
	return s.ReadStreamAfter(ctx, streamID, 0)
}

func (s *MemoryEventStore) ReadStreamAfter(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, error) {
	events, _, err := s.ReadTail(ctx, streamID, afterVersion)
	return events, err
}

func (s *MemoryEventStore) ReadTail(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readTailLocked(streamID, afterVersion)
}

func (s *MemoryEventStore) readTailLocked(streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	history := s.streams[streamID]
	if afterVersion < 0 {
		return nil, 0, fmt.Errorf("replay: after version cannot be negative")
	}
	if afterVersion > int64(len(history)) {
		return nil, 0, fmt.Errorf("replay: checkpoint %d is ahead of stream %q version %d", afterVersion, streamID, len(history))
	}

	result := make([]RecordedEvent, len(history[afterVersion:]))
	copy(result, history[afterVersion:])
	return result, int64(len(history)), nil
}

func (s *MemoryEventStore) StreamVersion(ctx context.Context, streamID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.streams[streamID])), nil
}

// WithAppendLock takes the same mutex that protects appends and reads. The
// memory store keeps a single mutex, so holding it here also makes ReadTail
// views stable while a projection cuts over.
func (s *MemoryEventStore) WithAppendLock(fn func(locked EventStore) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(memoryLockedView{store: s})
}

// memoryLockedView runs store operations while WithAppendLock holds the
// store mutex; it must not escape the callback.
type memoryLockedView struct {
	store *MemoryEventStore
}

func (memoryLockedView) Append(context.Context, string, int64, ...UnsavedEvent) ([]RecordedEvent, error) {
	return nil, fmt.Errorf("replay: appends are not allowed inside the append lock")
}

func (v memoryLockedView) ReadStream(ctx context.Context, streamID string) ([]RecordedEvent, error) {
	return v.ReadStreamAfter(ctx, streamID, 0)
}

func (v memoryLockedView) ReadStreamAfter(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, error) {
	events, _, err := v.ReadTail(ctx, streamID, afterVersion)
	return events, err
}

func (v memoryLockedView) ReadTail(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return v.store.readTailLocked(streamID, afterVersion)
}

func (v memoryLockedView) StreamVersion(ctx context.Context, streamID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return int64(len(v.store.streams[streamID])), nil
}
