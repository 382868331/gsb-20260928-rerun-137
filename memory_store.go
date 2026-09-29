package replay

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type MemoryEventStore struct {
	mu       sync.Mutex
	events   []Event
	versions map[string]int64
	watchers map[*memoryWatcher]struct{}
	nextID   int64
}

type memoryWatcher struct {
	after int64
	ch    chan Event
	done  <-chan struct{}
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		versions: make(map[string]int64),
		watchers: make(map[*memoryWatcher]struct{}),
	}
}

func (s *MemoryEventStore) Append(ctx context.Context, stream string, expectedVersion int64, inputs ...EnvelopeInput) ([]Event, error) {
	s.mu.Lock()
	current := s.versions[stream]
	if expectedVersion != AnyVersion && current != expectedVersion {
		s.mu.Unlock()
		return nil, ConcurrencyError{Stream: stream, Expected: expectedVersion, ActualVersion: current}
	}

	appended := make([]Event, 0, len(inputs))
	for _, input := range inputs {
		s.nextID++
		current++
		occurredAt := input.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = time.Now().UTC()
		}
		event := Event{
			Sequence:      s.nextID,
			Stream:        stream,
			StreamVersion: current,
			Type:          input.Type,
			Payload:       append(json.RawMessage(nil), input.Payload...),
			OccurredAt:    occurredAt,
		}
		s.events = append(s.events, event)
		s.versions[stream] = current
		appended = append(appended, event)
	}
	targets := make([]*memoryWatcher, 0)
	for watcher := range s.watchers {
		if appended[len(appended)-1].Sequence > watcher.after {
			targets = append(targets, watcher)
		}
	}
	s.mu.Unlock()

	for _, watcher := range targets {
		watcher.deliver(ctx, appended)
	}
	return appended, nil
}

func (w *memoryWatcher) deliver(ctx context.Context, events []Event) {
	for _, event := range events {
		if event.Sequence <= w.after {
			continue
		}
		select {
		case w.ch <- cloneEvent(event):
		case <-w.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *MemoryEventStore) ReadStream(_ context.Context, stream string) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Event, 0)
	for _, event := range s.events {
		if event.Stream == stream {
			result = append(result, cloneEvent(event))
		}
	}
	return result, nil
}

func (s *MemoryEventStore) ReadFrom(_ context.Context, afterSequence int64, limit int) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Event, 0)
	for _, event := range s.events {
		if event.Sequence <= afterSequence {
			continue
		}
		result = append(result, cloneEvent(event))
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (s *MemoryEventStore) LastSequence(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextID, nil
}

func (s *MemoryEventStore) Watch(ctx context.Context, afterSequence int64) <-chan Event {
	w := &memoryWatcher{after: afterSequence, ch: make(chan Event, 1), done: ctx.Done()}
	s.mu.Lock()
	s.watchers[w] = struct{}{}
	backlog := make([]Event, 0)
	for _, event := range s.events {
		if event.Sequence > afterSequence {
			backlog = append(backlog, cloneEvent(event))
		}
	}
	s.mu.Unlock()
	go func() {
		w.deliver(ctx, backlog)
	}()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.watchers, w)
		s.mu.Unlock()
	}()
	return w.ch
}

func cloneEvent(event Event) Event {
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	return event
}
