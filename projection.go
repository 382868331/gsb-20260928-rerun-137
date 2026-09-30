package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type Snapshot[S any] struct {
	StreamID string `json:"stream_id"`
	Version  int64  `json:"version"`
	Position int64  `json:"position"`
	State    S      `json:"state"`
}

type EventHandler[S any] func(state *S, event RecordedEvent) error

type ProjectionFailure struct {
	StreamID string `json:"stream_id"`
	Version  int64  `json:"version"`
	Position int64  `json:"position"`
	Type     string `json:"type"`
	Reason   string `json:"reason"`
}

func (f ProjectionFailure) Error() string {
	return fmt.Sprintf("replay: projection stopped at stream %q version %d event %q: %s", f.StreamID, f.Version, f.Type, f.Reason)
}

type Projector[S any] struct {
	store      EventStore
	streamID   string
	state      S
	version    int64
	position   int64
	handle     EventHandler[S]
	cloneState func(S) S
	mu         sync.Mutex
	failure    *ProjectionFailure
}

func NewProjector[S any](store EventStore, streamID string, initialState S, handler EventHandler[S], clone func(S) S) *Projector[S] {
	if clone == nil {
		clone = func(value S) S { return value }
	}
	return &Projector[S]{
		store:      store,
		streamID:   streamID,
		state:      initialState,
		handle:     handler,
		cloneState: clone,
	}
}

func (p *Projector[S]) Replay(ctx context.Context) error {
	events, err := p.store.ReadStreamAfter(ctx, p.streamID, p.version)
	if err != nil {
		return err
	}
	return p.Apply(events)
}

func (p *Projector[S]) Apply(events []RecordedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failure = nil
	for _, event := range events {
		if event.StreamID != p.streamID {
			return fmt.Errorf("replay: event %d belongs to stream %q, not %q", event.ID, event.StreamID, p.streamID)
		}
		if event.Version != p.version+1 {
			return fmt.Errorf("replay: expected version %d, got %d", p.version+1, event.Version)
		}
		if event.Position < p.position {
			return fmt.Errorf("replay: event position %d is before checkpoint %d", event.Position, p.position)
		}

		workingState := p.cloneState(p.state)
		if err := p.handle(&workingState, event); err != nil {
			p.failure = &ProjectionFailure{StreamID: p.streamID, Version: event.Version, Position: event.Position, Type: event.Type, Reason: err.Error()}
			return fmt.Errorf("replay: projection failed at stream %q version %d event %q: %w", p.streamID, event.Version, event.Type, err)
		}
		p.state = workingState
		p.version = event.Version
		p.position = event.Position
	}
	return nil
}

func (p *Projector[S]) Failure() (ProjectionFailure, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failure == nil {
		return ProjectionFailure{}, false
	}
	return *p.failure, true
}

func (p *Projector[S]) State() S {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cloneState(p.state)
}

func (p *Projector[S]) Version() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.version
}

func (p *Projector[S]) Position() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.position
}

func (p *Projector[S]) Snapshot() Snapshot[S] {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Snapshot[S]{StreamID: p.streamID, Version: p.version, Position: p.position, State: p.cloneState(p.state)}
}

func (p *Projector[S]) Restore(snapshot Snapshot[S]) error {
	if snapshot.StreamID != p.streamID {
		return fmt.Errorf("replay: snapshot belongs to stream %q, not %q", snapshot.StreamID, p.streamID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if snapshot.Version < 0 {
		return fmt.Errorf("replay: snapshot version cannot be negative")
	}
	if snapshot.Position < 0 {
		return fmt.Errorf("replay: snapshot position cannot be negative")
	}
	if snapshot.Version > 0 && snapshot.Position == 0 {
		return fmt.Errorf("replay: snapshot at version %d is missing its position", snapshot.Version)
	}
	p.state = p.cloneState(snapshot.State)
	p.version = snapshot.Version
	p.position = snapshot.Position
	p.failure = nil
	return nil
}

func DecodeEvent[T any](event RecordedEvent) (T, error) {
	var value T
	if err := json.Unmarshal(event.Data, &value); err != nil {
		return value, fmt.Errorf("decode %q event version %d: %w", event.Type, event.Version, err)
	}
	return value, nil
}

type Service[S any] struct {
	store     EventStore
	projector *Projector[S]
}

func NewService[S any](store EventStore, projector *Projector[S]) *Service[S] {
	return &Service[S]{store: store, projector: projector}
}

func (s *Service[S]) Commit(ctx context.Context, streamID string, expectedVersion int64, events ...UnsavedEvent) (int64, error) {
	if streamID != s.projector.streamID {
		return expectedVersion, fmt.Errorf("replay: service projector belongs to stream %q, not %q", s.projector.streamID, streamID)
	}
	recorded, err := s.store.Append(ctx, streamID, expectedVersion, events...)
	if err != nil {
		return expectedVersion, err
	}
	if err := s.projector.Apply(recorded); err != nil {
		return s.projector.Version(), err
	}
	return s.projector.Version(), nil
}

func (s *Service[S]) State() S {
	return s.projector.State()
}

func (s *Service[S]) Version() int64 {
	return s.projector.Version()
}
