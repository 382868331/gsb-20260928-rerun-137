package replay

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

type ProjectionSwitch[S any] struct {
	mu      sync.RWMutex
	store   EventStore
	stream  string
	cloner  func(S) S
	views   map[string]*Projector[S]
	current string
}

func NewProjectionSwitch[S any](store EventStore, streamID string, initial S, cloner func(S) S) *ProjectionSwitch[S] {
	return &ProjectionSwitch[S]{
		store:  store,
		stream: streamID,
		cloner: cloner,
		views:  make(map[string]*Projector[S]),
	}
}

func (p *ProjectionSwitch[S]) Register(name string, initial S, handler EventHandler[S]) *Projector[S] {
	p.mu.Lock()
	defer p.mu.Unlock()
	projector := NewProjector(p.store, p.stream, initial, handler, p.cloner)
	p.views[name] = projector
	return projector
}

func (p *ProjectionSwitch[S]) Rebuild(ctx context.Context, name string) error {
	p.mu.RLock()
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	return projector.Replay(ctx)
}

func (p *ProjectionSwitch[S]) Switch(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.views[name]; !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	p.current = name
	return nil
}

func (p *ProjectionSwitch[S]) Current(ctx context.Context) (string, S, int64, error) {
	p.mu.RLock()
	name := p.current
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		var zero S
		return "", zero, 0, fmt.Errorf("replay: current projection is not set")
	}
	_ = ctx
	return name, projector.State(), projector.Version(), nil
}

func (p *ProjectionSwitch[S]) Query(name string) (S, int64, error) {
	p.mu.RLock()
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		var zero S
		return zero, 0, fmt.Errorf("replay: projection %q is not registered", name)
	}
	return projector.State(), projector.Version(), nil
}

type TinyStateModel[S any] struct{}

func (TinyStateModel[S]) Verify(expectedVersion, actualVersion int64, expectedState, actualState S) error {
	if expectedVersion != actualVersion {
		return fmt.Errorf("replay: model version %d does not match live version %d", expectedVersion, actualVersion)
	}
	if !reflect.DeepEqual(expectedState, actualState) {
		return fmt.Errorf("replay: replayed state does not match live state")
	}
	return nil
}

// Cutover replays the projection's remaining tail and atomically makes it the
// current view while appends are held off by the store's append lock, so
// events that arrive during the catch-up are neither lost nor applied twice.
// The store must implement AppendLocker.
func (p *ProjectionSwitch[S]) Cutover(ctx context.Context, name string) error {
	locker, ok := p.store.(AppendLocker)
	if !ok {
		return fmt.Errorf("replay: event store %T does not support append locking", p.store)
	}
	p.mu.RLock()
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	return locker.WithAppendLock(func(locked EventStore) error {
		if err := projector.ReplayFrom(ctx, locked); err != nil {
			return err
		}
		p.mu.Lock()
		p.current = name
		p.mu.Unlock()
		return nil
	})
}

// RestoreSnapshot loads a snapshot into a registered projection so the next
// Rebuild or Cutover only replays events appended after the snapshot.
func (p *ProjectionSwitch[S]) RestoreSnapshot(name string, snapshot Snapshot[S]) error {
	p.mu.RLock()
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	return projector.Restore(snapshot)
}
