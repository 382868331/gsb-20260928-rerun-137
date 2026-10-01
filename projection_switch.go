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

type stableTailReader interface {
	readTailLocked(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, int64, error)
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

// RebuildAndSwitch rebuilds the named projection from complete history and
// only publishes it after the final read and application are protected by the
// store append lock. Appends before that read are included exactly once;
// appends after cut-over remain available to the new current view's next
// replay because they are not applied twice.
func (p *ProjectionSwitch[S]) RebuildAndSwitch(ctx context.Context, name string) error {
	p.mu.RLock()
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	if err := projector.Replay(ctx); err != nil {
		return err
	}
	return p.catchUpAndSwitch(ctx, name, projector)
}

// RestoreRebuildAndSwitch starts from an old snapshot, replays every newer
// confirmed event, and then atomically makes that view current. The previous
// current view remains queryable until the replacement has reached the same
// confirmed log position.
func (p *ProjectionSwitch[S]) RestoreRebuildAndSwitch(ctx context.Context, name string, snapshot Snapshot[S]) error {
	p.mu.RLock()
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	if err := projector.Restore(snapshot); err != nil {
		return err
	}
	if err := projector.Replay(ctx); err != nil {
		return err
	}
	return p.catchUpAndSwitch(ctx, name, projector)
}

func (p *ProjectionSwitch[S]) catchUpAndSwitch(ctx context.Context, name string, projector *Projector[S]) error {
	locker, ok := p.store.(AppendLocker)
	if !ok {
		return fmt.Errorf("replay: event store %T does not serialize appends during projection switch", p.store)
	}
	reader, ok := p.store.(stableTailReader)
	if !ok {
		return fmt.Errorf("replay: event store %T cannot read a stable tail under append lock", p.store)
	}
	return locker.WithAppendLock(func() error {
		events, _, err := reader.readTailLocked(ctx, p.stream, projector.Version())
		if err != nil {
			return err
		}
		if err := projector.Apply(events); err != nil {
			return err
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, ok := p.views[name]; !ok {
			return fmt.Errorf("replay: projection %q is not registered", name)
		}
		p.current = name
		return nil
	})
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
