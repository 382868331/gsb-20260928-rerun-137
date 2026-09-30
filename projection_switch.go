package replay

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

type SnapshotStore[S any] interface {
	Save(ctx context.Context, name string, snapshot Snapshot[S]) error
	Load(ctx context.Context, name string) (Snapshot[S], error)
}

type lockedEventStore interface {
	EventStore
	Lock()
	Unlock()
	streamVersionLocked(context.Context, string) (int64, error)
	readStreamAfterLocked(context.Context, string, int64) ([]RecordedEvent, error)
}

type ProjectionSwitch[S any] struct {
	mu           sync.RWMutex
	store        EventStore
	lockedStore  lockedEventStore
	stream       string
	cloner       func(S) S
	views        map[string]*Projector[S]
	current      string
	snapshots    map[string]SnapshotStore[S]
	currentStore CurrentStore
}

func NewProjectionSwitch[S any](store EventStore, streamID string, initial S, cloner func(S) S) *ProjectionSwitch[S] {
	switcher := &ProjectionSwitch[S]{
		store:     store,
		stream:    streamID,
		cloner:    cloner,
		views:     make(map[string]*Projector[S]),
		snapshots: make(map[string]SnapshotStore[S]),
	}
	if locked, ok := store.(lockedEventStore); ok {
		switcher.lockedStore = locked
	}
	return switcher
}

func (p *ProjectionSwitch[S]) WithSnapshotStore(store SnapshotStore[S]) *ProjectionSwitch[S] {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots["*"] = store
	return p
}

func (p *ProjectionSwitch[S]) WithNamedSnapshotStore(name string, store SnapshotStore[S]) *ProjectionSwitch[S] {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots[name] = store
	return p
}

func (p *ProjectionSwitch[S]) WithCurrentStore(store CurrentStore) *ProjectionSwitch[S] {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.currentStore = store
	return p
}

func (p *ProjectionSwitch[S]) Register(name string, initial S, handler EventHandler[S]) *Projector[S] {
	p.mu.Lock()
	defer p.mu.Unlock()
	projector := NewProjector(p.store, p.stream, initial, handler, p.cloner)
	p.views[name] = projector
	return projector
}

func (p *ProjectionSwitch[S]) Restore(ctx context.Context, name string) error {
	p.mu.RLock()
	projector, ok := p.views[name]
	store := p.snapshotStoreLocked(name)
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	if store == nil {
		return fmt.Errorf("replay: snapshot store is not configured")
	}
	snapshot, err := store.Load(ctx, name)
	if err != nil {
		return err
	}
	return projector.Restore(snapshot)
}

func (p *ProjectionSwitch[S]) RestoreCurrent(ctx context.Context) error {
	p.mu.Lock()
	store := p.currentStore
	p.mu.Unlock()
	if store == nil {
		return fmt.Errorf("replay: current projection store is not configured")
	}
	current, err := store.LoadCurrent(ctx)
	if err != nil {
		return err
	}
	if current.StreamID != p.stream {
		return fmt.Errorf("replay: current marker belongs to stream %q, not %q", current.StreamID, p.stream)
	}
	p.mu.RLock()
	projector, ok := p.views[current.Name]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: current projection %q is not registered", current.Name)
	}
	if version := projector.Version(); version > current.Version {
		return fmt.Errorf("replay: restored projection %q version %d is ahead of current marker %d", current.Name, version, current.Version)
	}
	if position := projector.Position(); position > current.Position {
		return fmt.Errorf("replay: restored projection %q position %d is ahead of current marker %d", current.Name, position, current.Position)
	}
	p.mu.Lock()
	p.current = current.Name
	p.mu.Unlock()
	return nil
}

func (p *ProjectionSwitch[S]) PublishSnapshot(ctx context.Context, name string) error {
	p.mu.RLock()
	projector, ok := p.views[name]
	store := p.snapshotStoreLocked(name)
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	if store == nil {
		return fmt.Errorf("replay: snapshot store is not configured")
	}
	if failure, failed := projector.Failure(); failed {
		return failure
	}
	return store.Save(ctx, name, projector.Snapshot())
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

func (p *ProjectionSwitch[S]) CatchUp(ctx context.Context, name string) error {
	return p.catchUp(ctx, name, nil, nil)
}

func (p *ProjectionSwitch[S]) Switch(ctx context.Context, name string) error {
	p.mu.RLock()
	currentStore := p.currentStore
	p.mu.RUnlock()
	var marker CurrentProjection
	if err := p.catchUp(ctx, name, nil, func(version int64, position int64) error {
		marker = CurrentProjection{StreamID: p.stream, Name: name, Version: version, Position: position}
		if currentStore != nil {
			return currentStore.SaveCurrent(ctx, marker)
		}
		return nil
	}); err != nil {
		return err
	}
	p.mu.Lock()
	p.current = name
	p.mu.Unlock()
	return nil
}

func (p *ProjectionSwitch[S]) SwitchAt(ctx context.Context, name string, targetVersion int64) error {
	return p.catchUp(ctx, name, &targetVersion, nil)
}

func (p *ProjectionSwitch[S]) catchUp(ctx context.Context, name string, targetVersion *int64, caught func(version int64, position int64) error) error {
	p.mu.RLock()
	projector, ok := p.views[name]
	hasLock := p.lockedStore != nil
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replay: projection %q is not registered", name)
	}
	if hasLock {
		p.lockedStore.Lock()
		defer p.lockedStore.Unlock()
	}
	var (
		endVersion int64
		events     []RecordedEvent
		err        error
	)
	if hasLock {
		endVersion, err = p.lockedStore.streamVersionLocked(ctx, p.stream)
	} else {
		endVersion, err = p.store.StreamVersion(ctx, p.stream)
	}
	if err != nil {
		return err
	}
	if targetVersion != nil {
		if *targetVersion < projector.Version() {
			return fmt.Errorf("replay: target version %d is before projection version %d", *targetVersion, projector.Version())
		}
		endVersion = *targetVersion
	}
	if hasLock {
		events, err = p.lockedStore.readStreamAfterLocked(ctx, p.stream, projector.Version())
	} else {
		events, err = p.store.ReadStreamAfter(ctx, p.stream, projector.Version())
	}
	if err != nil {
		return err
	}
	if targetVersion != nil && int64(len(events)) > 0 && events[len(events)-1].Version > endVersion {
		events = events[:endVersion-projector.Version()]
	}
	if err := projector.Apply(events); err != nil {
		return err
	}
	actualVersion := projector.Version()
	if actualVersion != endVersion {
		return fmt.Errorf("replay: projection %q reached version %d, target is %d", name, actualVersion, endVersion)
	}
	if caught != nil {
		if err := caught(actualVersion, projector.Position()); err != nil {
			return err
		}
	}
	return ctx.Err()
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

func (p *ProjectionSwitch[S]) Failure(name string) (ProjectionFailure, bool) {
	p.mu.RLock()
	projector, ok := p.views[name]
	p.mu.RUnlock()
	if !ok {
		return ProjectionFailure{}, false
	}
	return projector.Failure()
}

func (p *ProjectionSwitch[S]) snapshotStoreLocked(name string) SnapshotStore[S] {
	if store, ok := p.snapshots[name]; ok {
		return store
	}
	return p.snapshots["*"]
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
