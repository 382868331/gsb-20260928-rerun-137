package replay

import (
	"context"
	"fmt"
	"sync"
)

type projectionEntry struct {
	state State
	apply func(State, Event) (State, error)
}

type Manager struct {
	mu      sync.RWMutex
	active  string
	entries map[string]projectionEntry
}

func NewManager(name string, state State) *Manager {
	return &Manager{
		active:  name,
		entries: map[string]projectionEntry{name: {state: state}},
	}
}

func (m *Manager) Active() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active
}

func (m *Manager) Query(name string) (State, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.entries[name]
	return entry.state, ok
}

func (m *Manager) QueryActive() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entries[m.active].state
}

func (m *Manager) Stage(name string, state State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[name] = projectionEntry{state: state}
}

func (m *Manager) Register(name string, state State, apply func(State, Event) (State, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[name] = projectionEntry{state: state, apply: apply}
}

func (m *Manager) Promote(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.entries[name]; !ok {
		return fmt.Errorf("projection %q is not staged", name)
	}
	m.active = name
	return nil
}

func (m *Manager) Run(ctx context.Context, store EventStore) error {
	last, err := store.LastSequence(ctx)
	if err != nil {
		return err
	}
	events, err := store.ReadFrom(ctx, 0, 0)
	if err != nil {
		return err
	}
	m.mu.Lock()
	for _, event := range events {
		if err := m.applyEventLocked(event); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Unlock()

	ch := store.Watch(ctx, last)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-ch:
			m.mu.Lock()
			err := m.applyEventLocked(event)
			m.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}
}

func (m *Manager) applyEventLocked(event Event) error {
	for name, entry := range m.entries {
		if entry.apply == nil || entry.state.Version() >= event.Sequence {
			continue
		}
		next, err := entry.apply(entry.state, event)
		if err != nil {
			return err
		}
		entry.state = next
		m.entries[name] = entry
	}
	return nil
}
