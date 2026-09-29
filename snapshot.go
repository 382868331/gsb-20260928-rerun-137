package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type Snapshot struct {
	Projection string          `json:"projection"`
	Version    int64           `json:"version"`
	State      json.RawMessage `json:"state"`
}

type SnapshotStore interface {
	Load(ctx context.Context, projection string) (Snapshot, bool, error)
	Save(ctx context.Context, snapshot Snapshot) error
}

type MemorySnapshotStore struct {
	mu        sync.RWMutex
	snapshots map[string]Snapshot
}

func NewMemorySnapshotStore() *MemorySnapshotStore {
	return &MemorySnapshotStore{snapshots: make(map[string]Snapshot)}
}

func (s *MemorySnapshotStore) Load(_ context.Context, projection string) (Snapshot, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.snapshots[projection]
	if !ok {
		return Snapshot{}, false, nil
	}
	snapshot.State = append(json.RawMessage(nil), snapshot.State...)
	return snapshot, true, nil
}

func (s *MemorySnapshotStore) Save(_ context.Context, snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snapshot.Version < 0 {
		return fmt.Errorf("snapshot version cannot be negative")
	}
	snapshot.State = append(json.RawMessage(nil), snapshot.State...)
	s.snapshots[snapshot.Projection] = snapshot
	return nil
}
