package replay

import (
	"context"
	"encoding/json"
	"fmt"
)

type Runner[S State] struct {
	store      EventReader
	snapshots  SnapshotStore
	projection *Projection[S]
	newState   func() S
}

func NewRunner[S State](store EventReader, snapshots SnapshotStore, projection *Projection[S], newState func() S) *Runner[S] {
	return &Runner[S]{store: store, snapshots: snapshots, projection: projection, newState: newState}
}

func (r *Runner[S]) Restore(ctx context.Context) (S, error) {
	state := r.newState()
	var from int64
	if r.snapshots != nil {
		snapshot, ok, err := r.snapshots.Load(ctx, r.projection.Name())
		if err != nil {
			return state, err
		}
		if ok {
			loaded := new(S)
			if err := json.Unmarshal(snapshot.State, loaded); err != nil {
				return state, fmt.Errorf("load snapshot for projection %q: %w", r.projection.Name(), err)
			}
			state = *loaded
			if state.Version() != snapshot.Version {
				return state, fmt.Errorf("snapshot %q version mismatch: envelope %d, state %d", r.projection.Name(), snapshot.Version, state.Version())
			}
			from = snapshot.Version
		}
	}
	events, err := r.store.ReadFrom(ctx, from, 0)
	if err != nil {
		return state, err
	}
	for _, event := range events {
		state, err = r.projection.Apply(state, event)
		if err != nil {
			return state, err
		}
	}
	return state, nil
}

func (r *Runner[S]) SaveSnapshot(ctx context.Context, state S) error {
	if r.snapshots == nil {
		return fmt.Errorf("projection %q has no snapshot store", r.projection.Name())
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.snapshots.Save(ctx, Snapshot{Projection: r.projection.Name(), Version: state.Version(), State: data})
}
