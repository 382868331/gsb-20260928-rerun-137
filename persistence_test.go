package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testState struct {
	Value int `json:"value"`
}

func cloneTestState(state testState) testState { return state }

func applyTestEvent(state *testState, event RecordedEvent) error {
	switch event.Type {
	case "add":
		var payload struct {
			Amount int `json:"amount"`
		}
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			return err
		}
		state.Value += payload.Amount
	default:
		return fmt.Errorf("unknown event %q", event.Type)
	}
	return nil
}

func TestFileStorePersistsConfirmedEventsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	stream := "device-a"

	store, err := NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, stream, NoStreamVersion, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, stream, 1, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 3}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	events, version, err := reopened.ReadTail(ctx, stream, 0)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 || len(events) != 2 || events[0].Version != 1 || events[1].Version != 2 {
		t.Fatalf("events = %+v version = %d", events, version)
	}
	if _, err := reopened.Append(ctx, stream, 2, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 4}}); err != nil {
		t.Fatal(err)
	}
	projector := NewProjector(reopened, stream, testState{}, applyTestEvent, cloneTestState)
	if err := projector.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if projector.Version() != 3 || projector.State().Value != 9 {
		t.Fatalf("version=%d state=%+v", projector.Version(), projector.State())
	}
}

func TestFileStoreIsolatesUnpublishedTail(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	stream := "device-a"
	store, err := NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, stream, NoStreamVersion, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 2}}); err != nil {
		t.Fatal(err)
	}
	pending := []RecordedEvent{{
		ID:         99,
		StreamID:   stream,
		Version:    2,
		Type:       "add",
		Data:       json.RawMessage(`{"amount":100}`),
		OccurredAt: time.Now().UTC(),
	}}
	payload, _ := json.Marshal(pending)
	if err := os.WriteFile(filepath.Join(dir, "events", "pending.tmp"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	events, version, err := reopened.ReadTail(ctx, stream, 0)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || len(events) != 1 || events[0].Version != 1 {
		t.Fatalf("pending tail was treated as history: %+v version %d", events, version)
	}
	if _, err := reopened.Append(ctx, stream, 1, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 5}}); err != nil {
		t.Fatal(err)
	}
	fresh := NewProjector(reopened, stream, testState{}, applyTestEvent, cloneTestState)
	if err := fresh.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if fresh.Version() != 2 || fresh.State().Value != 7 {
		t.Fatalf("state after real append = %+v version %d", fresh.State(), fresh.Version())
	}
}

func TestSnapshotReopenAndCatchUpDoesNotSkipOrDoubleApply(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	stream := "device-a"
	store, err := NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := NewFileSnapshotStore[testState](dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent := func(version int64, amount int) {
		t.Helper()
		if _, err := store.Append(ctx, stream, version, UnsavedEvent{Type: "add", Data: map[string]int{"amount": amount}}); err != nil {
			t.Fatal(err)
		}
	}
	appendEvent(0, 1)
	appendEvent(1, 2)
	old := NewProjector(store, stream, testState{}, applyTestEvent, cloneTestState)
	if err := old.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Save(ctx, old.Snapshot()); err != nil {
		t.Fatal(err)
	}
	appendEvent(2, 3)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	snapshots2, err := NewFileSnapshotStore[testState](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshots2.Close()
	saved, err := snapshots2.Load(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Version != 2 || saved.State.Value != 3 {
		t.Fatalf("snapshot = %+v", saved)
	}
	fromSnapshot := NewProjector(store2, stream, testState{}, applyTestEvent, cloneTestState)
	if err := fromSnapshot.Restore(saved); err != nil {
		t.Fatal(err)
	}
	if err := fromSnapshot.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := NewProjector(store2, stream, testState{}, applyTestEvent, cloneTestState)
	if err := fresh.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if !projectorsMatch(fresh, fromSnapshot) || fromSnapshot.State().Value != 6 {
		t.Fatalf("fresh v%d=%+v, snapshot v%d=%+v", fresh.Version(), fresh.State(), fromSnapshot.Version(), fromSnapshot.State())
	}
}

func TestSnapshotRecoveryKeepsOldViewAndFinishesPublishedSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	stream := "device-a"
	store, err := NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := NewFileSnapshotStore[testState](dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, amount := range []int{2, 3} {
		if _, err := store.Append(ctx, stream, int64(i), UnsavedEvent{Type: "add", Data: map[string]int{"amount": amount}}); err != nil {
			t.Fatal(err)
		}
		projector := NewProjector(store, stream, testState{}, applyTestEvent, cloneTestState)
		if err := projector.Replay(ctx); err != nil {
			t.Fatal(err)
		}
		if err := snapshots.Save(ctx, projector.Snapshot()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, stream, 2, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 4}}); err != nil {
		t.Fatal(err)
	}
	dirForStream := snapshots.streamDir(stream)
	v3 := Snapshot[testState]{StreamID: stream, Version: 3, State: testState{Value: 9}}
	payload, _ := json.Marshal(v3)
	if err := os.WriteFile(filepath.Join(dirForStream, snapshotFileName(3)), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirForStream, ".incomplete.tmp"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Close(); err != nil {
		t.Fatal(err)
	}

	snapshots2, err := NewFileSnapshotStore[testState](dir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := snapshots2.Load(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != 3 || loaded.State.Value != 9 {
		t.Fatalf("recovered snapshot = %+v", loaded)
	}
	if _, err := os.Stat(filepath.Join(dirForStream, ".incomplete.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete snapshot was not removed: %v", err)
	}
}

func TestRestoreSwitchIncludesConcurrentAppendExactlyOnce(t *testing.T) {
	ctx := context.Background()
	stream := "device-a"
	store := NewMemoryEventStore()
	if _, err := store.Append(ctx, stream, NoStreamVersion, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 1}}); err != nil {
		t.Fatal(err)
	}
	old := NewProjector(store, stream, testState{}, applyTestEvent, cloneTestState)
	if err := old.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := old.Snapshot()
	if _, err := store.Append(ctx, stream, 1, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 2}}); err != nil {
		t.Fatal(err)
	}
	switcher := NewProjectionSwitch(store, stream, testState{}, cloneTestState)
	switcher.Register("old", testState{}, applyTestEvent)
	switcher.Register("new", testState{}, applyTestEvent)
	if err := switcher.Switch("old"); err != nil {
		t.Fatal(err)
	}
	switchDone := make(chan error, 1)
	go func() {
		switchDone <- switcher.RestoreRebuildAndSwitch(ctx, "new", snapshot)
	}()
	select {
	case err := <-switchDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("switch did not complete")
	}
	name, state, version, err := switcher.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if name != "new" || version != 2 || state.Value != 3 {
		t.Fatalf("current after cut-over=%s version=%d state=%+v", name, version, state)
	}
	if _, err := store.Append(ctx, stream, 2, UnsavedEvent{Type: "add", Data: map[string]int{"amount": 3}}); err != nil {
		t.Fatal(err)
	}
	if err := switcher.RebuildAndSwitch(ctx, "new"); err != nil {
		t.Fatal(err)
	}

	name, state, version, err = switcher.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if name != "new" || version != 3 || state.Value != 6 {
		t.Fatalf("current=%s version=%d state=%+v", name, version, state)
	}
	if err := switcher.RebuildAndSwitch(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	_, state, version, _ = switcher.Current(ctx)
	if version != 3 || state.Value != 6 {
		t.Fatalf("event applied twice: version=%d state=%+v", version, state)
	}
}

func projectorsMatch(expected, actual *Projector[testState]) bool {
	return expected.Version() == actual.Version() && expected.State() == actual.State()
}
