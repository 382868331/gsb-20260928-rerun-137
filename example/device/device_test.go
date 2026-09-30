package device_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"replayable-app"
	"replayable-app/example/device"
)

type failingSnapshotStore[S any] struct {
	inner replay.SnapshotStore[S]
	fail  bool
}

func (s *failingSnapshotStore[S]) Save(ctx context.Context, name string, snapshot replay.Snapshot[S]) error {
	if s.fail {
		s.fail = false
		return os.ErrPermission
	}
	return s.inner.Save(ctx, name, snapshot)
}

func (s *failingSnapshotStore[S]) Load(ctx context.Context, name string) (replay.Snapshot[S], error) {
	return s.inner.Load(ctx, name)
}

type failingCurrentStore struct {
	inner replay.CurrentStore
	fail  bool
}

func (s *failingCurrentStore) SaveCurrent(ctx context.Context, current replay.CurrentProjection) error {
	if s.fail {
		s.fail = false
		return os.ErrPermission
	}
	return s.inner.SaveCurrent(ctx, current)
}

func (s *failingCurrentStore) LoadCurrent(ctx context.Context) (replay.CurrentProjection, error) {
	return s.inner.LoadCurrent(ctx)
}

func TestPersistentDeviceProjectionRestartsThroughAllStages(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	eventPath := filepath.Join(directory, "events.jsonl")
	snapshotDir := filepath.Join(directory, "snapshots")
	currentPath := filepath.Join(directory, "current.json")
	streamID := "device:gateway-01"

	store, err := replay.NewFileEventStore(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: device.EventRegistered, Data: device.Registered{DeviceID: "gateway-01", Model: "GW-2"}},
		replay.UnsavedEvent{Type: device.EventStatusReported, Data: device.StatusReported{DeviceID: "gateway-01", Online: true, SignalDBM: -62}},
	); err != nil {
		t.Fatal(err)
	}

	store, err = replay.NewFileEventStore(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	if version, err := store.StreamVersion(ctx, streamID); err != nil || version != 2 {
		t.Fatalf("after append restart version = %d, err = %v", version, err)
	}
	if _, err := store.Append(ctx, streamID, 2,
		replay.UnsavedEvent{Type: device.EventFirmwareUpdated, Data: device.FirmwareUpdated{DeviceID: "gateway-01", Firmware: "1.4.2"}},
		replay.UnsavedEvent{Type: device.EventStatusReported, Data: device.StatusReported{DeviceID: "gateway-01", Online: true, SignalDBM: -55}},
	); err != nil {
		t.Fatal(err)
	}

	snapshots := replay.NewFileSnapshotStore[device.State](snapshotDir)
	current := replay.NewFileCurrentStore(currentPath)
	switcher := newDeviceSwitcher(store, streamID).
		WithSnapshotStore(snapshots).
		WithCurrentStore(current)
	v1 := switcher.Register("v1", device.NewState(), oldDeviceProjection)
	v2 := switcher.Register("v2", device.NewState(), device.Apply)
	if err := v1.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Switch(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := switcher.PublishSnapshot(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Rebuild(ctx, "v2"); err != nil {
		t.Fatal(err)
	}

	failingSnapshots := &failingSnapshotStore[device.State]{inner: snapshots, fail: true}
	interrupted := newDeviceSwitcher(store, streamID).WithSnapshotStore(failingSnapshots)
	interrupted.Register("v2", device.NewState(), device.Apply)
	if err := interrupted.Rebuild(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	if err := interrupted.PublishSnapshot(ctx, "v2"); err == nil {
		t.Fatal("expected interrupted snapshot publish")
	}
	if _, err := os.Stat(filepath.Join(snapshotDir, "v2.json")); !os.IsNotExist(err) {
		t.Fatalf("interrupted snapshot became visible, stat err = %v", err)
	}
	if err := switcher.PublishSnapshot(ctx, "v2"); err != nil {
		t.Fatal(err)
	}

	fresh := replay.NewProjector(store, streamID, device.NewState(), device.Apply, device.CloneState)
	if err := fresh.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	fromSnapshot := replay.NewProjector(store, streamID, device.NewState(), device.Apply, device.CloneState)
	if err := fromSnapshot.Restore(snapshotOrFatal(t, ctx, snapshots, "v2")); err != nil {
		t.Fatal(err)
	}
	if err := fromSnapshot.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := (replay.TinyStateModel[device.State]{}).Verify(fresh.Version(), v2.Version(), fresh.State(), v2.State()); err != nil {
		t.Fatal(err)
	}
	if err := (replay.TinyStateModel[device.State]{}).Verify(fresh.Version(), fromSnapshot.Version(), fresh.State(), fromSnapshot.State()); err != nil {
		t.Fatal(err)
	}

	failingCurrent := &failingCurrentStore{inner: current, fail: true}
	switcher.WithCurrentStore(failingCurrent)
	if err := switcher.Switch(ctx, "v2"); err == nil {
		t.Fatal("expected interrupted switch marker")
	}
	if name, state, version, err := switcher.Current(ctx); err != nil || name != "v1" || state.Devices["gateway-01"].Firmware != "" || version != 4 {
		t.Fatalf("current after failed switch = %q %+v %d %v", name, state, version, err)
	}
	if persisted, err := current.LoadCurrent(ctx); err != nil || persisted.Name != "v1" || persisted.Version != 4 {
		t.Fatalf("persisted current = %+v, err = %v", persisted, err)
	}

	if _, err := store.Append(ctx, streamID, 4,
		replay.UnsavedEvent{Type: device.EventStatusReported, Data: device.StatusReported{DeviceID: "gateway-01", Online: false, SignalDBM: -80}},
	); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Switch(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	if name, state, version, err := switcher.Current(ctx); err != nil || name != "v2" || version != 5 || state.Devices["gateway-01"].Online || state.Devices["gateway-01"].Signal != -80 {
		t.Fatalf("current = %q %+v %d %v", name, state, version, err)
	}
	if persisted, err := current.LoadCurrent(ctx); err != nil || persisted.Name != "v2" || persisted.Version != 5 {
		t.Fatalf("persisted switched current = %+v, err = %v", persisted, err)
	}

	store, err = replay.NewFileEventStore(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	restarted := newDeviceSwitcher(store, streamID).
		WithSnapshotStore(snapshots).
		WithCurrentStore(current)
	restarted.Register("v1", device.NewState(), oldDeviceProjection)
	restarted.Register("v2", device.NewState(), device.Apply)
	if err := restarted.Restore(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Restore(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.RestoreCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.CatchUp(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	if name, state, version, err := restarted.Current(ctx); err != nil || name != "v2" || version != 5 || state.Devices["gateway-01"].Signal != -80 {
		t.Fatalf("restarted current = %q %+v %d %v", name, state, version, err)
	}
}

func TestFailedDeviceProjectionKeepsCheckpointReasonAndOldView(t *testing.T) {
	ctx := context.Background()
	store := replay.NewMemoryEventStore()
	streamID := "device:bad"
	if _, err := store.Append(ctx, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: device.EventRegistered, Data: device.Registered{DeviceID: "sensor-1", Model: "S-9"}},
		replay.UnsavedEvent{Type: device.EventStatusReported, Data: device.StatusReported{DeviceID: "sensor-1", Online: true, SignalDBM: -70}},
		replay.UnsavedEvent{Type: device.EventFirmwareUpdated, Data: device.FirmwareUpdated{DeviceID: "sensor-1", Firmware: ""}},
	); err != nil {
		t.Fatal(err)
	}

	switcher := newDeviceSwitcher(store, streamID)
	v1 := switcher.Register("v1", device.NewState(), oldDeviceProjection)
	v2Projector := switcher.Register("v2", device.NewState(), device.Apply)
	if err := v1.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Switch(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Rebuild(ctx, "v2"); err == nil {
		t.Fatal("expected bad firmware event to stop projection")
	}
	firstFailure, ok := switcher.Failure("v2")
	if !ok || firstFailure.Version != 3 || firstFailure.Position != 3 || !strings.Contains(firstFailure.Reason, "firmware is empty") {
		t.Fatalf("unexpected failure %+v ok=%v", firstFailure, ok)
	}
	if version := v2Projector.Version(); version != 2 {
		t.Fatalf("failed projection version = %d, want 2", version)
	}
	if err := switcher.Rebuild(ctx, "v2"); err == nil {
		t.Fatal("second replay must retry the blocking event")
	}
	if secondFailure, ok := switcher.Failure("v2"); !ok || secondFailure != firstFailure {
		t.Fatalf("failure changed: first %+v second %+v", firstFailure, secondFailure)
	}
	if err := switcher.Switch(ctx, "v2"); err == nil {
		t.Fatal("failed projection must not replace current view")
	}
	if name, _, version, err := switcher.Current(ctx); err != nil || name != "v1" || version != 3 {
		t.Fatalf("current after failed switch = %q version %d err %v", name, version, err)
	}
}

func newDeviceSwitcher(store replay.EventStore, streamID string) *replay.ProjectionSwitch[device.State] {
	return replay.NewProjectionSwitch(store, streamID, device.NewState(), device.CloneState)
}

func oldDeviceProjection(state *device.State, event replay.RecordedEvent) error {
	if event.Type == device.EventFirmwareUpdated {
		return nil
	}
	return device.Apply(state, event)
}

func snapshotOrFatal[S any](t *testing.T, ctx context.Context, store replay.SnapshotStore[S], name string) replay.Snapshot[S] {
	t.Helper()
	snapshot, err := store.Load(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
