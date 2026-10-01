package device_test

import (
	"context"
	"testing"

	"replayable-app"
	"replayable-app/example/device"
)

func TestMultipleDevicesReopenSnapshotAndCatchUp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := replay.NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := replay.NewFileSnapshotStore[device.State](dir)
	if err != nil {
		t.Fatal(err)
	}

	devices := []string{"device-a", "device-b"}
	append := func(stream string, expected int64, event replay.UnsavedEvent) {
		t.Helper()
		if _, err := store.Append(ctx, stream, expected, event); err != nil {
			t.Fatal(err)
		}
	}
	for _, deviceID := range devices {
		append(deviceID, replay.NoStreamVersion, replay.UnsavedEvent{Type: device.EventRegistered, Data: device.Registered{Name: deviceID}})
	}
	append("device-a", 1, replay.UnsavedEvent{Type: device.EventOnlineChanged, Data: device.OnlineChanged{Online: true}})
	append("device-b", 1, replay.UnsavedEvent{Type: device.EventOnlineChanged, Data: device.OnlineChanged{Online: true}})
	projectA := replay.NewProjector(store, "device-a", device.NewState(), device.Apply, device.CloneState)
	if err := projectA.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	projectB := replay.NewProjector(store, "device-b", device.NewState(), device.Apply, device.CloneState)
	if err := projectB.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Save(ctx, projectA.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Save(ctx, projectB.Snapshot()); err != nil {
		t.Fatal(err)
	}

	append("device-a", 2, replay.UnsavedEvent{Type: device.EventReported, Data: device.Reported{Reading: 10}})
	append("device-b", 2, replay.UnsavedEvent{Type: device.EventSettingsApplied, Data: device.SettingsApplied{Mode: "fast", Interval: 5}})
	append("device-a", 3, replay.UnsavedEvent{Type: device.EventSettingsApplied, Data: device.SettingsApplied{Mode: "eco", Interval: 30}})
	append("device-b", 3, replay.UnsavedEvent{Type: device.EventReported, Data: device.Reported{Reading: 20}})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := replay.NewFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	snapshots2, err := replay.NewFileSnapshotStore[device.State](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshots2.Close()

	for _, deviceID := range devices {
		saved, err := snapshots2.Load(ctx, deviceID)
		if err != nil {
			t.Fatal(err)
		}
		fromSnapshot := replay.NewProjector(store2, deviceID, device.NewState(), device.Apply, device.CloneState)
		if err := fromSnapshot.Restore(saved); err != nil {
			t.Fatal(err)
		}
		if err := fromSnapshot.Replay(ctx); err != nil {
			t.Fatal(err)
		}
		full := replay.NewProjector(store2, deviceID, device.NewState(), device.Apply, device.CloneState)
		if err := full.Replay(ctx); err != nil {
			t.Fatal(err)
		}
		if err := (replay.TinyStateModel[device.State]{}).Verify(full.Version(), fromSnapshot.Version(), full.State(), fromSnapshot.State()); err != nil {
			t.Fatal(err)
		}
	}

	a := replay.NewProjector(store2, "device-a", device.NewState(), device.Apply, device.CloneState)
	b := replay.NewProjector(store2, "device-b", device.NewState(), device.Apply, device.CloneState)
	if err := a.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if a.State().Settings.Mode != "eco" || a.State().LatestReading != 10 || a.State().ReportCount != 1 {
		t.Fatalf("device-a = %+v", a.State())
	}
	if b.State().Settings.Mode != "fast" || b.State().LatestReading != 20 || b.State().ReportCount != 1 {
		t.Fatalf("device-b = %+v", b.State())
	}
}
