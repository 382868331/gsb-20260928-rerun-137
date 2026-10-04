package replay_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"replayable-app"
	"replayable-app/example/shop"
)

func appendEvent(t *testing.T, store replay.EventStore, streamID string, expected int64, events ...replay.UnsavedEvent) []replay.RecordedEvent {
	t.Helper()
	recorded, err := store.Append(context.Background(), streamID, expected, events...)
	if err != nil {
		t.Fatalf("append to %q: %v", streamID, err)
	}
	return recorded
}

func addTea() replay.UnsavedEvent {
	return replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Name: "Cloud Mist Tea", Quantity: 5}}
}

func TestFileStoreRecoversConfirmedEventsAfterReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-a"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, store, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, store, streamID, 1,
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-1", Items: map[string]int{"tea": 2}}},
		replay.UnsavedEvent{Type: shop.EventOrderPaid, Data: shop.OrderPaid{OrderID: "o-1"}},
	)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	projector := replay.NewProjector(reopened, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := projector.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if projector.Version() != 3 {
		t.Fatalf("version = %d, want 3", projector.Version())
	}
	state := projector.State()
	if state.Stock["tea"] != 3 || state.Revenue != 20 || state.Orders["o-1"].Status != "paid" {
		t.Fatalf("unexpected recovered state: %+v", state)
	}

	// Appends after reopen must continue the confirmed version chain.
	appendEvent(t, reopened, streamID, 3, addTea())
	if version, err := reopened.StreamVersion(ctx, streamID); err != nil || version != 4 {
		t.Fatalf("version = %d, err = %v", version, err)
	}
	if _, err := reopened.Append(ctx, streamID, 3, addTea()); err == nil {
		t.Fatal("expected concurrency error for stale expected version")
	} else if _, ok := err.(replay.ConcurrencyError); !ok {
		t.Fatalf("error = %T, want ConcurrencyError", err)
	}
}

func TestFileStoreIsolatesIncompleteTail(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-tail"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, store, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, store, streamID, 1, addTea())
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: a partial frame sits at the end of the log.
	logPath := filepath.Join(dir, "events.log")
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0x00, 0x00, 0x00, 0x01, 0xFF}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	version, err := reopened.StreamVersion(ctx, streamID)
	if err != nil || version != 2 {
		t.Fatalf("version = %d, err = %v; incomplete tail must be isolated", version, err)
	}
	events, err := reopened.ReadStream(ctx, streamID)
	if err != nil || len(events) != 2 {
		t.Fatalf("events = %d, err = %v", len(events), err)
	}

	// The log must still accept new confirmed events after truncation.
	appendEvent(t, reopened, streamID, 2, addTea())
	if version, _ := reopened.StreamVersion(ctx, streamID); version != 3 {
		t.Fatalf("version = %d, want 3", version)
	}
}

// corruptLastFrameByte flips one byte inside the payload of the last frame.
func corruptLastFrameByte(t *testing.T, logPath string) {
	t.Helper()
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xAB}, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreReportsCorruptConfirmedRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-corrupt"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, store, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, store, streamID, 1, addTea())
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Damage the second, already confirmed record.
	logPath := filepath.Join(dir, "events.log")
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	originalSize := info.Size()
	corruptLastFrameByte(t, logPath)

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// The confirmed prefix is served, but the view must say it is degraded.
	events, view, err := reopened.ReadStreamView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want confirmed prefix of 1", len(events))
	}
	if view.Kind != replay.ViewDegraded {
		t.Fatalf("view = %q, want %q", view.Kind, replay.ViewDegraded)
	}
	if view.Corruption == nil || view.Corruption.PreservedBytes <= 0 {
		t.Fatalf("corruption report missing: %+v", view.Corruption)
	}

	// The damaged file is preserved for the operator, not truncated away.
	if info, err := os.Stat(logPath); err != nil || info.Size() != originalSize {
		t.Fatalf("original log changed: size %v, err %v", info, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "events.corruption.json")); err != nil {
		t.Fatalf("persisted corruption report missing: %v", err)
	}

	// Appends are blocked while confirmed content is corrupt.
	if _, err := reopened.Append(ctx, streamID, 1, addTea()); err == nil {
		t.Fatal("expected append to be blocked on corrupt confirmed history")
	} else if _, ok := err.(replay.CorruptedHistoryError); !ok {
		t.Fatalf("error = %T, want CorruptedHistoryError", err)
	}
}

func TestSnapshotStoreKeepsOldSnapshotAcrossInterruptedSave(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-snap"
	snapshots := replay.NewFileSnapshotStore[shop.State](dir)

	first := replay.Snapshot[shop.State]{StreamID: streamID, Version: 2, State: shop.State{Revenue: 10}}
	if err := snapshots.Save(ctx, first); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash while writing the second snapshot: only the temp file exists.
	tmpPath := filepath.Join(dir, "snapshot-c2hvcDpkZXZpY2Utc25hcA.json.tmp")
	if err := os.WriteFile(tmpPath, []byte("{\"snapshot\":"), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, ok, err := snapshots.Load(ctx, streamID)
	if err != nil || !ok {
		t.Fatalf("load = ok %v err %v", ok, err)
	}
	if loaded.Version != 2 || loaded.State.Revenue != 10 {
		t.Fatalf("loaded %+v, want first snapshot", loaded)
	}

	second := replay.Snapshot[shop.State]{StreamID: streamID, Version: 5, State: shop.State{Revenue: 40}}
	if err := snapshots.Save(ctx, second); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err = snapshots.Load(ctx, streamID)
	if err != nil || !ok || loaded.Version != 5 || loaded.State.Revenue != 40 {
		t.Fatalf("loaded %+v ok %v err %v, want second snapshot", loaded, ok, err)
	}

	if _, ok, err := snapshots.Load(ctx, "shop:unknown"); err != nil || ok {
		t.Fatalf("unknown stream load = ok %v err %v", ok, err)
	}
}

func TestCutoverCatchesUpTailUnderAppendLock(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-cutover"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	appendEvent(t, store, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, store, streamID, 1,
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-1", Items: map[string]int{"tea": 2}}},
		replay.UnsavedEvent{Type: shop.EventOrderPaid, Data: shop.OrderPaid{OrderID: "o-1"}},
	)

	snapshots := replay.NewFileSnapshotStore[shop.State](dir)
	online := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := online.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Save(ctx, online.Snapshot()); err != nil {
		t.Fatal(err)
	}

	switcher := replay.NewProjectionSwitch(store, streamID, shop.NewState(), shop.CloneState)
	switcher.Register("v1", shop.NewState(), shop.Apply)
	if err := switcher.Rebuild(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Switch("v1"); err != nil {
		t.Fatal(err)
	}

	// New projection starts from the persisted snapshot and catches up.
	snapshot, ok, err := snapshots.Load(ctx, streamID)
	if err != nil || !ok {
		t.Fatalf("load snapshot ok %v err %v", ok, err)
	}
	switcher.Register("v2", shop.NewState(), shop.Apply)
	if err := switcher.RestoreSnapshot("v2", snapshot); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Rebuild(ctx, "v2"); err != nil {
		t.Fatal(err)
	}

	// Events keep arriving while v2 catches up and cuts over.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			version, err := store.StreamVersion(ctx, streamID)
			if err != nil {
				return
			}
			if _, err := store.Append(ctx, streamID, version, addTea()); err != nil {
				if _, ok := err.(replay.ConcurrencyError); !ok {
					return
				}
			}
		}
	}()

	if err := switcher.Cutover(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	// The appender may have committed events after the cut-over; with no
	// concurrent writers left, one more catch-up brings v2 to the final tail.
	if err := switcher.Rebuild(ctx, "v2"); err != nil {
		t.Fatal(err)
	}

	name, current, currentVersion, err := switcher.Current(ctx)
	if err != nil || name != "v2" {
		t.Fatalf("current = %q err %v", name, err)
	}
	streamVersion, err := store.StreamVersion(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}

	// The cut-over view must match a full replay of every confirmed event.
	fresh := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := fresh.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := (replay.TinyStateModel[shop.State]{}).Verify(fresh.Version(), currentVersion, fresh.State(), current); err != nil {
		t.Fatal(err)
	}
	if currentVersion < streamVersion {
		t.Fatalf("current version %d behind stream version %d", currentVersion, streamVersion)
	}
}

func TestMultipleDevicesMatchFullReplayAfterReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	devices := []string{"shop:device-1", "shop:device-2", "shop:device-3"}

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	live := make(map[string]*replay.Projector[shop.State])
	versions := make(map[string]int64)
	for _, device := range devices {
		live[device] = replay.NewProjector(store, device, shop.NewState(), shop.Apply, shop.CloneState)
	}
	// Interleave appends across devices, keeping each projector online.
	for round := 0; round < 4; round++ {
		for _, device := range devices {
			recorded := appendEvent(t, store, device, versions[device], addTea())
			if err := live[device].Apply(recorded); err != nil {
				t.Fatal(err)
			}
			versions[device] += int64(len(recorded))
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	model := replay.TinyStateModel[shop.State]{}
	for _, device := range devices {
		replayed := replay.NewProjector(reopened, device, shop.NewState(), shop.Apply, shop.CloneState)
		if err := replayed.Replay(ctx); err != nil {
			t.Fatal(err)
		}
		if err := model.Verify(live[device].Version(), replayed.Version(), live[device].State(), replayed.State()); err != nil {
			t.Fatalf("device %q: %v", device, err)
		}
		if replayed.Version() != 4 || replayed.State().Stock["tea"] != 20 {
			t.Fatalf("device %q state %+v version %d", device, replayed.State(), replayed.Version())
		}
	}
}

func TestSnapshotRestoreThenReplayMatchesFullReplayAfterReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-snapshot-reopen"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, store, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, store, streamID, 1,
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-1", Items: map[string]int{"tea": 1}}},
		replay.UnsavedEvent{Type: shop.EventOrderPaid, Data: shop.OrderPaid{OrderID: "o-1"}},
	)

	snapshots := replay.NewFileSnapshotStore[shop.State](dir)
	online := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := online.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.Save(ctx, online.Snapshot()); err != nil {
		t.Fatal(err)
	}
	appendEvent(t, store, streamID, 3, addTea())
	if err := online.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	snapshot, ok, err := snapshots.Load(ctx, streamID)
	if err != nil || !ok {
		t.Fatalf("load snapshot ok %v err %v", ok, err)
	}
	fromSnapshot := replay.NewProjector(reopened, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := fromSnapshot.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := fromSnapshot.Replay(ctx); err != nil {
		t.Fatal(err)
	}

	fresh := replay.NewProjector(reopened, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := fresh.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	model := replay.TinyStateModel[shop.State]{}
	if err := model.Verify(fresh.Version(), fromSnapshot.Version(), fresh.State(), fromSnapshot.State()); err != nil {
		t.Fatal(err)
	}
	if err := model.Verify(online.Version(), fromSnapshot.Version(), online.State(), fromSnapshot.State()); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreRoundTripsEventPayloads(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-payload"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	recorded := appendEvent(t, store, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-9", Items: map[string]int{"tea": 1, "cup": 2}}},
	)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	events, err := reopened.ReadStream(ctx, streamID)
	if err != nil || len(events) != 1 {
		t.Fatalf("events = %d, err = %v", len(events), err)
	}
	if events[0].ID != recorded[0].ID || events[0].Version != 1 || events[0].Type != shop.EventOrderPlaced {
		t.Fatalf("unexpected event %+v", events[0])
	}
	var payload shop.OrderPlaced
	if err := json.Unmarshal(events[0].Data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.OrderID != "o-9" || payload.Items["cup"] != 2 {
		t.Fatalf("unexpected payload %+v", payload)
	}
}
