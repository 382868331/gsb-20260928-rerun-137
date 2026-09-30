package replay

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFileEventStoreRecoversConfirmedEventAndDropsTornAppend(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store, err := NewFileEventStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "device:gateway-1", NoStreamVersion, UnsavedEvent{
		Type: "device.registered",
		Data: map[string]string{"device_id": "gateway-1", "model": "GW-2"},
	}); err != nil {
		t.Fatal(err)
	}
	tornFile, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tornFile.WriteString(`{"id":2,"position":2,"stream_id":"device:gateway-1"`); err != nil {
		tornFile.Close()
		t.Fatal(err)
	}
	if err := tornFile.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewFileEventStore(path)
	if err != nil {
		t.Fatal(err)
	}
	version, err := store.StreamVersion(ctx, "device:gateway-1")
	if err != nil || version != 1 {
		t.Fatalf("version = %d, err = %v", version, err)
	}
	events, err := store.ReadStream(ctx, "device:gateway-1")
	if err != nil || len(events) != 1 || events[0].Position != 1 {
		t.Fatalf("events = %+v, err = %v", events, err)
	}
	if _, err := store.Append(ctx, "device:gateway-1", 1, UnsavedEvent{
		Type: "device.status_reported",
		Data: map[string]any{"device_id": "gateway-1", "online": true},
	}); err != nil {
		t.Fatal(err)
	}
	events, err = store.ReadStream(ctx, "device:gateway-1")
	if err != nil || len(events) != 2 || events[1].Position != 2 || events[1].Version != 2 {
		t.Fatalf("events after restarted append = %+v, err = %v", events, err)
	}
}
