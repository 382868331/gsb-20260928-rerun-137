package shop_test

import (
	"context"
	"sync"
	"testing"

	"replayable-app"
	"replayable-app/example/shop"
)

func TestReplayRecoversFictionalBusinessState(t *testing.T) {
	ctx := context.Background()
	store := replay.NewMemoryEventStore()
	streamID := "shop:cloud-mist"

	recorded, err := store.Append(ctx, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Name: "Cloud Mist Tea", Quantity: 10}},
	)
	if err != nil {
		t.Fatal(err)
	}
	version := int64(len(recorded))
	if _, err := store.Append(ctx, streamID, version,
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-1", Items: map[string]int{"tea": 2}}},
		replay.UnsavedEvent{Type: shop.EventOrderPaid, Data: shop.OrderPaid{OrderID: "o-1"}},
	); err != nil {
		t.Fatal(err)
	}

	projector := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := projector.Replay(ctx); err != nil {
		t.Fatal(err)
	}

	state := projector.State()
	if projector.Version() != 3 {
		t.Fatalf("version = %d", projector.Version())
	}
	if state.Stock["tea"] != 8 || state.Revenue != 20 {
		t.Fatalf("unexpected recovered state: %+v", state)
	}
	if order := state.Orders["o-1"]; order.Status != "paid" {
		t.Fatalf("order status = %q", order.Status)
	}
}

func TestConcurrentAppendChecksExpectedVersion(t *testing.T) {
	ctx := context.Background()
	store := replay.NewMemoryEventStore()
	streamID := "shop:version"
	if _, err := store.Append(ctx, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Quantity: 1}},
	); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errors := make(chan error, 2)
	var wait sync.WaitGroup
	appendOnce := func() {
		defer wait.Done()
		<-start
		_, err := store.Append(ctx, streamID, 1,
			replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Quantity: 1}},
		)
		errors <- err
	}
	wait.Add(2)
	go appendOnce()
	go appendOnce()
	close(start)
	wait.Wait()
	close(errors)

	successes, failures := 0, 0
	for err := range errors {
		if err == nil {
			successes++
		} else {
			failures++
			if _, ok := err.(replay.ConcurrencyError); !ok {
				t.Fatalf("error = %T, want ConcurrencyError", err)
			}
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("successes = %d, failures = %d", successes, failures)
	}
	currentVersion, err := store.StreamVersion(ctx, streamID)
	if err != nil || currentVersion != 2 {
		t.Fatalf("version = %d, err = %v", currentVersion, err)
	}
}

func TestProjectionStopsAtBadEventAndDoesNotReportSuccess(t *testing.T) {
	ctx := context.Background()
	store := replay.NewMemoryEventStore()
	streamID := "shop:bad"
	if _, err := store.Append(ctx, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Quantity: 5}},
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-1", Items: map[string]int{"tea": 1}}},
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-2", Items: map[string]int{"tea": 99}}},
	); err != nil {
		t.Fatal(err)
	}

	projector := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	err := projector.Replay(ctx)
	if err == nil {
		t.Fatal("expected bad event error")
	}
	if projector.Version() != 2 {
		t.Fatalf("version = %d, want 2", projector.Version())
	}
	if state := projector.State(); state.Stock["tea"] != 4 || len(state.Orders) != 1 {
		t.Fatalf("bad event changed state: %+v", state)
	}
}

func TestSnapshotReplaysOnlyNewEventsWithoutSkippingOrDoubleCounting(t *testing.T) {
	ctx := context.Background()
	store := replay.NewMemoryEventStore()
	streamID := "shop:snapshot"
	if _, err := store.Append(ctx, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Quantity: 5}},
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-1", Items: map[string]int{"tea": 2}}},
		replay.UnsavedEvent{Type: shop.EventOrderPaid, Data: shop.OrderPaid{OrderID: "o-1"}},
	); err != nil {
		t.Fatal(err)
	}

	online := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := online.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := online.Snapshot()

	if _, err := store.Append(ctx, streamID, snapshot.Version,
		replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Quantity: 3}},
	); err != nil {
		t.Fatal(err)
	}
	if err := online.Replay(ctx); err != nil {
		t.Fatal(err)
	}

	fromSnapshot := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := fromSnapshot.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := fromSnapshot.Replay(ctx); err != nil {
		t.Fatal(err)
	}

	fresh := replay.NewProjector(store, streamID, shop.NewState(), shop.Apply, shop.CloneState)
	if err := fresh.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := (replay.TinyStateModel[shop.State]{}).Verify(online.Version(), fromSnapshot.Version(), online.State(), fromSnapshot.State()); err != nil {
		t.Fatal(err)
	}
	if err := (replay.TinyStateModel[shop.State]{}).Verify(online.Version(), fresh.Version(), online.State(), fresh.State()); err != nil {
		t.Fatal(err)
	}
	if online.State().Stock["tea"] != 6 || online.Version() != 4 {
		t.Fatalf("unexpected state %+v version %d", online.State(), online.Version())
	}
}

func TestNewProjectionRebuildsWhileOldProjectionRemainsQueryable(t *testing.T) {
	ctx := context.Background()
	store := replay.NewMemoryEventStore()
	streamID := "shop:projection-switch"
	if _, err := store.Append(ctx, streamID, replay.NoStreamVersion,
		replay.UnsavedEvent{Type: shop.EventItemAdded, Data: shop.ItemAdded{SKU: "tea", Quantity: 3}},
		replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-1", Items: map[string]int{"tea": 1}}},
		replay.UnsavedEvent{Type: shop.EventOrderPaid, Data: shop.OrderPaid{OrderID: "o-1"}},
	); err != nil {
		t.Fatal(err)
	}

	switcher := replay.NewProjectionSwitch(store, streamID, shop.NewState(), shop.CloneState)
	old := switcher.Register("v1", shop.NewState(), func(state *shop.State, event replay.RecordedEvent) error {
		if event.Type == shop.EventOrderPaid {
			return nil
		}
		return shop.Apply(state, event)
	})
	switcher.Register("v2", shop.NewState(), shop.Apply)
	if err := old.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Switch("v1"); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Rebuild(ctx, "v2"); err != nil {
		t.Fatal(err)
	}

	name, currentState, currentVersion, err := switcher.Current(ctx)
	if err != nil || name != "v1" || currentState.Revenue != 0 || currentVersion != 3 {
		t.Fatalf("current = %q state %+v version %d err %v", name, currentState, currentVersion, err)
	}
	newState, newVersion, err := switcher.Query("v2")
	if err != nil || newState.Revenue != 10 || newVersion != 3 {
		t.Fatalf("v2 state %+v version %d err %v", newState, newVersion, err)
	}
	if err := switcher.Switch("v2"); err != nil {
		t.Fatal(err)
	}
	name, currentState, _, err = switcher.Current(ctx)
	if err != nil || name != "v2" || currentState.Revenue != 10 {
		t.Fatalf("after switch current = %q state %+v err %v", name, currentState, err)
	}
}
