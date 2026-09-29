package replay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"replay-events"
)

const (
	itemOpened   = "inventory.opened"
	itemReceived = "inventory.received"
	itemShipped  = "inventory.shipped"
)

type itemOpenedEvent struct {
	SKU  string    `json:"sku"`
	When time.Time `json:"when"`
}

func (e itemOpenedEvent) EventType() string     { return itemOpened }
func (e itemOpenedEvent) OccurredAt() time.Time { return e.When }

type itemReceivedEvent struct {
	Quantity int `json:"quantity"`
}

func (e itemReceivedEvent) EventType() string { return itemReceived }

type itemShippedEvent struct {
	Quantity int `json:"quantity"`
}

func (e itemShippedEvent) EventType() string { return itemShipped }

type inventoryV1 struct {
	Seq      int64  `json:"seq"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

func (s inventoryV1) Version() int64 { return s.Seq }

type inventoryV2 struct {
	Seq      int64  `json:"seq"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Received int    `json:"received"`
	Shipped  int    `json:"shipped"`
}

func (s inventoryV2) Version() int64 { return s.Seq }

func inventoryCodec() *replay.Codec {
	codec := replay.NewCodec()
	codec.Register(itemOpened, func() any { return &itemOpenedEvent{} })
	codec.Register(itemReceived, func() any { return &itemReceivedEvent{} })
	codec.Register(itemShipped, func() any { return &itemShippedEvent{} })
	return codec
}

func v1Projection() *replay.Projection[inventoryV1] {
	p := replay.NewProjection[inventoryV1]("inventory-v1", inventoryCodec())
	p.On(itemOpened, func(state inventoryV1, envelope replay.Event, payload any) (inventoryV1, error) {
		opened := payload.(*itemOpenedEvent)
		state.Seq = envelope.Sequence
		state.SKU = opened.SKU
		return state, nil
	})
	p.On(itemReceived, func(state inventoryV1, event replay.Event, payload any) (inventoryV1, error) {
		event2 := payload.(*itemReceivedEvent)
		if event2.Quantity <= 0 {
			return state, errors.New("received quantity must be positive")
		}
		state.Quantity += event2.Quantity
		state.Seq = event.Sequence
		return state, nil
	})
	p.On(itemShipped, func(state inventoryV1, event replay.Event, payload any) (inventoryV1, error) {
		event2 := payload.(*itemShippedEvent)
		if event2.Quantity <= 0 {
			return state, errors.New("shipped quantity must be positive")
		}
		if event2.Quantity > state.Quantity {
			return state, errors.New("insufficient inventory")
		}
		state.Quantity -= event2.Quantity
		state.Seq = event.Sequence
		return state, nil
	})
	return p
}

func v2Projection() *replay.Projection[inventoryV2] {
	p := replay.NewProjection[inventoryV2]("inventory-v2", inventoryCodec())
	p.On(itemOpened, func(state inventoryV2, event replay.Event, payload any) (inventoryV2, error) {
		event2 := payload.(*itemOpenedEvent)
		state.SKU = event2.SKU
		state.Seq = event.Sequence
		return state, nil
	})
	p.On(itemReceived, func(state inventoryV2, event replay.Event, payload any) (inventoryV2, error) {
		event2 := payload.(*itemReceivedEvent)
		if event2.Quantity <= 0 {
			return state, errors.New("received quantity must be positive")
		}
		state.Quantity += event2.Quantity
		state.Received += event2.Quantity
		state.Seq = event.Sequence
		return state, nil
	})
	p.On(itemShipped, func(state inventoryV2, event replay.Event, payload any) (inventoryV2, error) {
		event2 := payload.(*itemShippedEvent)
		if event2.Quantity <= 0 || event2.Quantity > state.Quantity {
			return state, errors.New("invalid shipment")
		}
		state.Quantity -= event2.Quantity
		state.Shipped += event2.Quantity
		state.Seq = event.Sequence
		return state, nil
	})
	return p
}

func TestRecoverInventoryFromEvents(t *testing.T) {
	ctx := context.Background()
	store := replay.NewTypedEventStore(replay.NewMemoryEventStore(), inventoryCodec())
	if _, err := store.Append(ctx, "sku-1", replay.AnyVersion,
		itemOpenedEvent{SKU: "sku-1"},
		itemReceivedEvent{Quantity: 10},
		itemShippedEvent{Quantity: 3},
	); err != nil {
		t.Fatal(err)
	}

	runner := replay.NewRunner(store, nil, v1Projection(), func() inventoryV1 { return inventoryV1{} })
	state, err := runner.Restore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.SKU != "sku-1" || state.Quantity != 7 || state.Version() != 3 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestConcurrentSubmitChecksExpectedVersion(t *testing.T) {
	ctx := context.Background()
	store := replay.NewTypedEventStore(replay.NewMemoryEventStore(), inventoryCodec())
	events, err := store.Append(ctx, "sku-1", replay.AnyVersion, itemOpenedEvent{SKU: "sku-1"})
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		err error
	}
	done := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := store.Append(ctx, "sku-1", events[len(events)-1].StreamVersion, itemReceivedEvent{Quantity: 1})
			done <- result{err: err}
		}()
	}

	conflicts := 0
	for range 2 {
		result := <-done
		if result.err != nil {
			if !replay.IsConcurrencyError(result.err) {
				t.Fatalf("expected concurrency error, got %v", result.err)
			}
			conflicts++
		}
	}
	if conflicts != 1 {
		t.Fatalf("expected exactly one conflict, got %d", conflicts)
	}
}

func TestProjectionErrorStopsAtBadEvent(t *testing.T) {
	ctx := context.Background()
	store := replay.NewTypedEventStore(replay.NewMemoryEventStore(), inventoryCodec())
	_, err := store.Append(ctx, "sku-1", replay.AnyVersion,
		itemOpenedEvent{SKU: "sku-1"},
		itemReceivedEvent{Quantity: 5},
		itemShippedEvent{Quantity: 6},
		itemReceivedEvent{Quantity: 2},
	)
	if err != nil {
		t.Fatal(err)
	}

	runner := replay.NewRunner(store, nil, v1Projection(), func() inventoryV1 { return inventoryV1{} })
	state, err := runner.Restore(ctx)
	if err == nil {
		t.Fatal("expected projection error")
	}
	if state.Version() != 2 {
		t.Fatalf("bad event must not be applied: %+v", state)
	}
}

func TestRebuildProjectionKeepsOldQueryableUntilPromotion(t *testing.T) {
	ctx := context.Background()
	raw := replay.NewMemoryEventStore()
	store := replay.NewTypedEventStore(raw, inventoryCodec())
	_, err := store.Append(ctx, "sku-1", replay.AnyVersion,
		itemOpenedEvent{SKU: "sku-1"},
		itemReceivedEvent{Quantity: 10},
		itemShippedEvent{Quantity: 4},
	)
	if err != nil {
		t.Fatal(err)
	}

	oldRunner := replay.NewRunner(store, nil, v1Projection(), func() inventoryV1 { return inventoryV1{} })
	oldState, err := oldRunner.Restore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	newRunner := replay.NewRunner(store, nil, v2Projection(), func() inventoryV2 { return inventoryV2{} })
	newState, err := newRunner.Restore(ctx)
	if err != nil {
		t.Fatal(err)
	}

	manager := replay.NewManager("v1", oldState)
	if manager.Active() != "v1" {
		t.Fatal("old projection must initially be active")
	}
	manager.Stage("v2", newState)
	if active := manager.QueryActive().(inventoryV1); active.Quantity != 6 {
		t.Fatalf("old projection changed during rebuild: %+v", active)
	}
	if staged, ok := manager.Query("v2"); !ok || staged.(inventoryV2).Received != 10 || staged.(inventoryV2).Shipped != 4 {
		t.Fatalf("new projection unavailable for verification: %+v %v", staged, ok)
	}
	if err := manager.Promote("v2"); err != nil {
		t.Fatal(err)
	}
	if active := manager.QueryActive().(inventoryV2); active.Quantity != 6 || active.Received != 10 || active.Shipped != 4 {
		t.Fatalf("promoted projection mismatch: %+v", active)
	}
}

func TestSnapshotThenReplayAppliesEachEventOnce(t *testing.T) {
	ctx := context.Background()
	raw := replay.NewMemoryEventStore()
	store := replay.NewTypedEventStore(raw, inventoryCodec())
	_, err := store.Append(ctx, "sku-1", replay.AnyVersion,
		itemOpenedEvent{SKU: "sku-1"},
		itemReceivedEvent{Quantity: 8},
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := replay.NewMemorySnapshotStore()
	projection := v1Projection()
	runner := replay.NewRunner(store, snapshots, projection, func() inventoryV1 { return inventoryV1{} })
	state, err := runner.Restore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.SaveSnapshot(ctx, state); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "sku-1", 2,
		itemShippedEvent{Quantity: 3},
		itemReceivedEvent{Quantity: 4},
	); err != nil {
		t.Fatal(err)
	}

	restored, err := runner.Restore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Version() != 4 || restored.Quantity != 9 {
		t.Fatalf("snapshot replay double-applied or skipped events: %+v", restored)
	}
}

func TestSmallModelReconcilesReplayAndOnline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	raw := replay.NewMemoryEventStore()
	store := replay.NewTypedEventStore(raw, inventoryCodec())
	manager := replay.NewManager("v1", inventoryV1{})
	replay.RegisterProjection(manager, "v1", inventoryV1{}, v1Projection())
	replay.RegisterProjection(manager, "v2", inventoryV2{}, v2Projection())

	runErr := make(chan error, 1)
	go func() { runErr <- manager.Run(ctx, raw) }()

	if _, err := store.Append(ctx, "sku-1", replay.AnyVersion,
		itemOpenedEvent{SKU: "sku-1"},
		itemReceivedEvent{Quantity: 10},
	); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		online, ok := manager.Query("v2")
		return ok && online.(inventoryV2).Version() == 2
	})

	if err := manager.Promote("v2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "sku-1", 2,
		itemShippedEvent{Quantity: 4},
		itemReceivedEvent{Quantity: 2},
	); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		return manager.QueryActive().(inventoryV2).Version() == 4
	})
	replayed, err := replay.NewRunner(store, nil, v2Projection(), func() inventoryV2 { return inventoryV2{} }).Restore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	online := manager.QueryActive().(inventoryV2)
	if online != replayed {
		t.Fatalf("online/replay mismatch\nonline:  %+v\nreplayed: %+v", online, replayed)
	}
	if old, ok := manager.Query("v1"); !ok || old.(inventoryV1).Version() != 4 || old.(inventoryV1).Quantity != 8 {
		t.Fatalf("old projection must remain queryable after switch: %+v %v", old, ok)
	}

	cancel()
	<-runErr
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached")
}
