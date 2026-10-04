package replay_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"replayable-app"
	"replayable-app/example/shop"
)

// writeThreeEvents creates a store, appends three confirmed events and
// returns the dir plus a byte-for-byte saved copy of the log (the trusted
// material an operator saved before the damage).
func writeThreeEvents(t *testing.T, streamID string) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, store, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, store, streamID, 1, addTea())
	appendEvent(t, store, streamID, 2, addTea())
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	material, err := os.ReadFile(filepath.Join(dir, "events.log"))
	if err != nil {
		t.Fatal(err)
	}
	return dir, material
}

func TestRecoveryBranchFromTrustedMaterial(t *testing.T) {
	ctx := context.Background()
	streamID := "shop:device-recover"
	dir, material := writeThreeEvents(t, streamID)

	// The operator saved the trusted material elsewhere, then record 2 was
	// damaged in the original log.
	materialPath := filepath.Join(t.TempDir(), "trusted-copy.log")
	if err := os.WriteFile(materialPath, material, 0o644); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "events.log")
	corruptLastFrameByte(t, logPath) // damages record 3 (the tail record)
	damagedBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if view := store.View(); view.Kind != replay.ViewDegraded {
		t.Fatalf("view = %q, want degraded", view.Kind)
	}

	candidate, err := store.BuildRecoveryCandidate(materialPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.Conflicts) != 0 {
		t.Fatalf("unexpected conflicts: %+v", candidate.Conflicts)
	}
	if len(candidate.Events) != 1 || candidate.Events[0].Version != 3 {
		t.Fatalf("candidate events = %+v, want recovery of version 3", candidate.Events)
	}

	if err := store.CommitRecovery(ctx, candidate); err != nil {
		t.Fatal(err)
	}

	// Reads now come from the recovery branch and must say so.
	events, view, err := store.ReadStreamView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3 after recovery", len(events))
	}
	if view.Kind != replay.ViewRecoveryBranch {
		t.Fatalf("view = %q, want %q", view.Kind, replay.ViewRecoveryBranch)
	}
	if view.Recovery == nil || view.Recovery.OriginalFile != logPath || view.Recovery.MaterialPath != materialPath {
		t.Fatalf("manifest does not record original file and basis: %+v", view.Recovery)
	}
	if view.Corruption == nil {
		t.Fatal("corruption fact must stay visible on the recovery view")
	}

	// The original file is preserved byte-for-byte; appends continue on the
	// branch.
	if current, _ := os.ReadFile(logPath); string(current) != string(damagedBytes) {
		t.Fatal("original log was modified by the recovery")
	}
	appendEvent(t, store, streamID, 3, addTea())
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: original history and recovery branch are still distinguishable.
	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	events, view, err = reopened.ReadStreamView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind != replay.ViewRecoveryBranch || len(events) != 4 {
		t.Fatalf("after reopen view = %q events = %d, want recovery branch with 4 events", view.Kind, len(events))
	}
	if view.Recovery.OriginalFile != logPath || view.Corruption == nil {
		t.Fatalf("reopen lost the original-file or corruption facts: %+v", view)
	}
	if _, err := os.Stat(filepath.Join(dir, "events.recovery-1.json")); err != nil {
		t.Fatalf("persisted manifest missing: %v", err)
	}
}

func TestCorruptConfirmedRecordPlusHalfWrittenTail(t *testing.T) {
	ctx := context.Background()
	streamID := "shop:device-mixed"
	dir, _ := writeThreeEvents(t, streamID)
	logPath := filepath.Join(dir, "events.log")

	// Damage confirmed record 2: flip a byte inside the second frame. Then
	// add a half-written frame at the very end (crash mid-append).
	log, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	info, err := log.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.WriteAt([]byte{0xAB}, info.Size()/2); err != nil {
		t.Fatal(err)
	}
	if _, err := log.WriteAt([]byte{0x00, 0x00, 0x00, 0x01, 0xFF}, info.Size()); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// The half-written tail must not turn the damaged confirmed record into
	// a "normal tail": the store reports corruption, not a clean history.
	view := store.View()
	if view.Kind != replay.ViewDegraded || view.Corruption == nil {
		t.Fatalf("view = %+v, want degraded with corruption report", view)
	}
	version, err := store.StreamVersion(ctx, streamID)
	if err != nil || version != 1 {
		t.Fatalf("version = %d err %v, want confirmed prefix 1", version, err)
	}
}

func TestRecoveryCandidateKeepsGapsFromDamagedMaterial(t *testing.T) {
	streamID := "shop:device-gap"
	dir, _ := writeThreeEvents(t, streamID)

	// Trusted material only holds events 1 and 2; its own third frame is
	// damaged, so version 3 cannot be recovered from it.
	materialDir := t.TempDir()
	materialStore, err := replay.OpenFileEventStore(materialDir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, materialStore, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, materialStore, streamID, 1, addTea())
	appendEvent(t, materialStore, streamID, 2, addTea())
	if err := materialStore.Close(); err != nil {
		t.Fatal(err)
	}
	materialPath := filepath.Join(materialDir, "events.log")
	corruptLastFrameByte(t, materialPath)

	// Original log loses record 2 as well (complete frame damaged).
	logPath := filepath.Join(dir, "events.log")
	log, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	info, err := log.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.WriteAt([]byte{0xAB}, info.Size()/2); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	candidate, err := store.BuildRecoveryCandidate(materialPath)
	if err != nil {
		t.Fatal(err)
	}
	// Version 2 is recovered contiguously; version 3 stays a gap because the
	// material is itself damaged there. No continuous history is fabricated.
	if len(candidate.Events) != 1 || candidate.Events[0].Version != 2 {
		t.Fatalf("candidate events = %+v, want only version 2", candidate.Events)
	}
	if len(candidate.Gaps) == 0 {
		t.Fatal("unrecoverable range must be kept as a gap")
	}
}

func TestConflictingMaterialsRequireOperatorChoice(t *testing.T) {
	ctx := context.Background()
	streamID := "shop:device-conflict"
	dir, materialA := writeThreeEvents(t, streamID)

	// Material A: saved copy of the real log.
	materialAPath := filepath.Join(t.TempDir(), "material-a.log")
	if err := os.WriteFile(materialAPath, materialA, 0o644); err != nil {
		t.Fatal(err)
	}
	// Material B: agrees on version 1 but claims a different version 2.
	materialDirB := t.TempDir()
	storeB, err := replay.OpenFileEventStore(materialDirB)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, storeB, streamID, replay.NoStreamVersion, addTea())
	appendEvent(t, storeB, streamID, 1, replay.UnsavedEvent{Type: shop.EventOrderPlaced, Data: shop.OrderPlaced{OrderID: "o-x", Items: map[string]int{"tea": 9}}})
	if err := storeB.Close(); err != nil {
		t.Fatal(err)
	}
	materialBPath := filepath.Join(materialDirB, "events.log")

	// Damage confirmed record 2 in the original.
	logPath := filepath.Join(dir, "events.log")
	log, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	info, err := log.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.WriteAt([]byte{0xAB}, info.Size()/2); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	candidateA, err := store.BuildRecoveryCandidate(materialAPath)
	if err != nil {
		t.Fatal(err)
	}
	candidateB, err := store.BuildRecoveryCandidate(materialBPath)
	if err != nil {
		t.Fatal(err)
	}

	// The two materials conflict at version 2; neither is applied silently.
	conflicts := replay.CompareRecoveryCandidates(candidateA, candidateB)
	if len(conflicts) != 1 || conflicts[0].Version != 2 {
		t.Fatalf("conflicts = %+v, want one conflict at version 2", conflicts)
	}

	// A candidate that contradicts the confirmed original prefix is refused.
	materialDirC := t.TempDir()
	storeC, err := replay.OpenFileEventStore(materialDirC)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, storeC, streamID, replay.NoStreamVersion, replay.UnsavedEvent{Type: shop.EventOrderPaid, Data: shop.OrderPaid{OrderID: "o-y"}})
	if err := storeC.Close(); err != nil {
		t.Fatal(err)
	}
	candidateC, err := store.BuildRecoveryCandidate(filepath.Join(materialDirC, "events.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(candidateC.Conflicts) == 0 {
		t.Fatal("material contradicting the confirmed prefix must conflict")
	}
	if err := store.CommitRecovery(ctx, candidateC); err == nil {
		t.Fatal("conflicting candidate must not be committable")
	} else if _, ok := err.(replay.RecoveryConflictError); !ok {
		t.Fatalf("error = %T, want RecoveryConflictError", err)
	}

	// The operator chooses material A; the branch then serves 3 events and
	// every read names its view.
	if err := store.CommitRecovery(ctx, candidateA); err != nil {
		t.Fatal(err)
	}
	events, view, err := store.ReadStreamView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind != replay.ViewRecoveryBranch || len(events) != 3 {
		t.Fatalf("view = %q events = %d, want recovery branch with 3 events", view.Kind, len(events))
	}
	if view.Recovery.MaterialPath != materialAPath {
		t.Fatalf("recovery basis = %q, want %q", view.Recovery.MaterialPath, materialAPath)
	}
}
