package replay_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"replayable-app"
)

// corruptCommittedRecord rewrites one payload byte of the frame that starts
// at the given offset, leaving the frame complete but invalid.
func corruptCommittedRecord(t *testing.T, logPath string, offset int64) {
	t.Helper()
	file, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	header := make([]byte, 8)
	if _, err := file.ReadAt(header, offset); err != nil {
		t.Fatal(err)
	}
	length := int64(binary.BigEndian.Uint32(header[4:]))
	if length < 2 {
		t.Fatalf("frame at %d too small to corrupt safely", offset)
	}
	one := make([]byte, 1)
	if _, err := file.ReadAt(one, offset+8+length-2); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xFF
	if _, err := file.WriteAt(one, offset+8+length-2); err != nil {
		t.Fatal(err)
	}
}

// secondFrameOffset returns the log offset of the second committed frame.
func secondFrameOffset(t *testing.T, logPath string) int64 {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	return 8 + int64(binary.BigEndian.Uint32(data[4:8]))
}

func TestFileStoreDetectsCommittedCorruption(t *testing.T) {
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

	logPath := filepath.Join(dir, "events.log")
	sizeBefore, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	corruptCommittedRecord(t, logPath, secondFrameOffset(t, logPath))

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	damage, ok := reopened.Damage()
	if !ok {
		t.Fatal("expected committed corruption to be reported")
	}
	if damage.Offset != secondFrameOffset(t, logPath) {
		t.Fatalf("damage offset = %d, want %d", damage.Offset, secondFrameOffset(t, logPath))
	}
	report := reopened.OpenReport()
	if report.TruncatedTail || report.Damage == nil {
		t.Fatalf("open report = %+v, want damage without tail truncation", report)
	}

	// The confirmed prefix stays readable and is reported as damaged
	// original history, not as a clean tail.
	viewed, err := reopened.ReadStreamWithView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(viewed.Events) != 1 {
		t.Fatalf("events = %d, want the 1 confirmed event", len(viewed.Events))
	}
	if viewed.View.Kind != replay.ViewOriginal || !viewed.View.Damaged {
		t.Fatalf("view = %+v, want damaged original view", viewed.View)
	}

	// The corrupt bytes are preserved for the operator, not truncated away.
	sizeAfter, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if sizeAfter.Size() != sizeBefore.Size() {
		t.Fatalf("log size changed from %d to %d; damaged content must be preserved", sizeBefore.Size(), sizeAfter.Size())
	}

	// Appends are blocked until the operator resolves the damage.
	if _, err := reopened.Append(ctx, streamID, 1, addTea()); err == nil {
		t.Fatal("expected append to be rejected while committed corruption is unresolved")
	}
}

func TestFileStoreHalfWrittenTailIsNotDamage(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-halfwrite"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, store, streamID, replay.NoStreamVersion, addTea())
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

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

	report := reopened.OpenReport()
	if !report.TruncatedTail || report.Damage != nil {
		t.Fatalf("open report = %+v, want truncated tail without damage", report)
	}
	if _, ok := reopened.Damage(); ok {
		t.Fatal("unfinished tail write must not be reported as committed corruption")
	}
	appendEvent(t, reopened, streamID, 1, addTea())
	if version, _ := reopened.StreamVersion(ctx, streamID); version != 2 {
		t.Fatalf("version = %d, want 2", version)
	}
}

func TestRecoveryCandidateKeepsGapsAndFormsBranch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-recover"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var saved []replay.RecordedEvent
	for i := 0; i < 4; i++ {
		saved = append(saved, appendEvent(t, store, streamID, int64(i), addTea())...)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, "events.log")
	corruptCommittedRecord(t, logPath, secondFrameOffset(t, logPath))

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// Trusted material saved elsewhere covers versions 2 and 4 but not 3.
	material := replay.RecoveryMaterial{
		Source: "nightly-export",
		Events: []replay.RecordedEvent{saved[0], saved[1], saved[3]},
	}
	candidate, err := reopened.BuildRecoveryCandidate(streamID, material)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.Confirmed) != 1 {
		t.Fatalf("confirmed = %d, want 1", len(candidate.Confirmed))
	}
	if len(candidate.Recovered) != 2 || candidate.Recovered[0].Version != 2 || candidate.Recovered[1].Version != 4 {
		t.Fatalf("recovered = %+v, want versions 2 and 4", candidate.Recovered)
	}
	if len(candidate.Gaps) != 1 || candidate.Gaps[0] != (replay.VersionRange{From: 3, To: 3}) {
		t.Fatalf("gaps = %+v, want [3,3]", candidate.Gaps)
	}

	if err := reopened.ConfirmRecovery(candidate, material); err != nil {
		t.Fatal(err)
	}

	// The original file, the recovery basis, and the branch all remain.
	if _, err := os.Stat(logPath + ".original"); err != nil {
		t.Fatalf("original log must be preserved: %v", err)
	}
	recordData, err := os.ReadFile(filepath.Join(dir, "events.recovery.json"))
	if err != nil {
		t.Fatalf("recovery record must be persisted: %v", err)
	}
	var record replay.RecoveryRecord
	if err := json.Unmarshal(recordData, &record); err != nil {
		t.Fatal(err)
	}
	if record.ConfirmedThrough != 1 || record.RecoveredThrough != 4 || record.OriginalFile != "events.log.original" {
		t.Fatalf("recovery record = %+v", record)
	}
	if record.MaterialDigests["nightly-export"] != replay.DigestMaterial(material) {
		t.Fatalf("material digest = %+v", record.MaterialDigests)
	}

	// The branch keeps the gap and accepts new appends after the recovered head.
	viewed, err := reopened.ReadStreamWithView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if viewed.View.Kind != replay.ViewRecovery || viewed.View.Damaged {
		t.Fatalf("view = %+v, want recovery view", viewed.View)
	}
	if len(viewed.View.Gaps) != 1 || viewed.View.Gaps[0].From != 3 {
		t.Fatalf("view gaps = %+v, want gap at 3", viewed.View.Gaps)
	}
	if len(viewed.Events) != 3 {
		t.Fatalf("events = %d, want 3 (gap at 3 is not fabricated)", len(viewed.Events))
	}
	appendEvent(t, reopened, streamID, 4, addTea())

	// After a reopen the store still serves the recovery branch, distinct
	// from the original history, with the gap and the basis intact.
	again, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	viewed, err = again.ReadStreamWithView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if viewed.View.Kind != replay.ViewRecovery {
		t.Fatalf("view kind = %q after reopen, want recovery", viewed.View.Kind)
	}
	if viewed.View.Recovery == nil || viewed.View.Recovery.ConfirmedThrough != 1 {
		t.Fatalf("recovery basis = %+v after reopen", viewed.View.Recovery)
	}
	if len(viewed.Events) != 4 || viewed.Events[3].Version != 5 {
		t.Fatalf("events = %+v after reopen, want versions 1,2,4,5", viewed.Events)
	}
	if version, _ := again.StreamVersion(ctx, streamID); version != 5 {
		t.Fatalf("version = %d after reopen, want 5", version)
	}
}

func TestRecoveryCandidateReportsConflictingMaterials(t *testing.T) {
	dir := t.TempDir()
	streamID := "shop:device-conflict"

	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var saved []replay.RecordedEvent
	for i := 0; i < 3; i++ {
		saved = append(saved, appendEvent(t, store, streamID, int64(i), addTea())...)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, "events.log")
	corruptCommittedRecord(t, logPath, secondFrameOffset(t, logPath))

	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// A second material disagrees on version 2 and even on the confirmed
	// version 1; both disagreements must surface as conflicts and gaps.
	forged := saved[1]
	forged.Data = json.RawMessage(`{"sku":"forged"}`)
	forgedConfirmed := saved[0]
	forgedConfirmed.Data = json.RawMessage(`{"sku":"forged"}`)
	materialA := replay.RecoveryMaterial{Source: "export-a", Events: []replay.RecordedEvent{saved[0], saved[1], saved[2]}}
	materialB := replay.RecoveryMaterial{Source: "export-b", Events: []replay.RecordedEvent{forgedConfirmed, forged, saved[2]}}

	candidate, err := reopened.BuildRecoveryCandidate(streamID, materialA, materialB)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.Recovered) != 1 || candidate.Recovered[0].Version != 3 {
		t.Fatalf("recovered = %+v, want only version 3", candidate.Recovered)
	}
	if len(candidate.Gaps) != 1 || candidate.Gaps[0] != (replay.VersionRange{From: 2, To: 2}) {
		t.Fatalf("gaps = %+v, want [2,2]", candidate.Gaps)
	}
	if len(candidate.Conflicts) != 2 {
		t.Fatalf("conflicts = %+v, want conflicts on versions 1 and 2", candidate.Conflicts)
	}
	if candidate.Conflicts[0].Version != 1 || candidate.Conflicts[1].Version != 2 {
		t.Fatalf("conflict versions = %+v", candidate.Conflicts)
	}
}
