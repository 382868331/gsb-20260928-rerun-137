package replay_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"replayable-app"
)

// reopenDamagedStore writes count events to the stream, corrupts the
// second committed frame, and reopens the store in the damaged state.
func reopenDamagedStore(t *testing.T, dir, streamID string, count int) (*replay.FileEventStore, []replay.RecordedEvent) {
	t.Helper()
	store, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var saved []replay.RecordedEvent
	for i := 0; i < count; i++ {
		saved = append(saved, appendEvent(t, store, streamID, int64(i), addTea())...)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	corruptCommittedRecord(t, filepath.Join(dir, "events.log"), secondFrameOffset(t, filepath.Join(dir, "events.log")))
	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return reopened, saved
}

func TestReviewMultiRoundPartialConfirm(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-review"

	store, saved := reopenDamagedStore(t, dir, streamID, 5)

	// Round 1: material covers versions 2 and 4 only.
	materialV1 := replay.RecoveryMaterial{Source: "export-1", Events: []replay.RecordedEvent{saved[1], saved[3]}}
	revision1, err := store.SubmitReviewCandidate(streamID, materialV1)
	if err != nil {
		t.Fatal(err)
	}
	if revision1.Revision != 1 || len(revision1.Recovered) != 2 || len(revision1.Gaps) != 1 || revision1.Gaps[0] != (replay.VersionRange{From: 3, To: 3}) {
		t.Fatalf("revision 1 = %+v, want recovered {2,4} and gap [3,3]", revision1)
	}

	// The operator accepts [2,2], and resubmitting the same accept is a
	// no-op rather than a duplicate audit entry.
	decision, err := store.DecideRange(streamID, replay.VersionRange{From: 2, To: 2}, replay.ReviewAccept, "matches nightly export", "export-1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.DecideRange(streamID, replay.VersionRange{From: 2, To: 2}, replay.ReviewAccept, "matches nightly export", "export-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again, decision) {
		t.Fatalf("idempotent accept = %+v, want %+v", again, decision)
	}
	// Version 3 is not covered by revision 1, so it cannot be accepted yet.
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 3, To: 3}, replay.ReviewAccept, "no material", "export-1"); err == nil {
		t.Fatal("expected accept of an uncovered version to be rejected")
	}
	// The operator defers [4,4] pending more material.
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 4, To: 4}, replay.ReviewDefer, "waiting for finance export"); err != nil {
		t.Fatal(err)
	}

	status, err := store.ReviewStatus(streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Decisions) != 2 {
		t.Fatalf("decisions = %+v, want 2 entries (idempotent accept adds none)", status.Decisions)
	}
	if len(status.Accepted) != 1 || status.Accepted[0] != (replay.VersionRange{From: 2, To: 2}) {
		t.Fatalf("accepted = %+v, want [2,2]", status.Accepted)
	}
	if len(status.Deferred) != 1 || status.Deferred[0] != (replay.VersionRange{From: 4, To: 4}) {
		t.Fatalf("deferred = %+v, want [4,4]", status.Deferred)
	}
	if len(status.RemainingGaps) != 1 || status.RemainingGaps[0] != (replay.VersionRange{From: 3, To: 4}) {
		t.Fatalf("remaining gaps = %+v, want [3,4]", status.RemainingGaps)
	}

	// Saving and reopening resumes the review exactly where it was left.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	status, err = store.ReviewStatus(streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Decisions) != 2 || len(status.Revisions) != 1 || status.Resolved {
		t.Fatalf("status after reopen = %+v, want 2 decisions, 1 revision, unresolved", status)
	}

	// Round 2: supplementary material forms a new revision; revision 1 is
	// kept untouched.
	materialV2 := replay.RecoveryMaterial{Source: "export-2", Events: []replay.RecordedEvent{saved[1], saved[2], saved[3], saved[4]}}
	revision2, err := store.SubmitReviewCandidate(streamID, materialV1, materialV2)
	if err != nil {
		t.Fatal(err)
	}
	if revision2.Revision != 2 || len(revision2.Recovered) != 4 || len(revision2.Gaps) != 0 {
		t.Fatalf("revision 2 = %+v, want recovered {2,3,4,5} without gaps", revision2)
	}
	diff, err := store.DiffRevisions(streamID, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.NewlyRecovered) != 2 || diff.NewlyRecovered[0] != 3 || diff.NewlyRecovered[1] != 5 {
		t.Fatalf("diff = %+v, want newly recovered {3,5}", diff)
	}
	if len(diff.NewlyGapped) != 0 || len(diff.NewConflicts) != 0 {
		t.Fatalf("diff = %+v, want no new gaps or conflicts", diff)
	}

	// The deferred range can be accepted against the new revision, and a
	// reject keeps version 5 a gap. An accepted range cannot be rewritten.
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 3, To: 3}, replay.ReviewAccept, "covered by finance export", "export-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 4, To: 4}, replay.ReviewAccept, "covered by finance export", "export-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 5, To: 5}, replay.ReviewReject, "conflicts with ledger", "export-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 2, To: 2}, replay.ReviewReject, "changed my mind"); err == nil {
		t.Fatal("expected rewriting an accepted range to be rejected")
	}

	status, err = store.ReviewStatus(streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Accepted) != 3 || len(status.Rejected) != 1 || len(status.Deferred) != 0 {
		t.Fatalf("status = %+v, want 3 accepted, 1 rejected, defer superseded", status)
	}
	if len(status.RemainingGaps) != 1 || status.RemainingGaps[0] != (replay.VersionRange{From: 5, To: 5}) {
		t.Fatalf("remaining gaps = %+v, want [5,5]", status.RemainingGaps)
	}

	// Confirming forms the branch from accepted ranges only; version 5
	// stays a gap.
	if err := store.ConfirmReviewedRecovery(streamID); err != nil {
		t.Fatal(err)
	}
	viewed, err := store.ReadStreamWithView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if viewed.View.Kind != replay.ViewRecovery || len(viewed.Events) != 4 {
		t.Fatalf("view = %+v with %d events, want recovery branch with 4 events", viewed.View, len(viewed.Events))
	}
	if len(viewed.View.Gaps) != 1 || viewed.View.Gaps[0] != (replay.VersionRange{From: 5, To: 5}) {
		t.Fatalf("view gaps = %+v, want [5,5]", viewed.View.Gaps)
	}
	if viewed.View.Recovery == nil || len(viewed.View.Recovery.Decisions) != 5 {
		t.Fatalf("recovery basis = %+v, want the 5 review decisions recorded", viewed.View.Recovery)
	}
	if viewed.View.Recovery.MaterialDigests["export-2"] != replay.DigestMaterial(materialV2) {
		t.Fatalf("material digests = %+v", viewed.View.Recovery.MaterialDigests)
	}

	// After a reopen the resolved review and the branch basis survive, and
	// no further review decisions or candidates are accepted.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	status, err = store.ReviewStatus(streamID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Resolved || len(status.Decisions) != 5 || len(status.Revisions) != 2 {
		t.Fatalf("status after resolve = %+v, want resolved with 5 decisions and 2 revisions", status)
	}
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 5, To: 5}, replay.ReviewAccept, "too late"); err == nil {
		t.Fatal("expected decisions on a resolved review to be rejected")
	}
	if _, err := store.SubmitReviewCandidate(streamID, materialV2); err == nil {
		t.Fatal("expected new candidates on a resolved review to be rejected")
	}
	recordData, err := os.ReadFile(filepath.Join(dir, "events.recovery.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record replay.RecoveryRecord
	if err := json.Unmarshal(recordData, &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Decisions) != 5 || record.RecoveredThrough != 4 {
		t.Fatalf("persisted recovery record = %+v, want 5 decisions recovered through 4", record)
	}
}

func TestReviewCandidateDoesNotRewriteEarlierRevisions(t *testing.T) {
	dir := t.TempDir()
	streamID := "shop:device-revisions"

	store, saved := reopenDamagedStore(t, dir, streamID, 3)
	defer store.Close()

	materialV1 := replay.RecoveryMaterial{Source: "export-1", Events: []replay.RecordedEvent{saved[1]}}
	revision1, err := store.SubmitReviewCandidate(streamID, materialV1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecideRange(streamID, replay.VersionRange{From: 2, To: 2}, replay.ReviewAccept, "vouched by export-1", "export-1"); err != nil {
		t.Fatal(err)
	}

	// Supplementary material only adds a revision; revision 1 and the
	// recorded decision stay byte-identical.
	materialV2 := replay.RecoveryMaterial{Source: "export-2", Events: []replay.RecordedEvent{saved[1], saved[2]}}
	if _, err := store.SubmitReviewCandidate(streamID, materialV1, materialV2); err != nil {
		t.Fatal(err)
	}
	status, err := store.ReviewStatus(streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Revisions) != 2 {
		t.Fatalf("revisions = %d, want 2", len(status.Revisions))
	}
	if !jsonEqual(t, status.Revisions[0], revision1) {
		t.Fatalf("revision 1 changed after supplementary material: %+v", status.Revisions[0])
	}
	if len(status.Decisions) != 1 || status.Decisions[0].Revision != 1 {
		t.Fatalf("decisions = %+v, want the original decision against revision 1", status.Decisions)
	}
}

func jsonEqual(t *testing.T, a, b interface{}) bool {
	t.Helper()
	dataA, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	dataB, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return string(dataA) == string(dataB)
}
