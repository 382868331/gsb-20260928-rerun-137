package replay_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"replayable-app"
)

// setupDamagedStore appends four events, corrupts the second committed
// frame, and reopens the store in the damaged state.
func setupDamagedStore(t *testing.T, dir, streamID string) (*replay.FileEventStore, []replay.RecordedEvent) {
	t.Helper()
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
	return reopened, saved
}

func TestReviewDecisionsAreIdempotentAndLockedOnceAccepted(t *testing.T) {
	dir := t.TempDir()
	streamID := "shop:device-review-idempotent"
	store, saved := setupDamagedStore(t, dir, streamID)
	defer store.Close()

	material := replay.RecoveryMaterial{Source: "export", Events: []replay.RecordedEvent{saved[0], saved[1], saved[3]}}
	candidate, err := store.BuildRecoveryCandidate(streamID, material)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Version != 1 {
		t.Fatalf("candidate version = %d, want 1", candidate.Version)
	}

	accept := replay.ReviewDecision{
		Range:          replay.VersionRange{From: 2, To: 2},
		Kind:           replay.DecisionAccept,
		MaterialSource: "export",
		Rationale:      "matches nightly export",
	}
	if err := store.SubmitReviewDecision(streamID, accept); err != nil {
		t.Fatal(err)
	}
	// Re-submitting the identical decision is idempotent.
	if err := store.SubmitReviewDecision(streamID, accept); err != nil {
		t.Fatal(err)
	}
	state, err := store.ReviewState(streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Decisions) != 1 {
		t.Fatalf("decisions = %+v, want exactly 1 (idempotent resubmit)", state.Decisions)
	}
	if state.Decisions[0].DecidedAt.IsZero() {
		t.Fatal("decision must carry a timestamp")
	}

	// A different decision for an accepted range is rejected.
	different := accept
	different.Kind = replay.DecisionReject
	if err := store.SubmitReviewDecision(streamID, different); err == nil {
		t.Fatal("expected a different decision for an accepted range to be rejected")
	}
	differentRationale := accept
	differentRationale.Rationale = "changed my mind"
	if err := store.SubmitReviewDecision(streamID, differentRationale); err == nil {
		t.Fatal("expected a different rationale for an accepted range to be rejected")
	}

	// A decision outside the candidate is rejected.
	outOfRange := replay.ReviewDecision{Range: replay.VersionRange{From: 9, To: 9}, Kind: replay.DecisionAccept}
	if err := store.SubmitReviewDecision(streamID, outOfRange); err == nil {
		t.Fatal("expected a decision outside the candidate to be rejected")
	}
}

func TestPartialReviewKeepsGapsAndDefersConfirmation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-review-partial"
	store, saved := setupDamagedStore(t, dir, streamID)
	defer store.Close()

	// Material covers versions 2 and 4; version 3 stays a gap.
	material := replay.RecoveryMaterial{Source: "export", Events: []replay.RecordedEvent{saved[0], saved[1], saved[3]}}
	candidate, err := store.BuildRecoveryCandidate(streamID, material)
	if err != nil {
		t.Fatal(err)
	}

	accept := replay.ReviewDecision{Range: replay.VersionRange{From: 2, To: 2}, Kind: replay.DecisionAccept, MaterialSource: "export", Rationale: "verified"}
	reject := replay.ReviewDecision{Range: replay.VersionRange{From: 4, To: 4}, Kind: replay.DecisionReject, MaterialSource: "export", Rationale: "export looks tampered"}
	deferGap := replay.ReviewDecision{Range: replay.VersionRange{From: 3, To: 3}, Kind: replay.DecisionDefer, Rationale: "waiting for offsite copy"}
	for _, d := range []replay.ReviewDecision{accept, reject, deferGap} {
		if err := store.SubmitReviewDecision(streamID, d); err != nil {
			t.Fatal(err)
		}
	}

	// A deferred range blocks confirmation.
	if err := store.ConfirmRecovery(candidate, material); err == nil {
		t.Fatal("expected confirmation to be blocked by the deferred range")
	}

	// The deferral may be updated; the accepted range cannot.
	acceptGap := replay.ReviewDecision{Range: replay.VersionRange{From: 3, To: 3}, Kind: replay.DecisionAccept, Rationale: "offsite copy never arrived; keep the gap"}
	if err := store.SubmitReviewDecision(streamID, acceptGap); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRecovery(candidate, material); err != nil {
		t.Fatal(err)
	}

	// The branch holds only the accepted version; rejected and gap versions
	// stay gaps.
	viewed, err := store.ReadStreamWithView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(viewed.Events) != 2 || viewed.Events[1].Version != 2 {
		t.Fatalf("events = %+v, want versions 1 and 2", viewed.Events)
	}
	if len(viewed.View.Gaps) != 1 || viewed.View.Gaps[0] != (replay.VersionRange{From: 3, To: 4}) {
		t.Fatalf("gaps = %+v, want [3,4]", viewed.View.Gaps)
	}
	if viewed.View.Review == nil || len(viewed.View.Review.Decisions) != 3 {
		t.Fatalf("review = %+v, want 3 recorded decisions", viewed.View.Review)
	}
	if len(viewed.View.Review.Pending) != 0 {
		t.Fatalf("pending = %+v, want none after all ranges decided", viewed.View.Review.Pending)
	}

	// The recovery record keeps the decisions and the rationale.
	recordData, err := os.ReadFile(filepath.Join(dir, "events.recovery.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record replay.RecoveryRecord
	if err := json.Unmarshal(recordData, &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Decisions) != 3 || record.Generation != 1 {
		t.Fatalf("record = %+v, want 3 decisions and generation 1", record)
	}
}

func TestSupplementaryMaterialFormsNewCandidateVersion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	streamID := "shop:device-review-supplement"
	store, saved := setupDamagedStore(t, dir, streamID)
	defer store.Close()

	// First round: material covers versions 2 and 4; version 3 stays a gap.
	material := replay.RecoveryMaterial{Source: "export", Events: []replay.RecordedEvent{saved[0], saved[1], saved[3]}}
	candidate, err := store.BuildRecoveryCandidate(streamID, material)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRecovery(candidate, material); err != nil {
		t.Fatal(err)
	}

	// Supplementary material arrives for version 3; version 4 stays a gap.
	supplement := replay.RecoveryMaterial{Source: "offsite-copy", Events: []replay.RecordedEvent{saved[2]}}
	updated, err := store.BuildRecoveryCandidate(streamID, material, supplement)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 {
		t.Fatalf("candidate version = %d, want 2 after supplementary material", updated.Version)
	}
	if len(updated.Recovered) != 1 || updated.Recovered[0].Version != 3 {
		t.Fatalf("recovered = %+v, want version 3", updated.Recovered)
	}

	// The diff against the confirmed branch shows what would change.
	diff, err := store.CompareCandidateWithConfirmed(updated)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.NewlyRecovered) != 1 || diff.NewlyRecovered[0] != 3 {
		t.Fatalf("newly recovered = %+v, want [3]", diff.NewlyRecovered)
	}
	if len(diff.ResolvedGaps) != 1 || diff.ResolvedGaps[0] != (replay.VersionRange{From: 3, To: 3}) {
		t.Fatalf("resolved gaps = %+v, want [3,3]", diff.ResolvedGaps)
	}
	if len(diff.RemainingGaps) != 0 {
		t.Fatalf("remaining gaps = %+v, want none", diff.RemainingGaps)
	}

	// The confirmed branch is not silently rewritten: confirming the new
	// candidate without an explicit decision fails.
	if err := store.ConfirmRecovery(updated, material, supplement); err == nil {
		t.Fatal("expected confirmation without a decision to be rejected")
	}
	if version, _ := store.StreamVersion(ctx, streamID); version != 4 {
		t.Fatalf("version = %d, want 4: the confirmed branch must not change silently", version)
	}

	// An explicit accept decision confirms the supplementary material.
	accept := replay.ReviewDecision{Range: replay.VersionRange{From: 3, To: 3}, Kind: replay.DecisionAccept, MaterialSource: "offsite-copy", Rationale: "matches offsite copy"}
	if err := store.SubmitReviewDecision(streamID, accept); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRecovery(updated, material, supplement); err != nil {
		t.Fatal(err)
	}
	viewed, err := store.ReadStreamWithView(ctx, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(viewed.Events) != 4 || viewed.Events[2].Version != 3 {
		t.Fatalf("events = %+v, want versions 1,2,3,4", viewed.Events)
	}
	if len(viewed.View.Gaps) != 0 {
		t.Fatalf("gaps = %+v, want none", viewed.View.Gaps)
	}
	if viewed.View.Recovery.Generation != 2 {
		t.Fatalf("generation = %d, want 2", viewed.View.Recovery.Generation)
	}
	if _, err := os.Stat(filepath.Join(dir, "events.log.previous")); err != nil {
		t.Fatalf("previous branch must be preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "events.log.original")); err != nil {
		t.Fatalf("original log must still be preserved: %v", err)
	}
}

func TestReviewContinuesAfterSaveAndReopen(t *testing.T) {
	dir := t.TempDir()
	streamID := "shop:device-review-reopen"
	store, saved := setupDamagedStore(t, dir, streamID)

	material := replay.RecoveryMaterial{Source: "export", Events: []replay.RecordedEvent{saved[0], saved[1], saved[3]}}
	if _, err := store.BuildRecoveryCandidate(streamID, material); err != nil {
		t.Fatal(err)
	}
	accept := replay.ReviewDecision{Range: replay.VersionRange{From: 2, To: 2}, Kind: replay.DecisionAccept, MaterialSource: "export", Rationale: "verified"}
	if err := store.SubmitReviewDecision(streamID, accept); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// After a reopen the review continues where it stopped: the candidate
	// rebuilds at the same version and the recorded decision is intact.
	reopened, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.ReviewState(streamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Decisions) != 1 || state.Decisions[0].Kind != replay.DecisionAccept {
		t.Fatalf("decisions = %+v after reopen, want the recorded accept", state.Decisions)
	}
	candidate, err := reopened.BuildRecoveryCandidate(streamID, material)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Version != 1 {
		t.Fatalf("candidate version = %d after reopen, want 1 (same material)", candidate.Version)
	}
	if len(state.Pending) != 2 {
		t.Fatalf("pending = %+v after reopen, want ranges for versions 3 and 4", state.Pending)
	}

	// The remaining ranges are decided and the recovery is confirmed.
	acceptRest := replay.ReviewDecision{Range: replay.VersionRange{From: 4, To: 4}, Kind: replay.DecisionAccept, MaterialSource: "export", Rationale: "verified"}
	acceptGap := replay.ReviewDecision{Range: replay.VersionRange{From: 3, To: 3}, Kind: replay.DecisionAccept, Rationale: "no material; keep the gap"}
	for _, d := range []replay.ReviewDecision{acceptRest, acceptGap} {
		if err := reopened.SubmitReviewDecision(streamID, d); err != nil {
			t.Fatal(err)
		}
	}
	if err := reopened.ConfirmRecovery(candidate, material); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening again still serves the recovery branch with its review basis.
	again, err := replay.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	viewed, err := again.ReadStreamWithView(context.Background(), streamID)
	if err != nil {
		t.Fatal(err)
	}
	if viewed.View.Kind != replay.ViewRecovery || len(viewed.Events) != 3 {
		t.Fatalf("view = %+v events = %d, want recovery branch with 3 events", viewed.View, len(viewed.Events))
	}
	if viewed.View.Recovery == nil || len(viewed.View.Recovery.Decisions) != 3 {
		t.Fatalf("recovery = %+v, want 3 recorded decisions", viewed.View.Recovery)
	}
}
