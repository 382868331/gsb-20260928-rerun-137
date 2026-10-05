package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ReviewAction is the operator's decision on one damaged range of a
// recovery candidate.
type ReviewAction string

const (
	// ReviewAccept trusts the recovered events in the range; they join the
	// recovery branch on confirmation. Accept is final: a later decision
	// cannot rewrite it, and resubmitting the same accept is a no-op.
	ReviewAccept ReviewAction = "accept"
	// ReviewReject discards the recovered events in the range; the range
	// stays a gap. A later round may supersede a reject.
	ReviewReject ReviewAction = "reject"
	// ReviewDefer leaves the range undecided for a later round; it stays a
	// gap until accepted.
	ReviewDefer ReviewAction = "defer"
)

// RangeDecision records one operator decision on a damaged range: the
// action, the reason, and the trusted material the decision relied on.
// Decisions form an append-only audit log; the effective decision for a
// range is its latest entry.
type RangeDecision struct {
	Range     VersionRange `json:"range"`
	Action    ReviewAction `json:"action"`
	Reason    string       `json:"reason"`
	Materials []string     `json:"materials,omitempty"`
	// Revision is the candidate revision the decision was made against.
	Revision  int       `json:"revision"`
	DecidedAt time.Time `json:"decided_at"`
}

// CandidateRevision is one immutable version of a recovery candidate.
// Supplementary material only ever creates a new revision; earlier
// revisions are never rewritten, so a confirmed branch always keeps the
// exact basis it was confirmed from.
type CandidateRevision struct {
	Revision        int                `json:"revision"`
	MaterialDigests map[string]string  `json:"material_digests"`
	Recovered       []RecordedEvent    `json:"recovered"`
	Gaps            []VersionRange     `json:"gaps,omitempty"`
	Conflicts       []MaterialConflict `json:"conflicts,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
}

// ReviewRecord is the persisted multi-round review state of a damaged
// stream: every candidate revision and every decision, so a reopened store
// resumes the review exactly where it was left.
type ReviewRecord struct {
	StreamID  string              `json:"stream_id"`
	Damage    Damage              `json:"damage"`
	Revisions []CandidateRevision `json:"revisions,omitempty"`
	Decisions []RangeDecision     `json:"decisions,omitempty"`
	// Resolved is set once a reviewed recovery branch is confirmed.
	Resolved bool `json:"resolved,omitempty"`
}

// ReviewStatus is the current review view of a damaged stream: the
// revision history, the full decision log, the effective per-range
// decisions, and the gaps that would remain if confirmed now.
type ReviewStatus struct {
	StreamID      string
	Resolved      bool
	Revisions     []CandidateRevision
	Decisions     []RangeDecision
	Accepted      []VersionRange
	Rejected      []VersionRange
	Deferred      []VersionRange
	RemainingGaps []VersionRange
}

// RevisionDiff compares two candidate revisions, showing what
// supplementary material changed: versions newly covered, versions that
// became gaps, and conflicts that appeared.
type RevisionDiff struct {
	FromRevision   int
	ToRevision     int
	NewlyRecovered []int64
	NewlyGapped    []int64
	NewConflicts   []MaterialConflict
}

const reviewRecordFile = "events.review.json"

// SubmitReviewCandidate builds a recovery candidate from the current
// damage and the given trusted material, and appends it to the review
// record as a new immutable revision. Supplementary material never
// modifies earlier revisions; it only adds a new one.
func (s *FileEventStore) SubmitReviewCandidate(streamID string, materials ...RecoveryMaterial) (CandidateRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.damage == nil {
		return CandidateRevision{}, fmt.Errorf("replay: no committed corruption to review")
	}
	if s.review != nil && s.review.Resolved {
		return CandidateRevision{}, fmt.Errorf("replay: review for stream %q is already resolved by a confirmed recovery", streamID)
	}
	candidate, err := s.buildRecoveryCandidateLocked(streamID, materials...)
	if err != nil {
		return CandidateRevision{}, err
	}
	review := s.review
	if review == nil {
		review = &ReviewRecord{StreamID: streamID, Damage: *s.damage}
	}
	if review.StreamID != streamID {
		return CandidateRevision{}, fmt.Errorf("replay: an active review for stream %q already exists", review.StreamID)
	}
	revision := CandidateRevision{
		Revision:        len(review.Revisions) + 1,
		MaterialDigests: make(map[string]string),
		Recovered:       candidate.Recovered,
		Gaps:            candidate.Gaps,
		Conflicts:       candidate.Conflicts,
		CreatedAt:       s.now().UTC(),
	}
	for _, material := range materials {
		revision.MaterialDigests[material.Source] = DigestMaterial(material)
	}
	review.Revisions = append(review.Revisions, revision)
	if err := s.persistReviewRecord(review); err != nil {
		return CandidateRevision{}, err
	}
	s.review = review
	return revision, nil
}

// DecideRange records the operator's decision on one damaged range of the
// latest candidate revision, with the reason and the material it relies
// on. Resubmitting the same decision for an already accepted range is
// idempotent: it returns the recorded decision without adding a new entry.
// An accepted range can never be rewritten by a different decision.
func (s *FileEventStore) DecideRange(streamID string, rng VersionRange, action ReviewAction, reason string, materials ...string) (RangeDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	review, err := s.activeReviewLocked(streamID)
	if err != nil {
		return RangeDecision{}, err
	}
	if rng.From > rng.To || rng.From < 1 {
		return RangeDecision{}, fmt.Errorf("replay: invalid range [%d,%d]", rng.From, rng.To)
	}
	switch action {
	case ReviewAccept, ReviewReject, ReviewDefer:
	default:
		return RangeDecision{}, fmt.Errorf("replay: unknown review action %q", action)
	}
	confirmedThrough := headVersionLocked(s.streams[streamID])
	if rng.From <= confirmedThrough {
		return RangeDecision{}, fmt.Errorf("replay: range [%d,%d] overlaps the confirmed prefix through version %d", rng.From, rng.To, confirmedThrough)
	}

	latest := review.Revisions[len(review.Revisions)-1]
	if action == ReviewAccept {
		covered := make(map[int64]bool, len(latest.Recovered))
		for _, event := range latest.Recovered {
			covered[event.Version] = true
		}
		for version := rng.From; version <= rng.To; version++ {
			if !covered[version] {
				return RangeDecision{}, fmt.Errorf("replay: cannot accept range [%d,%d]: version %d is not covered by candidate revision %d", rng.From, rng.To, version, latest.Revision)
			}
		}
	}

	if effective, ok := effectiveDecision(review.Decisions, rng); ok && effective.Action == ReviewAccept {
		if action == ReviewAccept {
			// Idempotent: the range is already accepted; the repeated
			// decision changes nothing and adds no new entry.
			return effective, nil
		}
		return RangeDecision{}, fmt.Errorf("replay: range [%d,%d] is already accepted and cannot be rewritten", rng.From, rng.To)
	}

	decision := RangeDecision{
		Range:     rng,
		Action:    action,
		Reason:    reason,
		Materials: append([]string(nil), materials...),
		Revision:  latest.Revision,
		DecidedAt: s.now().UTC(),
	}
	review.Decisions = append(review.Decisions, decision)
	if err := s.persistReviewRecord(review); err != nil {
		return RangeDecision{}, err
	}
	return decision, nil
}

// ReviewStatus reports the current review view of the stream: every
// candidate revision, the full decision log, the effective decisions, and
// the gaps that would remain if the recovery were confirmed now.
func (s *FileEventStore) ReviewStatus(streamID string) (ReviewStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.review == nil || s.review.StreamID != streamID {
		return ReviewStatus{}, fmt.Errorf("replay: no review record for stream %q", streamID)
	}
	return s.reviewStatusLocked(s.review), nil
}

func (s *FileEventStore) reviewStatusLocked(review *ReviewRecord) ReviewStatus {
	status := ReviewStatus{
		StreamID:  review.StreamID,
		Resolved:  review.Resolved,
		Revisions: append([]CandidateRevision(nil), review.Revisions...),
		Decisions: append([]RangeDecision(nil), review.Decisions...),
	}
	effective := make(map[VersionRange]RangeDecision)
	for _, decision := range review.Decisions {
		effective[decision.Range] = decision
	}
	accepted := make(map[int64]bool)
	for _, decision := range effective {
		switch decision.Action {
		case ReviewAccept:
			status.Accepted = append(status.Accepted, decision.Range)
			for version := decision.Range.From; version <= decision.Range.To; version++ {
				accepted[version] = true
			}
		case ReviewReject:
			status.Rejected = append(status.Rejected, decision.Range)
		case ReviewDefer:
			status.Deferred = append(status.Deferred, decision.Range)
		}
	}
	sortRanges := func(ranges []VersionRange) {
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].From < ranges[j].From })
	}
	sortRanges(status.Accepted)
	sortRanges(status.Rejected)
	sortRanges(status.Deferred)

	if len(review.Revisions) > 0 {
		latest := review.Revisions[len(review.Revisions)-1]
		gaps := append([]VersionRange(nil), latest.Gaps...)
		for _, event := range latest.Recovered {
			if !accepted[event.Version] {
				gaps = appendGap(gaps, event.Version)
			}
		}
		status.RemainingGaps = mergeGaps(gaps)
	}
	return status
}

// DiffRevisions compares two candidate revisions of the stream's review,
// showing what supplementary material changed between them.
func (s *FileEventStore) DiffRevisions(streamID string, fromRevision, toRevision int) (RevisionDiff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.review == nil || s.review.StreamID != streamID {
		return RevisionDiff{}, fmt.Errorf("replay: no review record for stream %q", streamID)
	}
	find := func(number int) (CandidateRevision, error) {
		for _, revision := range s.review.Revisions {
			if revision.Revision == number {
				return revision, nil
			}
		}
		return CandidateRevision{}, fmt.Errorf("replay: no candidate revision %d for stream %q", number, streamID)
	}
	from, err := find(fromRevision)
	if err != nil {
		return RevisionDiff{}, err
	}
	to, err := find(toRevision)
	if err != nil {
		return RevisionDiff{}, err
	}
	diff := RevisionDiff{FromRevision: fromRevision, ToRevision: toRevision}
	recoveredIn := func(revision CandidateRevision) map[int64]bool {
		set := make(map[int64]bool, len(revision.Recovered))
		for _, event := range revision.Recovered {
			set[event.Version] = true
		}
		return set
	}
	fromSet, toSet := recoveredIn(from), recoveredIn(to)
	for version := range toSet {
		if !fromSet[version] {
			diff.NewlyRecovered = append(diff.NewlyRecovered, version)
		}
	}
	for version := range fromSet {
		if !toSet[version] {
			diff.NewlyGapped = append(diff.NewlyGapped, version)
		}
	}
	sort.Slice(diff.NewlyRecovered, func(i, j int) bool { return diff.NewlyRecovered[i] < diff.NewlyRecovered[j] })
	sort.Slice(diff.NewlyGapped, func(i, j int) bool { return diff.NewlyGapped[i] < diff.NewlyGapped[j] })
	knownConflicts := make(map[int64]bool)
	for _, conflict := range from.Conflicts {
		knownConflicts[conflict.Version] = true
	}
	for _, conflict := range to.Conflicts {
		if !knownConflicts[conflict.Version] {
			diff.NewConflicts = append(diff.NewConflicts, conflict)
		}
	}
	return diff, nil
}

// ConfirmReviewedRecovery forms a recovery branch from the ranges the
// operator accepted during review. Recovered events in rejected, deferred,
// or undecided ranges stay gaps. The confirmed branch records the review
// decisions as its basis; the review record is kept as resolved so the
// audit trail survives reopen.
func (s *FileEventStore) ConfirmReviewedRecovery(streamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	review, err := s.activeReviewLocked(streamID)
	if err != nil {
		return err
	}
	status := s.reviewStatusLocked(review)
	if len(status.Accepted) == 0 {
		return fmt.Errorf("replay: no accepted ranges to confirm for stream %q", streamID)
	}
	latest := review.Revisions[len(review.Revisions)-1]
	accepted := make(map[int64]bool)
	for _, rng := range status.Accepted {
		for version := rng.From; version <= rng.To; version++ {
			accepted[version] = true
		}
	}
	candidate := RecoveryCandidate{
		StreamID:  streamID,
		Confirmed: append([]RecordedEvent(nil), s.streams[streamID]...),
		Gaps:      status.RemainingGaps,
	}
	for _, event := range latest.Recovered {
		if accepted[event.Version] {
			candidate.Recovered = append(candidate.Recovered, event)
		}
	}
	if err := s.confirmRecoveryLocked(candidate); err != nil {
		return err
	}
	// The confirmed branch names the review decisions it was built from.
	s.recovery.Decisions = status.Decisions
	s.recovery.MaterialDigests = latest.MaterialDigests
	if err := s.persistRecoveryRecord(s.recovery); err != nil {
		return err
	}
	review.Resolved = true
	return s.persistReviewRecord(review)
}

// activeReviewLocked returns the unresolved review record for the stream.
func (s *FileEventStore) activeReviewLocked(streamID string) (*ReviewRecord, error) {
	if s.review == nil || s.review.StreamID != streamID {
		return nil, fmt.Errorf("replay: no review record for stream %q; submit a review candidate first", streamID)
	}
	if s.review.Resolved {
		return nil, fmt.Errorf("replay: review for stream %q is already resolved by a confirmed recovery", streamID)
	}
	if len(s.review.Revisions) == 0 {
		return nil, fmt.Errorf("replay: review for stream %q has no candidate revisions", streamID)
	}
	return s.review, nil
}

// effectiveDecision returns the latest decision recorded for exactly this
// range, if any.
func effectiveDecision(decisions []RangeDecision, rng VersionRange) (RangeDecision, bool) {
	for i := len(decisions) - 1; i >= 0; i-- {
		if decisions[i].Range == rng {
			return decisions[i], true
		}
	}
	return RangeDecision{}, false
}

// mergeGaps sorts and coalesces adjacent or overlapping gap ranges.
func mergeGaps(gaps []VersionRange) []VersionRange {
	if len(gaps) == 0 {
		return nil
	}
	sorted := append([]VersionRange(nil), gaps...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].From < sorted[j].From })
	merged := []VersionRange{sorted[0]}
	for _, rng := range sorted[1:] {
		last := &merged[len(merged)-1]
		if rng.From <= last.To+1 {
			if rng.To > last.To {
				last.To = rng.To
			}
			continue
		}
		merged = append(merged, rng)
	}
	return merged
}

// persistReviewRecord writes the review state with the same
// write-temp-then-rename discipline as the recovery record.
func (s *FileEventStore) persistReviewRecord(review *ReviewRecord) error {
	data, err := json.MarshalIndent(review, "", "  ")
	if err != nil {
		return fmt.Errorf("replay: encode review record: %w", err)
	}
	path := filepath.Join(s.dir, reviewRecordFile)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("replay: write review record: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replay: publish review record: %w", err)
	}
	return nil
}

// loadReviewRecord restores the review state on open so a reopened store
// resumes a multi-round review where it was left.
func (s *FileEventStore) loadReviewRecord() error {
	data, err := os.ReadFile(filepath.Join(s.dir, reviewRecordFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("replay: read review record: %w", err)
	}
	var review ReviewRecord
	if err := json.Unmarshal(data, &review); err != nil {
		return fmt.Errorf("replay: decode review record: %w", err)
	}
	s.review = &review
	return nil
}
