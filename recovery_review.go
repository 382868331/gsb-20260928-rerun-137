package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ReviewDecisionKind is the operator's verdict on one damaged range of a
// recovery candidate.
type ReviewDecisionKind string

const (
	// DecisionAccept approves the candidate content for the range.
	DecisionAccept ReviewDecisionKind = "accept"
	// DecisionReject rejects the candidate content; the range stays a gap.
	DecisionReject ReviewDecisionKind = "reject"
	// DecisionDefer postpones the decision; the range stays pending.
	DecisionDefer ReviewDecisionKind = "defer"
)

// ReviewDecision records what an operator decided about one damaged range,
// together with the material and rationale the decision relied on.
type ReviewDecision struct {
	Range          VersionRange       `json:"range"`
	Kind           ReviewDecisionKind `json:"kind"`
	MaterialSource string             `json:"material_source,omitempty"`
	Rationale      string             `json:"rationale,omitempty"`
	DecidedAt      time.Time          `json:"decided_at"`
}

func (d ReviewDecision) same(other ReviewDecision) bool {
	return d.Range == other.Range &&
		d.Kind == other.Kind &&
		d.MaterialSource == other.MaterialSource &&
		d.Rationale == other.Rationale
}

// reviewStateFile is the persisted review progress of one stream. It keeps
// the candidate version the decisions refer to, the material the candidate
// was built from, and every decision recorded so far, so a saved and
// reopened store continues the review exactly where it stopped.
type reviewStateFile struct {
	StreamID         string            `json:"stream_id"`
	CandidateVersion int               `json:"candidate_version"`
	MaterialDigests  map[string]string `json:"material_digests,omitempty"`
	ConfirmedThrough int64             `json:"confirmed_through"`
	Recovered        []int64           `json:"recovered,omitempty"`
	Gaps             []VersionRange    `json:"gaps,omitempty"`
	Decisions        []ReviewDecision  `json:"decisions,omitempty"`
}

// ReviewState is the operator-facing review progress of a stream: the
// candidate version under review, every recorded decision, and the ranges
// still pending a decision.
type ReviewState struct {
	StreamID         string
	CandidateVersion int
	Decisions        []ReviewDecision
	Pending          []VersionRange
}

// CandidateDiff compares a candidate with the currently confirmed branch:
// the versions that would be newly recovered, the gaps they resolve, and
// the gaps that remain.
type CandidateDiff struct {
	NewlyRecovered []int64
	ResolvedGaps   []VersionRange
	RemainingGaps  []VersionRange
}

const reviewStateFileName = "events.review.json"

// SubmitReviewDecision records the operator's decision for one damaged
// range of the current candidate. Re-submitting an identical decision for
// an already accepted range is idempotent; submitting a different decision
// for an accepted range is rejected, so a confirmed recovery branch is
// never silently rewritten. Decisions for deferred or rejected ranges may
// be updated by a new decision.
func (s *FileEventStore) SubmitReviewDecision(streamID string, decision ReviewDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.review == nil || s.review.StreamID != streamID {
		return fmt.Errorf("replay: no recovery candidate under review for stream %q; build one first", streamID)
	}
	if decision.Range.From <= 0 || decision.Range.To < decision.Range.From {
		return fmt.Errorf("replay: invalid decision range %+v", decision.Range)
	}
	switch decision.Kind {
	case DecisionAccept, DecisionReject, DecisionDefer:
	default:
		return fmt.Errorf("replay: unknown review decision kind %q", decision.Kind)
	}
	if err := s.validateDecisionRangeLocked(decision.Range); err != nil {
		return err
	}
	decision.DecidedAt = s.now().UTC()

	for i, existing := range s.review.Decisions {
		if existing.Range != decision.Range {
			continue
		}
		if existing.Kind == DecisionAccept {
			if existing.same(decision) {
				return nil
			}
			return fmt.Errorf("replay: range %+v is already accepted; a different decision would silently rewrite the confirmed branch", decision.Range)
		}
		s.review.Decisions[i] = decision
		return s.persistReviewStateLocked()
	}
	s.review.Decisions = append(s.review.Decisions, decision)
	return s.persistReviewStateLocked()
}

// validateDecisionRangeLocked ensures every version in the range belongs to
// the candidate under review: either recoverable from material or a gap.
func (s *FileEventStore) validateDecisionRangeLocked(r VersionRange) error {
	for version := r.From; version <= r.To; version++ {
		if versionInRanges(s.review.Gaps, version) {
			continue
		}
		if !int64InSlice(s.review.Recovered, version) {
			return fmt.Errorf("replay: version %d is not part of the candidate under review", version)
		}
	}
	return nil
}

// ReviewState reports the review progress of a stream: the candidate
// version under review, every recorded decision, and the ranges still
// pending a decision.
func (s *FileEventStore) ReviewState(streamID string) (ReviewState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.review == nil || s.review.StreamID != streamID {
		return ReviewState{}, fmt.Errorf("replay: no recovery candidate under review for stream %q", streamID)
	}
	return s.reviewStateLocked(), nil
}

func (s *FileEventStore) reviewStateLocked() ReviewState {
	state := ReviewState{
		StreamID:         s.review.StreamID,
		CandidateVersion: s.review.CandidateVersion,
		Decisions:        append([]ReviewDecision(nil), s.review.Decisions...),
	}
	for _, version := range s.review.Recovered {
		if s.decisionForVersionLocked(version) == nil {
			state.Pending = appendGap(state.Pending, version)
		}
	}
	for _, gap := range s.review.Gaps {
		for version := gap.From; version <= gap.To; version++ {
			if s.decisionForVersionLocked(version) == nil {
				state.Pending = appendGap(state.Pending, version)
			}
		}
	}
	return state
}

func (s *FileEventStore) decisionForVersionLocked(version int64) *ReviewDecision {
	for i := range s.review.Decisions {
		d := &s.review.Decisions[i]
		if d.Range.From <= version && version <= d.Range.To {
			return d
		}
	}
	return nil
}

// CompareCandidateWithConfirmed compares a candidate with the currently
// confirmed branch, so the operator can see what supplementary material
// would change before confirming it.
func (s *FileEventStore) CompareCandidateWithConfirmed(candidate RecoveryCandidate) (CandidateDiff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.review == nil || s.review.StreamID != candidate.StreamID {
		return CandidateDiff{}, fmt.Errorf("replay: no recovery candidate under review for stream %q", candidate.StreamID)
	}
	if candidate.Version != s.review.CandidateVersion {
		return CandidateDiff{}, fmt.Errorf("replay: candidate version %d is stale; current version is %d", candidate.Version, s.review.CandidateVersion)
	}
	diff := CandidateDiff{RemainingGaps: append([]VersionRange(nil), candidate.Gaps...)}
	confirmed := make(map[int64]bool)
	for _, event := range s.streams[candidate.StreamID] {
		confirmed[event.Version] = true
	}
	for _, event := range candidate.Recovered {
		if !confirmed[event.Version] {
			diff.NewlyRecovered = append(diff.NewlyRecovered, event.Version)
		}
	}
	sort.Slice(diff.NewlyRecovered, func(i, j int) bool { return diff.NewlyRecovered[i] < diff.NewlyRecovered[j] })
	for _, version := range diff.NewlyRecovered {
		diff.ResolvedGaps = appendGap(diff.ResolvedGaps, version)
	}
	return diff, nil
}

// trackCandidateLocked records the candidate under review: the version
// increments whenever the material set or the confirmed branch changes, so
// supplementary material forms a new candidate version instead of silently
// rewriting the one already under review. Identical rebuilds keep the
// current version.
func (s *FileEventStore) trackCandidateLocked(streamID string, candidate RecoveryCandidate, materials []RecoveryMaterial) error {
	digests := make(map[string]string, len(materials))
	for _, material := range materials {
		digests[material.Source] = DigestMaterial(material)
	}
	recovered := make([]int64, 0, len(candidate.Recovered))
	for _, event := range candidate.Recovered {
		recovered = append(recovered, event.Version)
	}
	confirmedThrough := headVersionLocked(candidate.Confirmed)

	if s.review != nil && s.review.StreamID == streamID &&
		s.review.ConfirmedThrough == confirmedThrough &&
		stringMapsEqual(s.review.MaterialDigests, digests) &&
		int64SlicesEqual(s.review.Recovered, recovered) &&
		versionRangesEqual(s.review.Gaps, candidate.Gaps) {
		return nil
	}

	version := 1
	var decisions []ReviewDecision
	if s.review != nil && s.review.StreamID == streamID {
		version = s.review.CandidateVersion + 1
		decisions = s.review.Decisions
	}
	s.review = &reviewStateFile{
		StreamID:         streamID,
		CandidateVersion: version,
		MaterialDigests:  digests,
		ConfirmedThrough: confirmedThrough,
		Recovered:        recovered,
		Gaps:             append([]VersionRange(nil), candidate.Gaps...),
		Decisions:        decisions,
	}
	return s.persistReviewStateLocked()
}

func (s *FileEventStore) persistReviewStateLocked() error {
	data, err := json.MarshalIndent(s.review, "", "  ")
	if err != nil {
		return fmt.Errorf("replay: encode review state: %w", err)
	}
	path := filepath.Join(s.dir, reviewStateFileName)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("replay: write review state: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replay: publish review state: %w", err)
	}
	return nil
}

func (s *FileEventStore) loadReviewState() error {
	data, err := os.ReadFile(filepath.Join(s.dir, reviewStateFileName))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("replay: read review state: %w", err)
	}
	var state reviewStateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("replay: decode review state: %w", err)
	}
	s.review = &state
	return nil
}

func versionInRanges(ranges []VersionRange, version int64) bool {
	for _, r := range ranges {
		if r.From <= version && version <= r.To {
			return true
		}
	}
	return false
}

func int64InSlice(values []int64, value int64) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func stringMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func int64SlicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func versionRangesEqual(a, b []VersionRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
