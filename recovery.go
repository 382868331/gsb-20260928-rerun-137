package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ViewKind identifies which history a FileEventStore is serving.
type ViewKind string

const (
	// ViewOriginal is the clean original log.
	ViewOriginal ViewKind = "original"
	// ViewDegraded is the original log with corrupt confirmed content:
	// only the confirmed prefix is served and appends are refused until a
	// recovery candidate is committed.
	ViewDegraded ViewKind = "degraded"
	// ViewRecoveryBranch is a new branch log built from the confirmed
	// prefix plus an operator-approved recovery candidate. The original
	// file is preserved untouched.
	ViewRecoveryBranch ViewKind = "recovery-branch"
)

// ViewInfo describes the view a read result belongs to.
type ViewInfo struct {
	Kind         ViewKind
	ActiveFile   string
	OriginalFile string
	Corruption   *CorruptionReport
	Recovery     *RecoveryManifest
}

// CorruptionReport records confirmed content that was found damaged on open.
// The original bytes are left in place so the operator can inspect or
// recover them; the report is also persisted next to the log.
type CorruptionReport struct {
	Offset         int64  `json:"offset"`
	Reason         string `json:"reason"`
	ConfirmedBytes int64  `json:"confirmed_bytes"`
	PreservedBytes int64  `json:"preserved_bytes"`
}

// CorruptedHistoryError is returned by appends while the store is degraded:
// confirmed content is damaged and no recovery has been committed yet.
type CorruptedHistoryError struct {
	Report CorruptionReport
}

func (e CorruptedHistoryError) Error() string {
	return fmt.Sprintf("replay: confirmed history is corrupt at offset %d (%s); appends are blocked until a recovery candidate is committed", e.Report.Offset, e.Report.Reason)
}

// RecoveryGap marks a range that a candidate could not recover. Gaps are
// kept as gaps: the store never fabricates continuous history across them.
type RecoveryGap struct {
	StreamID    string `json:"stream_id,omitempty"`
	FromVersion int64  `json:"from_version,omitempty"`
	ToVersion   int64  `json:"to_version,omitempty"`
	Reason      string `json:"reason"`
}

// RecoveryConflict records the same stream version appearing with different
// content in two places (candidate vs. original history, or two candidates).
type RecoveryConflict struct {
	StreamID string        `json:"stream_id"`
	Version  int64         `json:"version"`
	Source   string        `json:"source"`
	Expected RecordedEvent `json:"expected"`
	Actual   RecordedEvent `json:"actual"`
}

// RecoveryConflictError is returned when a recovery is attempted with a
// candidate that still has unresolved conflicts.
type RecoveryConflictError struct {
	Conflicts []RecoveryConflict
}

func (e RecoveryConflictError) Error() string {
	return fmt.Sprintf("replay: recovery candidate has %d unresolved conflicts", len(e.Conflicts))
}

// StreamRange marks recovered contiguous versions of one stream.
type StreamRange struct {
	StreamID    string `json:"stream_id"`
	FromVersion int64  `json:"from_version"`
	ToVersion   int64  `json:"to_version"`
}

// RecoveryCandidate is a proposed recovery built from trusted material saved
// elsewhere (for example a copy of the log made before the damage). It
// recovers only the contiguous per-stream versions that both exist in the
// material and agree with the confirmed original prefix; everything else is
// reported as gaps or conflicts.
type RecoveryCandidate struct {
	MaterialPath string
	Events       []RecordedEvent
	Ranges       []StreamRange
	Gaps         []RecoveryGap
	Conflicts    []RecoveryConflict
}

// RecoveryManifest records a committed recovery: the original file, the
// confirmed range at the time, and the material the recovery is based on.
// It is persisted next to the logs so a reopen can still distinguish the
// original history from the recovery branch.
type RecoveryManifest struct {
	ID              int              `json:"id"`
	CreatedAt       time.Time        `json:"created_at"`
	OriginalFile    string           `json:"original_file"`
	BranchFile      string           `json:"branch_file"`
	Corruption      CorruptionReport `json:"corruption"`
	MaterialPath    string           `json:"material_path"`
	RecoveredRanges []StreamRange    `json:"recovered_ranges"`
	Gaps            []RecoveryGap    `json:"gaps,omitempty"`
}

func sameEventContent(a, b RecordedEvent) bool {
	return a.StreamID == b.StreamID && a.Version == b.Version && a.Type == b.Type && bytes.Equal(a.Data, b.Data)
}

// BuildRecoveryCandidate scans trusted material (a framed event log saved
// elsewhere) and proposes which events beyond the confirmed original prefix
// can be recovered. Material that disagrees with the confirmed original
// history produces conflicts; versions the material cannot supply
// contiguously are left as gaps.
func (s *FileEventStore) BuildRecoveryCandidate(materialPath string) (*RecoveryCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	material, err := os.Open(materialPath)
	if err != nil {
		return nil, fmt.Errorf("replay: open recovery material: %w", err)
	}
	defer material.Close()
	scan, err := scanFrames(material)
	if err != nil {
		return nil, err
	}

	candidate := &RecoveryCandidate{MaterialPath: materialPath}
	if scan.corruptOffset >= 0 {
		candidate.Gaps = append(candidate.Gaps, RecoveryGap{
			Reason: fmt.Sprintf("material itself is corrupt at offset %d (%s); later content is not trusted", scan.corruptOffset, scan.corruptReason),
		})
	}
	if scan.incompleteTail {
		candidate.Gaps = append(candidate.Gaps, RecoveryGap{
			Reason: "material ends with an incomplete frame; its tail was never confirmed",
		})
	}

	materialByStream := make(map[string][]RecordedEvent)
	for _, event := range scan.events {
		materialByStream[event.StreamID] = append(materialByStream[event.StreamID], event)
	}

	for streamID, materialEvents := range materialByStream {
		confirmed := s.streams[streamID]
		// Overlapping versions must agree with the confirmed original.
		for i := 0; i < len(confirmed) && i < len(materialEvents); i++ {
			if !sameEventContent(confirmed[i], materialEvents[i]) {
				candidate.Conflicts = append(candidate.Conflicts, RecoveryConflict{
					StreamID: streamID,
					Version:  confirmed[i].Version,
					Source:   "original",
					Expected: confirmed[i],
					Actual:   materialEvents[i],
				})
			}
		}
		// Recover only the contiguous run right after the confirmed base;
		// a hole ends recovery for the stream and the rest stays a gap.
		base := int64(len(confirmed))
		var recovered []RecordedEvent
		for _, event := range materialEvents {
			if event.Version <= base {
				continue
			}
			if event.Version != base+int64(len(recovered))+1 {
				break
			}
			recovered = append(recovered, event)
		}
		if len(recovered) > 0 {
			candidate.Events = append(candidate.Events, recovered...)
			candidate.Ranges = append(candidate.Ranges, StreamRange{
				StreamID:    streamID,
				FromVersion: recovered[0].Version,
				ToVersion:   recovered[len(recovered)-1].Version,
			})
		}
		last := base + int64(len(recovered))
		if max := materialEvents[len(materialEvents)-1].Version; max > last {
			candidate.Gaps = append(candidate.Gaps, RecoveryGap{
				StreamID:    streamID,
				FromVersion: last + 1,
				ToVersion:   max,
				Reason:      "material does not provide a continuous version chain; the gap is preserved, not fabricated",
			})
		}
	}

	sort.Slice(candidate.Events, func(i, j int) bool { return candidate.Events[i].ID < candidate.Events[j].ID })
	sort.Slice(candidate.Ranges, func(i, j int) bool { return candidate.Ranges[i].StreamID < candidate.Ranges[j].StreamID })
	return candidate, nil
}

// CompareRecoveryCandidates reports where two candidates (for example built
// from two conflicting trusted materials) disagree on recovered content.
// Neither is applied automatically; the operator must choose.
func CompareRecoveryCandidates(a, b *RecoveryCandidate) []RecoveryConflict {
	byStreamVersion := make(map[string]map[int64]RecordedEvent)
	for _, event := range a.Events {
		versions := byStreamVersion[event.StreamID]
		if versions == nil {
			versions = make(map[int64]RecordedEvent)
			byStreamVersion[event.StreamID] = versions
		}
		versions[event.Version] = event
	}
	var conflicts []RecoveryConflict
	for _, event := range b.Events {
		other, ok := byStreamVersion[event.StreamID][event.Version]
		if !ok {
			continue
		}
		if !sameEventContent(other, event) {
			conflicts = append(conflicts, RecoveryConflict{
				StreamID: event.StreamID,
				Version:  event.Version,
				Source:   "candidate",
				Expected: other,
				Actual:   event,
			})
		}
	}
	return conflicts
}

// CommitRecovery applies an operator-approved candidate: it writes a new
// branch log containing the confirmed original history plus the recovered
// events, persists a manifest recording the original file, the confirmed
// range and the recovery basis, and switches the store to the branch view.
// The original log file is never modified.
func (s *FileEventStore) CommitRecovery(ctx context.Context, candidate *RecoveryCandidate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.corruption == nil {
		return fmt.Errorf("replay: no corruption to recover from")
	}
	if s.recovery != nil {
		return fmt.Errorf("replay: a recovery branch is already active")
	}
	if len(candidate.Conflicts) > 0 {
		return RecoveryConflictError{Conflicts: candidate.Conflicts}
	}

	id, err := s.nextRecoveryID()
	if err != nil {
		return err
	}
	branchPath := filepath.Join(s.dir, fmt.Sprintf("events.recovery-%d.log", id))

	// Recovered events get fresh IDs so the branch stays internally ordered.
	recovered := make([]RecordedEvent, len(candidate.Events))
	copy(recovered, candidate.Events)
	nextID := s.nextID
	for i := range recovered {
		nextID++
		recovered[i].ID = nextID
	}

	branch, err := os.OpenFile(branchPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("replay: create recovery branch log: %w", err)
	}
	var buf []byte
	for _, event := range append(append([]RecordedEvent(nil), s.history...), recovered...) {
		frame, err := json.Marshal(event)
		if err != nil {
			branch.Close()
			os.Remove(branchPath)
			return fmt.Errorf("replay: encode recovery branch event: %w", err)
		}
		buf = append(buf, frameHeader(frame)...)
		buf = append(buf, frame...)
	}
	if _, err := branch.Write(buf); err != nil {
		branch.Close()
		os.Remove(branchPath)
		return fmt.Errorf("replay: write recovery branch log: %w", err)
	}
	if err := branch.Sync(); err != nil {
		branch.Close()
		os.Remove(branchPath)
		return fmt.Errorf("replay: sync recovery branch log: %w", err)
	}
	if err := branch.Close(); err != nil {
		os.Remove(branchPath)
		return fmt.Errorf("replay: close recovery branch log: %w", err)
	}

	manifest := RecoveryManifest{
		ID:              id,
		CreatedAt:       s.now().UTC(),
		OriginalFile:    filepath.Join(s.dir, originalLogName),
		BranchFile:      branchPath,
		Corruption:      *s.corruption,
		MaterialPath:    candidate.MaterialPath,
		RecoveredRanges: candidate.Ranges,
		Gaps:            candidate.Gaps,
	}
	if err := s.persistManifest(manifest); err != nil {
		os.Remove(branchPath)
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}

	// Switch the active view to the branch; the original log stays untouched.
	branchLog, err := os.OpenFile(branchPath, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("replay: open recovery branch log: %w", err)
	}
	scan, err := scanFrames(branchLog)
	if err != nil {
		branchLog.Close()
		return err
	}
	if scan.corruptOffset >= 0 || scan.incompleteTail {
		branchLog.Close()
		return fmt.Errorf("replay: freshly written recovery branch did not scan clean")
	}
	s.log.Close()
	s.log = branchLog
	s.path = branchPath
	s.applyScan(scan)
	s.offset = scan.confirmedBytes
	s.recovery = &manifest
	return nil
}

func (s *FileEventStore) manifestPath(id int) string {
	return filepath.Join(s.dir, fmt.Sprintf("events.recovery-%d.json", id))
}

func (s *FileEventStore) persistManifest(manifest RecoveryManifest) error {
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("replay: encode recovery manifest: %w", err)
	}
	finalPath := s.manifestPath(manifest.ID)
	tmpPath := finalPath + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return fmt.Errorf("replay: write recovery manifest: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replay: publish recovery manifest: %w", err)
	}
	return nil
}

// persistCorruptionReport keeps the corruption fact on disk for the
// operator, next to the preserved original log.
func (s *FileEventStore) persistCorruptionReport() error {
	payload, err := json.MarshalIndent(s.corruption, "", "  ")
	if err != nil {
		return fmt.Errorf("replay: encode corruption report: %w", err)
	}
	path := filepath.Join(s.dir, "events.corruption.json")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("replay: write corruption report: %w", err)
	}
	return syncDir(s.dir)
}

func (s *FileEventStore) nextRecoveryID() (int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, fmt.Errorf("replay: list event store dir: %w", err)
	}
	maxID := 0
	for _, entry := range entries {
		var id int
		if n, _ := fmt.Sscanf(entry.Name(), "events.recovery-%d.json", &id); n == 1 && id > maxID {
			maxID = id
		}
	}
	return maxID + 1, nil
}

// loadRecoveryBranch re-activates a previously committed recovery after a
// reopen: the original log is re-scanned (and its corruption re-reported),
// then the branch log becomes the served view.
func (s *FileEventStore) loadRecoveryBranch() error {
	id, err := s.nextRecoveryID()
	if err != nil {
		return err
	}
	if id == 1 {
		return nil
	}
	manifestID := id - 1
	payload, err := os.ReadFile(s.manifestPath(manifestID))
	if err != nil {
		return fmt.Errorf("replay: read recovery manifest: %w", err)
	}
	var manifest RecoveryManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return fmt.Errorf("replay: decode recovery manifest: %w", err)
	}
	branchLog, err := os.OpenFile(manifest.BranchFile, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("replay: open recovery branch log: %w", err)
	}
	scan, err := scanFrames(branchLog)
	if err != nil {
		branchLog.Close()
		return err
	}
	if scan.corruptOffset >= 0 || scan.incompleteTail {
		branchLog.Close()
		return fmt.Errorf("replay: recovery branch log is damaged; original file %q is preserved", manifest.OriginalFile)
	}
	s.log.Close()
	s.log = branchLog
	s.path = manifest.BranchFile
	s.applyScan(scan)
	s.offset = scan.confirmedBytes
	s.recovery = &manifest
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("replay: open event store dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("replay: sync event store dir: %w", err)
	}
	return nil
}
