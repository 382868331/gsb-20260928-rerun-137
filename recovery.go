package replay

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"
)

// Damage describes committed log content that was found corrupt on open.
// Unlike an unfinished tail write, this is history that was once confirmed;
// the store keeps the corrupt bytes on disk and blocks appends until an
// operator resolves the damage through ConfirmRecovery.
type Damage struct {
	Offset int64  `json:"offset"`
	Length int64  `json:"length,omitempty"`
	Reason string `json:"reason"`
}

// OpenReport tells the two recovery outcomes apart: an isolated unfinished
// tail write versus preserved committed corruption.
type OpenReport struct {
	TruncatedTail bool    `json:"truncated_tail"`
	Damage        *Damage `json:"damage,omitempty"`
}

// VersionRange is an inclusive range of stream versions.
type VersionRange struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// RecoveryMaterial is trusted event material saved outside the event log
// (for example an exported copy). It is used to check how far the damaged
// history can be recovered.
type RecoveryMaterial struct {
	Source string
	Events []RecordedEvent
}

// MaterialConflict records that two trusted materials disagree on the
// content of the same stream version; that version stays a gap.
type MaterialConflict struct {
	StreamID string   `json:"stream_id"`
	Version  int64    `json:"version"`
	Sources  []string `json:"sources"`
}

// RecoveryCandidate is the recoverable view of a damaged stream: the prefix
// confirmed in the original log, the versions trusted material can vouch
// for, and the gaps nothing could cover. Gaps are kept as gaps; the
// candidate never fabricates a continuous history.
type RecoveryCandidate struct {
	StreamID  string
	Confirmed []RecordedEvent
	Recovered []RecordedEvent
	Gaps      []VersionRange
	Conflicts []MaterialConflict
}

// RecoveryRecord is the persisted basis of a confirmed recovery branch: the
// original file that was kept, the originally confirmed range, the damage
// that was found, and the material the recovery relied on.
type RecoveryRecord struct {
	StreamID         string            `json:"stream_id"`
	OriginalFile     string            `json:"original_file"`
	ConfirmedThrough int64             `json:"confirmed_through"`
	Damage           Damage            `json:"damage"`
	MaterialDigests  map[string]string `json:"material_digests"`
	Gaps             []VersionRange    `json:"gaps,omitempty"`
	RecoveredThrough int64             `json:"recovered_through"`
	RecoveredAt      time.Time         `json:"recovered_at"`
	// Decisions records the operator review decisions (range, action,
	// reason, material) this branch was confirmed from, if any.
	Decisions []RangeDecision `json:"decisions,omitempty"`
}

// ViewKind identifies which history a read result belongs to.
type ViewKind string

const (
	// ViewOriginal is the history as confirmed in the original event log.
	ViewOriginal ViewKind = "original"
	// ViewRecovery is the branch formed by an operator-confirmed recovery.
	ViewRecovery ViewKind = "recovery"
)

// StreamView describes the view a stream is read from, including unresolved
// damage and, on a recovery branch, the remaining gaps and the recovery
// basis.
type StreamView struct {
	Kind     ViewKind
	Damaged  bool
	Damage   *Damage
	Gaps     []VersionRange
	Recovery *RecoveryRecord
}

// ViewedEvents pairs read events with the view they belong to.
type ViewedEvents struct {
	Events []RecordedEvent
	View   StreamView
}

const recoveryRecordFile = "events.recovery.json"

// Damage reports the committed corruption found on open, if any.
func (s *FileEventStore) Damage() (Damage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.damage == nil {
		return Damage{}, false
	}
	return *s.damage, true
}

// OpenReport distinguishes an isolated unfinished tail write from preserved
// committed corruption after the last open.
func (s *FileEventStore) OpenReport() OpenReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	report := OpenReport{TruncatedTail: s.truncatedTail}
	if s.damage != nil {
		damage := *s.damage
		report.Damage = &damage
	}
	return report
}

// View describes the view a stream is currently read from.
func (s *FileEventStore) View(streamID string) StreamView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.viewLocked(streamID)
}

func (s *FileEventStore) viewLocked(streamID string) StreamView {
	view := StreamView{Kind: ViewOriginal}
	if s.recovery != nil {
		view.Kind = ViewRecovery
		recovery := *s.recovery
		view.Recovery = &recovery
	}
	if s.damage != nil {
		view.Damaged = true
		damage := *s.damage
		view.Damage = &damage
	}
	if gaps := s.gaps[streamID]; len(gaps) > 0 {
		view.Gaps = append([]VersionRange(nil), gaps...)
	}
	return view
}

// ReadStreamWithView reads the stream and states which view the result
// belongs to, so callers can tell original history from a recovery branch.
func (s *FileEventStore) ReadStreamWithView(ctx context.Context, streamID string) (ViewedEvents, error) {
	if err := ctx.Err(); err != nil {
		return ViewedEvents{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events, _, err := s.readTailLocked(streamID, 0)
	if err != nil {
		return ViewedEvents{}, err
	}
	return ViewedEvents{Events: events, View: s.viewLocked(streamID)}, nil
}

// BuildRecoveryCandidate checks saved trusted material against the damaged
// log and returns the recoverable view of the stream. Versions no material
// covers — or that materials disagree on — stay gaps. Material that
// contradicts the already confirmed prefix is reported as a conflict.
func (s *FileEventStore) BuildRecoveryCandidate(streamID string, materials ...RecoveryMaterial) (RecoveryCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buildRecoveryCandidateLocked(streamID, materials...)
}

// buildRecoveryCandidateLocked builds the candidate while the caller holds
// the store mutex (see BuildRecoveryCandidate).
func (s *FileEventStore) buildRecoveryCandidateLocked(streamID string, materials ...RecoveryMaterial) (RecoveryCandidate, error) {
	if s.damage == nil {
		return RecoveryCandidate{}, fmt.Errorf("replay: no committed corruption to recover from")
	}
	confirmed := append([]RecordedEvent(nil), s.streams[streamID]...)
	candidate := RecoveryCandidate{StreamID: streamID, Confirmed: confirmed}
	confirmedThrough := headVersionLocked(confirmed)

	byVersion := make(map[int64][]RecordedEvent)
	sourcesByVersion := make(map[int64][]string)
	var maxVersion int64
	for _, material := range materials {
		for _, event := range material.Events {
			if event.StreamID != streamID {
				continue
			}
			byVersion[event.Version] = append(byVersion[event.Version], event)
			sourcesByVersion[event.Version] = append(sourcesByVersion[event.Version], material.Source)
			if event.Version > maxVersion {
				maxVersion = event.Version
			}
		}
	}

	// Material must not contradict the prefix the original log confirmed.
	for version, events := range byVersion {
		if version > confirmedThrough {
			continue
		}
		for _, event := range events {
			if !recordedEventsEqual(event, confirmed[version-1]) {
				candidate.Conflicts = append(candidate.Conflicts, MaterialConflict{
					StreamID: streamID,
					Version:  version,
					Sources:  append([]string{"event-log"}, sourcesByVersion[version]...),
				})
				break
			}
		}
	}

	for version := confirmedThrough + 1; version <= maxVersion; version++ {
		events := byVersion[version]
		if len(events) == 0 {
			candidate.Gaps = appendGap(candidate.Gaps, version)
			continue
		}
		agreed := events[0]
		conflict := false
		for _, event := range events[1:] {
			if !recordedEventsEqual(event, agreed) {
				conflict = true
				break
			}
		}
		if conflict {
			candidate.Conflicts = append(candidate.Conflicts, MaterialConflict{
				StreamID: streamID,
				Version:  version,
				Sources:  sourcesByVersion[version],
			})
			candidate.Gaps = appendGap(candidate.Gaps, version)
			continue
		}
		candidate.Recovered = append(candidate.Recovered, agreed)
	}
	sort.Slice(candidate.Conflicts, func(i, j int) bool {
		return candidate.Conflicts[i].Version < candidate.Conflicts[j].Version
	})
	return candidate, nil
}

// ConfirmRecovery forms a new branch from an operator-approved candidate.
// The original log file is preserved next to the branch, and a recovery
// record keeps the original confirmed range, the damage, and the material
// the recovery relied on. After confirmation the store serves the recovery
// view and accepts new appends on top of the recovered head.
func (s *FileEventStore) ConfirmRecovery(candidate RecoveryCandidate, materials ...RecoveryMaterial) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.confirmRecoveryLocked(candidate, materials...)
}

// confirmRecoveryLocked forms the recovery branch while the caller holds the
// store mutex (see ConfirmRecovery).
func (s *FileEventStore) confirmRecoveryLocked(candidate RecoveryCandidate, materials ...RecoveryMaterial) error {
	if s.damage == nil {
		return fmt.Errorf("replay: no committed corruption to recover from")
	}
	streamID := candidate.StreamID
	confirmed := s.streams[streamID]
	if !reflect.DeepEqual(candidate.Confirmed, confirmed) {
		return fmt.Errorf("replay: recovery candidate for stream %q does not match the confirmed prefix; rebuild it before confirming", streamID)
	}

	branch := make([]RecordedEvent, 0, len(confirmed)+len(candidate.Recovered))
	branch = append(branch, confirmed...)
	branch = append(branch, candidate.Recovered...)

	// Write the branch log to a temporary file first, then move the original
	// log aside and publish the branch, so an interruption never loses both.
	tmpPath := s.path + ".branch.tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("replay: create branch log: %w", err)
	}
	var buf []byte
	for _, event := range branch {
		frame, err := json.Marshal(event)
		if err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("replay: encode recovered event: %w", err)
		}
		header := make([]byte, frameHeaderSize)
		binary.BigEndian.PutUint32(header[:4], crc32.ChecksumIEEE(frame))
		binary.BigEndian.PutUint32(header[4:], uint32(len(frame)))
		buf = append(buf, header...)
		buf = append(buf, frame...)
	}
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("replay: write branch log: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("replay: sync branch log: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replay: close branch log: %w", err)
	}

	originalPath := s.path + ".original"
	for n := 1; ; n++ {
		if _, err := os.Stat(originalPath); os.IsNotExist(err) {
			break
		}
		originalPath = fmt.Sprintf("%s.original-%d", s.path, n)
	}
	if err := s.log.Close(); err != nil {
		return fmt.Errorf("replay: close original event log: %w", err)
	}
	if err := os.Rename(s.path, originalPath); err != nil {
		return fmt.Errorf("replay: preserve original event log: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("replay: publish branch log: %w", err)
	}

	record := &RecoveryRecord{
		StreamID:         streamID,
		OriginalFile:     filepath.Base(originalPath),
		ConfirmedThrough: headVersionLocked(confirmed),
		Damage:           *s.damage,
		MaterialDigests:  make(map[string]string),
		Gaps:             candidate.Gaps,
		RecoveredThrough: headVersionLocked(branch),
		RecoveredAt:      s.now().UTC(),
	}
	for _, material := range materials {
		record.MaterialDigests[material.Source] = DigestMaterial(material)
	}
	for _, event := range candidate.Recovered {
		if event.ID > s.nextID {
			s.nextID = event.ID
		}
	}
	if err := s.persistRecoveryRecord(record); err != nil {
		return err
	}

	log, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("replay: reopen branch log: %w", err)
	}
	s.log = log
	s.offset = int64(len(buf))
	s.streams[streamID] = branch
	s.gaps[streamID] = append([]VersionRange(nil), candidate.Gaps...)
	s.recovery = record
	s.damage = nil
	return nil
}

// persistRecoveryRecord writes the recovery basis with the same
// write-temp-then-rename discipline as the snapshot store.
func (s *FileEventStore) persistRecoveryRecord(record *RecoveryRecord) error {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("replay: encode recovery record: %w", err)
	}
	path := filepath.Join(s.dir, recoveryRecordFile)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("replay: write recovery record: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replay: publish recovery record: %w", err)
	}
	return nil
}

// loadRecoveryRecord restores the recovery basis on open so a reopened store
// still serves the recovery view and reports its gaps.
func (s *FileEventStore) loadRecoveryRecord() error {
	data, err := os.ReadFile(filepath.Join(s.dir, recoveryRecordFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("replay: read recovery record: %w", err)
	}
	var record RecoveryRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return fmt.Errorf("replay: decode recovery record: %w", err)
	}
	s.recovery = &record
	if len(record.Gaps) > 0 {
		s.gaps[record.StreamID] = append([]VersionRange(nil), record.Gaps...)
	}
	return nil
}

// DigestMaterial fingerprints trusted material so a recovery record can name
// exactly what it relied on.
func DigestMaterial(material RecoveryMaterial) string {
	data, _ := json.Marshal(material.Events)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func recordedEventsEqual(a, b RecordedEvent) bool {
	return a.StreamID == b.StreamID &&
		a.Version == b.Version &&
		a.Type == b.Type &&
		string(a.Data) == string(b.Data)
}

func appendGap(gaps []VersionRange, version int64) []VersionRange {
	if n := len(gaps); n > 0 && gaps[n-1].To == version-1 {
		gaps[n-1].To = version
		return gaps
	}
	return append(gaps, VersionRange{From: version, To: version})
}
