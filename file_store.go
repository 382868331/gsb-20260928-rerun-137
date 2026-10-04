package replay

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileEventStore persists events in an append-only log so confirmed events
// survive process restarts. Each record is framed as
//
//	[4-byte CRC32 of payload][4-byte payload length][JSON payload]
//
// and is only visible to readers after the frame is fully written and fsynced.
// On open, the log is scanned and any incomplete or corrupt tail (for example
// from a crash mid-append) is truncated, so an unfinished append is isolated
// and never replayed as part of the confirmed history.
type FileEventStore struct {
	mu      sync.Mutex
	dir     string
	path    string
	log     *os.File
	offset  int64
	history []RecordedEvent
	streams map[string][]RecordedEvent
	nextID  int64
	now     func() time.Time

	corruption *CorruptionReport
	recovery   *RecoveryManifest
}

const frameHeaderSize = 8

// originalLogName is the file the operator's confirmed history lives in; a
// committed recovery never modifies it and writes its own branch log.
const originalLogName = "events.log"

// OpenFileEventStore opens (or creates) the event log in dir and recovers the
// confirmed history, truncating any unconfirmed tail.
func OpenFileEventStore(dir string) (*FileEventStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("replay: create event store dir: %w", err)
	}
	path := filepath.Join(dir, originalLogName)
	log, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("replay: open event log: %w", err)
	}
	store := &FileEventStore{
		dir:     dir,
		path:    path,
		log:     log,
		streams: make(map[string][]RecordedEvent),
		now:     time.Now,
	}
	if err := store.recover(); err != nil {
		log.Close()
		return nil, err
	}
	return store, nil
}

func (s *FileEventStore) recover() error {
	scan, err := scanFrames(s.log)
	if err != nil {
		return err
	}
	s.applyScan(scan)
	switch {
	case scan.corruptOffset >= 0:
		// A complete frame is damaged: confirmed content, not an
		// unfinished write. Preserve the file exactly as it is, record the
		// corruption for the operator, and open in the degraded view.
		info, statErr := s.log.Stat()
		if statErr != nil {
			return fmt.Errorf("replay: stat event log: %w", statErr)
		}
		s.corruption = &CorruptionReport{
			Offset:         scan.corruptOffset,
			Reason:         scan.corruptReason,
			ConfirmedBytes: scan.confirmedBytes,
			PreservedBytes: info.Size() - scan.corruptOffset,
		}
		s.offset = scan.confirmedBytes
		if err := s.persistCorruptionReport(); err != nil {
			return err
		}
	case scan.incompleteTail:
		// A partial frame at the end is an unconfirmed write; isolate it.
		if err := s.truncate(scan.confirmedBytes); err != nil {
			return err
		}
	default:
		s.offset = scan.confirmedBytes
	}
	return s.loadRecoveryBranch()
}

// frameScan is the result of scanning a framed event log.
type frameScan struct {
	events         []RecordedEvent
	nextID         int64
	confirmedBytes int64
	corruptOffset  int64
	corruptReason  string
	incompleteTail bool
}

// scanFrames reads every frame of f and classifies how the log ends: clean,
// an incomplete tail (unconfirmed write), or a corrupt complete frame
// (damaged confirmed content).
func scanFrames(f *os.File) (frameScan, error) {
	scan := frameScan{corruptOffset: -1}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return scan, fmt.Errorf("replay: seek event log: %w", err)
	}
	var offset int64
	for {
		header := make([]byte, frameHeaderSize)
		n, err := io.ReadFull(f, header)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				scan.incompleteTail = true
				break
			}
			return scan, fmt.Errorf("replay: read event log: %w", err)
		}
		checksum := binary.BigEndian.Uint32(header[:4])
		length := binary.BigEndian.Uint32(header[4:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				scan.incompleteTail = true
				break
			}
			return scan, fmt.Errorf("replay: read event log: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			scan.corruptOffset = offset
			scan.corruptReason = "checksum mismatch on a complete frame"
			break
		}
		var event RecordedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			scan.corruptOffset = offset
			scan.corruptReason = "complete frame does not decode: " + err.Error()
			break
		}
		scan.events = append(scan.events, event)
		if event.ID > scan.nextID {
			scan.nextID = event.ID
		}
		offset += frameHeaderSize + int64(length)
	}
	scan.confirmedBytes = offset
	return scan, nil
}

// applyScan replaces the in-memory history with a scan result.
func (s *FileEventStore) applyScan(scan frameScan) {
	s.history = append([]RecordedEvent(nil), scan.events...)
	s.streams = make(map[string][]RecordedEvent)
	for _, event := range scan.events {
		s.streams[event.StreamID] = append(s.streams[event.StreamID], event)
	}
	s.nextID = scan.nextID
}

// truncate isolates an unconfirmed tail: everything from offset onward is
// dropped and the log is synced so later appends start from a clean point.
func (s *FileEventStore) truncate(offset int64) error {
	if err := s.log.Truncate(offset); err != nil {
		return fmt.Errorf("replay: truncate unconfirmed tail: %w", err)
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("replay: sync truncated event log: %w", err)
	}
	if _, err := s.log.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("replay: seek event log: %w", err)
	}
	s.offset = offset
	return nil
}

func (s *FileEventStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...UnsavedEvent) ([]RecordedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(streamID, expectedVersion, events...)
}

func (s *FileEventStore) appendLocked(streamID string, expectedVersion int64, events ...UnsavedEvent) ([]RecordedEvent, error) {
	if s.corruption != nil && s.recovery == nil {
		return nil, CorruptedHistoryError{Report: *s.corruption}
	}
	history := s.streams[streamID]
	actualVersion := int64(len(history))
	if actualVersion != expectedVersion {
		return nil, ConcurrencyError{StreamID: streamID, ExpectedVersion: expectedVersion, ActualVersion: actualVersion}
	}

	recorded := make([]RecordedEvent, 0, len(events))
	var buf []byte
	for i, event := range events {
		if event.Type == "" {
			return nil, fmt.Errorf("replay: event at position %d has an empty type", i)
		}
		payload, err := json.Marshal(event.Data)
		if err != nil {
			return nil, fmt.Errorf("replay: encode event %q: %w", event.Type, err)
		}
		s.nextID++
		recordedEvent := RecordedEvent{
			ID:         s.nextID,
			StreamID:   streamID,
			Version:    actualVersion + int64(i) + 1,
			Type:       event.Type,
			Data:       payload,
			OccurredAt: s.now().UTC(),
		}
		frame, err := json.Marshal(recordedEvent)
		if err != nil {
			return nil, fmt.Errorf("replay: encode recorded event %q: %w", event.Type, err)
		}
		header := make([]byte, frameHeaderSize)
		binary.BigEndian.PutUint32(header[:4], crc32.ChecksumIEEE(frame))
		binary.BigEndian.PutUint32(header[4:], uint32(len(frame)))
		buf = append(buf, header...)
		buf = append(buf, frame...)
		recorded = append(recorded, recordedEvent)
	}

	committedOffset := s.offset
	if _, err := s.log.WriteAt(buf, committedOffset); err != nil {
		return nil, fmt.Errorf("replay: write event log: %w", err)
	}
	if err := s.log.Sync(); err != nil {
		// The append is not confirmed; isolate whatever was written so a
		// later recovery never replays it as history.
		_ = s.truncate(committedOffset)
		return nil, fmt.Errorf("replay: sync event log: %w", err)
	}
	s.offset = committedOffset + int64(len(buf))
	s.streams[streamID] = append(history, recorded...)
	s.history = append(s.history, recorded...)
	return recorded, nil
}

func (s *FileEventStore) ReadStream(ctx context.Context, streamID string) ([]RecordedEvent, error) {
	return s.ReadStreamAfter(ctx, streamID, 0)
}

func (s *FileEventStore) ReadStreamAfter(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, error) {
	events, _, err := s.ReadTail(ctx, streamID, afterVersion)
	return events, err
}

func (s *FileEventStore) ReadTail(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readTailLocked(streamID, afterVersion)
}

func (s *FileEventStore) readTailLocked(streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	history := s.streams[streamID]
	if afterVersion < 0 {
		return nil, 0, fmt.Errorf("replay: after version cannot be negative")
	}
	if afterVersion > int64(len(history)) {
		return nil, 0, fmt.Errorf("replay: checkpoint %d is ahead of stream %q version %d", afterVersion, streamID, len(history))
	}

	result := make([]RecordedEvent, len(history[afterVersion:]))
	copy(result, history[afterVersion:])
	return result, int64(len(history)), nil
}

func (s *FileEventStore) StreamVersion(ctx context.Context, streamID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.streams[streamID])), nil
}

// WithAppendLock takes the same mutex that protects appends and reads, so a
// projection can replay its tail and cut over without losing events that
// arrive in between.
func (s *FileEventStore) WithAppendLock(fn func(locked EventStore) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(fileLockedView{store: s})
}

// fileLockedView runs store operations while WithAppendLock holds the store
// mutex; it must not escape the callback.
type fileLockedView struct {
	store *FileEventStore
}

func (fileLockedView) Append(context.Context, string, int64, ...UnsavedEvent) ([]RecordedEvent, error) {
	return nil, fmt.Errorf("replay: appends are not allowed inside the append lock")
}

func (v fileLockedView) ReadStream(ctx context.Context, streamID string) ([]RecordedEvent, error) {
	return v.ReadStreamAfter(ctx, streamID, 0)
}

func (v fileLockedView) ReadStreamAfter(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, error) {
	events, _, err := v.ReadTail(ctx, streamID, afterVersion)
	return events, err
}

func (v fileLockedView) ReadTail(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return v.store.readTailLocked(streamID, afterVersion)
}

func (v fileLockedView) StreamVersion(ctx context.Context, streamID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return int64(len(v.store.streams[streamID])), nil
}

// View describes which history the store is currently serving: the clean
// original log, the degraded original log (corrupt confirmed content, not
// yet recovered), or a recovery branch committed by the operator.
func (s *FileEventStore) View() ViewInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.viewLocked()
}

func (s *FileEventStore) viewLocked() ViewInfo {
	view := ViewInfo{Kind: ViewOriginal, ActiveFile: s.path, OriginalFile: filepath.Join(s.dir, originalLogName)}
	if s.corruption != nil {
		report := *s.corruption
		view.Corruption = &report
		view.Kind = ViewDegraded
	}
	if s.recovery != nil {
		manifest := *s.recovery
		view.Recovery = &manifest
		view.Kind = ViewRecoveryBranch
	}
	return view
}

// ReadStreamView reads the stream like ReadStream and also reports which
// view the returned events belong to, so callers never mistake a degraded
// or recovered history for the clean original.
func (s *FileEventStore) ReadStreamView(ctx context.Context, streamID string) ([]RecordedEvent, ViewInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ViewInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events, _, err := s.readTailLocked(streamID, 0)
	if err != nil {
		return nil, ViewInfo{}, err
	}
	return events, s.viewLocked(), nil
}

// Close flushes and closes the event log.
func (s *FileEventStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("replay: sync event log: %w", err)
	}
	return s.log.Close()
}
