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
// On open, the log is scanned and the two failure shapes are told apart:
//
//   - an incomplete tail (a crash mid-append) is truncated, so an unfinished
//     append is isolated and never replayed as part of the confirmed history;
//   - a complete frame whose checksum or payload is invalid is committed
//     content that was damaged after confirmation. It is NOT truncated away:
//     the store opens in a damaged state, keeps the corrupt bytes on disk,
//     blocks further appends, and reports the Damage so an operator can
//     reconcile it with saved trusted material (see recovery.go).
type FileEventStore struct {
	mu            sync.Mutex
	dir           string
	path          string
	log           *os.File
	offset        int64
	streams       map[string][]RecordedEvent
	nextID        int64
	damage        *Damage
	truncatedTail bool
	recovery      *RecoveryRecord
	gaps          map[string][]VersionRange
	now           func() time.Time
}

const frameHeaderSize = 8

// maxFrameSize bounds a plausible frame length. A length header beyond this
// is treated as committed corruption, not as an unfinished write.
const maxFrameSize = 1 << 26

// OpenFileEventStore opens (or creates) the event log in dir and recovers the
// confirmed history, truncating any unconfirmed tail.
func OpenFileEventStore(dir string) (*FileEventStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("replay: create event store dir: %w", err)
	}
	path := filepath.Join(dir, "events.log")
	log, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("replay: open event log: %w", err)
	}
	store := &FileEventStore{
		dir:     dir,
		path:    path,
		log:     log,
		streams: make(map[string][]RecordedEvent),
		gaps:    make(map[string][]VersionRange),
		now:     time.Now,
	}
	if err := store.recover(); err != nil {
		log.Close()
		return nil, err
	}
	if err := store.loadRecoveryRecord(); err != nil {
		log.Close()
		return nil, err
	}
	return store, nil
}

func (s *FileEventStore) recover() error {
	if _, err := s.log.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("replay: seek event log: %w", err)
	}
	var offset int64
	for {
		header := make([]byte, frameHeaderSize)
		n, err := io.ReadFull(s.log, header)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return s.truncate(offset)
			}
			return fmt.Errorf("replay: read event log: %w", err)
		}
		checksum := binary.BigEndian.Uint32(header[:4])
		length := binary.BigEndian.Uint32(header[4:])
		if length > maxFrameSize {
			// An implausible length header means a confirmed frame was
			// damaged on disk, not a crash mid-append.
			s.setDamage(Damage{Offset: offset, Reason: "frame length header is implausible"})
			break
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(s.log, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return s.truncate(offset)
			}
			return fmt.Errorf("replay: read event log: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			s.setDamage(Damage{Offset: offset, Length: int64(length), Reason: "checksum mismatch on a complete committed frame"})
			break
		}
		var event RecordedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			s.setDamage(Damage{Offset: offset, Length: int64(length), Reason: "committed frame payload is not valid JSON"})
			break
		}
		s.streams[event.StreamID] = append(s.streams[event.StreamID], event)
		if event.ID > s.nextID {
			s.nextID = event.ID
		}
		offset += frameHeaderSize + int64(length)
	}
	s.offset = offset
	return nil
}

// setDamage records committed corruption found during recovery. The damaged
// bytes stay on disk untouched so the operator keeps the evidence.
func (s *FileEventStore) setDamage(d Damage) {
	s.damage = &d
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
	s.truncatedTail = true
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
	if s.damage != nil {
		return nil, fmt.Errorf("replay: event log has committed corruption at offset %d (%s); resolve it through recovery before appending", s.damage.Offset, s.damage.Reason)
	}
	history := s.streams[streamID]
	actualVersion := headVersionLocked(history)
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
	head := headVersionLocked(history)
	if afterVersion < 0 {
		return nil, 0, fmt.Errorf("replay: after version cannot be negative")
	}
	if afterVersion > head {
		return nil, 0, fmt.Errorf("replay: checkpoint %d is ahead of stream %q version %d", afterVersion, streamID, head)
	}

	// Filter by recorded version rather than slicing by position: a recovery
	// branch may carry gaps where no trusted material could cover a version.
	result := make([]RecordedEvent, 0, len(history))
	for _, event := range history {
		if event.Version > afterVersion {
			result = append(result, event)
		}
	}
	return result, head, nil
}

func (s *FileEventStore) StreamVersion(ctx context.Context, streamID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return headVersionLocked(s.streams[streamID]), nil
}

// headVersionLocked reports the highest confirmed version of a stream. For a
// continuous history this equals the event count; for a recovery branch with
// gaps it is the version of the last confirmed event.
func headVersionLocked(history []RecordedEvent) int64 {
	if len(history) == 0 {
		return 0
	}
	return history[len(history)-1].Version
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
	return headVersionLocked(v.store.streams[streamID]), nil
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
