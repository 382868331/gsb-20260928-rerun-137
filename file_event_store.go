package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const segmentIDWidth = 20

// FileEventStore stores each successful Append as one immutable segment.
// A segment is written and fsynced under a temporary name, then atomically
// renamed into the log. Temporary segments remain isolated after a crash and
// are never treated as confirmed history.
type FileEventStore struct {
	root     string
	eventDir string
	lockPath string
	lockFile *os.File
	mu       sync.RWMutex
	now      func() time.Time
}

func NewFileEventStore(root string) (*FileEventStore, error) {
	eventDir := filepath.Join(root, "events")
	if err := os.MkdirAll(eventDir, 0o755); err != nil {
		return nil, fmt.Errorf("replay: create event store: %w", err)
	}
	if err := removePendingSegments(eventDir); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(root, "event-store.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("replay: open event store lock: %w", err)
	}
	if err := fsyncDir(root); err != nil {
		lockFile.Close()
		return nil, err
	}
	return &FileEventStore{
		root:     root,
		eventDir: eventDir,
		lockPath: lockPath,
		lockFile: lockFile,
		now:      time.Now,
	}, nil
}

func (s *FileEventStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return nil
	}
	err := s.lockFile.Close()
	s.lockFile = nil
	return err
}

func (s *FileEventStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...UnsavedEvent) ([]RecordedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	if streamID == "" {
		return nil, fmt.Errorf("replay: stream id cannot be empty")
	}
	if expectedVersion < 0 {
		return nil, fmt.Errorf("replay: expected version cannot be negative")
	}

	var recorded []RecordedEvent
	err := s.withAppendLock(func() error {
		segments, err := s.loadSegmentsLocked()
		if err != nil {
			return err
		}
		actualVersion, nextID, err := streamStatus(segments, streamID)
		if err != nil {
			return err
		}
		if actualVersion != expectedVersion {
			return ConcurrencyError{StreamID: streamID, ExpectedVersion: expectedVersion, ActualVersion: actualVersion}
		}

		recorded = make([]RecordedEvent, 0, len(events))
		for i, event := range events {
			if event.Type == "" {
				return fmt.Errorf("replay: event at position %d has an empty type", i)
			}
			payload, err := json.Marshal(event.Data)
			if err != nil {
				return fmt.Errorf("replay: encode event %q: %w", event.Type, err)
			}
			nextID++
			recorded = append(recorded, RecordedEvent{
				ID:         nextID,
				StreamID:   streamID,
				Version:    actualVersion + int64(i) + 1,
				Type:       event.Type,
				Data:       payload,
				OccurredAt: s.now().UTC(),
			})
		}
		return s.commitSegmentLocked(recorded)
	})
	if err != nil {
		return nil, err
	}
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
	if afterVersion < 0 {
		return nil, 0, fmt.Errorf("replay: after version cannot be negative")
	}

	var events []RecordedEvent
	var version int64
	err := s.withReadLock(func() error {
		segments, err := s.loadSegmentsLocked()
		if err != nil {
			return err
		}
		events, version, err = readTailLocked(segments, streamID, afterVersion)
		return err
	})
	return events, version, err
}

func (s *FileEventStore) StreamVersion(ctx context.Context, streamID string) (int64, error) {
	_, version, err := s.ReadTail(ctx, streamID, 0)
	return version, err
}

func (s *FileEventStore) WithAppendLock(fn func() error) error {
	return s.withAppendLock(fn)
}

func (s *FileEventStore) readTailLocked(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	segments, err := s.loadSegmentsLocked()
	if err != nil {
		return nil, 0, err
	}
	return readTailLocked(segments, streamID, afterVersion)
}

func (s *FileEventStore) withReadLock(fn func() error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_SH); err != nil {
		return fmt.Errorf("replay: acquire shared log lock: %w", err)
	}
	defer syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	return fn()
}

func (s *FileEventStore) withAppendLock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("replay: acquire exclusive log lock: %w", err)
	}
	defer syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	return fn()
}

func (s *FileEventStore) commitSegmentLocked(events []RecordedEvent) error {
	payload, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("replay: encode event segment: %w", err)
	}
	name := segmentName(events[0].ID, events[len(events)-1].ID)
	finalPath := filepath.Join(s.eventDir, name)
	tempPath := finalPath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("replay: create event segment: %w", err)
	}
	writeFailed := false
	if _, err := file.Write(payload); err != nil {
		writeFailed = true
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("replay: write event segment: %w", err)
	}
	if err := file.Sync(); err != nil {
		writeFailed = true
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("replay: fsync event segment: %w", err)
	}
	if err := file.Close(); err != nil {
		writeFailed = true
		os.Remove(tempPath)
		return fmt.Errorf("replay: close event segment: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		if !writeFailed {
			os.Remove(tempPath)
		}
		return fmt.Errorf("replay: publish event segment: %w", err)
	}
	return fsyncDir(s.eventDir)
}

func (s *FileEventStore) loadSegmentsLocked() ([][]RecordedEvent, error) {
	// Pending files are deliberately ignored. They may contain only part of
	// an Append and cannot be treated as a complete committed batch.
	entries, err := os.ReadDir(s.eventDir)
	if err != nil {
		return nil, fmt.Errorf("replay: read event log: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	segments := make([][]RecordedEvent, 0, len(names))
	var lastSegmentEnd int64
	for _, name := range names {
		payload, err := os.ReadFile(filepath.Join(s.eventDir, name))
		if err != nil {
			return nil, fmt.Errorf("replay: read event segment %q: %w", name, err)
		}
		var events []RecordedEvent
		if err := json.Unmarshal(payload, &events); err != nil {
			return nil, fmt.Errorf("replay: corrupt confirmed event segment %q: %w", name, err)
		}
		if len(events) == 0 {
			return nil, fmt.Errorf("replay: empty confirmed event segment %q", name)
		}
		startID, endID, ok, err := parseSegmentName(name)
		if err != nil || !ok || events[0].ID != startID || events[len(events)-1].ID != endID {
			return nil, fmt.Errorf("replay: event segment %q does not match its event id range", name)
		}
		if startID <= lastSegmentEnd {
			return nil, fmt.Errorf("replay: event segment %q is out of order", name)
		}
		for i := 1; i < len(events); i++ {
			if events[i].ID != events[i-1].ID+1 {
				return nil, fmt.Errorf("replay: event segment %q has a non-contiguous id range", name)
			}
		}
		lastSegmentEnd = endID
		segments = append(segments, events)
	}
	return segments, nil
}

func removePendingSegments(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("replay: scan event log: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		if err := removeIfExists(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("replay: remove unpublished event segment: %w", err)
		}
	}
	return nil
}

func readTailLocked(segments [][]RecordedEvent, streamID string, afterVersion int64) ([]RecordedEvent, int64, error) {
	version, _, err := streamStatus(segments, streamID)
	if err != nil {
		return nil, 0, err
	}
	if afterVersion > version {
		return nil, 0, fmt.Errorf("replay: checkpoint %d is ahead of stream %q version %d", afterVersion, streamID, version)
	}
	result := make([]RecordedEvent, 0, max(0, version-afterVersion))
	for _, segment := range segments {
		for _, event := range segment {
			if event.StreamID == streamID && event.Version > afterVersion {
				result = append(result, event)
			}
		}
	}
	return result, version, nil
}

func streamStatus(segments [][]RecordedEvent, streamID string) (int64, int64, error) {
	streamVersionByID := make(map[string]int64)
	var version int64
	var nextID int64
	for _, segment := range segments {
		for _, event := range segment {
			if event.ID <= 0 {
				return 0, 0, fmt.Errorf("replay: event has invalid id %d", event.ID)
			}
			if event.ID > nextID {
				nextID = event.ID
			}
			expected := streamVersionByID[event.StreamID] + 1
			if event.Version != expected {
				return 0, 0, fmt.Errorf("replay: stream %q has version gap or duplicate: expected %d, got %d", event.StreamID, expected, event.Version)
			}
			streamVersionByID[event.StreamID] = event.Version
			if event.StreamID == streamID {
				version = event.Version
			}
		}
	}
	return version, nextID, nil
}

func segmentName(startID, endID int64) string {
	return fmt.Sprintf("%0*d-%0*d.json", segmentIDWidth, startID, segmentIDWidth, endID)
}

func parseSegmentName(name string) (int64, int64, bool, error) {
	parts := strings.Split(strings.TrimSuffix(name, ".json"), "-")
	if len(parts) != 2 {
		return 0, 0, false, nil
	}
	startID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, nil
	}
	endID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || startID <= 0 || endID < startID {
		return 0, 0, false, nil
	}
	return startID, endID, true, nil
}

func streamHash(streamID string) string {
	hash := sha256.Sum256([]byte(streamID))
	return hex.EncodeToString(hash[:])
}

func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("replay: open directory %q for fsync: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("replay: fsync directory %q: %w", path, err)
	}
	return nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
