package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileEventStore struct {
	mu      sync.Mutex
	path    string
	streams map[string][]RecordedEvent
	nextID  int64
}

func NewFileEventStore(path string) (*FileEventStore, error) {
	store := &FileEventStore{
		path:    path,
		streams: make(map[string][]RecordedEvent),
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("replay: create event store directory: %w", err)
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileEventStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("replay: open event store: %w", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("replay: sync event store directory entry: %w", err)
	}

	validSize := int64(0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	seenPositions := make(map[int64]struct{})
	lastPosition := int64(0)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			return fmt.Errorf("replay: event store contains an empty line")
		}
		var event RecordedEvent
		if err := json.Unmarshal(line, &event); err != nil {
			break
		}
		if event.ID > s.nextID {
			s.nextID = event.ID
		}
		if event.ID <= 0 || event.Position <= 0 || event.ID != event.Position {
			return fmt.Errorf("replay: event store has invalid event id %d position %d", event.ID, event.Position)
		}
		if event.Position <= lastPosition {
			return fmt.Errorf("replay: event store position %d is not ordered", event.Position)
		}
		if _, exists := seenPositions[event.Position]; exists {
			return fmt.Errorf("replay: event store contains duplicate position %d", event.Position)
		}
		seenPositions[event.Position] = struct{}{}
		lastPosition = event.Position
		s.streams[event.StreamID] = append(s.streams[event.StreamID], event)
		validSize += int64(len(line) + 1)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("replay: read event store: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("replay: inspect event store: %w", err)
	}
	if stat.Size() != validSize {
		if err := file.Truncate(validSize); err != nil {
			return fmt.Errorf("replay: remove incomplete event: %w", err)
		}
		if err := file.Sync(); err != nil {
			return fmt.Errorf("replay: sync event store: %w", err)
		}
	}
	for streamID, history := range s.streams {
		for i, event := range history {
			if event.Version != int64(i+1) {
				return fmt.Errorf("replay: event store has a gap in stream %q at version %d", streamID, event.Version)
			}
		}
	}
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

	history := s.streams[streamID]
	actualVersion := int64(len(history))
	if actualVersion != expectedVersion {
		return nil, ConcurrencyError{StreamID: streamID, ExpectedVersion: expectedVersion, ActualVersion: actualVersion}
	}

	file, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("replay: open event store for append: %w", err)
	}
	defer file.Close()

	recorded := make([]RecordedEvent, 0, len(events))
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
			Position:   s.nextID,
			StreamID:   streamID,
			Version:    actualVersion + int64(i) + 1,
			Type:       event.Type,
			Data:       payload,
			OccurredAt: time.Now().UTC(),
		}
		line, err := json.Marshal(recordedEvent)
		if err != nil {
			return nil, fmt.Errorf("replay: encode stored event: %w", err)
		}
		line = append(line, '\n')
		if _, err := file.Write(line); err != nil {
			return nil, fmt.Errorf("replay: append event: %w", err)
		}
		if err := file.Sync(); err != nil {
			return nil, fmt.Errorf("replay: persist event: %w", err)
		}
		recorded = append(recorded, recordedEvent)
		s.streams[streamID] = append(s.streams[streamID], recordedEvent)
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return nil, err
	}
	return recorded, nil
}

func (s *FileEventStore) ReadStream(ctx context.Context, streamID string) ([]RecordedEvent, error) {
	return s.ReadStreamAfter(ctx, streamID, 0)
}

func (s *FileEventStore) ReadStreamAfter(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if afterVersion < 0 {
		return nil, fmt.Errorf("replay: after version cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readStreamAfterLocked(ctx, streamID, afterVersion)
}

func (s *FileEventStore) readStreamAfterLocked(ctx context.Context, streamID string, afterVersion int64) ([]RecordedEvent, error) {
	history := s.streams[streamID]
	if afterVersion > int64(len(history)) {
		return nil, fmt.Errorf("replay: checkpoint %d is ahead of stream %q version %d", afterVersion, streamID, len(history))
	}
	result := make([]RecordedEvent, len(history[afterVersion:]))
	copy(result, history[afterVersion:])
	return result, nil
}

func (s *FileEventStore) StreamVersion(ctx context.Context, streamID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamVersionLocked(ctx, streamID)
}

func (s *FileEventStore) streamVersionLocked(ctx context.Context, streamID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return int64(len(s.streams[streamID])), nil
}

func (s *FileEventStore) Lock()   { s.mu.Lock() }
func (s *FileEventStore) Unlock() { s.mu.Unlock() }

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("replay: open event directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("replay: sync event directory: %w", err)
	}
	return nil
}
