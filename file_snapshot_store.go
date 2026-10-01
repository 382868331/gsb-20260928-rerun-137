package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var ErrSnapshotNotFound = errors.New("replay: snapshot not found")

type SnapshotStore[S any] interface {
	Save(ctx context.Context, snapshot Snapshot[S]) error
	Load(ctx context.Context, streamID string) (Snapshot[S], error)
	Recover(ctx context.Context) error
}

// FileSnapshotStore keeps every published snapshot immutable. A separate
// current pointer is atomically replaced, so interruption while writing or
// publishing a replacement leaves the previous pointer and snapshot usable.
type FileSnapshotStore[S any] struct {
	root     string
	baseDir  string
	lockPath string
	lockFile *os.File
	mu       sync.RWMutex
}

func NewFileSnapshotStore[S any](root string) (*FileSnapshotStore[S], error) {
	baseDir := filepath.Join(root, "snapshots")
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("replay: create snapshot store: %w", err)
	}
	lockPath := filepath.Join(root, "snapshot-store.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("replay: open snapshot store lock: %w", err)
	}
	if err := fsyncDir(root); err != nil {
		lockFile.Close()
		return nil, err
	}
	store := &FileSnapshotStore[S]{
		root:     root,
		baseDir:  baseDir,
		lockPath: lockPath,
		lockFile: lockFile,
	}
	if err := store.Recover(context.Background()); err != nil {
		lockFile.Close()
		return nil, err
	}
	return store, nil
}

func (s *FileSnapshotStore[S]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return nil
	}
	err := s.lockFile.Close()
	s.lockFile = nil
	return err
}

func (s *FileSnapshotStore[S]) Save(ctx context.Context, snapshot Snapshot[S]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot.StreamID == "" {
		return fmt.Errorf("replay: snapshot stream id cannot be empty")
	}
	if snapshot.Version < 0 {
		return fmt.Errorf("replay: snapshot version cannot be negative")
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("replay: encode snapshot: %w", err)
	}

	return s.withLock(func() error {
		dir := s.streamDir(snapshot.StreamID)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("replay: create snapshot directory: %w", err)
		}
		if err := fsyncDir(s.baseDir); err != nil {
			return err
		}
		finalPath := filepath.Join(dir, snapshotFileName(snapshot.Version))
		if _, err := os.Stat(finalPath); errors.Is(err, fs.ErrNotExist) {
			tempPath := filepath.Join(dir, fmt.Sprintf(".v%020d.%d.tmp", snapshot.Version, os.Getpid()))
			tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return fmt.Errorf("replay: write snapshot: %w", err)
			}
			if _, err := tempFile.Write(payload); err != nil {
				tempFile.Close()
				return fmt.Errorf("replay: write snapshot: %w", err)
			}
			if err := tempFile.Sync(); err != nil {
				tempFile.Close()
				return fmt.Errorf("replay: fsync snapshot: %w", err)
			}
			if err := tempFile.Close(); err != nil {
				return fmt.Errorf("replay: close snapshot: %w", err)
			}
			if err := os.Rename(tempPath, finalPath); err != nil {
				os.Remove(tempPath)
				return fmt.Errorf("replay: publish snapshot: %w", err)
			}
			if err := fsyncDir(dir); err != nil {
				return err
			}
		} else if err != nil {
			return fmt.Errorf("replay: inspect snapshot: %w", err)
		} else {
			var published Snapshot[S]
			payload, err := os.ReadFile(finalPath)
			if err != nil {
				return fmt.Errorf("replay: inspect immutable snapshot: %w", err)
			}
			if err := json.Unmarshal(payload, &published); err != nil {
				return fmt.Errorf("replay: inspect immutable snapshot: %w", err)
			}
			if published.StreamID != snapshot.StreamID || published.Version != snapshot.Version || !snapshotsEqual(published, snapshot) {
				return fmt.Errorf("replay: immutable snapshot at version %d already exists with different state", snapshot.Version)
			}
		}

		current, err := s.currentVersionLocked(snapshot.StreamID)
		if errors.Is(err, ErrSnapshotNotFound) {
			err = nil
		} else if err != nil {
			return err
		}
		if snapshot.Version >= current {
			return s.publishCurrentLocked(snapshot.StreamID, snapshot.Version)
		}
		return nil
	})
}

func (s *FileSnapshotStore[S]) Load(ctx context.Context, streamID string) (Snapshot[S], error) {
	if err := ctx.Err(); err != nil {
		return Snapshot[S]{}, err
	}
	var snapshot Snapshot[S]
	err := s.withReadLock(func() error {
		version, err := s.currentVersionLocked(streamID)
		if err != nil {
			return err
		}
		payload, err := os.ReadFile(filepath.Join(s.streamDir(streamID), snapshotFileName(version)))
		if err != nil {
			return fmt.Errorf("replay: read current snapshot: %w", err)
		}
		if err := json.Unmarshal(payload, &snapshot); err != nil {
			return fmt.Errorf("replay: decode current snapshot: %w", err)
		}
		if snapshot.StreamID != streamID || snapshot.Version != version {
			return fmt.Errorf("replay: current snapshot pointer does not match snapshot file")
		}
		return nil
	})
	return snapshot, err
}

func (s *FileSnapshotStore[S]) Recover(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.withLock(func() error {
		entries, err := os.ReadDir(s.baseDir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return os.MkdirAll(s.baseDir, 0o755)
			}
			return fmt.Errorf("replay: scan snapshots: %w", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			dir := filepath.Join(s.baseDir, entry.Name())
			files, err := os.ReadDir(dir)
			if err != nil {
				return fmt.Errorf("replay: scan snapshot directory: %w", err)
			}
			var latest int64 = -1
			var latestPath string
			for _, file := range files {
				name := file.Name()
				if file.IsDir() {
					continue
				}
				if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
					if err := removeIfExists(filepath.Join(dir, name)); err != nil {
						return fmt.Errorf("replay: remove incomplete snapshot: %w", err)
					}
					continue
				}
				version, ok, err := parseSnapshotFileName(name)
				if !ok || err != nil {
					continue
				}
				if version > latest {
					latest = version
					latestPath = filepath.Join(dir, name)
				}
			}
			if latest < 0 {
				continue
			}
			var snapshot Snapshot[S]
			payload, err := os.ReadFile(latestPath)
			if err != nil {
				return fmt.Errorf("replay: inspect published snapshot: %w", err)
			}
			if err := json.Unmarshal(payload, &snapshot); err != nil {
				return fmt.Errorf("replay: corrupt snapshot %q: %w", latestPath, err)
			}
			_ = snapshot
			if err := s.publishCurrentLockedByDir(dir, latest); err != nil {
				return err
			}
		}
		return fsyncDir(s.baseDir)
	})
}

func (s *FileSnapshotStore[S]) withReadLock(fn func() error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_SH); err != nil {
		return fmt.Errorf("replay: acquire shared snapshot lock: %w", err)
	}
	defer syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	return fn()
}

func (s *FileSnapshotStore[S]) withLock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("replay: acquire exclusive snapshot lock: %w", err)
	}
	defer syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	return fn()
}

func (s *FileSnapshotStore[S]) streamDir(streamID string) string {
	return filepath.Join(s.baseDir, "stream-"+streamHash(streamID))
}

func (s *FileSnapshotStore[S]) currentPath(streamID string) string {
	return filepath.Join(s.streamDir(streamID), "current.json")
}

func (s *FileSnapshotStore[S]) currentVersionLocked(streamID string) (int64, error) {
	payload, err := os.ReadFile(s.currentPath(streamID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, ErrSnapshotNotFound
		}
		return 0, fmt.Errorf("replay: read snapshot pointer: %w", err)
	}
	var pointer struct {
		Version int64 `json:"version"`
	}
	if err := json.Unmarshal(payload, &pointer); err != nil {
		return 0, fmt.Errorf("replay: decode snapshot pointer: %w", err)
	}
	if pointer.Version < 0 {
		return 0, fmt.Errorf("replay: snapshot pointer has negative version")
	}
	return pointer.Version, nil
}

func (s *FileSnapshotStore[S]) publishCurrentLocked(streamID string, version int64) error {
	return s.publishCurrentLockedByDir(s.streamDir(streamID), version)
}

func (s *FileSnapshotStore[S]) publishCurrentLockedByDir(dir string, version int64) error {
	payload, err := json.Marshal(struct {
		Version int64 `json:"version"`
	}{Version: version})
	if err != nil {
		return err
	}
	tempPath := filepath.Join(dir, ".current.json.tmp")
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("replay: write snapshot pointer: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return fmt.Errorf("replay: write snapshot pointer: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("replay: fsync snapshot pointer: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("replay: close snapshot pointer: %w", err)
	}
	if err := os.Rename(tempPath, filepath.Join(dir, "current.json")); err != nil {
		return fmt.Errorf("replay: switch snapshot pointer: %w", err)
	}
	return fsyncDir(dir)
}

func snapshotFileName(version int64) string {
	return fmt.Sprintf("v%020d.json", version)
}

func parseSnapshotFileName(name string) (int64, bool, error) {
	if !strings.HasPrefix(name, "v") || !strings.HasSuffix(name, ".json") {
		return 0, false, nil
	}
	versionText := strings.TrimSuffix(strings.TrimPrefix(name, "v"), ".json")
	version, err := strconv.ParseInt(versionText, 10, 64)
	if err != nil {
		return 0, false, nil
	}
	return version, true, err
}

func snapshotsEqual[S any](left, right Snapshot[S]) bool {
	return reflect.DeepEqual(left, right)
}
