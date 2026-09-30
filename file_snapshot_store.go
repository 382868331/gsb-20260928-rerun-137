package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type FileSnapshotStore[S any] struct {
	dir string
}

func NewFileSnapshotStore[S any](dir string) *FileSnapshotStore[S] {
	return &FileSnapshotStore[S]{dir: dir}
}

func (s *FileSnapshotStore[S]) path(name string) string {
	return filepath.Join(s.dir, name+".json")
}

func (s *FileSnapshotStore[S]) Save(ctx context.Context, name string, snapshot Snapshot[S]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("replay: snapshot name is required")
	}
	if existing, err := s.Load(ctx, name); err == nil {
		if existing.StreamID != snapshot.StreamID {
			return fmt.Errorf("replay: snapshot belongs to stream %q, not %q", snapshot.StreamID, existing.StreamID)
		}
		if existing.Version > snapshot.Version || existing.Position > snapshot.Position {
			return fmt.Errorf("replay: snapshot at version %d position %d cannot replace version %d position %d", snapshot.Version, snapshot.Position, existing.Version, existing.Position)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("replay: create snapshot directory: %w", err)
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("replay: encode snapshot: %w", err)
	}
	target := s.path(name)
	temporary := target + ".tmp"
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return fmt.Errorf("replay: write snapshot: %w", err)
	}
	tempFile, err := os.Open(temporary)
	if err != nil {
		return fmt.Errorf("replay: open snapshot: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("replay: sync snapshot: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("replay: close snapshot: %w", err)
	}
	if err := os.Rename(temporary, target); err != nil {
		return fmt.Errorf("replay: publish snapshot: %w", err)
	}
	return syncDir(s.dir)
}

func (s *FileSnapshotStore[S]) Load(ctx context.Context, name string) (Snapshot[S], error) {
	if err := ctx.Err(); err != nil {
		return Snapshot[S]{}, err
	}
	data, err := os.ReadFile(s.path(name))
	if err != nil {
		return Snapshot[S]{}, fmt.Errorf("replay: read snapshot %q: %w", name, err)
	}
	var snapshot Snapshot[S]
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Snapshot[S]{}, fmt.Errorf("replay: decode snapshot %q: %w", name, err)
	}
	return snapshot, nil
}
