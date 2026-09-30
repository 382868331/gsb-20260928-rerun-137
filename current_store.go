package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type CurrentProjection struct {
	StreamID string `json:"stream_id"`
	Name     string `json:"name"`
	Version  int64  `json:"version"`
	Position int64  `json:"position"`
}

type CurrentStore interface {
	SaveCurrent(ctx context.Context, current CurrentProjection) error
	LoadCurrent(ctx context.Context) (CurrentProjection, error)
}

type FileCurrentStore struct {
	path string
}

func NewFileCurrentStore(path string) *FileCurrentStore {
	return &FileCurrentStore{path: path}
}

func (s *FileCurrentStore) SaveCurrent(ctx context.Context, current CurrentProjection) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if current.Name == "" {
		return fmt.Errorf("replay: current projection name is required")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("replay: create current projection directory: %w", err)
	}
	data, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("replay: encode current projection: %w", err)
	}
	temporary := s.path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return fmt.Errorf("replay: write current projection: %w", err)
	}
	file, err := os.Open(temporary)
	if err != nil {
		return fmt.Errorf("replay: open current projection: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("replay: sync current projection: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("replay: close current projection: %w", err)
	}
	if err := os.Rename(temporary, s.path); err != nil {
		return fmt.Errorf("replay: publish current projection: %w", err)
	}
	return syncDir(filepath.Dir(s.path))
}

func (s *FileCurrentStore) LoadCurrent(ctx context.Context) (CurrentProjection, error) {
	if err := ctx.Err(); err != nil {
		return CurrentProjection{}, err
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return CurrentProjection{}, fmt.Errorf("replay: read current projection: %w", err)
	}
	var current CurrentProjection
	if err := json.Unmarshal(data, &current); err != nil {
		return CurrentProjection{}, fmt.Errorf("replay: decode current projection: %w", err)
	}
	return current, nil
}
