package replay

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
)

// FileSnapshotStore persists snapshots on disk so a projection can be
// restored after a restart. A snapshot is only published once it has been
// fully written, fsynced and atomically renamed into place; a crash while
// writing, publishing or switching therefore always leaves the previously
// published snapshot usable.
type FileSnapshotStore[S any] struct {
	dir string
}

type snapshotEnvelope[S any] struct {
	Snapshot Snapshot[S] `json:"snapshot"`
	Checksum uint32      `json:"checksum"`
}

func NewFileSnapshotStore[S any](dir string) *FileSnapshotStore[S] {
	return &FileSnapshotStore[S]{dir: dir}
}

func (s *FileSnapshotStore[S]) snapshotPath(streamID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(streamID))
	return filepath.Join(s.dir, "snapshot-"+encoded+".json")
}

// Save writes the snapshot to a temporary file, fsyncs it, then atomically
// renames it over the previous snapshot and fsyncs the directory. If the
// process dies at any point before the rename, Load still returns the old
// snapshot; after the rename, Load returns the new one.
func (s *FileSnapshotStore[S]) Save(ctx context.Context, snapshot Snapshot[S]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("replay: create snapshot dir: %w", err)
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("replay: encode snapshot for stream %q: %w", snapshot.StreamID, err)
	}
	envelope, err := json.Marshal(snapshotEnvelope[S]{Snapshot: snapshot, Checksum: crc32.ChecksumIEEE(payload)})
	if err != nil {
		return fmt.Errorf("replay: encode snapshot envelope for stream %q: %w", snapshot.StreamID, err)
	}

	finalPath := s.snapshotPath(snapshot.StreamID)
	tmpPath := finalPath + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("replay: open snapshot temp file: %w", err)
	}
	if _, err := tmp.Write(envelope); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("replay: write snapshot temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("replay: sync snapshot temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replay: close snapshot temp file: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replay: publish snapshot for stream %q: %w", snapshot.StreamID, err)
	}

	dir, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("replay: open snapshot dir: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("replay: sync snapshot dir: %w", err)
	}
	return nil
}

// Load returns the last published snapshot for the stream. A leftover
// temporary file from an interrupted Save is ignored, so the previously
// published snapshot remains the usable view.
func (s *FileSnapshotStore[S]) Load(ctx context.Context, streamID string) (Snapshot[S], bool, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot[S]{}, false, err
	}
	data, err := os.ReadFile(s.snapshotPath(streamID))
	if errors.Is(err, fs.ErrNotExist) {
		return Snapshot[S]{}, false, nil
	}
	if err != nil {
		return Snapshot[S]{}, false, fmt.Errorf("replay: read snapshot for stream %q: %w", streamID, err)
	}

	var envelope snapshotEnvelope[S]
	if err := json.Unmarshal(data, &envelope); err != nil {
		return Snapshot[S]{}, false, fmt.Errorf("replay: decode snapshot for stream %q: %w", streamID, err)
	}
	payload, err := json.Marshal(envelope.Snapshot)
	if err != nil {
		return Snapshot[S]{}, false, fmt.Errorf("replay: re-encode snapshot for stream %q: %w", streamID, err)
	}
	if crc32.ChecksumIEEE(payload) != envelope.Checksum {
		return Snapshot[S]{}, false, fmt.Errorf("replay: snapshot for stream %q failed checksum", streamID)
	}
	if envelope.Snapshot.StreamID != streamID {
		return Snapshot[S]{}, false, fmt.Errorf("replay: snapshot belongs to stream %q, not %q", envelope.Snapshot.StreamID, streamID)
	}
	return envelope.Snapshot, true, nil
}

// frameChecksum exposes the CRC used for log frames so tests can build
// well-formed corrupt tails.
func frameChecksum(payload []byte) uint32 {
	return crc32.ChecksumIEEE(payload)
}

func frameHeader(payload []byte) []byte {
	header := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(header[:4], frameChecksum(payload))
	binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
	return header
}
