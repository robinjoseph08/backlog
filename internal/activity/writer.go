package activity

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Writer appends normalized Activity while the raw Worker log remains the
// source protocol evidence. Its errors are observational and callers must not
// use them to determine the Worker result.
type Writer struct {
	mu        sync.Mutex
	file      *os.File
	path      string
	encoder   *json.Encoder
	projector Projector
	now       func() time.Time
}

func NewWriter(path string, appendExisting bool) (*Writer, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if appendExisting {
		flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		err = fmt.Errorf("open Worker Activity projection: %w", err)
		markUnavailable(path, err)
		return nil, err
	}
	if !appendExisting {
		_ = os.Remove(UnavailablePath(path))
	}
	return &Writer{file: file, path: path, encoder: json.NewEncoder(file), now: time.Now}, nil
}

// Observe appends a record only when the raw event is semantically meaningful.
func (w *Writer) Observe(record []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry, semantic, err := w.projector.Observe(record, w.now())
	if err != nil {
		markUnavailable(w.path, err)
		return err
	}
	if !semantic {
		return nil
	}
	if err := w.encoder.Encode(entry); err != nil {
		err = fmt.Errorf("append Worker Activity projection: %w", err)
		markUnavailable(w.path, err)
		return err
	}
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Close(); err != nil {
		markUnavailable(w.path, err)
		return err
	}
	return nil
}

// UnavailablePath identifies the best-effort diagnostic marker for a failed
// projection. Followers can rebuild from raw evidence without trusting a
// projection that may contain gaps.
func UnavailablePath(projectionPath string) string {
	return projectionPath + ".unavailable"
}

func markUnavailable(path string, projectionErr error) {
	_ = os.WriteFile(UnavailablePath(path), []byte(projectionErr.Error()+"\n"), 0o600)
}
