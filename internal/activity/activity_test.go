package activity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriterIgnoresUnknownRecordsAndMarksObservationFailure(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "run.activity.jsonl")
	writer, err := NewWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Observe([]byte(`{"type":"future_event","futureField":"ignored"}`)); err != nil {
		t.Fatal(err)
	}
	projection, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection) != 0 {
		t.Fatalf("unknown record was projected: %s", projection)
	}
	if err := writer.Observe([]byte(`{"type":`)); err == nil {
		t.Fatal("malformed observation succeeded")
	}
	if _, err := os.Stat(UnavailablePath(path)); err != nil {
		t.Fatalf("observation failure marker: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterMarksAppendAndCloseFailures(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "run.activity.jsonl")
	writer, err := NewWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Observe([]byte(`{"type":"agent_start"}`)); err == nil {
		t.Fatal("append to closed projection succeeded")
	}
	if err := writer.Close(); err == nil {
		t.Fatal("closing an already closed projection succeeded")
	}
	if _, err := os.Stat(UnavailablePath(path)); err != nil {
		t.Fatalf("append/close failure marker: %v", err)
	}
}

func TestNewWriterClearsStaleFailureMarkerForFreshProjection(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "run.activity.jsonl")
	if err := os.WriteFile(UnavailablePath(path), []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(UnavailablePath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh projection retained stale failure marker: %v", err)
	}
}
