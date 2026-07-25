package activity

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestProjectorMarksCompletedResponsesAndKeepsParallelToolOperation(t *testing.T) {
	t.Parallel()

	var projector Projector
	observedAt := time.Now()
	completed, semantic, err := projector.Observe([]byte(`{"type":"message_end","message":{"role":"assistant","content":{},"usage":{"totalTokens":"unknown"}}}`), observedAt)
	if err != nil || !semantic || !completed.ResponseCompleted || completed.TokensKnown {
		t.Fatalf("completed response = %#v, semantic = %t, err = %v", completed, semantic, err)
	}
	for _, record := range []string{
		`{"type":"tool_execution_start","toolCallId":"first","toolName":"read"}`,
		`{"type":"tool_execution_start","toolCallId":"second","toolName":"bash"}`,
	} {
		if _, semantic, err := projector.Observe([]byte(record), observedAt); err != nil || !semantic {
			t.Fatalf("observe %s: semantic = %t, err = %v", record, semantic, err)
		}
	}
	ended, semantic, err := projector.Observe([]byte(`{"type":"tool_execution_end","toolCallId":"second","isError":false}`), observedAt)
	if err != nil || !semantic {
		t.Fatalf("end parallel tool: semantic = %t, err = %v", semantic, err)
	}
	if ended.Operation != "read" {
		t.Fatalf("operation after parallel tool ended = %q, want read", ended.Operation)
	}
}

func TestNewWriterResumeRequiresAndPreservesExistingProjection(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	missingPath := filepath.Join(directory, "missing.activity.jsonl")
	if writer, err := NewWriter(missingPath, true); err == nil || writer != nil {
		t.Fatalf("resumed missing projection = %#v, err = %v", writer, err)
	}
	if _, err := os.Stat(UnavailablePath(missingPath)); err != nil {
		t.Fatalf("missing resumed projection diagnostic: %v", err)
	}

	path := filepath.Join(directory, "existing.activity.jsonl")
	original := []byte("existing projection\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Observe([]byte(`{"type":"agent_start"}`)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(contents, original) || !bytes.Contains(contents, []byte(`"description":"Worker started"`)) {
		t.Fatalf("resumed projection = %q", contents)
	}
}

func TestWriterRecordsEachEntryObservationTime(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "timed.activity.jsonl")
	writer, err := NewWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	times := []time.Time{
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2026, 1, 2, 3, 4, 9, 0, time.UTC),
	}
	index := 0
	writer.now = func() time.Time {
		value := times[index]
		index++
		return value
	}
	for _, record := range []string{`{"type":"agent_start"}`, `{"type":"agent_end"}`} {
		if err := writer.Observe([]byte(record)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(contents), []byte{'\n'})
	for lineIndex, line := range lines {
		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatal(err)
		}
		if !entry.ObservedAt.Equal(times[lineIndex]) {
			t.Fatalf("entry %d observed at %v, want %v", lineIndex, entry.ObservedAt, times[lineIndex])
		}
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
