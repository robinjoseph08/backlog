package activity

import (
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
	completed, semantic, err := projector.Observe([]byte(`{"type":"message_end","message":{"role":"assistant","content":[]}}`), observedAt)
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
