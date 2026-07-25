package activity

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestProjectorTracksSubagentMeaningfulChangesAndCoalescesOnlyFeedRendering(t *testing.T) {
	t.Parallel()

	var projector Projector
	started := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	if _, semantic, err := projector.Observe([]byte(`{"type":"tool_execution_start","toolCallId":"agent-1","toolName":"Agent"}`), started); err != nil || !semantic {
		t.Fatalf("start Agent tool: semantic = %t, err = %v", semantic, err)
	}
	initial := `{"type":"tool_execution_update","toolCallId":"agent-1","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"first hidden output"}],"details":{"description":"Implement Follow","status":"queued","activity":"thinking","turnCount":1,"toolUses":0,"tokens":"1.0k token","durationMs":0,"spinnerFrame":0}}}`
	entry, semantic, err := projector.Observe([]byte(initial), started)
	if err != nil || !semantic || entry.SuppressFeed || entry.Subagent == nil || entry.Subagent.ApproxTokens == nil || *entry.Subagent.ApproxTokens != 1000 {
		t.Fatalf("initial Subagent entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
	for _, milestone := range []string{"status: queued", "activity: thinking", "reached turn 1"} {
		if !strings.Contains(entry.Description, milestone) {
			t.Fatalf("initial Subagent entry omitted %q: %#v", milestone, entry)
		}
	}

	cosmetic := `{"type":"tool_execution_update","toolCallId":"agent-1","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"first hidden output"}],"details":{"description":"Implement Follow","status":"queued","activity":"thinking","turnCount":1,"toolUses":0,"tokens":"1.0k token","durationMs":100,"spinnerFrame":7}}}`
	if entry, semantic, err := projector.Observe([]byte(cosmetic), started.Add(100*time.Millisecond)); err != nil || semantic {
		t.Fatalf("cosmetic Subagent entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}

	outputChanged := strings.Replace(cosmetic, "first hidden output", "second hidden output", 1)
	entry, semantic, err = projector.Observe([]byte(outputChanged), started.Add(200*time.Millisecond))
	if err != nil || !semantic || !entry.SuppressFeed {
		t.Fatalf("changed output entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
	if strings.Contains(entry.Description, "hidden output") {
		t.Fatalf("changed output leaked into Activity: %#v", entry)
	}

	statusChanged := strings.Replace(outputChanged, `"status":"queued"`, `"status":"running"`, 1)
	entry, semantic, err = projector.Observe([]byte(statusChanged), started.Add(300*time.Millisecond))
	if err != nil || !semantic || entry.SuppressFeed {
		t.Fatalf("status milestone entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
	turnChanged := strings.Replace(statusChanged, `"turnCount":1`, `"turnCount":2`, 1)
	entry, semantic, err = projector.Observe([]byte(turnChanged), started.Add(400*time.Millisecond))
	if err != nil || !semantic || entry.SuppressFeed || !strings.Contains(entry.Description, "reached turn 2") {
		t.Fatalf("turn milestone entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}

	activityChanged := strings.Replace(turnChanged, `"activity":"thinking"`, `"activity":"testing"`, 1)
	entry, semantic, err = projector.Observe([]byte(activityChanged), started.Add(500*time.Millisecond))
	if err != nil || !semantic || !entry.SuppressFeed {
		t.Fatalf("rapid activity entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
	laterActivity := strings.Replace(activityChanged, `"activity":"testing"`, `"activity":"writing"`, 1)
	entry, semantic, err = projector.Observe([]byte(laterActivity), started.Add(1500*time.Millisecond))
	if err != nil || !semantic || entry.SuppressFeed {
		t.Fatalf("later activity entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}

	descriptionChanged := strings.Replace(laterActivity, "Implement Follow", "Review Follow", 1)
	entry, semantic, err = projector.Observe([]byte(descriptionChanged), started.Add(1600*time.Millisecond))
	if err != nil || !semantic || !entry.SuppressFeed || !strings.Contains(entry.Description, "description changed") {
		t.Fatalf("description-only entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
	toolUsesChanged := strings.Replace(descriptionChanged, `"toolUses":0`, `"toolUses":4`, 1)
	entry, semantic, err = projector.Observe([]byte(toolUsesChanged), started.Add(1700*time.Millisecond))
	if err != nil || !semantic || !entry.SuppressFeed || !strings.Contains(entry.Description, "tool uses: 4") {
		t.Fatalf("tool-use-only entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
	tokensChanged := strings.Replace(toolUsesChanged, `"tokens":"1.0k token"`, `"tokens":"1.9k token"`, 1)
	entry, semantic, err = projector.Observe([]byte(tokensChanged), started.Add(1800*time.Millisecond))
	if err != nil || !semantic || !entry.SuppressFeed || !strings.Contains(entry.Description, "approximate tokens: ~1900") {
		t.Fatalf("token-only entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}

	completion := `{"type":"tool_execution_end","toolCallId":"agent-1","toolName":"Agent","result":{"content":[{"type":"text","text":"hidden result"}],"details":{"description":"Implement Follow","status":"completed","turnCount":2,"toolUses":3,"tokens":"2.0k tokens","durationMs":2000}},"isError":false}`
	entry, semantic, err = projector.Observe([]byte(completion), started.Add(2*time.Second))
	if err != nil || !semantic || !entry.OperationChanged || entry.Operation != "model" {
		t.Fatalf("completed Subagent entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
}

func TestProjectorIgnoresMalformedSubagentContentChurn(t *testing.T) {
	t.Parallel()

	var projector Projector
	observedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	if _, semantic, err := projector.Observe([]byte(`{"type":"tool_execution_start","toolCallId":"agent-malformed","toolName":"Agent"}`), observedAt); err != nil || !semantic {
		t.Fatalf("start Agent tool: semantic = %t, err = %v", semantic, err)
	}
	first := `{"type":"tool_execution_update","toolCallId":"agent-malformed","toolName":"Agent","partialResult":{"content":{"spinnerFrame":1},"details":{"description":"Review","status":"running","activity":"reading","turnCount":1,"toolUses":0,"tokens":"100 tokens","durationMs":0}}}`
	if _, semantic, err := projector.Observe([]byte(first), observedAt); err != nil || !semantic {
		t.Fatalf("initial malformed content: semantic = %t, err = %v", semantic, err)
	}
	second := strings.Replace(first, `"spinnerFrame":1`, `"spinnerFrame":2`, 1)
	if entry, semantic, err := projector.Observe([]byte(second), observedAt.Add(100*time.Millisecond)); err != nil || semantic {
		t.Fatalf("malformed content churn entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
}

func TestProjectorRestoresParallelAgentOperationAndMarksFailures(t *testing.T) {
	t.Parallel()

	var projector Projector
	observedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	for _, id := range []string{"agent-one", "agent-two"} {
		record := fmt.Sprintf(`{"type":"tool_execution_start","toolCallId":%q,"toolName":"Agent"}`, id)
		if _, semantic, err := projector.Observe([]byte(record), observedAt); err != nil || !semantic {
			t.Fatalf("start %s: semantic = %t, err = %v", id, semantic, err)
		}
	}
	update := `{"type":"tool_execution_update","toolCallId":"agent-one","toolName":"Agent","partialResult":{"details":{"description":"Implement","status":"running","activity":"testing","turnCount":1,"toolUses":1,"tokens":"100 tokens","durationMs":100}}}`
	if _, semantic, err := projector.Observe([]byte(update), observedAt); err != nil || !semantic {
		t.Fatalf("update first Agent: semantic = %t, err = %v", semantic, err)
	}
	failure := `{"type":"tool_execution_end","toolCallId":"agent-one","toolName":"Agent","result":{"details":{}},"isError":true}`
	entry, semantic, err := projector.Observe([]byte(failure), observedAt.Add(time.Second))
	if err != nil || !semantic || entry.Subagent == nil || entry.Subagent.Status != "failed" || entry.Subagent.Activity != "testing" || entry.Description != `Subagent [agent-one] "n/a" failed` {
		t.Fatalf("failed Agent entry = %#v, semantic = %t, err = %v", entry, semantic, err)
	}
	if !entry.OperationChanged || entry.Operation != "Agent" {
		t.Fatalf("failed parallel Agent operation = %#v", entry)
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
