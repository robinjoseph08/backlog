package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestFollowRawSelectsExactTerminalRunAndWritesCompleteRecordsVerbatim(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	selectedLog := filepath.Join(directory, "selected.jsonl")
	otherLog := filepath.Join(directory, "other.jsonl")
	want := "{\"type\":\"first\",\"spacing\": true}\n{\"type\":\"second\"}\r\n"
	if err := os.WriteFile(selectedLog, []byte(want+`{"partial":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(otherLog, []byte("wrong\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 1, RunID: "run-exact-other", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: otherLog},
		{Issue: 2, RunID: "run-exact", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: selectedLog},
	}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := followRaw(context.Background(), store, "run-exact", &output, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != want {
		t.Fatalf("raw output = %q, want %q", got, want)
	}
	after, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("following terminal Run changed state")
	}
}

func TestFollowRawStreamsAppendedCompleteRecordsWithoutLosingPartialRecord(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "active.jsonl")
	largePartial := `{"record":"` + strings.Repeat("x", 70*1024)
	if err := os.WriteFile(logPath, []byte("{\"record\":1}\n"+largePartial), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	running := scheduler.Run{
		Issue: 3, RunID: "active", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: 123, ProcessIdentity: "123:start", StartedAt: time.Now(), LogPath: logPath,
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{running},
		Leases: []scheduler.Lease{{LeaseID: "active", Issue: 3, RunID: "active"}},
	}); err != nil {
		t.Fatal(err)
	}

	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() { done <- followRaw(context.Background(), store, "active", &output, 5*time.Millisecond) }()
	waitForBuffer(t, &output, "{\"record\":1}\n")
	if got := output.String(); got != "{\"record\":1}\n" {
		t.Fatalf("unterminated record was emitted early: %q", got)
	}
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(log, `"}`+"\n{\"record\":3}\n"); err != nil {
		log.Close()
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	waitForBuffer(t, &output, "{\"record\":3}\n")
	running.Status = scheduler.StatusFailed
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{running}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("follower did not exit after Run became terminal")
	}
	want := "{\"record\":1}\n" + largePartial + `"}` + "\n{\"record\":3}\n"
	if got := output.String(); got != want {
		t.Fatalf("raw output = %q, want %q", got, want)
	}
}

func TestFollowRawDrainsRecordAppendedAsWorkerLogCloses(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "terminal-race.jsonl")
	if err := os.WriteFile(logPath, []byte("before-terminal\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := &closingFollowSource{run: scheduler.Run{
		Issue: 4, RunID: "terminal-race", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: 444, ProcessIdentity: "444:start", StartedAt: time.Now(), LogPath: logPath, WorkerLogOpen: true,
	}}

	var output bytes.Buffer
	if err := followRaw(context.Background(), source, source.run.RunID, &output, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "before-terminal\nafter-terminal\n"; got != want {
		t.Fatalf("raw output = %q, want %q", got, want)
	}
}

func TestFollowRawWaitsForActiveRunLogPath(t *testing.T) {
	directory := t.TempDir()
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{Issue: 4, RunID: "starting", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: "starting", Issue: 4, RunID: "starting"}},
	}); err != nil {
		t.Fatal(err)
	}
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() { done <- followRaw(context.Background(), store, run.RunID, &output, 5*time.Millisecond) }()

	logPath := filepath.Join(directory, "starting.jsonl")
	if err := os.WriteFile(logPath, []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run.LogPath = logPath
	run.Status = scheduler.StatusFailed
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("follower did not observe the new log path")
	}
	if got := output.String(); got != "ready\n" {
		t.Fatalf("raw output = %q", got)
	}
}

func TestFollowRawWaitsForWorktreeReadyRunLogPath(t *testing.T) {
	directory := t.TempDir()
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{Issue: 5, RunID: "worktree-ready", Status: scheduler.StatusWorktreeReady, WorkerMode: scheduler.WorkerModePrint}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() { done <- followRaw(context.Background(), store, run.RunID, &output, 5*time.Millisecond) }()

	logPath := filepath.Join(directory, "worktree-ready.jsonl")
	if err := os.WriteFile(logPath, []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run.LogPath = logPath
	run.Status = scheduler.StatusFailed
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("follower did not observe the worktree-ready Run log path")
	}
	if got := output.String(); got != "ready\n" {
		t.Fatalf("raw output = %q", got)
	}
}

func TestFollowRawReportsSelectedRunLogDiagnostics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  scheduler.Status
		logPath string
		want    string
	}{
		{name: "terminal Run without durable path", status: scheduler.StatusNeedsHuman, want: "no Worker log available"},
		{name: "running Run without durable path", status: scheduler.StatusRunning, want: "no Worker log available"},
		{name: "missing file", status: scheduler.StatusNeedsHuman, logPath: filepath.Join(t.TempDir(), "missing.jsonl"), want: "is unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			store := state.FileStore{Path: filepath.Join(directory, "state.json")}
			run := scheduler.Run{
				Issue: 5, RunID: "selected-run", Status: test.status, WorkerMode: scheduler.WorkerModePrint,
				LogPath: test.logPath, PID: 555, ProcessIdentity: "555:start", StartedAt: time.Now(),
			}
			value := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}
			if scheduler.RequiresLease(test.status) {
				value.Leases = []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}}
			}
			if err := store.Save(value); err != nil {
				t.Fatal(err)
			}
			err := followRaw(context.Background(), store, "selected-run", io.Discard, time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), `Run "selected-run"`) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want selected Run diagnostic containing %q", err, test.want)
			}
		})
	}
}

func TestFollowCommandUsesNormalizedActivityByDefault(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "default.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "lifecycle", Description: "Worker settled",
	})
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 28, RunID: "default-normalized", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath,
	}}}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := followCommand(context.Background(), []string{"default-normalized", "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.Contains(got, "Run: default-normalized") || !strings.Contains(got, "Worker Activity (latest 20)") || !strings.Contains(got, "Worker settled") {
		t.Fatalf("default Follow output = %q", got)
	}
}

func TestFollowCommandUnknownRunDoesNotChangeState(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 6, RunID: "known-run", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint,
	}}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = followCommand(context.Background(), []string{"unknown-run", "--raw", "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard)
	if err == nil || !strings.Contains(err.Error(), `Run "unknown-run" was not found`) {
		t.Fatalf("error = %v", err)
	}
	after, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("unknown Run changed state:\n before: %s\n after: %s", before, after)
	}
	if output.Len() != 0 {
		t.Fatalf("unknown Run output = %q", output.String())
	}
	for _, binding := range []string{stateDirectoryBindingFile, legacyStateDirectoryBindingFile} {
		if _, err := os.Stat(filepath.Join(repository, ".git", binding)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unknown Run created repository state binding %s: %v", binding, err)
		}
	}
}

func TestFollowRawDetachesOnCancellation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	logPath := filepath.Join(directory, "active.jsonl")
	if err := os.WriteFile(logPath, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 7, RunID: "cancel", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: 777, ProcessIdentity: "777:start", StartedAt: time.Now(), LogPath: logPath,
	}
	wantState := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "cancel", Issue: 7, RunID: "cancel"}}}
	if err := store.Save(wantState); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(store.Path)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- followRaw(ctx, store, run.RunID, io.Discard, 5*time.Millisecond) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("detach returned error: %v", err)
	}
	after, _ := os.ReadFile(store.Path)
	if !bytes.Equal(after, before) {
		t.Fatal("detaching follower changed state")
	}
}

func TestFollowNormalizedDetachesOnCancellationWithoutChangingState(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	logPath := filepath.Join(directory, "normalized-cancel.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "lifecycle", Description: "Worker started",
	})
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 28, RunID: "normalized-cancel", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 2828, ProcessIdentity: "2828:start", StartedAt: time.Now(), LogPath: logPath,
		SessionID: "normalized-cancel", SessionDir: filepath.Join(directory, "session"),
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- followNormalized(ctx, store, run.RunID, io.Discard, io.Discard, 5*time.Millisecond, time.Now)
	}()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("normalized detach returned error: %v", err)
	}
	after, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("detaching normalized follower changed state")
	}
}

func TestFollowRawChecksCancellationWhileEmittingBacklog(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	logPath := filepath.Join(directory, "large.jsonl")
	contents := bytes.Repeat([]byte("complete record\n"), 16*1024)
	if err := os.WriteFile(logPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 27, RunID: "large", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath,
	}}}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	output := &cancelingWriter{cancel: cancel}
	done := make(chan error, 1)
	go func() { done <- followRaw(ctx, store, "large", output, time.Millisecond) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("detach while emitting: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not detach while emitting existing records")
	}
	if got := output.Len(); got == 0 || got >= len(contents) {
		t.Fatalf("bytes emitted before detach = %d, want between 0 and %d", got, len(contents))
	}
}

func TestFollowRawReportsChangedLogIdentity(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	firstPath := filepath.Join(directory, "first.jsonl")
	if err := os.WriteFile(firstPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	source := &sequenceFollowSource{runs: []scheduler.Run{
		{Issue: 8, RunID: "changed", Status: scheduler.StatusRunning, LogPath: firstPath},
		{Issue: 8, RunID: "changed", Status: scheduler.StatusRunning, LogPath: filepath.Join(directory, "second.jsonl")},
	}}
	if err := followRaw(context.Background(), source, "changed", io.Discard, time.Millisecond); err == nil ||
		!strings.Contains(err.Error(), `Run "changed" Worker log changed`) {
		t.Fatalf("changed-log error = %v", err)
	}
}

func TestRawLogStreamReportsReadAndShortWriteFailures(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "log.jsonl")
	if err := os.WriteFile(path, []byte("complete\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	closed, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := (&rawLogStream{file: closed, output: io.Discard}).emitAvailable(context.Background()); err == nil || !strings.Contains(err.Error(), "inspect raw JSONL") {
		t.Fatalf("closed-file error = %v", err)
	}
	if err := writeAll(zeroWriter{}, []byte("record")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero-write error = %v, want io.ErrShortWrite", err)
	}
	writer := &partialWriter{}
	if err := writeAll(writer, []byte("record")); err != nil {
		t.Fatal(err)
	}
	if writer.String() != "record" {
		t.Fatalf("partial-writer output = %q", writer.String())
	}
}

func TestFollowRawPropagatesStateAndOutputFailuresWithRunContext(t *testing.T) {
	t.Parallel()

	stateFailure := failingFollowSource{err: errors.New("state denied")}
	if err := followRaw(context.Background(), stateFailure, "state-run", io.Discard, time.Millisecond); err == nil ||
		!strings.Contains(err.Error(), `follow Run "state-run": read runner state: state denied`) {
		t.Fatalf("state error = %v", err)
	}

	directory := t.TempDir()
	logPath := filepath.Join(directory, "log.jsonl")
	if err := os.WriteFile(logPath, []byte("complete\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 8, RunID: "output-run", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := followRaw(context.Background(), store, "output-run", failingWriter{}, time.Millisecond); err == nil ||
		!strings.Contains(err.Error(), `follow Run "output-run" Worker log`) || !strings.Contains(err.Error(), "output denied") {
		t.Fatalf("output error = %v", err)
	}
}

func TestFollowRequiresRunID(t *testing.T) {
	t.Parallel()

	if _, _, err := splitFollowArguments(nil); err == nil || !strings.Contains(err.Error(), "backlog follow <run-id> [--raw]") {
		t.Fatalf("missing-Run error = %v", err)
	}
}

func TestSplitFollowArgumentsAcceptsRunIDBeforeOrAfterFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args      []string
		wantFlags []string
	}{
		{[]string{"run-1", "--raw"}, []string{"--raw"}},
		{[]string{"--raw", "run-1"}, []string{"--raw"}},
		{[]string{"--repo-dir", "/tmp/repo", "run-1", "--raw"}, []string{"--repo-dir", "/tmp/repo", "--raw"}},
		{[]string{"--state-dir=/tmp/state", "run-1", "--raw"}, []string{"--state-dir=/tmp/state", "--raw"}},
	}
	for _, test := range tests {
		runID, flags, err := splitFollowArguments(test.args)
		if err != nil {
			t.Fatalf("split %q: %v", test.args, err)
		}
		if runID != "run-1" || !slices.Equal(flags, test.wantFlags) {
			t.Fatalf("split %q = %q, %q, want flags %q", test.args, runID, flags, test.wantFlags)
		}
	}
}

func TestFollowNormalizedReplaysSemanticWorkerActivityWithUsageAndPrivacy(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "worker.jsonl")
	records := []string{
		`{"type":"response","id":"prompt"}`,
		`{"type":"agent_start"}`,
		`{"type":"turn_start"}`,
		`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"private streaming text"},"message":{"usage":{"totalTokens":0}}}`,
		`{"type":"message_update","assistantMessageEvent":{"delta":"private streaming text","type":"text_delta"},"message":{"usage":{"totalTokens":0}}}`,
		`{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"private reasoning"}}`,
		`{"type":"tool_execution_start","toolCallId":"tool-1","toolName":"bash","args":{"command":"secret argument"}}`,
		`{"type":"tool_execution_update","toolCallId":"tool-1","toolName":"bash","partialResult":{"durationMs":10,"spinnerFrame":"one"}}`,
		`{"type":"tool_execution_update","toolCallId":"tool-1","toolName":"bash","partialResult":{"durationMs":20,"spinnerFrame":"two"}}`,
		`{"type":"tool_execution_update","toolCallId":"tool-1","toolName":"bash","partialResult":{"output":"secret result","durationMs":20}}`,
		`{"type":"tool_execution_update","toolCallId":"tool-1","toolName":"bash","partialResult":{"output":"secret result","durationMs":30}}`,
		`{"type":"tool_execution_end","toolCallId":"tool-1","toolName":"bash","result":{"content":"full secret result"},"isError":false}`,
		`{"type":"message_end","message":{"role":"assistant","content":[{"type":"thinking","thinking":"hidden chain of thought"},{"type":"text","text":"Visible final answer"}],"usage":{"totalTokens":123}}}`,
		`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Second visible answer"}],"usage":{"totalTokens":77}}}`,
		`{"type":"auto_retry_start","attempt":1,"errorMessage":"temporary failure"}`,
		`{"type":"auto_retry_end","attempt":1}`,
		`{"type":"compaction_start"}`,
		`{"type":"summarization_retry_scheduled","attempt":2}`,
		`{"type":"summarization_retry_attempt_start"}`,
		`{"type":"summarization_retry_finished"}`,
		`{"type":"compaction_end"}`,
		`{"type":"turn_end"}`,
		`{"type":"agent_end"}`,
		`{"type":"agent_settled"}`,
		`{"type":"future_event","reasoning":"unknown secret"}`,
	}
	contents := strings.Join(records, "\n") + "\n" + `{"type":"message_end","message":`
	if err := os.WriteFile(logPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		Issue: 28, IssueTitle: "Normalized Worker Activity", IssueURL: "https://example.test/issues/28",
		RunID: "run-28", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC,
		LogPath: logPath, StartedAt: time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
		WorkerStartedAt: time.Date(2026, 1, 1, 11, 45, 0, 0, time.UTC),
	}
	source := &sequenceFollowSource{runs: []scheduler.Run{run}}
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var output, diagnostics bytes.Buffer
	if err := followNormalized(context.Background(), source, run.RunID, &output, &diagnostics, time.Millisecond, func() time.Time { return fixedNow }); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, want := range []string{
		"Run: run-28", "Issue: #28  Normalized Worker Activity  https://example.test/issues/28", "State: merged",
		"Elapsed: 1h0m0s", "Activity age: n/a", "Current Worker operation: n/a",
		"Completed Worker turns: 1", "Completed Worker tokens: 200", "Visible final answer", "Second visible answer",
		"Tool bash started", "Tool bash output changed", "Tool bash completed", "Worker retry 1 started: temporary failure",
		"Worker retry 1 ended", "Context compaction started", "Compaction retry 2 scheduled", "Compaction retry started",
		"Compaction retry finished", "Context compaction ended", "Worker settled",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("normalized output missing %q:\n%s", want, got)
		}
	}
	if count := strings.Count(got, "Tool bash output changed"); count != 1 {
		t.Fatalf("changed tool output entries = %d, want 1:\n%s", count, got)
	}
	if count := strings.Count(got, "Model streaming"); count != 2 {
		t.Fatalf("model streaming entries = %d, want one text and one reasoning change:\n%s", count, got)
	}
	for _, private := range []string{"private streaming text", "private reasoning", "secret argument", "secret result", "hidden chain of thought", "unknown secret"} {
		if strings.Contains(got, private) {
			t.Fatalf("normalized output exposed %q:\n%s", private, got)
		}
	}
	if !strings.Contains(diagnostics.String(), "replayed Activity age is n/a") {
		t.Fatalf("diagnostics = %q", diagnostics.String())
	}
}

func TestFollowNormalizedLimitsInitialProjectionToLatestTwentyAndReportsAge(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "worker.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, 2, 3, 4, 5, 0, 0, time.UTC)
	entries := make([]activity.Entry, 25)
	for index := range entries {
		entries[index] = activity.Entry{
			Version: activity.CurrentVersion, ObservedAt: observedAt, Kind: "turn",
			Description: fmt.Sprintf("Event %02d", index+1), TurnDelta: 1,
		}
	}
	writeActivityEntries(t, activity.PathForLog(logPath), entries...)
	run := scheduler.Run{
		Issue: 28, RunID: "projection", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC,
		LogPath: logPath, StartedAt: observedAt.Add(-time.Hour),
	}
	var output bytes.Buffer
	if err := followNormalized(context.Background(), &sequenceFollowSource{runs: []scheduler.Run{run}}, run.RunID, &output, io.Discard, time.Millisecond, func() time.Time {
		return observedAt.Add(5 * time.Second)
	}); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	if strings.Contains(got, "Event 05") || !strings.Contains(got, "Event 06") || !strings.Contains(got, "Event 25") {
		t.Fatalf("initial latest-20 window is wrong:\n%s", got)
	}
	if count := strings.Count(got, "  2026-"); count != 20 {
		t.Fatalf("initial Activity count = %d, want 20:\n%s", count, got)
	}
	if !strings.Contains(got, "Activity age: 5s") || !strings.Contains(got, "Completed Worker turns: 25") || !strings.Contains(got, "Completed Worker tokens: n/a") {
		t.Fatalf("projection summary is incomplete:\n%s", got)
	}
}

func TestFollowSummaryStopsElapsedAtTerminalUpdateWithoutCompletionTime(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 3, 4, 1, 0, 0, 0, time.UTC)
	updatedAt := startedAt.Add(time.Hour)
	for _, status := range []scheduler.Status{scheduler.StatusFailed, scheduler.StatusNeedsHuman} {
		status := status
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			run := scheduler.Run{Issue: 28, RunID: "terminal-elapsed", Status: status, StartedAt: startedAt, UpdatedAt: updatedAt}
			var output bytes.Buffer
			if err := printFollowSummary(&output, run, followMetrics{}, updatedAt.Add(24*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if got := output.String(); !strings.Contains(got, "Elapsed: 1h0m0s") {
				t.Fatalf("terminal summary = %q", got)
			}
		})
	}
}

func TestFollowNormalizedStreamsProjectionAndPrintsTerminalSummary(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "live.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	projectionPath := activity.PathForLog(logPath)
	observedAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: observedAt, Kind: "lifecycle", Description: "Worker started",
		Operation: "starting", OperationChanged: true,
	})
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 28, RunID: "live-normalized", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 2800, ProcessIdentity: "2800:start", LogPath: logPath, WorkerLogOpen: true, StartedAt: observedAt.Add(-time.Minute),
		SessionID: "session-live-normalized", SessionDir: filepath.Join(directory, "session"),
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followNormalized(context.Background(), store, run.RunID, &output, io.Discard, 5*time.Millisecond, func() time.Time {
			return observedAt.Add(10 * time.Second)
		})
	}()
	waitForBuffer(t, &output, "Worker started")
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: observedAt.Add(9 * time.Second), Kind: "model",
		Description: "Assistant response completed: done", ResponseCompleted: true, TokensKnown: true, TokenDelta: 77,
	})
	waitForBuffer(t, &output, "Assistant response completed: done")
	run.Status = scheduler.StatusFailed
	run.WorkerLogOpen = false
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("normalized follower did not exit at terminal state")
	}
	got := output.String()
	for _, want := range []string{"Current Worker operation: starting", "Run state changed to failed", "Terminal Run summary:", "State: failed", "Completed Worker tokens: 77"} {
		if !strings.Contains(got, want) {
			t.Fatalf("live normalized output missing %q:\n%s", want, got)
		}
	}
}

func TestFollowNormalizedRetainsPartialProjectionRecordUntilCompleted(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "partial-projection.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	projectionPath := activity.PathForLog(logPath)
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "lifecycle", Description: "Worker started",
	})
	completed, err := json.Marshal(activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "model", Description: "Assistant response completed: after partial",
		ResponseCompleted: true, TokensKnown: true, TokenDelta: 31,
	})
	if err != nil {
		t.Fatal(err)
	}
	half := len(completed) / 2
	projection, err := os.OpenFile(projectionPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Write(completed[:half]); err != nil {
		projection.Close()
		t.Fatal(err)
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 28, RunID: "partial-projection", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 2829, ProcessIdentity: "2829:start", LogPath: logPath, WorkerLogOpen: true, StartedAt: time.Now(),
		SessionID: "session-partial-projection", SessionDir: filepath.Join(directory, "session"),
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followNormalized(ctx, store, run.RunID, &output, io.Discard, 5*time.Millisecond, time.Now)
	}()
	waitForBuffer(t, &output, "Worker started")
	projection, err = os.OpenFile(projectionPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projection.Write(append(completed[half:], '\n')); err != nil {
		projection.Close()
		t.Fatal(err)
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}
	waitForBuffer(t, &output, "Assistant response completed: after partial")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(output.String(), "Assistant response completed: after partial"); count != 1 {
		t.Fatalf("completed partial entries = %d, output = %q", count, output.String())
	}
}

func TestFollowNormalizedReportsMissingUsageWhenAnyCompletedResponseOmitsIt(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "missing-usage.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath),
		activity.Entry{
			Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "model", Description: "Assistant response completed: known",
			ResponseCompleted: true, TokensKnown: true, TokenDelta: 41,
		},
		activity.Entry{
			Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "model", Description: "Assistant response completed: missing",
			ResponseCompleted: true,
		},
	)
	run := scheduler.Run{Issue: 28, RunID: "missing-usage", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC, LogPath: logPath}
	var output bytes.Buffer
	if err := followNormalized(context.Background(), &sequenceFollowSource{runs: []scheduler.Run{run}}, run.RunID, &output, io.Discard, time.Millisecond, time.Now); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.Contains(got, "Completed Worker tokens: n/a") {
		t.Fatalf("missing usage output = %q", got)
	}
}

func TestFollowNormalizedFallsBackWhenLiveProjectionBecomesUnavailable(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "live-fallback.jsonl")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_start\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	projectionPath := activity.PathForLog(logPath)
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "lifecycle", Description: "Worker started",
		Operation: "starting", OperationChanged: true,
	})
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 28, RunID: "live-fallback", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 2828, ProcessIdentity: "2828:start", LogPath: logPath, WorkerLogOpen: true, StartedAt: time.Now().Add(-time.Minute),
		SessionID: "session-live-fallback", SessionDir: filepath.Join(directory, "session"),
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followNormalized(context.Background(), store, run.RunID, &output, &diagnostics, 5*time.Millisecond, time.Now)
	}()
	waitForBuffer(t, &output, "Worker started")
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(log, "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"done\"}],\"usage\":{\"totalTokens\":55}}}\n"); err != nil {
		log.Close()
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activity.UnavailablePath(projectionPath), []byte("append failed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForBuffer(t, &diagnostics, "Activity projection unavailable: append failed")
	waitForBuffer(t, &output, "Assistant response completed: done")
	run.Status = scheduler.StatusFailed
	run.WorkerLogOpen = false
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("normalized fallback follower did not exit at terminal state")
	}
	got := output.String()
	if strings.Count(got, "Worker started") != 1 || !strings.Contains(got, "Completed Worker tokens: 55") || !strings.Contains(diagnostics.String(), "replayed Activity age is n/a") {
		t.Fatalf("live fallback output = %q, diagnostics = %q", got, diagnostics.String())
	}
}

func TestFollowNormalizedFallsBackWhenLiveProjectionCannotBeRead(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "live-read-fallback.jsonl")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_start\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	projectionPath := activity.PathForLog(logPath)
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "lifecycle", Description: "Worker started",
		Operation: "starting", OperationChanged: true,
	})
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 28, RunID: "live-read-fallback", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 2830, ProcessIdentity: "2830:start", LogPath: logPath, WorkerLogOpen: true, StartedAt: time.Now().Add(-time.Minute),
		SessionID: "session-live-read-fallback", SessionDir: filepath.Join(directory, "session"),
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followNormalized(context.Background(), store, run.RunID, &output, &diagnostics, 5*time.Millisecond, time.Now)
	}()
	waitForBuffer(t, &output, "Worker started")
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(log, "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[],\"usage\":{\"totalTokens\":89}}}\n"); err != nil {
		log.Close()
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(projectionPath); err != nil {
		t.Fatal(err)
	}
	waitForBuffer(t, &diagnostics, "Activity projection unavailable:")
	waitForBuffer(t, &output, "Assistant response completed")
	run.Status = scheduler.StatusFailed
	run.WorkerLogOpen = false
	run.UpdatedAt = time.Now()
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("normalized read-fallback follower did not exit")
	}
	got := output.String()
	if strings.Count(got, "Worker started") != 1 || !strings.Contains(got, "Completed Worker tokens: 89") {
		t.Fatalf("live read fallback output = %q, diagnostics = %q", got, diagnostics.String())
	}
}

func TestFollowNormalizedPropagatesOutputFailure(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "output-failure.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "lifecycle", Description: "Worker settled",
	})
	run := scheduler.Run{Issue: 28, RunID: "output-failure", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath}
	if err := followNormalized(context.Background(), &sequenceFollowSource{runs: []scheduler.Run{run}}, run.RunID, failingWriter{}, io.Discard, time.Millisecond, time.Now); err == nil || !strings.Contains(err.Error(), "output denied") {
		t.Fatalf("normalized output error = %v", err)
	}
}

func TestFollowNormalizedDiagnosesBadProjectionWithoutFailingRunObservation(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "bad-projection.jsonl")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_settled\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	valid, err := json.Marshal(activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now(), Kind: "lifecycle", Description: "Worker settled",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activity.PathForLog(logPath), append([]byte("not-json\n"), append(valid, '\n')...), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{Issue: 28, RunID: "bad-projection", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC, LogPath: logPath}
	var output, diagnostics bytes.Buffer
	if err := followNormalized(context.Background(), &sequenceFollowSource{runs: []scheduler.Run{run}}, run.RunID, &output, &diagnostics, time.Millisecond, time.Now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Worker settled") || !strings.Contains(diagnostics.String(), "unusable Activity projection record; replaying raw Worker Activity") {
		t.Fatalf("output = %q, diagnostics = %q", output.String(), diagnostics.String())
	}
}

func writeActivityEntries(t *testing.T, path string, entries ...activity.Entry) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

type synchronizedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(data)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func waitForBuffer(t *testing.T, buffer *synchronizedBuffer, contains string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buffer.String(), contains) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("buffer %q never contained %q", buffer.String(), contains)
}

type failingFollowSource struct{ err error }

func (s failingFollowSource) Preview() (state.State, bool, error) {
	return state.State{}, false, s.err
}

type sequenceFollowSource struct {
	mu   sync.Mutex
	runs []scheduler.Run
}

func (s *sequenceFollowSource) Preview() (state.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run := s.runs[0]
	if len(s.runs) > 1 {
		s.runs = s.runs[1:]
	}
	return state.State{Runs: []scheduler.Run{run}}, false, nil
}

type closingFollowSource struct {
	mu       sync.Mutex
	run      scheduler.Run
	previews int
}

func (s *closingFollowSource) Preview() (state.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.previews++
	if s.previews == 2 {
		s.run.Status = scheduler.StatusMerged
	}
	if s.previews == 3 {
		log, err := os.OpenFile(s.run.LogPath, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			return state.State{}, false, err
		}
		if _, err := io.WriteString(log, "after-terminal\n"); err != nil {
			log.Close()
			return state.State{}, false, err
		}
		if err := log.Close(); err != nil {
			return state.State{}, false, err
		}
		s.run.WorkerLogOpen = false
	}
	return state.State{Runs: []scheduler.Run{s.run}}, false, nil
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("output denied") }

type cancelingWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
	once   sync.Once
}

func (w *cancelingWriter) Write(data []byte) (int, error) {
	written, err := w.Buffer.Write(data)
	w.once.Do(w.cancel)
	return written, err
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type partialWriter struct{ bytes.Buffer }

func (w *partialWriter) Write(data []byte) (int, error) {
	if len(data) > 1 {
		data = data[:1]
	}
	return w.Buffer.Write(data)
}
