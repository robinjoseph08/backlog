package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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
	if err := os.WriteFile(logPath, []byte("{\"record\":1}\n{\"record\""), 0o600); err != nil {
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
	if _, err := io.WriteString(log, ":2}\n{\"record\":3}\n"); err != nil {
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
	want := "{\"record\":1}\n{\"record\":2}\n{\"record\":3}\n"
	if got := output.String(); got != want {
		t.Fatalf("raw output = %q, want %q", got, want)
	}
}

func TestFollowRawDrainsRecordsAppendedAfterTerminalStateBeforeWorkerExit(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "terminal-race.jsonl")
	if err := os.WriteFile(logPath, []byte("before-terminal\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 4, RunID: "terminal-race", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: 444, ProcessIdentity: "444:start", StartedAt: time.Now(), LogPath: logPath,
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}

	workerExitObserved := make(chan struct{})
	probeReached := make(chan struct{})
	var probeOnce sync.Once
	probe := func(selected scheduler.Run) (bool, error) {
		if selected.RunID != run.RunID {
			return false, errors.New("unexpected Run")
		}
		select {
		case <-workerExitObserved:
			return false, nil
		default:
			probeOnce.Do(func() { close(probeReached) })
			return true, nil
		}
	}
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followRawWithProcessProbe(context.Background(), store, run.RunID, &output, 5*time.Millisecond, probe)
	}()
	waitForBuffer(t, &output, "before-terminal\n")

	run.Status = scheduler.StatusMerged
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probeReached:
	case <-time.After(2 * time.Second):
		t.Fatal("follower did not wait for terminal Worker's process group")
	}
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(log, "after-terminal\n"); err != nil {
		log.Close()
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	close(workerExitObserved)

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("follower did not exit after Worker process-group exit")
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
	if err := (&rawLogStream{file: closed, output: io.Discard}).emitAvailable(); err == nil || !strings.Contains(err.Error(), "read raw JSONL") {
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

func TestFollowRequiresRawModeAndRunID(t *testing.T) {
	t.Parallel()

	if err := followCommand(context.Background(), []string{"run-1"}, io.Discard, io.Discard); err == nil || err.Error() != "follow currently requires --raw" {
		t.Fatalf("missing-raw error = %v", err)
	}
	if _, _, err := splitFollowArguments(nil); err == nil || !strings.Contains(err.Error(), "backlog follow <run-id> --raw") {
		t.Fatalf("missing-Run error = %v", err)
	}
}

func TestSplitFollowArgumentsAcceptsRunIDBeforeOrAfterFlags(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"run-1", "--raw"},
		{"--raw", "run-1"},
		{"--repo-dir", "/tmp/repo", "run-1", "--raw"},
		{"--state-dir=/tmp/state", "run-1", "--raw"},
	} {
		runID, flags, err := splitFollowArguments(args)
		if err != nil {
			t.Fatalf("split %q: %v", args, err)
		}
		if runID != "run-1" || len(flags) == 0 {
			t.Fatalf("split %q = %q, %q", args, runID, flags)
		}
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

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("output denied") }

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type partialWriter struct{ bytes.Buffer }

func (w *partialWriter) Write(data []byte) (int, error) {
	if len(data) > 1 {
		data = data[:1]
	}
	return w.Buffer.Write(data)
}
