package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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
	if got := output.String(); !strings.Contains(got, "Run: default-normalized") || !strings.Contains(got, "Run Activity (latest 20)") || !strings.Contains(got, "Worker settled") {
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

func TestFollowCommandResolvesIssueToActiveLeaseAndLatestHistoricalRun(t *testing.T) {
	t.Parallel()

	repository := initializeFollowRepository(t)
	started := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	tests := []struct {
		name  string
		issue string
		state state.State
		want  string
	}{
		{
			name:  "active Lease",
			issue: "30",
			state: state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
				{Issue: 30, RunID: "historical", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, StartedAt: started.Add(time.Hour)},
				{Issue: 30, RunID: "leased", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, StartedAt: started},
			}, Leases: []scheduler.Lease{{LeaseID: "lease-30", Issue: 30, RunID: "leased"}}},
			want: "Run: leased",
		},
		{
			name:  "latest history with Run ID tie breaker",
			issue: "31",
			state: state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
				{Issue: 31, RunID: "older", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, StartedAt: started.Add(-time.Second)},
				{Issue: 31, RunID: "tie-a", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, StartedAt: started},
				{Issue: 31, RunID: "tie-z", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, StartedAt: started},
			}},
			want: "Run: tie-z",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateDir := t.TempDir()
			if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(test.state); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := followCommand(context.Background(), []string{test.issue, "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard); err != nil {
				t.Fatal(err)
			}
			if got := output.String(); !strings.Contains(got, test.want) {
				t.Fatalf("issue Follow output = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFollowCommandIssueWithoutRunDoesNotChangeState(t *testing.T) {
	t.Parallel()

	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 29, RunID: "other", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint,
	}}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = followCommand(context.Background(), []string{"30", "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "issue #30 has no Run to Follow") {
		t.Fatalf("missing issue Run error = %v", err)
	}
	after, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || output.Len() != 0 {
		t.Fatalf("missing issue Run changed state or printed output: before=%s after=%s output=%q", before, after, output.String())
	}
}

func TestFollowCommandPrefersExactNumericRunIDOverIssueSelection(t *testing.T) {
	t.Parallel()

	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 1, RunID: "42", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 42, RunID: "issue-42", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint},
	}}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := followCommand(context.Background(), []string{"42", "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.Contains(got, "Run: 42\n") || strings.Contains(got, "Run: issue-42") {
		t.Fatalf("numeric exact Run-ID output = %q", got)
	}
}

func TestFollowCommandIssueSelectionRemainsOnResolvedRunWhenReplacementAppears(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	selected := scheduler.Run{
		Issue: 30, RunID: "selected-once", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: identity, StartedAt: time.Now(),
	}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{selected}, Leases: []scheduler.Lease{{
		LeaseID: "selected-once", Issue: selected.Issue, RunID: selected.RunID,
	}}}); err != nil {
		t.Fatal(err)
	}
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followCommand(context.Background(), []string{"30", "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard)
	}()
	waitForBuffer(t, &output, "Run: selected-once")
	selected.Status = scheduler.StatusFailed
	replacement := scheduler.Run{
		Issue: 30, RunID: "replacement", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: identity, StartedAt: time.Now().Add(time.Second),
	}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{selected, replacement}, Leases: []scheduler.Lease{{
		LeaseID: "replacement", Issue: replacement.Issue, RunID: replacement.RunID,
	}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("issue follower did not finish selected terminal Run")
	}
	if got := output.String(); strings.Contains(got, "Run: replacement") || !strings.Contains(got, "Terminal Run summary:\nRun: selected-once") {
		t.Fatalf("stable issue attachment output = %q", got)
	}
}

func TestFollowCommandReportsVerifiedWorkerLivenessAndRunnerSupervision(t *testing.T) {
	repository := initializeFollowRepository(t)
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name            string
		identity        string
		supervise       bool
		wantLiveness    string
		wantSupervision string
	}{
		{name: "live Worker and supervised Run", identity: identity, supervise: true, wantLiveness: "alive (PID", wantSupervision: "SUPERVISED"},
		{name: "reused PID and unsupervised Run", identity: fmt.Sprintf("%d:different start", os.Getpid()), wantLiveness: "dead (stale PID", wantSupervision: "UNSUPERVISED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateDir := t.TempDir()
			run := scheduler.Run{
				Issue: 30, RunID: "observed", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
				PID: os.Getpid(), ProcessIdentity: test.identity, StartedAt: time.Now(),
			}
			if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
				Version: state.CurrentVersion, Runs: []scheduler.Run{run},
				Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
			}); err != nil {
				t.Fatal(err)
			}
			if test.supervise {
				supervision, err := establishRunnerSupervision(filepath.Join(repository, ".git"))
				if err != nil {
					t.Fatal(err)
				}
				defer supervision.Release()
			}
			ctx, cancel := context.WithCancel(context.Background())
			var output synchronizedBuffer
			done := make(chan error, 1)
			go func() {
				done <- followCommand(ctx, []string{run.RunID, "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard)
			}()
			waitForBuffer(t, &output, "Worker liveness:")
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			got := output.String()
			if !strings.Contains(got, "Worker liveness: "+test.wantLiveness) || !strings.Contains(got, "Runner supervision: "+test.wantSupervision) {
				t.Fatalf("observed Follow output = %q", got)
			}
			if strings.Contains(strings.ToLower(got), "stalled") {
				t.Fatalf("quiet Activity was labeled stalled: %q", got)
			}
		})
	}
}

func TestFollowWorkerLivenessHandlesIncompleteAndRetainedIdentities(t *testing.T) {
	t.Parallel()

	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		run  scheduler.Run
		want string
	}{
		{name: "PID without identity", run: scheduler.Run{PID: os.Getpid()}, want: fmt.Sprintf("unknown (PID %d has no persisted process-start identity)", os.Getpid())},
		{name: "retained live identity", run: scheduler.Run{ProcessIdentity: identity}, want: fmt.Sprintf("alive (PID %d and process-start identity verified)", os.Getpid())},
		{name: "invalid retained identity", run: scheduler.Run{ProcessIdentity: "invalid"}, want: "unknown (persisted process-start identity has no valid PID)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := followWorkerLiveness(test.run); got != test.want {
				t.Fatalf("Worker liveness = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFollowCommandKeepsObservingUnsupervisedRunAndReportsReturningSupervision(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		Issue: 30, RunID: "supervision-returns", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: identity, StartedAt: time.Now(),
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followCommand(ctx, []string{run.RunID, "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard)
	}()
	waitForBuffer(t, &output, "Runner supervision: UNSUPERVISED")
	supervision, err := establishRunnerSupervision(filepath.Join(repository, ".git"))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer supervision.Release()
	waitForBuffer(t, &output, "Runner supervision changed to SUPERVISED")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFollowCommandReportsWorkerDeathAndKeepsObserving(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	worker := exec.Command("sleep", "30")
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if worker.ProcessState == nil {
			_ = worker.Process.Kill()
			_ = worker.Wait()
		}
	}()
	identity, err := pidStartIdentity(worker.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		Issue: 30, RunID: "Worker-dies", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: worker.Process.Pid, ProcessIdentity: identity, StartedAt: time.Now(),
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{
		LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID,
	}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followCommand(ctx, []string{run.RunID, "--repo-dir", repository, "--state-dir", stateDir}, &output, io.Discard)
	}()
	waitForBuffer(t, &output, "Worker liveness: alive")
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Wait(); err != nil && !strings.Contains(err.Error(), "signal: killed") {
		t.Fatal(err)
	}
	waitForBuffer(t, &output, "Worker liveness changed to dead")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFollowRawCommandPrintsResolvedIssueRunIDWithoutChangingJSONL(t *testing.T) {
	t.Parallel()

	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "raw.jsonl")
	if err := os.WriteFile(logPath, []byte("record\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 30, RunID: "raw-by-issue", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath,
	}}}); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	if err := followCommand(context.Background(), []string{"30", "--raw", "--repo-dir", repository, "--state-dir", stateDir}, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if output.String() != "record\n" || !strings.Contains(diagnostics.String(), "Run: raw-by-issue\n") ||
		!strings.Contains(diagnostics.String(), "Runner supervision: n/a (terminal Run)\n") ||
		!strings.Contains(diagnostics.String(), "Worker liveness: absent\n") {
		t.Fatalf("raw output = %q, diagnostics = %q", output.String(), diagnostics.String())
	}
}

func TestFollowRawCommandReportsObservationChangesOnStderr(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "raw.jsonl")
	if err := os.WriteFile(logPath, []byte("record\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	worker := exec.Command("sleep", "30")
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if worker.ProcessState == nil {
			_ = worker.Process.Kill()
			_ = worker.Wait()
		}
	}()
	identity, err := pidStartIdentity(worker.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		Issue: 30, RunID: "raw-observations", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: worker.Process.Pid, ProcessIdentity: identity, LogPath: logPath, WorkerLogOpen: true, StartedAt: time.Now(),
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	supervision, err := establishRunnerSupervision(filepath.Join(repository, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var output, diagnostics synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followCommand(ctx, []string{run.RunID, "--raw", "--repo-dir", repository, "--state-dir", stateDir}, &output, &diagnostics)
	}()
	waitForBuffer(t, &output, "record\n")
	waitForBuffer(t, &diagnostics, "Runner supervision: SUPERVISED")
	waitForBuffer(t, &diagnostics, "Worker liveness: alive")
	if err := supervision.Release(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Wait(); err != nil && !strings.Contains(err.Error(), "signal: killed") {
		t.Fatal(err)
	}
	waitForBuffer(t, &diagnostics, "Runner supervision changed to UNSUPERVISED")
	waitForBuffer(t, &diagnostics, "Worker liveness changed to dead")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "record\n" {
		t.Fatalf("raw JSONL changed while reporting observations: %q", got)
	}
}

func initializeFollowRepository(t *testing.T) string {
	t.Helper()
	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	return repository
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

func TestResolveFollowSelectorReportsStateFailure(t *testing.T) {
	t.Parallel()

	_, err := resolveFollowSelector(failingFollowSource{err: errors.New("state denied")}, "30")
	if err == nil || !strings.Contains(err.Error(), `resolve Follow selector "30": read runner state: state denied`) {
		t.Fatalf("selector state error = %v", err)
	}
}

func TestFollowRawCommandPropagatesResolvedIdentityOutputFailure(t *testing.T) {
	t.Parallel()

	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "raw.jsonl")
	if err := os.WriteFile(logPath, []byte("record\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 30, RunID: "raw-output-failure", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := followCommand(context.Background(), []string{"30", "--raw", "--repo-dir", repository, "--state-dir", stateDir}, io.Discard, failingWriter{}); err == nil || !strings.Contains(err.Error(), "output denied") {
		t.Fatalf("raw identity output error = %v", err)
	}
}

func TestRepositoryFollowSourceReportsMalformedRunnerIdentityAsUnsupervised(t *testing.T) {
	t.Parallel()

	commonDirectory := t.TempDir()
	if err := os.WriteFile(filepath.Join(commonDirectory, runnerSupervisionFile), []byte("not JSON"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := repositoryFollowSource{commonDirectory: commonDirectory}
	if _, err := source.RunnerSupervised(); err == nil || !strings.Contains(err.Error(), "read Runner supervision marker") {
		t.Fatalf("Runner supervision observation error = %v", err)
	}
	observation := observeFollowRun(source, scheduler.Run{Status: scheduler.StatusRunning})
	if !strings.HasPrefix(observation.supervision, "UNSUPERVISED") {
		t.Fatalf("Follow supervision with malformed Runner identity = %q", observation.supervision)
	}
}

func TestRepositoryFollowSourceRejectsReusedRunnerPID(t *testing.T) {
	t.Parallel()

	commonDirectory := t.TempDir()
	if err := writeRunnerProcessIdentity(filepath.Join(commonDirectory, runnerSupervisionFile), runnerProcessIdentity{
		PID: os.Getpid(), ProcessIdentity: fmt.Sprintf("%d:different start", os.Getpid()),
	}); err != nil {
		t.Fatal(err)
	}
	if supervised, err := (repositoryFollowSource{commonDirectory: commonDirectory}).RunnerSupervised(); err != nil || supervised {
		t.Fatalf("Runner supervision with reused PID = %t, %v", supervised, err)
	}
}

func TestRepositoryFollowSourceDoesNotBlockRunnerCoordination(t *testing.T) {
	t.Parallel()

	commonDirectory := t.TempDir()
	source := repositoryFollowSource{commonDirectory: commonDirectory}
	if supervised, err := source.RunnerSupervised(); err != nil || supervised {
		t.Fatalf("initial Runner supervision = %t, %v", supervised, err)
	}
	lock, err := acquireRepositoryLock(commonDirectory)
	if err != nil {
		t.Fatalf("passive Follow observation blocked Runner coordination: %v", err)
	}
	defer lock.Release()
}

func TestRepositoryFollowSourceDoesNotTreatResetAsRunnerSupervision(t *testing.T) {
	t.Parallel()

	commonDirectory := t.TempDir()
	lock, err := acquireResetReadLock(commonDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	source := repositoryFollowSource{commonDirectory: commonDirectory}
	if supervised, err := source.RunnerSupervised(); err != nil || supervised {
		t.Fatalf("Runner supervision during Reset = %t, %v", supervised, err)
	}
}

func TestResettingRunRemainsUnsupervisedWithActiveRunner(t *testing.T) {
	t.Parallel()

	commonDirectory := t.TempDir()
	supervision, err := establishRunnerSupervision(commonDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer supervision.Release()
	observation := observeFollowRun(repositoryFollowSource{commonDirectory: commonDirectory}, scheduler.Run{Status: scheduler.StatusResetting})
	if observation.supervision != "UNSUPERVISED" {
		t.Fatalf("resetting Run supervision = %q", observation.supervision)
	}
}

func TestFollowObservationThrottleForTerminalRunWithOpenLog(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 27, 1, 2, 3, 0, time.UTC)
	nextObservation := now.Add(time.Second)
	if !followObservationDue(scheduler.StatusFailed, true, now, nextObservation) {
		t.Fatal("terminal transition did not trigger an immediate observation")
	}
	if followObservationDue(scheduler.StatusFailed, false, now.Add(50*time.Millisecond), nextObservation) {
		t.Fatal("terminal Run with an open log bypassed the observation throttle")
	}
	if !followObservationDue(scheduler.StatusFailed, false, nextObservation, nextObservation) {
		t.Fatal("terminal Run was not observed when the throttle elapsed")
	}
}

func TestFollowRequiresRunID(t *testing.T) {
	t.Parallel()

	if _, _, err := splitFollowArguments(nil); err == nil || !strings.Contains(err.Error(), "backlog follow <run-id|positive-issue-number> [--raw]") {
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
		"Completed Worker turns: 1", "Completed Worker tokens: 200", "Observed tokens: 200", "Visible final answer", "Second visible answer",
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

func TestFollowCommandActivityAgeAdvancesOnlyForSemanticWorkerAndSubagentChanges(t *testing.T) {
	repository := initializeFollowRepository(t)
	observedAt := time.Date(2026, 7, 27, 1, 2, 3, 0, time.UTC)
	now := observedAt.Add(10 * time.Second)
	workerStart := `{"type":"tool_execution_start","toolCallId":"worker-tool","toolName":"bash","args":{"command":"private Worker arguments"}}`
	workerUpdate := `{"type":"tool_execution_update","toolCallId":"worker-tool","toolName":"bash","partialResult":{"output":"private Worker result","durationMs":10,"spinnerFrame":1}}`
	subagentStart := `{"type":"tool_execution_start","toolCallId":"subagent-tool","toolName":"Agent","args":{"prompt":"private Subagent prompt"}}`
	subagentSnapshot := `{"type":"tool_execution_update","toolCallId":"subagent-tool","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"private Subagent output"}],"details":{"description":"Check Activity age","status":"running","activity":"reviewing","turnCount":1,"toolUses":1,"tokens":"100 tokens","durationMs":10,"spinnerFrame":1}}}`

	tests := []struct {
		name      string
		records   []timedWorkerRecord
		wantAge   string
		wantEntry string
	}{
		{
			name: "meaningful Worker output advances age",
			records: []timedWorkerRecord{
				{observedAt, workerStart},
				{observedAt, workerUpdate},
				{observedAt.Add(4 * time.Second), strings.Replace(workerUpdate, "private Worker result", "changed private Worker result", 1)},
			},
			wantAge: "6s", wantEntry: "Tool bash output changed",
		},
		{
			name: "repeated spinner and duration-only Worker updates retain age",
			records: []timedWorkerRecord{
				{observedAt, workerStart},
				{observedAt, workerUpdate},
				{observedAt.Add(4 * time.Second), workerUpdate},
				{observedAt.Add(5 * time.Second), strings.Replace(workerUpdate, `"spinnerFrame":1`, `"spinnerFrame":2`, 1)},
				{observedAt.Add(6 * time.Second), strings.Replace(workerUpdate, `"durationMs":10`, `"durationMs":20`, 1)},
			},
			wantAge: "10s", wantEntry: "Tool bash output changed",
		},
		{
			name: "meaningful Subagent operation advances age",
			records: []timedWorkerRecord{
				{observedAt, subagentStart},
				{observedAt, subagentSnapshot},
				{observedAt.Add(4 * time.Second), strings.Replace(subagentSnapshot, `"activity":"reviewing"`, `"activity":"testing"`, 1)},
			},
			wantAge: "6s", wantEntry: "activity: testing",
		},
		{
			name: "repeated spinner and duration-only Subagent snapshots retain age",
			records: []timedWorkerRecord{
				{observedAt, subagentStart},
				{observedAt, subagentSnapshot},
				{observedAt.Add(4 * time.Second), subagentSnapshot},
				{observedAt.Add(5 * time.Second), strings.Replace(subagentSnapshot, `"spinnerFrame":1`, `"spinnerFrame":2`, 1)},
				{observedAt.Add(6 * time.Second), strings.Replace(subagentSnapshot, `"durationMs":10`, `"durationMs":20`, 1)},
			},
			wantAge: "10s", wantEntry: "Check Activity age",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateDir := t.TempDir()
			logPath := filepath.Join(stateDir, "activity-age.jsonl")
			writeProjectedActivityFixture(t, logPath, test.records)
			run := scheduler.Run{
				Issue: 56, IssueTitle: "Activity age boundary", RunID: "activity-age", Status: scheduler.StatusMerged,
				WorkerMode: scheduler.WorkerModeRPC, LogPath: logPath, StartedAt: observedAt.Add(-time.Minute), UpdatedAt: now,
				SessionID: "activity-age", SessionDir: filepath.Join(stateDir, "sessions", "activity-age"),
			}
			if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
				Version: state.CurrentVersion, Runs: []scheduler.Run{run},
			}); err != nil {
				t.Fatal(err)
			}
			var output, diagnostics bytes.Buffer
			if err := followCommandWithClock(context.Background(), []string{
				run.RunID, "--repo-dir", repository, "--state-dir", stateDir,
			}, &output, &diagnostics, func() time.Time { return now }); err != nil {
				t.Fatal(err)
			}
			got := output.String()
			if !strings.Contains(got, "Activity age: "+test.wantAge) || !strings.Contains(got, test.wantEntry) {
				t.Fatalf("CLI-visible Activity age/output missing %q/%q:\n%s", test.wantAge, test.wantEntry, got)
			}
			for _, private := range []string{"private Worker arguments", "private Worker result", "changed private Worker result", "private Subagent prompt", "private Subagent output"} {
				if strings.Contains(got, private) {
					t.Fatalf("normalized Activity age fixture exposed %q:\n%s", private, got)
				}
			}
			if diagnostics.Len() != 0 {
				t.Fatalf("Follow diagnostics = %q", diagnostics.String())
			}
		})
	}
}

func TestFollowNormalizedShowsDistinctCoalescedSubagentActivityAndApproximateUsage(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "subagents.jsonl")
	records := []string{
		`{"type":"agent_start"}`,
		`{"type":"turn_start"}`,
		`{"type":"message_end","message":{"role":"assistant","content":[],"usage":{"totalTokens":100}}}`,
		`{"type":"turn_end"}`,
		`{"type":"tool_execution_start","toolCallId":"agent-one-identity","toolName":"Agent","args":{"prompt":"full private implementation prompt"}}`,
		`{"type":"tool_execution_update","toolCallId":"agent-one-identity","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"private partial output"}],"details":{"description":"Implement Follow metrics","status":"running","activity":"thinking","turnCount":1,"toolUses":0,"tokens":"1.0k token","durationMs":0,"spinnerFrame":0}}}`,
		`{"type":"tool_execution_update","toolCallId":"agent-one-identity","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"private partial output"}],"details":{"description":"Implement Follow metrics","status":"running","activity":"thinking","turnCount":1,"toolUses":0,"tokens":"1.0k token","durationMs":500,"spinnerFrame":7}}}`,
		`{"type":"tool_execution_update","toolCallId":"agent-one-identity","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"different private output"}],"details":{"description":"Implement Follow metrics","status":"running","activity":"editing","turnCount":1,"toolUses":1,"tokens":"1.2k token","durationMs":600,"spinnerFrame":8}}}`,
		`{"type":"tool_execution_update","toolCallId":"agent-one-identity","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"different private output"}],"details":{"description":"Implement Follow metrics","status":"running","activity":"testing","turnCount":2,"toolUses":3,"tokens":"1.8k token","durationMs":700,"spinnerFrame":9}}}`,
		`{"type":"tool_execution_start","toolCallId":"agent-two-identity","toolName":"Agent","args":{"prompt":"full private review prompt"}}`,
		`{"type":"tool_execution_update","toolCallId":"agent-two-identity","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"private review output"}],"details":{"description":"Review Follow changes","status":"running","activity":"reviewing","turnCount":1,"toolUses":1,"tokens":"500 token","durationMs":100}}}`,
		`{"type":"tool_execution_end","toolCallId":"agent-one-identity","toolName":"Agent","result":{"content":[{"type":"text","text":"full private implementation result"}],"details":{"description":"Implement Follow metrics","status":"completed","turnCount":3,"toolUses":5,"tokens":"2.5k token","durationMs":2500}},"isError":false}`,
		`{"type":"tool_execution_end","toolCallId":"agent-two-identity","toolName":"Agent","result":{"content":[{"type":"text","text":"full private review result"}],"details":{"description":"Review Follow changes","status":"completed","turnCount":2,"toolUses":3,"tokens":"1.5k token","durationMs":1500}},"isError":false}`,
		`{"type":"agent_settled"}`,
	}
	if err := os.WriteFile(logPath, []byte(strings.Join(records, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{Issue: 29, RunID: "subagents", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC, LogPath: logPath}
	var output bytes.Buffer
	if err := followNormalized(context.Background(), &sequenceFollowSource{runs: []scheduler.Run{run}}, run.RunID, &output, io.Discard, time.Millisecond, time.Now); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, want := range []string{
		"Completed Worker turns: 1", "Completed Worker tokens: 100", "Subagents: 2 (0 active)",
		"Approximate Subagent turns: ~5", "Approximate Subagent tool uses: ~8", "Approximate Subagent tokens: ~4000", "Observed tokens: ~4100",
		"Subagent 1 [agent-one-identi]: Implement Follow metrics | status: completed | operation: testing | turns: ~3 | tool uses: ~5 | duration: 2.5s | tokens: ~2500",
		"Subagent 2 [agent-two-identi]: Review Follow changes | status: completed", "reached turn 2", "completed (completed)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("Subagent output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "activity: editing") || strings.Contains(got, "private") || strings.Contains(got, "spinner") {
		t.Fatalf("Subagent output exposed coalesced or private telemetry:\n%s", got)
	}
	if count := strings.Count(got, `Subagent [agent-one-identi] "Implement Follow metrics" status: running`); count != 1 {
		t.Fatalf("initial Subagent snapshots = %d, want 1:\n%s", count, got)
	}
}

func TestFollowMetricsSelectsMostRecentlyActiveSubagentIncludingSuppressedUpdates(t *testing.T) {
	t.Parallel()

	turns, tools := 1, 1
	tokens := int64(100)
	observedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	metrics := followMetrics{}
	metrics.apply(activity.Entry{ObservedAt: observedAt, Description: "first", Subagent: &activity.SubagentSnapshot{
		ID: "first", Description: "First", Status: "running", Activity: "reading", Turns: &turns, ToolUses: &tools, ApproxTokens: &tokens, Active: true,
	}})
	metrics.apply(activity.Entry{ObservedAt: observedAt.Add(time.Second), Description: "second", Subagent: &activity.SubagentSnapshot{
		ID: "second", Description: "Second", Status: "running", Activity: "testing", Turns: &turns, ToolUses: &tools, ApproxTokens: &tokens, Active: true,
	}})
	if active, deepest := metrics.activeSubagentSummary(); active != 2 || deepest != `Subagent "Second": testing` {
		t.Fatalf("initial active summary = %d, %q", active, deepest)
	}
	metrics.apply(activity.Entry{ObservedAt: observedAt.Add(1100 * time.Millisecond), Description: "first suppressed", SuppressFeed: true, Subagent: &activity.SubagentSnapshot{
		ID: "first", Description: "First", Status: "running", Activity: "writing", Turns: &turns, ToolUses: &tools, ApproxTokens: &tokens, Active: true,
	}})
	if active, deepest := metrics.activeSubagentSummary(); active != 2 || deepest != `Subagent "First": writing` {
		t.Fatalf("suppressed active summary = %d, %q", active, deepest)
	}
}

func TestFlushPendingSubagentActivityEventuallyPrintsLatestOperation(t *testing.T) {
	t.Parallel()

	turns, tools := 1, 1
	tokens := int64(100)
	observedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	metrics := followMetrics{}
	metrics.apply(activity.Entry{ObservedAt: observedAt, Description: "Subagent reading", Subagent: &activity.SubagentSnapshot{
		ID: "pending", Description: "Pending", Status: "running", Activity: "reading", Turns: &turns, ToolUses: &tools, ApproxTokens: &tokens, Active: true,
	}})
	metrics.apply(activity.Entry{ObservedAt: observedAt.Add(100 * time.Millisecond), Description: "Subagent writing", SuppressFeed: true, Subagent: &activity.SubagentSnapshot{
		ID: "pending", Description: "Pending", Status: "running", Activity: "writing", Turns: &turns, ToolUses: &tools, ApproxTokens: &tokens, Active: true,
	}})
	var output bytes.Buffer
	if flushed, err := flushPendingSubagentActivity(&output, &metrics, observedAt.Add(time.Second)); err != nil || flushed {
		t.Fatalf("early flush = %t, err = %v, output = %q", flushed, err, output.String())
	}
	if flushed, err := flushPendingSubagentActivity(&output, &metrics, observedAt.Add(1200*time.Millisecond)); err != nil || !flushed {
		t.Fatalf("due flush = %t, err = %v", flushed, err)
	}
	if got := output.String(); !strings.Contains(got, "Subagent writing") {
		t.Fatalf("flushed Activity = %q", got)
	}
	if flushed, err := flushPendingSubagentActivity(&output, &metrics, observedAt.Add(2*time.Second)); err != nil || flushed {
		t.Fatalf("duplicate flush = %t, err = %v", flushed, err)
	}
}

func TestFollowSummaryRejectsOverflowingTelemetry(t *testing.T) {
	t.Parallel()

	maxTokens, one := int64(math.MaxInt64), int64(1)
	turns, tools := 1, 1
	metrics := followMetrics{}
	metrics.apply(activity.Entry{ResponseCompleted: true, TokensKnown: true, TokenDelta: math.MaxInt64})
	metrics.apply(activity.Entry{ResponseCompleted: true, TokensKnown: true, TokenDelta: 1})
	metrics.apply(activity.Entry{Subagent: &activity.SubagentSnapshot{ID: "one", Turns: &turns, ToolUses: &tools, ApproxTokens: &maxTokens}})
	metrics.apply(activity.Entry{Subagent: &activity.SubagentSnapshot{ID: "two", Turns: &turns, ToolUses: &tools, ApproxTokens: &one}})
	var output bytes.Buffer
	if err := printFollowSummary(&output, scheduler.Run{RunID: "overflow"}, metrics, followObservation{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, want := range []string{"Completed Worker tokens: n/a", "Approximate Subagent tokens: n/a", "Observed tokens: n/a"} {
		if !strings.Contains(got, want) {
			t.Fatalf("overflow summary missing %q:\n%s", want, got)
		}
	}
	if duration := displaySubagentDuration(&maxTokens, false, time.Time{}, time.Time{}); duration != "n/a" {
		t.Fatalf("overflow duration = %q", duration)
	}
}

func TestFollowNormalizedRefreshesAgeForSuppressedSubagentActivityAndRetainsMilestones(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "coalesced.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	turnOne, turnTwo, tools := 1, 2, 3
	tokens := int64(1200)
	writeActivityEntries(t, activity.PathForLog(logPath),
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: started, Kind: "subagent", Description: "Subagent started", Subagent: &activity.SubagentSnapshot{ID: "rapid", Description: "Implement rapidly", Status: "running", Activity: "reading", Turns: &turnOne, ToolUses: &tools, ApproxTokens: &tokens, Active: true}},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: started.Add(100 * time.Millisecond), Kind: "subagent", Description: "coalesced editing", SuppressFeed: true, Subagent: &activity.SubagentSnapshot{ID: "rapid", Description: "Implement rapidly", Status: "running", Activity: "editing", Turns: &turnOne, ToolUses: &tools, ApproxTokens: &tokens, Active: true}},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: started.Add(200 * time.Millisecond), Kind: "subagent", Description: "Subagent reached turn 2", Subagent: &activity.SubagentSnapshot{ID: "rapid", Description: "Implement rapidly", Status: "running", Activity: "testing", Turns: &turnTwo, ToolUses: &tools, ApproxTokens: &tokens, Active: true}},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: started.Add(900 * time.Millisecond), Kind: "subagent", Description: "coalesced final operation", SuppressFeed: true, Subagent: &activity.SubagentSnapshot{ID: "rapid", Description: "Implement rapidly", Status: "running", Activity: "writing", Turns: &turnTwo, ToolUses: &tools, ApproxTokens: &tokens, Active: true}},
	)
	run := scheduler.Run{Issue: 29, RunID: "rapid", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC, LogPath: logPath}
	var output bytes.Buffer
	if err := followNormalized(context.Background(), &sequenceFollowSource{runs: []scheduler.Run{run}}, run.RunID, &output, io.Discard, time.Millisecond, func() time.Time { return started.Add(1300 * time.Millisecond) }); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, want := range []string{"Activity age: 0s", "Subagent reached turn 2", `Deepest current operation: Subagent "Implement rapidly": writing`} {
		if !strings.Contains(got, want) {
			t.Fatalf("coalesced output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "coalesced editing") || strings.Contains(got, "coalesced final operation") {
		t.Fatalf("suppressed entries reached the feed:\n%s", got)
	}
}

func TestFollowNormalizedDisplaysMalformedSubagentTelemetryAsUnavailable(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "unavailable-subagent.jsonl")
	records := []string{
		`{"type":"tool_execution_start","toolCallId":"malformed-agent","toolName":"Agent"}`,
		`{"type":"tool_execution_update","toolCallId":"malformed-agent","toolName":"Agent","partialResult":{"content":"hidden output","details":{"description":42,"status":[],"activity":{},"turnCount":"many","toolUses":-1,"tokens":"unknown","durationMs":"long","spinnerFrame":4}}}`,
		`{"type":"tool_execution_end","toolCallId":"malformed-agent","toolName":"Agent","result":{"content":"hidden full result","details":"unavailable"},"isError":false}`,
	}
	if err := os.WriteFile(logPath, []byte(strings.Join(records, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{Issue: 29, RunID: "unavailable-subagent", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC, LogPath: logPath}
	var output bytes.Buffer
	if err := followNormalized(context.Background(), &sequenceFollowSource{runs: []scheduler.Run{run}}, run.RunID, &output, io.Discard, time.Millisecond, time.Now); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, want := range []string{"Subagents: 1 (0 active)", "Subagent 1 [malformed-agent]: n/a | status: n/a | operation: n/a | turns: n/a | tool uses: n/a | duration: n/a | tokens: n/a", "Approximate Subagent tokens: n/a"} {
		if !strings.Contains(got, want) {
			t.Fatalf("unavailable output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "hidden output") || strings.Contains(got, "hidden full result") {
		t.Fatalf("unavailable telemetry leaked tool output:\n%s", got)
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
	if strings.Contains(strings.ToLower(got), "stalled") {
		t.Fatalf("quiet Activity age was labeled stalled:\n%s", got)
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
			if err := printFollowSummary(&output, run, followMetrics{}, followObservation{}, updatedAt.Add(24*time.Hour)); err != nil {
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
	turns, tools := 1, 2
	tokens, duration := int64(1200), int64(800)
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: observedAt.Add(8 * time.Second), Kind: "subagent",
		Description: `Subagent [live-review] "Review changes" status: running; activity: reviewing; reached turn 1`,
		Subagent: &activity.SubagentSnapshot{ID: "live-review", Description: "Review changes", Status: "running", Activity: "reviewing",
			Turns: &turns, ToolUses: &tools, ApproxTokens: &tokens, DurationMillis: &duration, Active: true},
	})
	waitForBuffer(t, &output, `Subagent summary: 1 (1 active) | Deepest current operation: Subagent "Review changes": reviewing`)
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
	for _, want := range []string{"Current Worker operation: starting", `Subagent summary: 1 (1 active) | Deepest current operation: Subagent "Review changes": reviewing`, "duration: 2.8s", "Run state changed to failed", "Terminal Run summary:", "State: failed", "Activity age: 1s", "Completed Worker tokens: 77"} {
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

func TestObserveRunOnceRetainsLiveClockForFollowUpdates(t *testing.T) {
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "running.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	projectionPath := activity.PathForLog(logPath)
	clock := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: clock, Kind: "lifecycle", Description: "Worker started",
	})
	run := scheduler.Run{Issue: 31, RunID: "live-clock", Status: scheduler.StatusRunning, LogPath: logPath}
	observed, source := observeRunOnce(&sequenceFollowSource{}, run, io.Discard, func() time.Time { return clock })
	if source == nil {
		t.Fatal("one-shot observation did not retain its Activity source")
	}
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: clock, Kind: "subagent", Description: "Subagent still working", SuppressFeed: true,
		Subagent: &activity.SubagentSnapshot{ID: "clock", Description: "Clock", Status: "running", Active: true},
	})
	clock = clock.Add(2 * time.Second)
	var output bytes.Buffer
	if err := printNewActivity(&output, &observed.metrics, source); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Subagent still working") {
		t.Fatalf("Follow Activity source retained a frozen clock: %q", output.String())
	}
}

type timedWorkerRecord struct {
	observedAt time.Time
	record     string
}

func writeProjectedActivityFixture(t *testing.T, logPath string, records []timedWorkerRecord) {
	t.Helper()
	var raw strings.Builder
	projection, err := os.OpenFile(activity.PathForLog(logPath), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(projection)
	projector := activity.Projector{}
	for _, record := range records {
		raw.WriteString(record.record)
		raw.WriteByte('\n')
		entry, semantic, err := projector.Observe([]byte(record.record), record.observedAt)
		if err != nil {
			projection.Close()
			t.Fatal(err)
		}
		if semantic {
			if err := encoder.Encode(entry); err != nil {
				projection.Close()
				t.Fatal(err)
			}
		}
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte(raw.String()), 0o600); err != nil {
		t.Fatal(err)
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
	deadline := time.Now().Add(5 * time.Second)
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
