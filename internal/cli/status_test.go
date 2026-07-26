package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestStatusPresentsOperationalSectionsWithSharedRunObservation(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	logPath := filepath.Join(stateDir, "running.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	turns, tools := 2, 3
	subagentTokens := int64(1500)
	writeActivityEntries(t, activity.PathForLog(logPath),
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: now.Add(-8 * time.Second), Kind: "tool", Description: "Tool edit started", Operation: "edit", OperationChanged: true},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: now.Add(-7 * time.Second), Kind: "model", Description: "Assistant response completed", ResponseCompleted: true, TokensKnown: true, TokenDelta: 100},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: now.Add(-6 * time.Second), Kind: "turn", Description: "Worker turn completed", TurnDelta: 1},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: now.Add(-5 * time.Second), Kind: "subagent", Description: "Subagent testing", Subagent: &activity.SubagentSnapshot{
			ID: "status-subagent", Description: "Implement status", Status: "running", Activity: "testing",
			Turns: &turns, ToolUses: &tools, ApproxTokens: &subagentTokens, Active: true,
		}},
	)
	processIdentity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	suspended := statusSuspendedRun(now)
	runs := []scheduler.Run{
		{Issue: 1, IssueTitle: "Create worktree", RunID: "claimed", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint, Branch: "agent/issue-1"},
		{Issue: 2, RunID: "worktree", Status: scheduler.StatusWorktreeReady, WorkerMode: scheduler.WorkerModePrint, Branch: "agent/issue-2"},
		{Issue: 3, IssueTitle: "Observable progress", IssueURL: "https://example.test/issues/3", RunID: "running", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
			PID: os.Getpid(), ProcessIdentity: processIdentity, StartedAt: now.Add(-time.Minute), LogPath: logPath, Branch: "agent/issue-3"},
		{Issue: 4, RunID: "waiting", Status: scheduler.StatusWaitingForMerge, WorkerMode: scheduler.WorkerModePrint, PullRequest: "https://example.test/pull/4"},
		suspended,
		{Issue: 6, RunID: "failed-retained", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "inspect protocol failure"},
		{Issue: 7, RunID: "human-retained", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint, Error: "verify GitHub outcome"},
		{Issue: 8, RunID: "resetting-retained", Status: scheduler.StatusResetting, WorkerMode: scheduler.WorkerModePrint, Error: "remote branch remains"},
		{Issue: 9, IssueTitle: "Old failure", RunID: "failed-history", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "historical diagnostic"},
		{Issue: 10, RunID: "merged-history", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, PullRequest: "https://example.test/pull/10"},
		{Issue: 11, RunID: "reset-history", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint},
	}
	leased := make([]scheduler.Lease, 0, 8)
	for _, run := range runs[:8] {
		leased = append(leased, scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID})
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Repo: "acme/\x1b[31mwidgets", Runs: runs, Leases: leased}); err != nil {
		t.Fatal(err)
	}

	output := runStatusCommand(t, repository, stateDir)
	active := statusSectionOutput(t, output, "Active", "Attention Required")
	for _, want := range []string{
		"#1  Create worktree", "State: claimed", "#2  worktree-ready", "#3  Observable progress  running",
		"Issue: https://example.test/issues/3", "Worker liveness: alive", "Activity age:",
		`Current deepest operation: Subagent "Implement status": testing`, "Turns: Worker 1 | Subagent ~2", "Observed tokens: ~1600",
		"#4  waiting-for-merge", "waiting for merge reconciliation; Worker not active", "#5  suspended", "Worker stopped; Resume available",
	} {
		if !strings.Contains(active, want) {
			t.Fatalf("Active section missing %q:\n%s", want, active)
		}
	}
	if strings.Contains(active, "Activity age: n/a") {
		t.Fatalf("running Run lost available Activity age:\n%s", active)
	}
	attention := statusSectionOutput(t, output, "Attention Required", "History")
	for _, want := range []string{"#6  failed", "inspect protocol failure", "#7  needs-human", "human judgment required", "#8  resetting", "rerun backlog reset"} {
		if !strings.Contains(attention, want) {
			t.Fatalf("Attention Required section missing %q:\n%s", want, attention)
		}
	}
	if strings.Contains(attention, "failed-history") || strings.Contains(attention, "historical diagnostic") {
		t.Fatalf("released historical failure appeared as attention:\n%s", attention)
	}
	history := statusSectionOutput(t, output, "History", "")
	for _, want := range []string{"#9  Old failure  failed", "Run: failed-history", "historical diagnostic", "#10  merged", "Completion: verified merged", "#11  reset", "Reset completed; Lease released"} {
		if !strings.Contains(history, want) {
			t.Fatalf("History section missing %q:\n%s", want, history)
		}
	}
	if strings.Contains(output, "\x1b") {
		t.Fatalf("redirected status contained a terminal control sequence: %q", output)
	}
}

func TestStatusMovesRunFromActiveToAttentionToHistoryWithLeaseLifecycle(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{Issue: 31, IssueTitle: "Movement", RunID: "moving", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: identity, StartedAt: time.Now()}
	lease := scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}

	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease}}); err != nil {
		t.Fatal(err)
	}
	active := runStatusCommand(t, repository, stateDir)
	if !strings.Contains(statusSectionOutput(t, active, "Active", "Attention Required"), "Run: moving") {
		t.Fatalf("running leased Run was not Active:\n%s", active)
	}

	run.Status, run.PID, run.ProcessIdentity, run.Error = scheduler.StatusFailed, 0, "", "worker stopped"
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease}}); err != nil {
		t.Fatal(err)
	}
	attention := runStatusCommand(t, repository, stateDir)
	if !strings.Contains(statusSectionOutput(t, attention, "Attention Required", "History"), "Run: moving") {
		t.Fatalf("failed retained Run was not Attention Required:\n%s", attention)
	}

	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	history := runStatusCommand(t, repository, stateDir)
	if strings.Contains(statusSectionOutput(t, history, "Attention Required", "History"), "Run: moving") ||
		!strings.Contains(statusSectionOutput(t, history, "History", ""), "Run: moving") {
		t.Fatalf("released failed Run did not move exclusively to History:\n%s", history)
	}
}

func TestStatusLoadsLegacyRunWithUnavailableTelemetry(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	legacy := `{"version":1,"repo":"acme/widgets","runs":[{"issue":12,"runId":"legacy-running","status":"running","pid":2147483646,"processIdentity":"2147483646:old","startedAt":"2026-07-01T00:00:00Z"}]}`
	if err := os.WriteFile(filepath.Join(stateDir, "state.json"), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	output := runStatusCommand(t, repository, stateDir)
	active := statusSectionOutput(t, output, "Active", "Attention Required")
	for _, want := range []string{"#12  running", "Worker liveness: dead", "Activity age: n/a", "Current deepest operation: n/a", "Turns: Worker n/a | Subagent n/a", "Observed tokens: n/a"} {
		if !strings.Contains(active, want) {
			t.Fatalf("legacy Active output missing %q:\n%s", want, active)
		}
	}
	persisted, err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Load()
	if err != nil || persisted.Version != state.CurrentVersion || len(persisted.Leases) != 1 {
		t.Fatalf("legacy status migration = %#v, %v", persisted, err)
	}
}

func TestStatusJSONRemainsLifecycleStateWithoutObservationCounters(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 31, IssueTitle: "Shared status", IssueURL: "https://example.test/issues/31", RunID: "json", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint,
	}}}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--repo-dir", repository, "--state-dir", stateDir, "--json"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("status JSON exit = %d, stderr = %q", exit, stderr.String())
	}
	jsonStatus := stdout.String()
	for _, want := range []string{`"issueTitle": "Shared status"`, `"issueUrl": "https://example.test/issues/31"`, `"status": "merged"`} {
		if !strings.Contains(jsonStatus, want) {
			t.Fatalf("status JSON missing %q: %s", want, jsonStatus)
		}
	}
	for _, forbidden := range []string{`"turns"`, `"tokens"`, `"activityAge"`, `"workerLiveness"`} {
		if strings.Contains(jsonStatus, forbidden) {
			t.Fatalf("status JSON included normalized observation field %q: %s", forbidden, jsonStatus)
		}
	}
}

func TestPrintPlainStatusReturnsOutputFailure(t *testing.T) {
	err := printPlainStatus(failingStatusWriter{}, state.State{Version: state.CurrentVersion}, &sequenceFollowSource{}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "status output failed") {
		t.Fatalf("output error = %v", err)
	}
}

type failingStatusWriter struct{}

func (failingStatusWriter) Write([]byte) (int, error) {
	return 0, errors.New("status output failed")
}

func statusSuspendedRun(now time.Time) scheduler.Run {
	sessionDir := "/sessions/suspended"
	worktree := "/worktrees/suspended"
	return scheduler.Run{
		Issue: 5, RunID: "suspended", Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
		SessionID: "session-suspended", SessionDir: sessionDir, Worktree: worktree, SuspendedAt: timePointer(now.Add(-time.Hour)),
		Continuation: &scheduler.ContinuationBoundary{
			SessionID: "session-suspended", SessionFile: filepath.Join(sessionDir, "session.jsonl"), Worktree: worktree,
			LeafID: "leaf", EntryCount: 1, SHA256: strings.Repeat("a", 64), VerifiedAt: now.Add(-time.Hour),
		},
	}
}

func runStatusCommand(t *testing.T, repository, stateDir string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr); exit != 0 {
		t.Fatalf("status exit = %d, stderr = %q", exit, stderr.String())
	}
	return stdout.String()
}

func statusSectionOutput(t *testing.T, output, section, next string) string {
	t.Helper()
	start := strings.Index(output, "\n"+section+" (")
	if start < 0 {
		t.Fatalf("status output has no %s section:\n%s", section, output)
	}
	start++
	if next == "" {
		return output[start:]
	}
	end := strings.Index(output[start:], "\n"+next+" (")
	if end < 0 {
		t.Fatalf("status output has no %s section after %s:\n%s", next, section, output)
	}
	return output[start : start+end]
}
