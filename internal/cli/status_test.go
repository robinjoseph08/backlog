package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
			ID: "status-subagent", Description: "Implement \x1bstatus", Status: "running", Activity: "test\ting",
			Turns: &turns, ToolUses: &tools, ApproxTokens: &subagentTokens, Active: true,
		}},
	)
	processIdentity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	suspended := statusSuspendedRun(now)
	completedAt := now.Add(-30 * time.Minute)
	runs := []scheduler.Run{
		{Issue: 1, IssueTitle: "Create \x1bworktree", RunID: "claimed", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint, Branch: "agent/issue-\t1"},
		{Issue: 2, RunID: "worktree", Status: scheduler.StatusWorktreeReady, WorkerMode: scheduler.WorkerModePrint, Branch: "agent/issue-2"},
		{Issue: 3, IssueTitle: "Observable progress", IssueURL: "https://example.test/\rissues/3", RunID: "running", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
			PID: os.Getpid(), ProcessIdentity: processIdentity, StartedAt: now.Add(-time.Minute), LogPath: logPath, Branch: "agent/issue-3"},
		{Issue: 4, RunID: "waiting", Status: scheduler.StatusWaitingForMerge, WorkerMode: scheduler.WorkerModePrint, PullRequest: "https://example.test/pull/4"},
		suspended,
		{Issue: 6, RunID: "failed-retained", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "inspect protocol failure"},
		{Issue: 7, RunID: "human-retained", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint, Error: "verify GitHub outcome"},
		{Issue: 8, RunID: "resetting-retained", Status: scheduler.StatusResetting, WorkerMode: scheduler.WorkerModePrint, Error: "remote branch remains"},
		{Issue: 9, IssueTitle: "Old failure", RunID: "failed-\x1bhistory", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "historical \rdiagnostic"},
		{Issue: 10, RunID: "merged-history", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, PullRequest: "https://example.test/pull/10",
			CompletedAt: &completedAt, CleanupPending: true, Error: "completion verified; worktree cleanup remains pending"},
		{Issue: 11, RunID: "reset-history", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint, Error: "preserved Reset diagnostic"},
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
		"#1  Create worktree", "State: claimed", "Branch: agent/issue-1", "#2  worktree-ready",
		"Worker has not been released to begin AFK work", "Branch: agent/issue-2", "#3  Observable progress  running",
		"Issue: https://example.test/issues/3", "Worker liveness: alive", "Activity age:",
		`Current deepest operation: Subagent "Implement \x1bstatus": testing`, "Turns: Worker 1 | Subagent ~2", "Observed tokens: ~1600", "Branch: agent/issue-3",
		"#4  waiting-for-merge", "waiting for merge reconciliation; Worker not active", "Pull request: https://example.test/pull/4",
		"#5  suspended", "continuation recorded; Runner will recheck Resume eligibility", "Suspended: " + now.Add(-time.Hour).Format(time.RFC3339),
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
		t.Fatalf("released failed Run appeared as attention:\n%s", attention)
	}
	history := statusSectionOutput(t, output, "History", "")
	for _, want := range []string{
		"#9  Old failure  failed", "Run: failed-history", "historical diagnostic", "#10  merged", "Completion: verified merged",
		"Pull request: https://example.test/pull/10", "Completed: " + completedAt.Format(time.RFC3339),
		"Completion cleanup: pending; the next runner startup will retry", "completion verified; worktree cleanup remains pending",
		"#11  reset", "Reset completed; Lease released", "preserved Reset diagnostic",
	} {
		if !strings.Contains(history, want) {
			t.Fatalf("History section missing %q:\n%s", want, history)
		}
	}
	for _, control := range []string{"\x1b", "\r", "\t"} {
		if strings.Contains(output, control) {
			t.Fatalf("redirected status contained control %q: %q", control, output)
		}
	}
	for _, run := range runs {
		id := plainStatusValue(run.RunID)
		if count := strings.Count(output, "Run: "+id+" | State:"); count != 1 {
			t.Fatalf("Run %q appeared %d times, want exactly once:\n%s", id, count, output)
		}
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
	assertStatusRunSection(t, active, "moving", "Active")

	run.Status, run.PID, run.ProcessIdentity, run.Error = scheduler.StatusFailed, 0, "", "worker stopped"
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease}}); err != nil {
		t.Fatal(err)
	}
	attention := runStatusCommand(t, repository, stateDir)
	assertStatusRunSection(t, attention, "moving", "Attention Required")

	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	history := runStatusCommand(t, repository, stateDir)
	assertStatusRunSection(t, history, "moving", "History")
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

func TestStatusVerifiesWorkerProcessIdentity(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	run := scheduler.Run{
		Issue: 13, RunID: "stale-pid", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: fmt.Sprintf("%d:different-start", os.Getpid()), StartedAt: time.Now(),
	}
	lease := scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease},
	}); err != nil {
		t.Fatal(err)
	}

	active := statusSectionOutput(t, runStatusCommand(t, repository, stateDir), "Active", "Attention Required")
	if !strings.Contains(active, "Worker liveness: dead (stale PID") || strings.Contains(active, "Worker liveness: alive") {
		t.Fatalf("status did not verify the process-start identity:\n%s", active)
	}
}

func TestStatusLoadsWhenRunningTelemetrySourceIsUnavailable(t *testing.T) {
	repository := initializeFollowRepository(t)
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		logPath func(string) string
	}{
		{name: "missing", logPath: func(stateDir string) string { return filepath.Join(stateDir, "missing.jsonl") }},
		{name: "unreadable directory", logPath: func(stateDir string) string { return stateDir }},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateDir := t.TempDir()
			run := scheduler.Run{
				Issue: 14, RunID: "unavailable", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
				PID: os.Getpid(), ProcessIdentity: identity, StartedAt: time.Now(), LogPath: test.logPath(stateDir),
			}
			lease := scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}
			if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
				Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease},
			}); err != nil {
				t.Fatal(err)
			}

			active := statusSectionOutput(t, runStatusCommand(t, repository, stateDir), "Active", "Attention Required")
			for _, want := range []string{"Activity age: n/a", "Current deepest operation: n/a", "Turns: Worker n/a | Subagent n/a", "Observed tokens: n/a"} {
				if !strings.Contains(active, want) {
					t.Fatalf("status with unavailable telemetry missing %q:\n%s", want, active)
				}
			}
		})
	}
}

func TestStatusUsesExactSharedActivityAge(t *testing.T) {
	stateDir := t.TempDir()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	logPath := filepath.Join(stateDir, "running.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-5 * time.Second), Kind: "tool", Description: "Tool read started",
	})
	run := scheduler.Run{Issue: 15, RunID: "activity-age", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath}
	current := state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	var output bytes.Buffer
	if err := printPlainStatus(&output, current, &sequenceFollowSource{}, now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Activity age: 5s") {
		t.Fatalf("status Activity age did not use the latest shared Activity timestamp:\n%s", output.String())
	}
}

func TestStatusMarksInvalidWorkerTurnTelemetryUnavailable(t *testing.T) {
	stateDir := t.TempDir()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	logPath := filepath.Join(stateDir, "running.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now, Kind: "turn", Description: "corrupt turn", TurnDelta: -1,
	})
	run := scheduler.Run{Issue: 16, RunID: "invalid-turn", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath}
	current := state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run},
		Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	var output bytes.Buffer
	if err := printPlainStatus(&output, current, &sequenceFollowSource{}, now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Turns: Worker n/a | Subagent n/a") {
		t.Fatalf("invalid Worker turn telemetry did not degrade to unavailable:\n%s", output.String())
	}
}

func TestStatusJSONRemainsLifecycleStateWithoutObservationCounters(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	current := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 31, IssueTitle: "Shared status", IssueURL: "https://example.test/issues/31", RunID: "json", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint,
	}}}
	if err := store.Save(current); err != nil {
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
	expectedJSON, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	var expected, actual any
	if err := json.Unmarshal(expectedJSON, &expected); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(stdout.Bytes(), &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("status JSON differs from the complete lifecycle state:\nactual: %s\nexpected: %s", stdout.String(), expectedJSON)
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

func assertStatusRunSection(t *testing.T, output, runID, expectedSection string) {
	t.Helper()
	sections := []struct {
		name string
		next string
	}{
		{name: "Active", next: "Attention Required"},
		{name: "Attention Required", next: "History"},
		{name: "History"},
	}
	for _, section := range sections {
		contains := strings.Contains(statusSectionOutput(t, output, section.name, section.next), "Run: "+runID+" | State:")
		if contains != (section.name == expectedSection) {
			t.Fatalf("Run %q membership in %s = %t, want %t:\n%s", runID, section.name, contains, section.name == expectedSection, output)
		}
	}
	if count := strings.Count(output, "Run: "+runID+" | State:"); count != 1 {
		t.Fatalf("Run %q appeared %d times, want exactly once:\n%s", runID, count, output)
	}
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
