package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
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
	attention := statusSectionOutput(t, output, "Attention Required", "Outcomes to Acknowledge")
	for _, want := range []string{"#6  failed", "inspect protocol failure", "#7  needs-human", "human judgment required", "#8  resetting", "rerun backlog reset"} {
		if !strings.Contains(attention, want) {
			t.Fatalf("Attention Required section missing %q:\n%s", want, attention)
		}
	}
	if strings.Contains(attention, "failed-history") || strings.Contains(attention, "historical diagnostic") {
		t.Fatalf("released failed Run appeared as attention:\n%s", attention)
	}
	outcomes := statusSectionOutput(t, output, "Outcomes to Acknowledge", "Recent Completions")
	for _, want := range []string{"#9  Old failure  failed", "Run: failed-history", "historical diagnostic"} {
		if !strings.Contains(outcomes, want) {
			t.Fatalf("Outcomes section missing %q:\n%s", want, outcomes)
		}
	}
	completions := statusSectionOutput(t, output, "Recent Completions", "")
	for _, want := range []string{
		"#10  merged", "Completion: verified merged", "Pull request: https://example.test/pull/10",
		"Completed: " + completedAt.Format(time.RFC3339), "Completion cleanup: pending; retry with backlog resolve or the next runner startup",
		"completion verified; worktree cleanup remains pending",
	} {
		if !strings.Contains(completions, want) {
			t.Fatalf("Recent Completions section missing %q:\n%s", want, completions)
		}
	}
	if strings.Contains(output, "Run: reset-history | State:") {
		t.Fatalf("completed Reset appeared in concise status:\n%s", output)
	}
	for _, control := range []string{"\x1b", "\r", "\t"} {
		if strings.Contains(output, control) {
			t.Fatalf("redirected status contained control %q: %q", control, output)
		}
	}
	for _, run := range runs {
		id := plainStatusValue(run.RunID)
		want := 1
		if run.Status == scheduler.StatusReset {
			want = 0
		}
		if count := strings.Count(output, "Run: "+id+" | State:"); count != want {
			t.Fatalf("Run %q appeared %d times, want %d:\n%s", id, count, want, output)
		}
	}
}

func TestRedirectedStatusKeepsCanonicalIssueAndPullRequestMetadataPlain(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	completedAt := time.Now().UTC().Add(-time.Minute)
	waiting := scheduler.Run{
		Issue: 71, IssueTitle: "Waiting with resources", IssueURL: "https://github.com/acme/widgets/issues/71",
		RunID: "waiting-resources", Status: scheduler.StatusWaitingForMerge, WorkerMode: scheduler.WorkerModePrint,
		PullRequest: "https://github.com/acme/widgets/pull/171",
	}
	merged := scheduler.Run{
		Issue: 72, IssueTitle: "Merged with resources", IssueURL: "https://github.com/acme/widgets/issues/72",
		RunID: "merged-resources", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint,
		PullRequest: "https://github.com/acme/widgets/pull/172", CompletedAt: &completedAt,
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", Runs: []scheduler.Run{waiting, merged},
		Leases: []scheduler.Lease{{LeaseID: waiting.RunID, Issue: waiting.Issue, RunID: waiting.RunID}},
	}); err != nil {
		t.Fatal(err)
	}

	output := runStatusCommand(t, repository, stateDir)
	for _, want := range []string{
		"Issue: " + waiting.IssueURL,
		"Pull request: " + waiting.PullRequest,
		"Issue: " + merged.IssueURL,
		"Pull request: " + merged.PullRequest,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("redirected status omitted plain resource metadata %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "\x1b") {
		t.Fatalf("redirected status emitted OSC or other terminal controls for canonical resources: %q", output)
	}
}

func TestStatusConciseProjectionAndFullHistoryAreCompleteOrderedAndExplicit(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	acknowledgedAt := base.Add(48 * time.Hour)
	runs := []scheduler.Run{
		{Issue: 1, RunID: "active", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(time.Hour)},
		{Issue: 2, RunID: "attention", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(2 * time.Hour)},
		{Issue: 3, RunID: "outcome-a", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(3 * time.Hour)},
		{Issue: 4, RunID: "outcome-z", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(3 * time.Hour)},
		{Issue: 5, RunID: "acknowledged", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(4 * time.Hour), AcknowledgedAt: &acknowledgedAt},
		{Issue: 6, RunID: "reset", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(5 * time.Hour)},
	}
	for index := 0; index < 12; index++ {
		completedAt := base.Add(time.Duration(10+index) * time.Hour)
		runs = append(runs, scheduler.Run{
			Issue: 100 + index, RunID: fmt.Sprintf("completion-%02d", index), Status: scheduler.StatusMerged,
			WorkerMode: scheduler.WorkerModePrint, UpdatedAt: completedAt, CompletedAt: &completedAt, CleanupPending: index == 0,
		})
	}
	current := state.State{Version: state.CurrentVersion, Runs: runs, Leases: []scheduler.Lease{
		{LeaseID: "active", Issue: 1, RunID: "active"}, {LeaseID: "attention", Issue: 2, RunID: "attention"},
	}}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(current); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}

	output := runStatusCommand(t, repository, stateDir)
	for _, want := range []string{"Runs: 18 total | 15 displayed", "Acknowledged outcomes hidden by default: 1", "Outcomes to Acknowledge (2)", "Recent Completions (11)"} {
		if !strings.Contains(output, want) {
			t.Fatalf("concise status missing %q:\n%s", want, output)
		}
	}
	for _, hidden := range []string{"Run: acknowledged | State:", "Run: reset | State:", "Run: completion-01 | State:"} {
		if strings.Contains(output, hidden) {
			t.Fatalf("concise status included hidden Run %q:\n%s", hidden, output)
		}
	}
	for _, visible := range []string{"Run: outcome-a | State:", "Run: outcome-z | State:", "Run: completion-00 | State:", "Run: completion-02 | State:", "Run: completion-11 | State:"} {
		if !strings.Contains(output, visible) {
			t.Fatalf("concise status omitted %q:\n%s", visible, output)
		}
	}
	outcomes := statusSectionOutput(t, output, "Outcomes to Acknowledge", "Recent Completions")
	if strings.Index(outcomes, "Run: outcome-z") > strings.Index(outcomes, "Run: outcome-a") {
		t.Fatalf("deterministic Run-ID tie breaker was not newest first:\n%s", outcomes)
	}
	completions := statusSectionOutput(t, output, "Recent Completions", "")
	if strings.Index(completions, "Run: completion-11") > strings.Index(completions, "Run: completion-10") {
		t.Fatalf("recent Completions were not newest first:\n%s", completions)
	}
	after, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("default status changed state")
	}

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--all", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr); exit != 0 {
		t.Fatalf("status --all exit=%d stderr=%q", exit, stderr.String())
	}
	all := stdout.String()
	if !strings.Contains(all, "Runs: 18 total | 18 displayed") || !strings.Contains(all, "History (16)") ||
		!strings.Contains(all, "Run: acknowledged | State: failed\n    Acknowledged: "+acknowledgedAt.Format(time.RFC3339)) {
		t.Fatalf("full status did not expose complete acknowledged history:\n%s", all)
	}
	for _, run := range runs {
		if count := strings.Count(all, "Run: "+run.RunID+" | State:"); count != 1 {
			t.Fatalf("full status Run %q count=%d, want 1:\n%s", run.RunID, count, all)
		}
	}

	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), []string{"status", "--json", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr); exit != 0 {
		t.Fatalf("status --json exit=%d stderr=%q", exit, stderr.String())
	}
	var jsonState state.State
	if err := json.Unmarshal(stdout.Bytes(), &jsonState); err != nil {
		t.Fatal(err)
	}
	if len(jsonState.Runs) != len(runs) || findAcknowledgmentRun(t, jsonState, "acknowledged").AcknowledgedAt == nil {
		t.Fatalf("JSON status was filtered or omitted acknowledgment metadata: %#v", jsonState)
	}
}

func TestStatusProjectsStructuredRecoveryDiagnosticsInConciseAndFullViews(t *testing.T) {
	now := time.Date(2026, 7, 29, 4, 0, 0, 0, time.UTC)
	firstRecovery := now.Add(-time.Hour)
	resumeAfter := now.Add(30 * time.Second)
	runs := []scheduler.Run{
		{Issue: 201, RunID: "provider", Status: scheduler.StatusSuspended, FailureClass: scheduler.FailureProviderExhaustion, WorkflowStage: "normal-review", ProviderContinuationAttempts: 1, ResumeAfter: &resumeAfter, Error: "provider retries exhausted", UpdatedAt: now},
		{Issue: 202, RunID: "base", Status: scheduler.StatusFailed, FailureClass: scheduler.FailureBaseAdvancement, WorkflowStage: "integration-refresh", Error: "base advanced", UpdatedAt: now.Add(-time.Minute)},
		{Issue: 203, RunID: "validation", Status: scheduler.StatusFailed, FailureClass: scheduler.FailureValidation, WorkflowStage: "validation", Error: "validation failed", UpdatedAt: now.Add(-2 * time.Minute)},
		{Issue: 204, RunID: "repair", Status: scheduler.StatusFailed, FailureClass: scheduler.FailureRepairBudgetExhaustion, WorkflowStage: "blocked", BlockerKind: "evidence-unavailable", BlockerCause: "hosted evidence absent", BlockerFingerprint: "browser-check", RecoveryCount: 2, FirstRecoveredAt: &firstRecovery, LastRecoveredAt: &now, Error: "repair budget exhausted", UpdatedAt: now.Add(-3 * time.Minute)},
		{Issue: 205, RunID: "unsafe", Status: scheduler.StatusNeedsHuman, FailureClass: scheduler.FailureUnsafeContinuation, WorkflowStage: "publish", Error: "checkpoint hash changed", UpdatedAt: now.Add(-4 * time.Minute)},
	}
	current := state.State{Version: state.CurrentVersion, Runs: runs, Leases: []scheduler.Lease{
		{LeaseID: "provider", Issue: 201, RunID: "provider"}, {LeaseID: "unsafe", Issue: 205, RunID: "unsafe"},
	}}
	for _, full := range []bool{false, true} {
		var output bytes.Buffer
		if err := printPlainStatusProjection(&output, current, &sequenceFollowSource{}, now, full); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{
			"Failure class: provider-exhaustion", "Failure class: base-advancement", "Failure class: validation-failure",
			"Failure class: repair-budget-exhaustion", "Failure class: unsafe-continuation-evidence",
			"Workflow stage: normal-review", "Workflow stage: integration-refresh", "Workflow stage: validation", "Workflow stage: blocked", "Workflow stage: publish",
			"Blocker kind: evidence-unavailable", "Blocker cause: hosted evidence absent", "Blocker fingerprint: browser-check",
			"Provider cooldown until: " + resumeAfter.Format(time.RFC3339), "Provider continuations: 1 of 1", "Explicit recoveries: 2",
		} {
			if !strings.Contains(output.String(), want) {
				t.Fatalf("status full=%t missing %q:\n%s", full, want, output.String())
			}
		}
	}
}

func TestStatusKeepsOperationalSectionsUnboundedAndOrdersEverySectionNewestFirst(t *testing.T) {
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	runs := []scheduler.Run{
		{Issue: 100, RunID: "active-a", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(40 * time.Hour)},
		{Issue: 101, RunID: "active-z", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(40 * time.Hour)},
		{Issue: 102, RunID: "history-a", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(50 * time.Hour)},
		{Issue: 103, RunID: "history-z", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: base.Add(50 * time.Hour)},
	}
	leases := []scheduler.Lease{
		{LeaseID: "active-a", Issue: 100, RunID: "active-a"},
		{LeaseID: "active-z", Issue: 101, RunID: "active-z"},
	}
	for index := 0; index < 12; index++ {
		updatedAt := base.Add(time.Duration(index) * time.Hour)
		attentionID := fmt.Sprintf("attention-%02d", index)
		outcomeID := fmt.Sprintf("outcome-%02d", index)
		runs = append(runs,
			scheduler.Run{Issue: 200 + index, RunID: attentionID, Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updatedAt},
			scheduler.Run{Issue: 300 + index, RunID: outcomeID, Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updatedAt},
		)
		leases = append(leases, scheduler.Lease{LeaseID: attentionID, Issue: 200 + index, RunID: attentionID})
	}
	current := state.State{Version: state.CurrentVersion, Runs: runs, Leases: leases}

	var concise bytes.Buffer
	if err := printPlainStatusProjection(&concise, current, &sequenceFollowSource{}, base.Add(60*time.Hour), false); err != nil {
		t.Fatal(err)
	}
	active := statusSectionOutput(t, concise.String(), "Active", "Attention Required")
	attention := statusSectionOutput(t, concise.String(), "Attention Required", "Outcomes to Acknowledge")
	outcomes := statusSectionOutput(t, concise.String(), "Outcomes to Acknowledge", "Recent Completions")
	if strings.Count(attention, "Run: attention-") != 12 || strings.Count(outcomes, "Run: outcome-") != 12 {
		t.Fatalf("operational sections were bounded:\n%s", concise.String())
	}
	for _, ordering := range []struct {
		section string
		newer   string
		older   string
	}{
		{section: active, newer: "Run: active-z", older: "Run: active-a"},
		{section: attention, newer: "Run: attention-11", older: "Run: attention-10"},
		{section: outcomes, newer: "Run: outcome-11", older: "Run: outcome-10"},
	} {
		if !strings.Contains(ordering.section, ordering.newer) || !strings.Contains(ordering.section, ordering.older) ||
			strings.Index(ordering.section, ordering.newer) > strings.Index(ordering.section, ordering.older) {
			t.Fatalf("section was not newest first: newer=%q older=%q\n%s", ordering.newer, ordering.older, ordering.section)
		}
	}

	var full bytes.Buffer
	if err := printPlainStatusProjection(&full, current, &sequenceFollowSource{}, base.Add(60*time.Hour), true); err != nil {
		t.Fatal(err)
	}
	history := statusSectionOutput(t, full.String(), "History", "")
	if strings.Count(history, "Run: outcome-") != 12 || strings.Index(history, "Run: history-z") > strings.Index(history, "Run: history-a") {
		t.Fatalf("full History was incomplete or not deterministically newest first:\n%s", history)
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
	assertStatusRunSection(t, history, "moving", "Outcomes to Acknowledge")
}

func TestStatusLoadsLegacyRunWithUnavailableTelemetry(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	legacy := `{"version":1,"repo":"acme/widgets","runs":[{"issue":12,"runId":"legacy-running","status":"running","pid":2147483646,"processIdentity":"2147483646:old","startedAt":"2026-07-01T00:00:00Z"}]}`
	statePath := filepath.Join(stateDir, "state.json")
	if err := os.WriteFile(statePath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	output := runStatusCommand(t, repository, stateDir)
	active := statusSectionOutput(t, output, "Active", "Attention Required")
	for _, want := range []string{"#12  running", "Worker liveness: dead", "Activity age: n/a", "Current deepest operation: n/a", "Turns: Worker n/a | Subagent n/a", "Observed tokens: n/a"} {
		if !strings.Contains(active, want) {
			t.Fatalf("legacy Active output missing %q:\n%s", want, active)
		}
	}
	persisted, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(persisted, []byte(legacy)) {
		t.Fatalf("status persisted legacy migration: %s", persisted)
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

func TestStatusCommandPresentsElapsedQuietAgeAndUnavailableTurnsFromSharedProgress(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	logPath := filepath.Join(stateDir, "quiet.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-10 * time.Minute), Kind: "tool", Description: "Tool test started",
	})
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	running := scheduler.Run{
		Issue: 58, RunID: "quiet", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: identity, LogPath: logPath, StartedAt: now.Add(-time.Hour),
	}
	withoutStart := scheduler.Run{Issue: 59, RunID: "not-started", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint}
	current := state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{running, withoutStart},
		Leases: []scheduler.Lease{
			{LeaseID: running.RunID, Issue: running.Issue, RunID: running.RunID},
			{LeaseID: withoutStart.RunID, Issue: withoutStart.Issue, RunID: withoutStart.RunID},
		},
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(current); err != nil {
		t.Fatal(err)
	}
	got := runStatusCommand(t, repository, stateDir)
	quietStart := strings.Index(got, "Run: quiet | State: running")
	if quietStart < 0 {
		t.Fatalf("running status Run missing:\n%s", got)
	}
	quietEnd := strings.Index(got[quietStart:], "Run: not-started | State: claimed")
	if quietEnd < 0 {
		t.Fatalf("claimed status Run missing:\n%s", got)
	}
	quietOutput := got[quietStart : quietStart+quietEnd]
	for _, want := range []string{"Elapsed: 1h0m", "(quiet)", "Turns: Worker n/a | Subagent n/a"} {
		if !strings.Contains(quietOutput, want) {
			t.Fatalf("quiet status output missing %q:\n%s", want, quietOutput)
		}
	}
	if !strings.Contains(got, "Run: not-started | State: claimed\n    Elapsed: n/a") {
		t.Fatalf("status did not show unavailable elapsed time:\n%s", got)
	}
	if strings.Contains(strings.ToLower(got), "stalled") {
		t.Fatalf("quiet status presentation implied a stalled state:\n%s", got)
	}
}

func TestStatusDisplaysElapsedForEveryRunState(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	now := startedAt.Add(2 * time.Hour)
	terminalUpdate := startedAt.Add(30 * time.Minute)
	completedAt := startedAt.Add(time.Hour)
	for _, status := range []scheduler.Status{
		scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge, scheduler.StatusSuspended, scheduler.StatusResetting,
		scheduler.StatusReset, scheduler.StatusMerged, scheduler.StatusFailed, scheduler.StatusNeedsHuman,
	} {
		status := status
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			run := scheduler.Run{Issue: 58, RunID: string(status), Status: status, StartedAt: startedAt, UpdatedAt: terminalUpdate}
			want := "Elapsed: 2h0m0s"
			if scheduler.IsTerminal(status) {
				want = "Elapsed: 30m0s"
			}
			if status == scheduler.StatusMerged {
				run.CompletedAt = &completedAt
				want = "Elapsed: 1h0m0s"
			}
			observed := runObservation{run: run, observed: now}
			var output bytes.Buffer
			printer := statusPrinter{output: &output}
			printer.run(statusRun{run: run, observation: observed})
			if printer.err != nil {
				t.Fatal(printer.err)
			}
			if !strings.Contains(output.String(), want) {
				t.Fatalf("status output missing %q:\n%s", want, output.String())
			}
		})
	}
}

func TestStatusGuidesInterruptedExternalResolutionRecovery(t *testing.T) {
	run := scheduler.Run{Issue: 58, RunID: "interrupted-resolution", Status: scheduler.StatusResolvingExternally}
	var output bytes.Buffer
	printer := statusPrinter{output: &output}
	printer.run(statusRun{run: run, observation: runObservation{run: run, observed: time.Now()}})
	if printer.err != nil {
		t.Fatal(printer.err)
	}
	want := "External Resolution is incomplete; close the issue if needed and rerun backlog resolve, or Reset the Run with backlog reset; a supervising Runner will retry at startup or during watch polling while no Owned Worker is present"
	if !strings.Contains(output.String(), want) {
		t.Fatalf("interrupted External Resolution status missing %q:\n%s", want, output.String())
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

func TestPrintRunFinalSummaryUsesSharedAttentionPresentation(t *testing.T) {
	runs := []scheduler.Run{
		{Issue: 1, RunID: "failed", Status: scheduler.StatusFailed, Error: "inspect failure"},
		{Issue: 2, RunID: "needs-human", Status: scheduler.StatusNeedsHuman, Error: "verify outcome"},
		{Issue: 3, RunID: "resetting", Status: scheduler.StatusResetting, Error: "finish Reset"},
	}
	leases := make([]scheduler.Lease, 0, len(runs))
	for _, run := range runs {
		leases = append(leases, scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID})
	}
	current := state.State{Version: state.CurrentVersion, Runs: runs, Leases: leases}
	var output bytes.Buffer
	if err := printRunFinalSummary(&output, current, &sequenceFollowSource{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Attention Required (3)", "#1  failed", "inspect failure", "#2  needs-human",
		"human judgment required", "#3  resetting", "rerun backlog reset",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("final summary missing %q:\n%s", want, output.String())
		}
	}
	if strings.Contains(output.String(), "History (") {
		t.Fatalf("final summary included History:\n%s", output.String())
	}
}

func TestPrintRunFinalReportIncludesOnlyInvocationCompletionsAndOutcome(t *testing.T) {
	completedAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", Runs: []scheduler.Run{
		{Issue: 10, RunID: "old-completion", Status: scheduler.StatusMerged, CompletedAt: &completedAt},
		{
			Issue: 11, IssueTitle: "Completed now", IssueURL: "https://example.test/issues/11",
			RunID: "new-completion", Status: scheduler.StatusMerged, PullRequest: "https://example.test/pulls/41", CompletedAt: &completedAt,
		},
		{Issue: 12, RunID: "old-retry", Status: scheduler.StatusFailed, Error: "transient retry history"},
		{
			Issue: 13, IssueTitle: "Still active", IssueURL: "https://example.test/issues/13",
			RunID: "active-run", Status: scheduler.StatusRunning,
		},
		{
			Issue: 14, IssueTitle: "Operator decision", IssueURL: "https://example.test/issues/14",
			RunID: "attention-run", Status: scheduler.StatusNeedsHuman, Error: "review retained work",
		},
	}, Leases: []scheduler.Lease{
		{LeaseID: "active-run", Issue: 13, RunID: "active-run"},
		{LeaseID: "attention-run", Issue: 14, RunID: "attention-run"},
	}}
	var output bytes.Buffer
	if err := printRunFinalReport(&output, current, &sequenceFollowSource{}, completedAt, map[string]struct{}{"old-completion": {}}, "Drain complete"); err != nil {
		t.Fatal(err)
	}
	want := `
Final aggregate summary
Final outcome: Drain complete
Repository: acme/widgets
Runs: 5
Active Leases: 2

Completions produced (1)
  #11  Completed now  merged
    Issue: https://example.test/issues/11
    Run: new-completion | State: merged
    Pull request: https://example.test/pulls/41

Active (1)
  #13  Still active  running
    Issue: https://example.test/issues/13
    Run: active-run | State: running

Attention Required (1)
  #14  Operator decision  needs-human
    Issue: https://example.test/issues/14
    Run: attention-run | State: needs-human
    Diagnostic: review retained work
`
	if output.String() != want {
		t.Fatalf("final report ordering or boundaries changed:\ngot:\n%s\nwant:\n%s", output.String(), want)
	}
	for _, omitted := range []string{"old-completion", "old-retry", "transient retry history", "Operational messages", "Admission health"} {
		if strings.Contains(output.String(), omitted) {
			t.Fatalf("final report reproduced invocation-external or transient text %q:\n%s", omitted, output.String())
		}
	}
}

func TestClassifyPersistedStatusSectionsPreservesRunOrder(t *testing.T) {
	current := state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{
			{RunID: "historical-first", Status: scheduler.StatusMerged},
			{RunID: "active-first", Status: scheduler.StatusRunning},
			{RunID: "attention-first", Status: scheduler.StatusFailed},
			{RunID: "active-second", Status: scheduler.StatusWaitingForMerge},
			{RunID: "attention-second", Status: scheduler.StatusNeedsHuman},
			{RunID: "historical-second", Status: scheduler.StatusFailed},
		},
		Leases: []scheduler.Lease{
			{RunID: "active-first"}, {RunID: "attention-first"},
			{RunID: "active-second"}, {RunID: "attention-second"},
		},
	}
	sections := classifyPersistedStatusSections(current)
	for section, want := range map[statusSection][]string{
		statusActive:    {"active-first", "active-second"},
		statusAttention: {"attention-first", "attention-second"},
		statusHistory:   {"historical-first", "historical-second"},
	} {
		got := make([]string, 0, len(sections[section]))
		for _, classified := range sections[section] {
			got = append(got, classified.run.RunID)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("section %d Run order = %v, want %v", section, got, want)
		}
	}
}

func TestPrintRunFinalReportOmitsLiveObservationTelemetry(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	logPath := filepath.Join(t.TempDir(), "retained-live.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-time.Second), Kind: "tool",
		Description: "Tool edit started", Operation: "edit", OperationChanged: true, TurnDelta: 2,
		TokensKnown: true, TokenDelta: 1200,
	})
	processIdentity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	runs := []scheduler.Run{
		{
			Issue: 20, IssueTitle: "Still active", IssueURL: "https://example.test/issues/20",
			RunID: "active-live", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
			PID: os.Getpid(), ProcessIdentity: processIdentity, LogPath: logPath, StartedAt: now.Add(-time.Minute),
		},
		{
			Issue: 21, IssueTitle: "Retained live Worker", IssueURL: "https://example.test/issues/21",
			RunID: "attention-live", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint,
			PID: os.Getpid(), ProcessIdentity: processIdentity, Error: "verify retained Worker outcome",
		},
	}
	current := state.State{Version: state.CurrentVersion, Runs: runs, Leases: []scheduler.Lease{
		{LeaseID: "active-live", Issue: 20, RunID: "active-live"},
		{LeaseID: "attention-live", Issue: 21, RunID: "attention-live"},
	}}
	var output bytes.Buffer
	if err := printRunFinalReport(&output, current, finalReportObservationTrap{}, now, nil, "Error: context canceled"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Final outcome: Error: context canceled", "Active (1)", "#20  Still active  running",
		"Issue: https://example.test/issues/20", "Run: active-live | State: running", "Attention Required (1)",
		"#21  Retained live Worker  needs-human", "Issue: https://example.test/issues/21",
		"Run: attention-live | State: needs-human", "Diagnostic: verify retained Worker outcome",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("final report missing %q:\n%s", want, output.String())
		}
	}
	for _, omitted := range []string{"Worker liveness:", "retained Worker liveness:", "Activity age:", "Current deepest operation:", "Turns:", "Observed tokens:", "Elapsed:"} {
		if strings.Contains(output.String(), omitted) {
			t.Fatalf("final report retained live telemetry %q:\n%s", omitted, output.String())
		}
	}
}

type finalReportObservationTrap struct{}

func (finalReportObservationTrap) Preview() (state.State, bool, error) {
	panic("final report observed live state")
}

func (finalReportObservationTrap) RunnerSupervised() (bool, error) {
	panic("final report observed Runner supervision")
}

func TestPrintRunFinalReportDoesNotReadRawActivity(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "blocking-worker-log")
	if err := syscall.Mkfifo(logPath, 0o600); err != nil {
		t.Fatal(err)
	}
	holder, err := os.OpenFile(logPath, os.O_RDWR|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()

	run := scheduler.Run{Issue: 22, RunID: "blocking-activity", Status: scheduler.StatusRunning, LogPath: logPath}
	current := state.State{
		Version: state.CurrentVersion,
		Runs:    []scheduler.Run{run},
		Leases:  []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	result := make(chan error, 1)
	go func() {
		result <- printRunFinalReport(io.Discard, current, &sequenceFollowSource{}, time.Now(), nil, "Drain complete")
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		_ = holder.Close()
		<-result
		t.Fatal("final report blocked reading raw Worker Activity")
	}
}

func TestPrintRunFinalSummaryReturnsOutputFailure(t *testing.T) {
	err := printRunFinalSummary(failingStatusWriter{}, state.State{Version: state.CurrentVersion}, &sequenceFollowSource{}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "status output failed") {
		t.Fatalf("final summary output error = %v", err)
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
		{name: "Attention Required", next: "Outcomes to Acknowledge"},
		{name: "Outcomes to Acknowledge", next: "Recent Completions"},
		{name: "Recent Completions"},
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
