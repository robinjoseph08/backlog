package recovery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

type memoryStore struct{ value state.State }

func (s *memoryStore) Load() (state.State, error)   { return s.value, nil }
func (s *memoryStore) Save(value state.State) error { s.value = value; return nil }

type fakeGitHub struct {
	issue ghadapter.OwnedRunIssue
	pulls []ghadapter.OwnedRunPullRequest
}

func (f fakeGitHub) OwnedRunResources(context.Context, string, int, string) (ghadapter.OwnedRunIssue, []ghadapter.OwnedRunPullRequest, error) {
	return f.issue, f.pulls, nil
}

type fakeWorktrees struct{ verifyErr error }

func (f fakeWorktrees) Verify(context.Context, worktree.Assignment) error { return f.verifyErr }
func (fakeWorktrees) Cleanup(context.Context, worktree.Assignment) error  { return nil }

type fakeGit struct{ identity GitIdentity }

func (f fakeGit) Verify(context.Context, scheduler.Run) (GitIdentity, error) { return f.identity, nil }

func TestRecoveryDerivesOfflineBoundaryAndSuspendsSameRun(t *testing.T) {
	run, store := recoverableFixture(t)
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	module := newTestModule(t, store, fakeGitHub{issue: openIssue(run.Issue)}, func(int) (bool, error) { return false, nil }, now)

	plan, err := module.Inspect(context.Background(), fmt.Sprint(run.Issue))
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !plan.DerivedOffline || plan.Boundary.WorkflowStage != "afk-coordinator" || plan.Outcome != OutcomeSuspend {
		t.Fatalf("plan = %#v", plan)
	}
	result, err := module.Recover(context.Background(), plan)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	got := store.value.Runs[0]
	if result.Outcome != OutcomeAlready || got.Status != scheduler.StatusSuspended || got.RunID != run.RunID || got.Branch != run.Branch || got.Worktree != run.Worktree || got.SessionID != run.SessionID || got.Continuation == nil {
		t.Fatalf("recovered Run = %#v, result = %#v", got, result)
	}
	if got.Error != "repair budget exhausted" || got.PreservedCause != got.Error || got.RecoveryCount != 1 || got.FirstRecoveredAt == nil || !got.FirstRecoveredAt.Equal(now) || got.LastRecoveredAt == nil || !got.LastRecoveredAt.Equal(now) || len(store.value.Leases) != 1 {
		t.Fatalf("Recovery metadata = %#v", got)
	}

	idempotent, err := module.Inspect(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("inspect idempotent: %v", err)
	}
	if _, err := module.Recover(context.Background(), idempotent); err != nil {
		t.Fatalf("idempotent recover: %v", err)
	}
	if store.value.Runs[0].RecoveryCount != 1 {
		t.Fatalf("Recovery count after idempotence = %d", store.value.Runs[0].RecoveryCount)
	}
}

func TestRecoveryAcceptsConclusiveAbsenceFromRetainedProcessIdentity(t *testing.T) {
	run, store := recoverableFixture(t)
	run.ProcessIdentity = "456:old worker start"
	store.value.Runs[0] = run
	var checked []int
	absent := func(pid int) (bool, error) {
		checked = append(checked, pid)
		return false, nil
	}
	module := newTestModule(t, store, fakeGitHub{issue: openIssue(run.Issue)}, absent, time.Now())
	if _, err := module.Inspect(context.Background(), run.RunID); err != nil {
		t.Fatalf("inspect retained process identity: %v", err)
	}
	if len(checked) != 2 || checked[0] != 456 || checked[1] != 456 {
		t.Fatalf("absence checks = %v", checked)
	}
}

func TestRecoveryRefusesLiveOrUncertainWorkerWithoutChangingLease(t *testing.T) {
	run, store := recoverableFixture(t)
	run.PID = 123
	run.ProcessIdentity = "123:started"
	store.value.Runs[0] = run
	for _, test := range []struct {
		name  string
		alive func(int) (bool, error)
		want  string
	}{
		{name: "live", alive: func(pid int) (bool, error) { return pid > 0, nil }, want: "still live"},
		{name: "uncertain", alive: func(int) (bool, error) { return false, os.ErrPermission }, want: "absence"},
	} {
		t.Run(test.name, func(t *testing.T) {
			module := newTestModule(t, store, fakeGitHub{issue: openIssue(run.Issue)}, test.alive, time.Now())
			_, err := module.Inspect(context.Background(), run.RunID)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if len(store.value.Leases) != 1 || store.value.Runs[0].Status != scheduler.StatusFailed {
				t.Fatalf("refusal mutated state = %#v", store.value)
			}
		})
	}
}

func TestRecoveryRefusesUncertainProcessGroupAndAmbiguousOfflineSession(t *testing.T) {
	run, store := recoverableFixture(t)
	run.PID = 123
	run.ProcessIdentity = "123:started"
	store.value.Runs[0] = run
	module, err := New(Config{
		Store: store, GitHub: fakeGitHub{issue: openIssue(run.Issue)}, Worktrees: fakeWorktrees{},
		Git: fakeGit{identity: GitIdentity{LocalCommit: strings.Repeat("a", 40)}}, Repo: "acme/widgets",
		ProcessAlive: func(int) (bool, error) { return false, nil }, ProcessGroupAlive: func(int) (bool, error) { return true, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := module.Inspect(context.Background(), run.RunID); err == nil || !strings.Contains(err.Error(), "process group is still live") {
		t.Fatalf("process-group error = %v", err)
	}
	if len(store.value.Leases) != 1 {
		t.Fatal("process-group refusal released Lease")
	}

	run.PID, run.ProcessIdentity = 0, ""
	store.value.Runs[0] = run
	if err := os.WriteFile(filepath.Join(run.SessionDir, "ambiguous.jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	module = newTestModule(t, store, fakeGitHub{issue: openIssue(run.Issue)}, func(int) (bool, error) { return false, nil }, time.Now())
	if _, err := module.Inspect(context.Background(), run.RunID); err == nil || !strings.Contains(err.Error(), "exactly one is required") {
		t.Fatalf("ambiguous-session error = %v", err)
	}
	if len(store.value.Leases) != 1 || store.value.Runs[0].Status != scheduler.StatusFailed {
		t.Fatalf("ambiguous-session refusal mutated state = %#v", store.value)
	}
}

func TestRecoveryRefusesPendingToolAndChangedWorkflowCheckpoint(t *testing.T) {
	run, store := recoverableFixture(t)
	sessionFile := filepath.Join(run.SessionDir, "session.jsonl")
	pending := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":\"session-42\",\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call-1\"}]}}\n", run.Worktree)
	if err := os.WriteFile(sessionFile, []byte(pending), 0o600); err != nil {
		t.Fatal(err)
	}
	module := newTestModule(t, store, fakeGitHub{issue: openIssue(run.Issue)}, func(int) (bool, error) { return false, nil }, time.Now())
	if _, err := module.Inspect(context.Background(), run.RunID); err == nil || !strings.Contains(err.Error(), "without durable results") {
		t.Fatalf("pending-tool error = %v", err)
	}
	if len(store.value.Leases) != 1 {
		t.Fatal("pending-tool refusal released Lease")
	}

	run, store = recoverableFixture(t)
	gitDir := filepath.Join(run.Worktree, ".git")
	if err := os.Mkdir(gitDir, 0o700); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(gitDir, "ship-it-checkpoint-v1.md")
	if err := os.WriteFile(checkpoint, []byte("# Ship-it checkpoint v1\nStage: review\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	module = newTestModule(t, store, fakeGitHub{issue: openIssue(run.Issue)}, func(int) (bool, error) { return false, nil }, time.Now())
	plan, err := module.Inspect(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checkpoint, []byte("# Ship-it checkpoint v1\nStage: publish\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Recover(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "Plan changed") {
		t.Fatalf("checkpoint-change error = %v", err)
	}
	if store.value.Runs[0].Status != scheduler.StatusFailed || len(store.value.Leases) != 1 {
		t.Fatalf("checkpoint refusal mutated state = %#v", store.value)
	}
}

func TestRecoveryPreservesUnarmedExpectedPullRequestForFreshResume(t *testing.T) {
	run, store := recoverableFixture(t)
	pullURL := "https://github.com/acme/widgets/pull/8"
	github := fakeGitHub{issue: openIssue(run.Issue), pulls: []ghadapter.OwnedRunPullRequest{{
		Number: 8, URL: pullURL, Branch: run.Branch, Commit: strings.Repeat("b", 40), State: "open",
	}}}
	module := newTestModule(t, store, github, func(int) (bool, error) { return false, nil }, time.Now())
	plan, err := module.Inspect(context.Background(), run.RunID)
	if err != nil || plan.Outcome != OutcomeSuspend || plan.PullRequest != pullURL {
		t.Fatalf("plan = %#v, %v", plan, err)
	}
	if _, err := module.Recover(context.Background(), plan); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := store.value.Runs[0]; got.Status != scheduler.StatusSuspended || got.PullRequest != pullURL || len(store.value.Leases) != 1 {
		t.Fatalf("recovered expected pull request = %#v", store.value)
	}
}

func TestRecoveryReconcilesArmedExpectedPullRequestWithoutLaunchingContinuation(t *testing.T) {
	run, store := recoverableFixture(t)
	github := fakeGitHub{
		issue: openIssue(run.Issue),
		pulls: []ghadapter.OwnedRunPullRequest{{Number: 7, URL: "https://github.com/acme/widgets/pull/7", Branch: run.Branch, State: "open", AutoMergeArmed: true}},
	}
	module := newTestModule(t, store, github, func(int) (bool, error) { return false, nil }, time.Now())
	plan, err := module.Inspect(context.Background(), run.RunID)
	if err != nil || plan.Outcome != OutcomeWaiting {
		t.Fatalf("plan = %#v, %v", plan, err)
	}
	if _, err := module.Recover(context.Background(), plan); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := store.value.Runs[0]; got.Status != scheduler.StatusWaitingForMerge || got.PullRequest != github.pulls[0].URL || len(store.value.Leases) != 1 {
		t.Fatalf("waiting reconciliation = %#v", store.value)
	}
}

func recoverableFixture(t *testing.T) (scheduler.Run, *memoryStore) {
	t.Helper()
	root := t.TempDir()
	worktreePath := filepath.Join(root, "worktree")
	sessionDir := filepath.Join(root, "sessions")
	if err := os.MkdirAll(worktreePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	content := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":\"session-42\",\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"continue\"}}\n", worktreePath)
	if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
		Branch: "agent/issue-42-run-42", Worktree: worktreePath, SessionName: "afk #42", SessionID: "session-42", SessionDir: sessionDir,
		Error: "repair budget exhausted", FailureClass: scheduler.FailureRepairBudgetExhaustion, StartedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now(),
	}
	return run, &memoryStore{value: state.State{Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}}}}
}

func newTestModule(t *testing.T, store *memoryStore, github fakeGitHub, alive func(int) (bool, error), now time.Time) *Module {
	t.Helper()
	module, err := New(Config{
		Store: store, GitHub: github, Worktrees: fakeWorktrees{}, Git: fakeGit{identity: GitIdentity{LocalCommit: strings.Repeat("a", 40), RemoteCommit: strings.Repeat("b", 40)}}, Repo: "acme/widgets",
		Now: func() time.Time { return now }, ProcessAlive: alive, ProcessGroupAlive: alive,
	})
	if err != nil {
		t.Fatal(err)
	}
	return module
}

func openIssue(number int) ghadapter.OwnedRunIssue {
	return ghadapter.OwnedRunIssue{Number: number, State: "open", Labels: []string{"in-progress", "spec"}}
}
