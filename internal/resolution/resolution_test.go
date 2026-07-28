package resolution

import (
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestPolicyAcceptsSupportedClosureReasonsAndPreservesHumanLabels(t *testing.T) {
	for _, reason := range []string{"completed", "not-planned"} {
		snapshot := retirement.Snapshot{
			Repository: "acme/widgets",
			Run:        scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
			Lease:      scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"},
			Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: reason,
				Labels: []string{"in-progress", "ready-for-agent", "needs-info", "spec"}},
		}
		plan, err := retirement.Build(Policy("run-42"), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		joined := make([]string, len(plan.Actions))
		for index, action := range plan.Actions {
			joined[index] = action.String()
		}
		text := strings.Join(joined, "\n")
		for _, want := range []string{"resolving-externally", "remove issue label in-progress", "remove issue label ready-for-agent", "resolved-externally"} {
			if !strings.Contains(text, want) {
				t.Fatalf("%s plan missing %q: %s", reason, want, text)
			}
		}
		if strings.Contains(text, "needs-info") || strings.Contains(text, "spec") {
			t.Fatalf("human/unrelated label mutated: %s", text)
		}
	}
}

func TestPolicyEligibilityCoversIncompleteLeasedLifecycle(t *testing.T) {
	policy := Policy("run")
	eligible := make(map[scheduler.Status]bool, len(policy.EligibleStatuses))
	for _, status := range policy.EligibleStatuses {
		eligible[status] = true
	}

	for _, test := range []struct {
		status scheduler.Status
		target scheduler.Status
	}{
		{scheduler.StatusClaimed, scheduler.StatusResolvingExternally},
		{scheduler.StatusWorktreeReady, scheduler.StatusResolvingExternally},
		{scheduler.StatusRunning, scheduler.StatusResolvingExternally},
		{scheduler.StatusSuspended, scheduler.StatusResolvingExternally},
		{scheduler.StatusFailed, scheduler.StatusResolvingExternally},
		{scheduler.StatusNeedsHuman, scheduler.StatusResolvingExternally},
		{scheduler.StatusWaitingForMerge, scheduler.StatusResolvingExternally},
		{scheduler.StatusResetting, scheduler.StatusResolvingExternally},
		{scheduler.StatusResolvingExternally, scheduler.StatusResolvedExternally},
	} {
		t.Run(string(test.status), func(t *testing.T) {
			if !eligible[test.status] {
				t.Fatalf("incomplete leased status %s is not eligible", test.status)
			}
			if !policy.CanTransition(test.status, test.target) {
				t.Fatalf("incomplete leased status %s cannot transition to %s", test.status, test.target)
			}
		})
	}

	if !eligible[scheduler.StatusResolvedExternally] {
		t.Fatal("Historical External Resolution rerun is not eligible")
	}
	for _, status := range []scheduler.Status{scheduler.StatusReset, scheduler.StatusMerged} {
		t.Run(string(status), func(t *testing.T) {
			if eligible[status] {
				t.Fatalf("terminal status %s is unexpectedly eligible", status)
			}
			if policy.CanTransition(status, scheduler.StatusResolvingExternally) || policy.CanTransition(status, scheduler.StatusResolvedExternally) {
				t.Fatalf("terminal status %s has an External Resolution transition", status)
			}
		})
	}
}

func TestPolicyRefusesOpenAndUnsupportedClosureState(t *testing.T) {
	for _, test := range []struct {
		name  string
		issue retirement.Issue
		want  string
	}{
		{name: "open", issue: retirement.Issue{Number: 42, URL: "https://example.test/42", Open: true}, want: "close the issue and rerun backlog resolve, or Reset the Run with backlog reset"},
		{name: "missing reason", issue: retirement.Issue{Number: 42, URL: "https://example.test/42"}, want: "unsupported or unavailable GitHub closure reason"},
		{name: "unsupported reason", issue: retirement.Issue{Number: 42, URL: "https://example.test/42", ClosureReason: "future"}, want: "unsupported or unavailable GitHub closure reason"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := retirement.Snapshot{Run: scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusFailed}, Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"}, Issue: test.issue}
			if _, err := retirement.Build(Policy("run"), snapshot); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Build() error = %v, want refusal containing %q", err, test.want)
			}
		})
	}
}

func TestPolicyPlansCompleteOwnedArtifactRetirementInSafeOrder(t *testing.T) {
	commit := strings.Repeat("a", 40)
	snapshot := retirement.Snapshot{
		Run: scheduler.Run{
			Issue: 42, RunID: "run", Status: scheduler.StatusFailed,
			Branch: "agent/run", Worktree: "/state/worktrees/run", WorkerMode: scheduler.WorkerModeRPC,
			SessionID: "backlog-run", SessionDir: "/state/sessions/run",
		},
		Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
		Issue: retirement.Issue{
			Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed",
			Labels: []string{"in-progress", "ready-for-agent", "needs-info", "spec"},
		},
		PullRequests: []retirement.PullRequest{{
			Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: commit,
			State: retirement.PullRequestOpen, AutoMergeArmed: true,
		}},
		RemoteBranch: retirement.Branch{Name: "agent/run", Commit: commit, Present: true},
		LocalBranch:  retirement.Branch{Name: "agent/run", Commit: commit, Present: true},
		Worktree:     retirement.Worktree{Path: "/state/worktrees/run", Branch: "agent/run", Commit: commit, Present: true},
		Session: retirement.Session{
			ID: "backlog-run", Dir: "/state/sessions/run", ArchiveDir: "/state/history/sessions/run", Present: true,
		},
	}

	plan, err := retirement.Build(Policy("run"), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var actions []string
	for _, action := range plan.Actions {
		actions = append(actions, action.String())
	}
	want := []string{
		"mark Run run resolving-externally while retaining Lease lease",
		"disable auto-merge for pull request #9 (https://github.com/acme/widgets/pull/9)",
		"explain External Resolution on pull request #9 (https://github.com/acme/widgets/pull/9)",
		"close unmerged pull request #9 (https://github.com/acme/widgets/pull/9)",
		"delete remote branch agent/run at " + commit,
		"remove local worktree /state/worktrees/run for agent/run at " + commit,
		"delete local branch agent/run at " + commit,
		"archive Pi session backlog-run from /state/sessions/run to /state/history/sessions/run",
		"remove issue label in-progress from https://github.com/acme/widgets/issues/42",
		"remove issue label ready-for-agent from https://github.com/acme/widgets/issues/42",
		"mark Run run resolved-externally and release Lease lease",
	}
	if strings.Join(actions, "\n") != strings.Join(want, "\n") {
		t.Fatalf("actions = %q, want %q", actions, want)
	}
	if strings.Contains(strings.Join(actions, "\n"), "needs-info") || strings.Contains(strings.Join(actions, "\n"), "spec") {
		t.Fatalf("plan mutates preserved labels: %q", actions)
	}
}

func TestWaitingForMergePlansDurableProgressBeforeDisarming(t *testing.T) {
	commit := strings.Repeat("a", 40)
	snapshot := retirement.Snapshot{
		Run: scheduler.Run{
			Issue: 42, RunID: "run", Status: scheduler.StatusWaitingForMerge, Branch: "agent/run",
			PullRequest: "https://github.com/acme/widgets/pull/9",
		},
		Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
		Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
		PullRequests: []retirement.PullRequest{{
			Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: commit,
			State: retirement.PullRequestOpen, AutoMergeArmed: true,
		}},
	}
	plan, err := retirement.Build(Policy("run"), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) < 2 || !strings.Contains(plan.Actions[0].String(), "resolving-externally") || !strings.Contains(plan.Actions[1].String(), "disable auto-merge") {
		t.Fatalf("waiting-for-merge action order = %#v", plan.Actions)
	}
}

func TestPolicyTreatsAlreadyRetiredArtifactsAsSatisfied(t *testing.T) {
	commit := strings.Repeat("a", 40)
	snapshot := retirement.Snapshot{
		Run: scheduler.Run{
			Issue: 42, RunID: "run", Status: scheduler.StatusResolvingExternally,
			Branch: "agent/run", Worktree: "/state/worktrees/run", WorkerMode: scheduler.WorkerModeRPC,
			SessionID: "backlog-run", SessionDir: "/state/sessions/run",
		},
		Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
		Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
		PullRequests: []retirement.PullRequest{{
			Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: commit,
			State: retirement.PullRequestClosed, Explained: true,
		}},
		RemoteBranch: retirement.Branch{Name: "agent/run"},
		LocalBranch:  retirement.Branch{Name: "agent/run"},
		Worktree:     retirement.Worktree{Path: "/state/worktrees/run", Branch: "agent/run"},
		Session: retirement.Session{
			ID: "backlog-run", Dir: "/state/sessions/run", ArchiveDir: "/state/history/sessions/run", Archived: true,
		},
	}

	plan, err := retirement.Build(Policy("run"), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || !strings.Contains(plan.Actions[0].String(), "resolved-externally") {
		t.Fatalf("already-retired actions = %#v", plan.Actions)
	}
}

func TestMergedExpectedPullRequestPlansCompletionBeforeClosureReasonEligibility(t *testing.T) {
	for _, test := range []struct{ name, reason string }{{"missing", ""}, {"future", "future"}} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := retirement.Snapshot{
				Run:          scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusWaitingForMerge, PullRequest: "https://github.com/acme/widgets/pull/9", Branch: "agent/run"},
				Lease:        scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
				Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: test.reason},
				PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: strings.Repeat("a", 40), State: retirement.PullRequestMerged}},
			}
			plan, err := retirement.Build(Policy("run"), snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if plan.TerminalState != scheduler.StatusMerged || len(plan.Actions) != 1 || !strings.Contains(plan.Actions[0].String(), "record Completion") {
				t.Fatalf("Completion plan = %#v", plan)
			}
		})
	}
}

func TestOnlyRecordedExpectedPullRequestCanPlanCompletion(t *testing.T) {
	const (
		branch   = "agent/run"
		expected = "https://github.com/acme/widgets/pull/9"
	)
	for _, expectedState := range []retirement.PullRequestState{retirement.PullRequestOpen, retirement.PullRequestClosed} {
		t.Run(string(expectedState), func(t *testing.T) {
			expectedPull := retirement.PullRequest{Number: 9, URL: expected, Branch: branch, Commit: strings.Repeat("a", 40), State: expectedState}
			snapshot := retirement.Snapshot{
				Run:   scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusWaitingForMerge, PullRequest: expected, Branch: branch},
				Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
				Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
				PullRequests: []retirement.PullRequest{
					expectedPull,
					{Number: 10, URL: "https://github.com/acme/widgets/pull/10", Branch: branch, Commit: strings.Repeat("b", 40), State: retirement.PullRequestMerged},
				},
			}
			if plan, err := retirement.Build(Policy("run"), snapshot); err == nil || !strings.Contains(err.Error(), "merged pull request is not the expected pull request "+expected) || plan.TerminalState == scheduler.StatusMerged {
				t.Fatalf("unrelated merged pull request plan = %#v, error = %v", plan, err)
			}

			snapshot.PullRequests = []retirement.PullRequest{expectedPull}
			plan, err := retirement.Build(Policy("run"), snapshot)
			if err != nil || plan.TerminalState != scheduler.StatusResolvedExternally {
				t.Fatalf("unmerged expected pull request plan = %#v, error = %v", plan, err)
			}
			joined := make([]string, len(plan.Actions))
			for index, action := range plan.Actions {
				joined[index] = action.String()
			}
			if expectedState == retirement.PullRequestOpen && (!strings.Contains(strings.Join(joined, "\n"), "explain External Resolution") || !strings.Contains(strings.Join(joined, "\n"), "close unmerged pull request")) {
				t.Fatalf("open expected pull request retirement actions = %q", joined)
			}
			if expectedState == retirement.PullRequestClosed && strings.Contains(strings.Join(joined, "\n"), "close unmerged pull request") {
				t.Fatalf("closed expected pull request planned closure again: %q", joined)
			}
		})
	}

	t.Run("unrelated branch identity", func(t *testing.T) {
		snapshot := retirement.Snapshot{
			Run:   scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusWaitingForMerge, PullRequest: expected, Branch: branch},
			Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
			Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
			PullRequests: []retirement.PullRequest{
				{Number: 9, URL: expected, Branch: branch, Commit: strings.Repeat("a", 40), State: retirement.PullRequestOpen},
				{Number: 10, URL: "https://github.com/acme/widgets/pull/10", Branch: "unrelated", Commit: strings.Repeat("b", 40), State: retirement.PullRequestMerged},
			},
		}
		if plan, err := retirement.Build(Policy("run"), snapshot); err == nil || !strings.Contains(err.Error(), "incomplete or mismatched branch identity") || plan.TerminalState == scheduler.StatusMerged {
			t.Fatalf("unrelated branch Completion plan = %#v, error = %v", plan, err)
		}
	})
}

func TestMergedExpectedPullRequestStillRequiresClosedIssue(t *testing.T) {
	snapshot := retirement.Snapshot{
		Run:          scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusWaitingForMerge, PullRequest: "https://github.com/acme/widgets/pull/9", Branch: "agent/run"},
		Lease:        scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
		Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", Open: true},
		PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: strings.Repeat("a", 40), State: retirement.PullRequestMerged}},
	}
	if _, err := retirement.Build(Policy("run"), snapshot); err == nil || !strings.Contains(err.Error(), "Completion requires a verified GitHub closure") {
		t.Fatalf("open issue Completion error = %v", err)
	}
}

func TestSelectorUsesLeaseAndHistoricalRerunPreservesResolutionMetadata(t *testing.T) {
	resolvedAt := time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC)
	current := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 42, RunID: "old", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 42, RunID: "active", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 7, RunID: "historical", Status: scheduler.StatusResolvedExternally, WorkerMode: scheduler.WorkerModePrint, ResolvedExternallyAt: &resolvedAt, ClosureReason: "completed"},
	}, Leases: []scheduler.Lease{{LeaseID: "active", Issue: 42, RunID: "active"}}}
	run, lease, err := Policy("42").SelectRun(current)
	if err != nil || run.RunID != "active" || lease.RunID != "active" {
		t.Fatalf("issue selection = %#v %#v %v", run, lease, err)
	}
	run, lease, err = Policy("historical").SelectRun(current)
	if err != nil || run.ResolvedExternallyAt != &resolvedAt || lease.LeaseID != "" {
		t.Fatalf("historical selection = %#v %#v %v", run, lease, err)
	}
}
