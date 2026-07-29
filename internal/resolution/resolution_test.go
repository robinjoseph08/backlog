package resolution

import (
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestNewAutomaticReconcilerRequiresRepositoryAndCompleteRetirementConfiguration(t *testing.T) {
	if _, err := NewAutomaticReconciler(retirement.Config{}, ""); err == nil || !strings.Contains(err.Error(), "repository is empty") {
		t.Fatalf("empty automatic repository error = %v", err)
	}
	if _, err := NewAutomaticReconciler(retirement.Config{}, "acme/widgets"); err == nil || !strings.Contains(err.Error(), "configuration is incomplete") {
		t.Fatalf("incomplete automatic retirement error = %v", err)
	}
}

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
	if !eligible[scheduler.StatusMerged] {
		t.Fatal("Historical merged Completion cleanup is not eligible")
	}
	if policy.CanTransition(scheduler.StatusMerged, scheduler.StatusResolvingExternally) || policy.CanTransition(scheduler.StatusMerged, scheduler.StatusResolvedExternally) {
		t.Fatal("Historical merged Completion cleanup has an External Resolution transition")
	}
	if eligible[scheduler.StatusReset] {
		t.Fatal("reset Historical Run is unexpectedly eligible")
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

func TestMergedCompletionPlansDurableProgressBeforeDestructiveCleanup(t *testing.T) {
	commit := strings.Repeat("a", 40)
	snapshot := retirement.Snapshot{
		Run: scheduler.Run{
			Issue: 42, RunID: "run", Status: scheduler.StatusFailed,
			PullRequest: "https://github.com/acme/widgets/pull/9", Branch: "agent/run",
		},
		Lease:        scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
		Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
		PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: commit, State: retirement.PullRequestMerged}},
		RemoteBranch: retirement.Branch{Name: "agent/run", Commit: commit, Present: true},
	}

	plan, err := retirement.Build(Policy("run"), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 3 || !strings.Contains(plan.Actions[0].String(), "mark Run run resolving-externally") ||
		!strings.Contains(plan.Actions[1].String(), "delete remote branch agent/run") ||
		!strings.Contains(plan.Actions[2].String(), "record Completion") {
		t.Fatalf("merged Completion cleanup order = %#v", plan.Actions)
	}
}

func TestHistoricalMergedCompletionPlansRemainingCleanupWithoutLease(t *testing.T) {
	commit := strings.Repeat("a", 40)
	snapshot := retirement.Snapshot{
		Run: scheduler.Run{
			Issue: 42, RunID: "run", Status: scheduler.StatusMerged, CleanupPending: true,
			PullRequest: "https://github.com/acme/widgets/pull/9", Branch: "agent/run",
			Worktree: "/state/worktrees/run", WorkerMode: scheduler.WorkerModeRPC,
			SessionID: "backlog-run", SessionDir: "/state/sessions/run",
		},
		Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", Labels: []string{"in-progress", "ready-for-agent", "spec"}},
		PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: commit, State: retirement.PullRequestMerged}},
		LocalBranch:  retirement.Branch{Name: "agent/run", Commit: commit, Present: true},
		Worktree:     retirement.Worktree{Path: "/state/worktrees/run", Branch: "agent/run", Commit: commit, Present: true},
		Session:      retirement.Session{ID: "backlog-run", Dir: "/state/sessions/run", ArchiveDir: "/state/history/sessions/run", Present: true},
	}

	plan, err := retirement.Build(Policy("run"), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var actions []string
	for _, action := range plan.Actions {
		actions = append(actions, action.String())
	}
	text := strings.Join(actions, "\n")
	for _, want := range []string{"remove local worktree", "delete local branch", "archive Pi session", "remove issue label in-progress", "remove issue label ready-for-agent", "clear pending Completion cleanup"} {
		if !strings.Contains(text, want) {
			t.Fatalf("Historical Completion cleanup omitted %q: %s", want, text)
		}
	}
	if plan.Operation != "Completion Cleanup" || plan.TerminalState != scheduler.StatusMerged || strings.Contains(text, "Lease") || strings.Contains(text, "resolving-externally") {
		t.Fatalf("Historical Completion cleanup Plan = %#v", plan)
	}

	staleMetadata := snapshot
	staleMetadata.Run.CleanupPending = false
	plan, err = retirement.Build(Policy("run"), staleMetadata)
	if err != nil {
		t.Fatal(err)
	}
	actions = actions[:0]
	for _, action := range plan.Actions {
		actions = append(actions, action.String())
	}
	text = strings.Join(actions, "\n")
	for _, want := range []string{"remove local worktree", "delete local branch", "archive Pi session", "remove issue label in-progress", "remove issue label ready-for-agent"} {
		if !strings.Contains(text, want) {
			t.Fatalf("Historical Completion cleanup with stale metadata omitted %q: %s", want, text)
		}
	}
	if strings.Contains(text, "clear pending Completion cleanup") {
		t.Fatalf("Historical Completion cleanup with already-clear metadata planned finalization: %s", text)
	}

	clean := staleMetadata
	clean.LocalBranch.Present = false
	clean.Worktree.Present = false
	clean.Session.Present = false
	clean.Session.Archived = true
	clean.Issue.Labels = []string{"spec"}
	plan, err = retirement.Build(Policy("run"), clean)
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("fully cleaned Historical Completion Plan = %#v, error = %v", plan, err)
	}
}

func TestHistoricalMergedCompletionRefusesMismatchedArtifactCommitIdentity(t *testing.T) {
	commit := strings.Repeat("a", 40)
	snapshot := retirement.Snapshot{
		Run:          scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusMerged, CleanupPending: true, PullRequest: "https://github.com/acme/widgets/pull/9", Branch: "agent/run"},
		Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42"},
		PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: commit, State: retirement.PullRequestMerged}},
		LocalBranch:  retirement.Branch{Name: "agent/run", Commit: strings.Repeat("b", 40), Present: true},
	}
	if plan, err := retirement.Build(Policy("run"), snapshot); err == nil || !strings.Contains(err.Error(), "artifact commit identity") || len(plan.Actions) != 0 {
		t.Fatalf("mismatched Historical Completion Plan = %#v, error = %v", plan, err)
	}
}

func TestMergedCompletionRefusesMismatchedArtifactCommitIdentity(t *testing.T) {
	const branch = "agent/run"
	mergedCommit := strings.Repeat("a", 40)
	mismatchedCommit := strings.Repeat("b", 40)
	for _, test := range []struct {
		name   string
		mutate func(*retirement.Snapshot)
	}{
		{name: "remote branch", mutate: func(snapshot *retirement.Snapshot) {
			snapshot.RemoteBranch = retirement.Branch{Name: branch, Commit: mismatchedCommit, Present: true}
		}},
		{name: "local branch", mutate: func(snapshot *retirement.Snapshot) {
			snapshot.LocalBranch = retirement.Branch{Name: branch, Commit: mismatchedCommit, Present: true}
		}},
		{name: "worktree", mutate: func(snapshot *retirement.Snapshot) {
			snapshot.Run.Worktree = "/state/worktrees/run"
			snapshot.Worktree = retirement.Worktree{Path: snapshot.Run.Worktree, Branch: branch, Commit: mismatchedCommit, Present: true}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := retirement.Snapshot{
				Run: scheduler.Run{
					Issue: 42, RunID: "run", Status: scheduler.StatusFailed,
					PullRequest: "https://github.com/acme/widgets/pull/9", Branch: branch,
				},
				Lease:        scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
				Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
				PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: branch, Commit: mergedCommit, State: retirement.PullRequestMerged}},
			}
			test.mutate(&snapshot)

			plan, err := retirement.Build(Policy("run"), snapshot)
			if err == nil || !strings.Contains(err.Error(), "artifact commit identity does not match the merged expected pull request head") || len(plan.Actions) != 0 {
				t.Fatalf("mismatched merged Completion plan = %#v, error = %v", plan, err)
			}
		})
	}
}

func TestExternalResolutionCannotFinalizeRecoveredCompletionWithWeakerPolicy(t *testing.T) {
	snapshot := retirement.Snapshot{
		Run: scheduler.Run{
			Issue: 42, RunID: "run", Status: scheduler.StatusResolvingExternally,
			PullRequest: "https://github.com/acme/widgets/pull/9", Branch: "agent/run",
			RecoveredRetirementRequired: true,
		},
		Lease:        scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
		Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed", Labels: []string{"in-progress"}},
		PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: strings.Repeat("a", 40), State: retirement.PullRequestMerged}},
		RemoteBranch: retirement.Branch{Name: "agent/run", Commit: strings.Repeat("a", 40), Present: true},
	}
	if plan, err := retirement.Build(Policy("run"), snapshot); err == nil || !strings.Contains(err.Error(), "full recovered retirement policy") || plan.TerminalState == scheduler.StatusMerged {
		t.Fatalf("weaker recovered Completion plan = %#v, error = %v", plan, err)
	}
}

func TestRecoveredCompletionPolicyValidatesCompletionEvidence(t *testing.T) {
	const (
		branch      = "agent/run"
		pullRequest = "https://github.com/acme/widgets/pull/9"
	)
	head := strings.Repeat("a", 40)
	baseline := func() retirement.Snapshot {
		return retirement.Snapshot{
			Run: scheduler.Run{
				Issue: 42, RunID: "run", Status: scheduler.StatusFailed, Branch: branch,
				PullRequest: pullRequest, Worktree: "/state/worktrees/run", WorkerMode: scheduler.WorkerModeRPC,
				SessionID: "backlog-run", SessionDir: "/state/sessions/run", RecoveryCount: 1,
			},
			Lease:        scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
			Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", Labels: []string{"in-progress"}},
			PullRequests: []retirement.PullRequest{{Number: 9, URL: pullRequest, Branch: branch, Commit: head, State: retirement.PullRequestMerged}},
			RemoteBranch: retirement.Branch{Name: branch, Commit: head, Present: true},
			LocalBranch:  retirement.Branch{Name: branch, Commit: head, Present: true},
			Worktree:     retirement.Worktree{Path: "/state/worktrees/run", Branch: branch, Commit: head, Present: true},
			Session:      retirement.Session{ID: "backlog-run", Dir: "/state/sessions/run", ArchiveDir: "/state/history/sessions/run", Present: true},
		}
	}

	plan, err := retirement.Build(RecoveredCompletionPolicy("run"), baseline())
	if err != nil || plan.TerminalState != scheduler.StatusMerged || plan.Operation != "Recovered Completion" {
		t.Fatalf("valid Recovered Completion plan = %#v, error = %v", plan, err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*retirement.Snapshot)
		want   string
	}{
		{name: "closed issue", mutate: func(snapshot *retirement.Snapshot) { snapshot.Issue.Open = true }, want: "verified GitHub closure"},
		{name: "exactly one pull request", mutate: func(snapshot *retirement.Snapshot) {
			snapshot.PullRequests = append(snapshot.PullRequests, retirement.PullRequest{Number: 10, URL: "https://github.com/acme/widgets/pull/10", Branch: branch, Commit: head, State: retirement.PullRequestOpen})
		}, want: "one merged expected pull request and a closed issue"},
		{name: "expected pull request", mutate: func(snapshot *retirement.Snapshot) {
			snapshot.Run.PullRequest = "https://github.com/acme/widgets/pull/10"
		}, want: "recorded pull request"},
		{name: "remote commit", mutate: func(snapshot *retirement.Snapshot) { snapshot.RemoteBranch.Commit = strings.Repeat("b", 40) }, want: "artifact commit identity"},
		{name: "local commit", mutate: func(snapshot *retirement.Snapshot) { snapshot.LocalBranch.Commit = strings.Repeat("b", 40) }, want: "artifact commit identity"},
		{name: "worktree commit", mutate: func(snapshot *retirement.Snapshot) { snapshot.Worktree.Commit = strings.Repeat("b", 40) }, want: "artifact commit identity"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := baseline()
			test.mutate(&snapshot)
			if plan, err := retirement.Build(RecoveredCompletionPolicy("run"), snapshot); err == nil || !strings.Contains(err.Error(), test.want) || len(plan.Actions) != 0 {
				t.Fatalf("unsafe Recovered Completion plan = %#v, error = %v, want %q", plan, err, test.want)
			}
		})
	}

	historical := baseline()
	historical.Run.Status = scheduler.StatusMerged
	historical.Lease = scheduler.Lease{}
	historical.Issue.Labels = nil
	historical.RemoteBranch.Present = false
	historical.LocalBranch.Present = false
	historical.Worktree.Present = false
	historical.Session.Present = false
	historical.Session.Archived = true
	if plan, err := retirement.Build(RecoveredCompletionPolicy("run"), historical); err != nil || len(plan.Actions) != 0 {
		t.Fatalf("retired Historical Recovered Completion plan = %#v, error = %v", plan, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*retirement.Snapshot)
	}{
		{name: "remote branch", mutate: func(snapshot *retirement.Snapshot) { snapshot.RemoteBranch.Present = true }},
		{name: "local branch", mutate: func(snapshot *retirement.Snapshot) { snapshot.LocalBranch.Present = true }},
		{name: "worktree", mutate: func(snapshot *retirement.Snapshot) { snapshot.Worktree.Present = true }},
		{name: "active session", mutate: func(snapshot *retirement.Snapshot) {
			snapshot.Session.Present = true
			snapshot.Session.Archived = false
		}},
	} {
		t.Run("historical "+test.name, func(t *testing.T) {
			snapshot := historical
			test.mutate(&snapshot)
			if plan, err := retirement.Build(RecoveredCompletionPolicy("run"), snapshot); err == nil || !strings.Contains(err.Error(), "historical Recovered Completion still has active owned artifacts") || len(plan.Actions) != 0 {
				t.Fatalf("active Historical Recovered Completion plan = %#v, error = %v", plan, err)
			}
		})
	}
}

func TestExpectedBranchPullRequestIdentityForCompletion(t *testing.T) {
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
			if expectedState == retirement.PullRequestClosed && (!strings.Contains(strings.Join(joined, "\n"), "explain External Resolution") || strings.Contains(strings.Join(joined, "\n"), "close unmerged pull request")) {
				t.Fatalf("closed expected pull request actions = %q, want explanation without repeated closure", joined)
			}
		})
	}

	t.Run("merged pull request discovered from expected branch", func(t *testing.T) {
		snapshot := retirement.Snapshot{
			Run:   scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusFailed, Branch: branch},
			Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
			Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
			PullRequests: []retirement.PullRequest{{
				Number: 10, URL: "https://github.com/acme/widgets/pull/10", Branch: branch,
				Commit: strings.Repeat("b", 40), State: retirement.PullRequestMerged,
			}},
		}
		plan, err := retirement.Build(Policy("run"), snapshot)
		if err != nil || plan.TerminalState != scheduler.StatusMerged || len(plan.Actions) != 1 || !strings.Contains(plan.Actions[0].String(), "record Completion") {
			t.Fatalf("discovered merged pull request plan = %#v, error = %v", plan, err)
		}
	})

	t.Run("multiple unrecorded merged pull requests", func(t *testing.T) {
		snapshot := retirement.Snapshot{
			Run:   scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusFailed, Branch: branch},
			Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
			Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
			PullRequests: []retirement.PullRequest{
				{Number: 10, URL: "https://github.com/acme/widgets/pull/10", Branch: branch, Commit: strings.Repeat("b", 40), State: retirement.PullRequestMerged},
				{Number: 11, URL: "https://github.com/acme/widgets/pull/11", Branch: branch, Commit: strings.Repeat("c", 40), State: retirement.PullRequestMerged},
			},
		}
		if plan, err := retirement.Build(Policy("run"), snapshot); err == nil || !strings.Contains(err.Error(), "multiple merged pull requests") || plan.TerminalState == scheduler.StatusMerged {
			t.Fatalf("ambiguous merged pull request plan = %#v, error = %v", plan, err)
		}
	})

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

func TestSelectorRefusesHistoricalRunCleanupWhileNewerRunOwnsIssue(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 42, RunID: "historical", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 42, RunID: "active", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
	}, Leases: []scheduler.Lease{{LeaseID: "active-lease", Issue: 42, RunID: "active"}}}

	run, lease, err := Policy("historical").SelectRun(current)
	if err == nil || !strings.Contains(err.Error(), "owned by leased Run active") || run.RunID != "" || lease.LeaseID != "" {
		t.Fatalf("Historical Run selection with newer Lease = %#v %#v %v", run, lease, err)
	}
}

func TestSelectorUsesLeaseAndHistoricalRerunPreservesResolutionMetadata(t *testing.T) {
	resolvedAt := time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC)
	current := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 42, RunID: "old", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 42, RunID: "active", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 7, RunID: "pending-merged", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, CleanupPending: true},
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
	run, lease, err = Policy("7").SelectRun(current)
	if err != nil || run.RunID != "pending-merged" || lease.LeaseID != "" {
		t.Fatalf("pending Historical Completion issue selection = %#v %#v %v", run, lease, err)
	}
	current.Runs[2].CleanupPending = false
	run, lease, err = Policy("pending-merged").SelectRun(current)
	if err != nil || run.RunID != "pending-merged" || lease.LeaseID != "" {
		t.Fatalf("exact cleaned Historical Completion selection = %#v %#v %v", run, lease, err)
	}
	run, lease, err = Policy("7").SelectRun(current)
	if err != nil || run.RunID != "pending-merged" || lease.LeaseID != "" {
		t.Fatalf("cleaned Historical Completion issue selection = %#v %#v %v", run, lease, err)
	}
}
