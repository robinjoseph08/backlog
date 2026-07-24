package reset

import (
	"strings"
	"testing"

	"github.com/robinjoseph08/backlog/internal/scheduler"
)

func TestBuildTailorsOrderedResetActions(t *testing.T) {
	plan, err := Build(Snapshot{
		Run:          scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusSuspended, Branch: "agent/issue-42-run-42", Worktree: "/state/worktrees/issue-42-run-42", SessionID: "backlog-run-42", SessionDir: "/state/sessions/run-42", PullRequest: "https://github.com/acme/widgets/pull/7"},
		Lease:        scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"},
		Issue:        Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", Open: true, Labels: []string{"in-progress", "spec"}},
		PullRequests: []PullRequest{{Number: 7, URL: "https://github.com/acme/widgets/pull/7", Branch: "agent/issue-42-run-42", Commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", State: PullRequestOpen, AutoMergeArmed: true}},
		RemoteBranch: Branch{Name: "agent/issue-42-run-42", Commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Present: true},
		LocalBranch:  Branch{Name: "agent/issue-42-run-42", Commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Present: true},
		Worktree:     Worktree{Path: "/state/worktrees/issue-42-run-42", Branch: "agent/issue-42-run-42", Commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Present: true},
		Session:      Session{ID: "backlog-run-42", Dir: "/state/sessions/run-42", Present: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"disable auto-merge for pull request #7 (https://github.com/acme/widgets/pull/7)",
		"explain Reset on pull request #7 (https://github.com/acme/widgets/pull/7)",
		"close unmerged pull request #7 (https://github.com/acme/widgets/pull/7)",
		"delete remote branch agent/issue-42-run-42 at aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"remove local worktree /state/worktrees/issue-42-run-42 for agent/issue-42-run-42 at bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"delete local branch agent/issue-42-run-42 at bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"retire Pi session backlog-run-42 in /state/sessions/run-42",
		"remove issue label in-progress from https://github.com/acme/widgets/issues/42",
		"add issue label ready-for-agent to https://github.com/acme/widgets/issues/42",
		"mark Run run-42 reset and release Lease lease-42",
	}
	if strings.Join(plan.Actions, "\n") != strings.Join(want, "\n") {
		t.Fatalf("actions =\n%s\nwant =\n%s", strings.Join(plan.Actions, "\n"), strings.Join(want, "\n"))
	}
}

func TestBuildOrdersWaitingForMergeRecordedPullRequestBeforeOtherBranchPullRequests(t *testing.T) {
	snapshot := minimalSnapshot([]string{"ready-for-agent"})
	snapshot.Run.Status = scheduler.StatusWaitingForMerge
	snapshot.Run.Branch = "agent/issue-42-run-42"
	snapshot.Run.PullRequest = "https://github.com/acme/widgets/pull/8"
	snapshot.PullRequests = []PullRequest{
		{Number: 7, URL: "https://github.com/acme/widgets/pull/7", Branch: snapshot.Run.Branch, Commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", State: PullRequestOpen, AutoMergeArmed: true},
		{Number: 8, URL: snapshot.Run.PullRequest, Branch: snapshot.Run.Branch, Commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", State: PullRequestOpen, AutoMergeArmed: true},
	}

	plan, err := Build(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"disable auto-merge for pull request #8 (https://github.com/acme/widgets/pull/8)",
		"disable auto-merge for pull request #7 (https://github.com/acme/widgets/pull/7)",
		"explain Reset on pull request #7 (https://github.com/acme/widgets/pull/7)",
		"close unmerged pull request #7 (https://github.com/acme/widgets/pull/7)",
		"explain Reset on pull request #8 (https://github.com/acme/widgets/pull/8)",
		"close unmerged pull request #8 (https://github.com/acme/widgets/pull/8)",
		"mark Run run-42 reset and release Lease lease-42",
	}
	if strings.Join(plan.Actions, "\n") != strings.Join(want, "\n") {
		t.Fatalf("actions =\n%s\nwant =\n%s", strings.Join(plan.Actions, "\n"), strings.Join(want, "\n"))
	}
}

func TestBuildOmitsAlreadySatisfiedActionsForEveryManagedLabelCombination(t *testing.T) {
	tests := []struct {
		name   string
		labels []string
		want   []string
	}{
		{name: "neither", want: []string{
			"add issue label ready-for-agent to https://github.com/acme/widgets/issues/42",
			"mark Run run-42 reset and release Lease lease-42",
		}},
		{name: "in progress", labels: []string{"in-progress"}, want: []string{
			"remove issue label in-progress from https://github.com/acme/widgets/issues/42",
			"add issue label ready-for-agent to https://github.com/acme/widgets/issues/42",
			"mark Run run-42 reset and release Lease lease-42",
		}},
		{name: "ready", labels: []string{"ready-for-agent"}, want: []string{
			"mark Run run-42 reset and release Lease lease-42",
		}},
		{name: "both", labels: []string{"in-progress", "ready-for-agent"}, want: []string{
			"remove issue label in-progress from https://github.com/acme/widgets/issues/42",
			"mark Run run-42 reset and release Lease lease-42",
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			plan, err := Build(minimalSnapshot(test.labels))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(plan.Actions, "\n") != strings.Join(test.want, "\n") {
				t.Fatalf("actions = %q, want %q", plan.Actions, test.want)
			}
		})
	}
}

func TestBuildPlansEverySafeRunStatus(t *testing.T) {
	statuses := []scheduler.Status{
		scheduler.StatusClaimed,
		scheduler.StatusWorktreeReady,
		scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge,
		scheduler.StatusFailed,
		scheduler.StatusNeedsHuman,
		scheduler.StatusSuspended,
		scheduler.StatusResetting,
	}
	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			snapshot := minimalSnapshot([]string{"ready-for-agent"})
			snapshot.Run.Status = status
			if _, err := Build(snapshot); err != nil {
				t.Fatalf("safe stopped Run status %s was refused: %v", status, err)
			}
		})
	}
}

func TestBuildAlreadyResetRequiresNoLeaseOrFinalAction(t *testing.T) {
	snapshot := minimalSnapshot([]string{"ready-for-agent"})
	snapshot.Run.Status = scheduler.StatusReset
	snapshot.Lease = scheduler.Lease{}
	plan, err := Build(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("already reset actions = %q", plan.Actions)
	}

	snapshot.Lease = scheduler.Lease{LeaseID: "old", Issue: 42, RunID: "run-42"}
	if _, err := Build(snapshot); err == nil || !strings.Contains(err.Error(), "still has an active Lease") {
		t.Fatalf("old Lease error = %v", err)
	}
}

func TestBuildRefusesEveryHumanWorkflowLabel(t *testing.T) {
	for _, label := range []string{"needs-triage", "needs-info", "ready-for-human", "wontfix", "WONTFIX"} {
		t.Run(label, func(t *testing.T) {
			snapshot := minimalSnapshot([]string{label})
			if _, err := Build(snapshot); err == nil {
				t.Fatalf("human workflow label %q was plannable", label)
			}
		})
	}
}

func TestBuildRefusesUnsafeStates(t *testing.T) {
	tests := map[string]func(*Snapshot){
		"merged Run": func(s *Snapshot) { s.Run.Status = scheduler.StatusMerged },
		"merged pull request": func(s *Snapshot) {
			s.PullRequests = []PullRequest{{Number: 7, URL: "https://example.test/pull/7", Branch: "agent/issue-42-run-42", Commit: "abc", State: PullRequestMerged}}
		},
		"unexplained issue closure":     func(s *Snapshot) { s.Issue.Open = false },
		"human workflow label":          func(s *Snapshot) { s.Issue.Labels = append(s.Issue.Labels, "needs-info") },
		"mismatched Lease":              func(s *Snapshot) { s.Lease.RunID = "other" },
		"missing recorded pull request": func(s *Snapshot) { s.Run.PullRequest = "https://example.test/pull/missing" },
		"unknown pull request state": func(s *Snapshot) {
			s.PullRequests = []PullRequest{{Number: 7, URL: "https://example.test/pull/7", Branch: "agent/issue-42-run-42", Commit: "abc", State: "unknown"}}
		},
		"closed pull request with armed auto-merge": func(s *Snapshot) {
			s.PullRequests = []PullRequest{{Number: 7, URL: "https://example.test/pull/7", Branch: "agent/issue-42-run-42", Commit: "abc", State: PullRequestClosed, AutoMergeArmed: true}}
		},
		"mismatched remote branch": func(s *Snapshot) {
			s.RemoteBranch = Branch{Name: "other", Commit: "abc", Present: true}
		},
		"mismatched pull request branch": func(s *Snapshot) {
			s.Run.Branch = "agent/issue-42-run-42"
			s.PullRequests = []PullRequest{{Number: 7, URL: "https://example.test/pull/7", Branch: "other", Commit: "abc", State: PullRequestOpen}}
		},
		"mismatched Pi session": func(s *Snapshot) {
			s.Session = Session{ID: "other", Dir: "/other", Present: true}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := minimalSnapshot([]string{"spec"})
			mutate(&snapshot)
			if _, err := Build(snapshot); err == nil {
				t.Fatal("unsafe state was plannable")
			}
		})
	}
}

func minimalSnapshot(labels []string) Snapshot {
	return Snapshot{
		Run:   scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed},
		Lease: scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"},
		Issue: Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", Open: true, Labels: append(append([]string{}, labels...), "spec")},
	}
}
