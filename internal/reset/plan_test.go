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
		PullRequests: []PullRequest{{Number: 7, URL: "https://github.com/acme/widgets/pull/7", State: PullRequestOpen, AutoMergeArmed: true}},
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

func TestBuildOmitsAlreadySatisfiedActionsForEveryManagedLabelCombination(t *testing.T) {
	for _, labels := range [][]string{{}, {"in-progress"}, {"ready-for-agent"}, {"in-progress", "ready-for-agent"}} {
		t.Run(strings.Join(labels, "+"), func(t *testing.T) {
			plan, err := Build(minimalSnapshot(labels))
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(plan.Actions, "\n")
			if strings.Contains(joined, "pull request") || strings.Contains(joined, "branch") || strings.Contains(joined, "worktree") || strings.Contains(joined, "Pi session") {
				t.Fatalf("already absent action included: %q", joined)
			}
			if strings.Contains(joined, "spec") {
				t.Fatalf("unrelated label became an action: %q", joined)
			}
			wantRemove := contains(labels, "in-progress")
			wantAdd := !contains(labels, "ready-for-agent")
			if strings.Contains(joined, "remove issue label in-progress") != wantRemove || strings.Contains(joined, "add issue label ready-for-agent") != wantAdd {
				t.Fatalf("labels %v produced %q", labels, joined)
			}
		})
	}
}

func TestBuildRefusesUnsafeStates(t *testing.T) {
	tests := map[string]func(*Snapshot){
		"merged Run": func(s *Snapshot) { s.Run.Status = scheduler.StatusMerged },
		"merged pull request": func(s *Snapshot) {
			s.PullRequests = []PullRequest{{Number: 7, URL: "https://example.test/pull/7", State: PullRequestMerged}}
		},
		"unexplained issue closure":     func(s *Snapshot) { s.Issue.Open = false },
		"human workflow label":          func(s *Snapshot) { s.Issue.Labels = append(s.Issue.Labels, "needs-info") },
		"mismatched Lease":              func(s *Snapshot) { s.Lease.RunID = "other" },
		"missing recorded pull request": func(s *Snapshot) { s.Run.PullRequest = "https://example.test/pull/missing" },
		"unknown pull request state": func(s *Snapshot) {
			s.PullRequests = []PullRequest{{Number: 7, URL: "https://example.test/pull/7", State: "unknown"}}
		},
		"closed pull request with armed auto-merge": func(s *Snapshot) {
			s.PullRequests = []PullRequest{{Number: 7, URL: "https://example.test/pull/7", State: PullRequestClosed, AutoMergeArmed: true}}
		},
		"mismatched remote branch": func(s *Snapshot) {
			s.RemoteBranch = Branch{Name: "other", Commit: "abc", Present: true}
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

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
