// Package reset supplies Reset lifecycle policy to the shared owned Run
// retirement module.
package reset

import (
	"fmt"
	"strings"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

type PullRequestState = retirement.PullRequestState

const (
	PullRequestOpen   = retirement.PullRequestOpen
	PullRequestClosed = retirement.PullRequestClosed
	PullRequestMerged = retirement.PullRequestMerged
)

type Issue = retirement.Issue
type PullRequest = retirement.PullRequest
type Branch = retirement.Branch
type Worktree = retirement.Worktree
type Session = retirement.Session
type Snapshot = retirement.Snapshot
type Plan = retirement.Plan

var humanWorkflowLabels = map[string]struct{}{
	"needs-triage":    {},
	"needs-info":      {},
	"ready-for-human": {},
	"wontfix":         {},
}

// Policy defines Reset eligibility, explanation wording, managed label outcome,
// terminal state, and diagnostic requirements. Artifact rules remain owned by
// retirement.Service.
func Policy(issue int) retirement.Policy {
	return retirement.Policy{
		Operation:        "Reset",
		SelectRun:        func(current state.State) (scheduler.Run, scheduler.Lease, error) { return selectRun(current, issue) },
		ValidateSnapshot: validateSnapshot,
		EligibleStatuses: []scheduler.Status{
			scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
			scheduler.StatusWaitingForMerge, scheduler.StatusFailed, scheduler.StatusSuspended,
			scheduler.StatusNeedsHuman, scheduler.StatusResetting, scheduler.StatusReset,
		},
		CanTransition:     scheduler.CanTransition,
		Explanation:       Explanation,
		ExplanationAction: "explain Reset",
		Labels: retirement.LabelOutcome{
			Remove: []string{"in-progress"},
			Add:    []string{"ready-for-agent"},
		},
		ProgressStatus:     scheduler.StatusResetting,
		TerminalStatus:     scheduler.StatusReset,
		RequireDurableLogs: true,
	}
}

func validateSnapshot(snapshot retirement.Snapshot) error {
	for _, label := range snapshot.Issue.Labels {
		if _, blocks := humanWorkflowLabels[strings.ToLower(label)]; blocks {
			return fmt.Errorf("issue #%d has human workflow label %q", snapshot.Issue.Number, label)
		}
	}
	if !snapshot.Issue.Open {
		return fmt.Errorf("issue #%d is closed without verified Completion; refusing unexplained closure", snapshot.Issue.Number)
	}
	return nil
}

// Explanation is the exact durable pull request explanation used by Reset.
func Explanation(run scheduler.Run) string {
	return fmt.Sprintf("%s\nBacklog is resetting Run %s for issue #%d. This pull request is being closed as part of abandoning the incomplete Run.", CommentMarker(run.RunID), run.RunID, run.Issue)
}

func CommentMarker(runID string) string {
	return "<!-- backlog-reset:" + runID + " -->"
}

// Build is the Reset planner facade retained for focused policy tests.
func Build(snapshot Snapshot) (Plan, error) {
	return retirement.Build(Policy(snapshot.Run.Issue), snapshot)
}

func NextPullRequestForReset(snapshot Snapshot) (PullRequest, bool) {
	return retirement.NextPullRequest(snapshot)
}

func selectRun(current state.State, issue int) (scheduler.Run, scheduler.Lease, error) {
	for _, lease := range current.Leases {
		if lease.Issue != issue {
			continue
		}
		for _, run := range current.Runs {
			if run.RunID == lease.RunID && run.Issue == issue {
				return run, lease, nil
			}
		}
		return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("Lease %s for issue #%d has an invalid Run reference", lease.LeaseID, issue)
	}
	for index := len(current.Runs) - 1; index >= 0; index-- {
		run := current.Runs[index]
		if run.Issue == issue && run.Status == scheduler.StatusReset {
			for _, lease := range current.Leases {
				if lease.RunID == run.RunID {
					return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("reset Run %s still has old Lease %s", run.RunID, lease.LeaseID)
				}
			}
			return run, scheduler.Lease{}, nil
		}
	}
	return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("issue #%d has no active Lease or historical reset Run", issue)
}
