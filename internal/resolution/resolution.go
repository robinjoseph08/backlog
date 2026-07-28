// Package resolution supplies the External Resolution lifecycle policy to the
// shared owned Run retirement module.
package resolution

import (
	"fmt"
	"strconv"
	"time"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

// Policy recognizes a GitHub-verified closure as External Resolution unless
// a merged pull request from the expected Run branch establishes Completion.
func Policy(selector string) retirement.Policy {
	return retirement.Policy{
		Operation:        "External Resolution",
		SelectRun:        func(current state.State) (scheduler.Run, scheduler.Lease, error) { return selectRun(current, selector) },
		ValidateSnapshot: validateSnapshot,
		EligibleStatuses: []scheduler.Status{
			scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
			scheduler.StatusWaitingForMerge, scheduler.StatusFailed, scheduler.StatusSuspended,
			scheduler.StatusNeedsHuman, scheduler.StatusResetting,
			scheduler.StatusResolvingExternally, scheduler.StatusResolvedExternally,
		},
		CanTransition:              canTransition,
		Explanation:                Explanation,
		ExplanationAction:          "explain External Resolution",
		Labels:                     retirement.LabelOutcome{Remove: []string{"in-progress", "ready-for-agent"}},
		ProgressStatus:             scheduler.StatusResolvingExternally,
		TerminalStatus:             scheduler.StatusResolvedExternally,
		RecordMissingLogWarn:       true,
		RequireClosureReason:       true,
		AllowMergedCompletion:      true,
		VerifyHistoricalOnly:       true,
		MarkProgressBeforeMutation: true,
		RequireClosedExplanation:   true,
		FinalizeMetadata: func(run *scheduler.Run, snapshot retirement.Snapshot, now time.Time) {
			run.ResolvedExternallyAt = &now
			run.ClosureReason = snapshot.Issue.ClosureReason
			run.CompletedAt = nil
		},
	}
}

func canTransition(from, to scheduler.Status) bool {
	if from == scheduler.StatusResolvingExternally {
		return to == scheduler.StatusResolvedExternally
	}
	if to != scheduler.StatusResolvingExternally {
		return false
	}
	switch from {
	case scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge, scheduler.StatusFailed, scheduler.StatusSuspended,
		scheduler.StatusNeedsHuman, scheduler.StatusResetting:
		return true
	default:
		return false
	}
}

func validateSnapshot(snapshot retirement.Snapshot) error {
	if snapshot.Issue.Open {
		return fmt.Errorf("issue #%d is open; close the issue and rerun backlog resolve, or Reset the Run with backlog reset", snapshot.Issue.Number)
	}
	if snapshot.Issue.ClosureReason != "completed" && snapshot.Issue.ClosureReason != "not-planned" {
		return fmt.Errorf("issue #%d has unsupported or unavailable GitHub closure reason %q", snapshot.Issue.Number, snapshot.Issue.ClosureReason)
	}
	return nil
}

// Explanation is the exact durable pull request explanation used while
// retiring an owned unmerged pull request.
func Explanation(run scheduler.Run) string {
	return fmt.Sprintf("%s\nBacklog is externally resolving Run %s for closed issue #%d. This unmerged pull request no longer owns the issue outcome.", CommentMarker(run.RunID), run.RunID, run.Issue)
}

func CommentMarker(runID string) string { return "<!-- backlog-external-resolution:" + runID + " -->" }

func selectRun(current state.State, selector string) (scheduler.Run, scheduler.Lease, error) {
	for _, run := range current.Runs {
		if run.RunID != selector {
			continue
		}
		if run.Status == scheduler.StatusResolvedExternally {
			return historicalRun(current, run)
		}
		for _, lease := range current.Leases {
			if lease.RunID == run.RunID && lease.Issue == run.Issue {
				return run, lease, nil
			}
		}
		return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("Run %q is Historical and is not externally resolved", selector)
	}

	issue, err := strconv.Atoi(selector)
	if err != nil || issue <= 0 {
		return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("Run %q was not found", selector)
	}
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
		if run.Issue == issue && run.Status == scheduler.StatusResolvedExternally {
			return historicalRun(current, run)
		}
	}
	return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("issue #%d has no incomplete leased Run or Historical External Resolution", issue)
}

func historicalRun(current state.State, run scheduler.Run) (scheduler.Run, scheduler.Lease, error) {
	for _, lease := range current.Leases {
		if lease.RunID == run.RunID {
			return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("externally resolved Run %s still has old Lease %s", run.RunID, lease.LeaseID)
		}
	}
	return run, scheduler.Lease{}, nil
}
