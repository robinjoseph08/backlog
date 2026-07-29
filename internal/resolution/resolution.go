// Package resolution supplies the External Resolution lifecycle policy to the
// shared owned Run retirement module.
package resolution

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

// Policy recognizes a GitHub-verified closure as External Resolution. A merged
// expected pull request establishes Completion only after fail-closed recovered
// state and owned-artifact identity checks pass.
func Policy(selector string) retirement.Policy {
	return retirement.Policy{
		Operation:                            "External Resolution",
		CompletionOperation:                  "Completion",
		SelectRun:                            func(current state.State) (scheduler.Run, scheduler.Lease, error) { return selectRun(current, selector) },
		ValidateSnapshot:                     validateSnapshot,
		ValidateMergedCompletionSnapshot:     validateMergedCompletionSnapshot,
		ValidateHistoricalCompletionSnapshot: validateHistoricalCompletionSnapshot,
		EligibleStatuses: []scheduler.Status{
			scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
			scheduler.StatusWaitingForMerge, scheduler.StatusFailed, scheduler.StatusSuspended,
			scheduler.StatusNeedsHuman, scheduler.StatusResetting,
			scheduler.StatusResolvingExternally, scheduler.StatusResolvedExternally,
			scheduler.StatusMerged,
		},
		CanTransition:                    canTransition,
		Explanation:                      Explanation,
		ExplanationAction:                "explain External Resolution",
		Labels:                           retirement.LabelOutcome{Remove: []string{"in-progress", "ready-for-agent"}},
		ProgressStatus:                   scheduler.StatusResolvingExternally,
		TerminalStatus:                   scheduler.StatusResolvedExternally,
		RecordMissingLogWarn:             true,
		RequireClosureReason:             true,
		AllowMergedCompletion:            true,
		RetireMergedCompletionArtifacts:  true,
		AllowHistoricalCompletionCleanup: true,
		VerifyHistoricalOnly:             true,
		MarkProgressBeforeMutation:       true,
		RequireClosedExplanation:         true,
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

func validateMergedCompletionSnapshot(snapshot retirement.Snapshot, merged retirement.PullRequest) error {
	if snapshot.Run.RecoveredRetirementRequired || snapshot.Run.RecoveryCount > 0 {
		return errors.New("recovered Completion requires the full recovered retirement policy")
	}
	return validateHistoricalCompletionSnapshot(snapshot, merged)
}

func validateHistoricalCompletionSnapshot(snapshot retirement.Snapshot, merged retirement.PullRequest) error {
	if snapshot.Worktree.Changed {
		return errors.New("Historical Completion worktree has uncommitted changes; cleanup will not force-remove changed artifacts")
	}
	if snapshot.RemoteBranch.Present && snapshot.RemoteBranch.Commit != merged.Commit ||
		snapshot.LocalBranch.Present && snapshot.LocalBranch.Commit != merged.Commit ||
		snapshot.Worktree.Present && snapshot.Worktree.Commit != merged.Commit {
		return errors.New("Completion artifact commit identity does not match the merged expected pull request head")
	}
	return nil
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
		if run.Status == scheduler.StatusResolvedExternally || run.Status == scheduler.StatusMerged {
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
		if run.Issue == issue && run.Status == scheduler.StatusMerged && run.CleanupPending {
			return historicalRun(current, run)
		}
	}
	for index := len(current.Runs) - 1; index >= 0; index-- {
		run := current.Runs[index]
		if run.Issue == issue && run.Status == scheduler.StatusMerged {
			return historicalRun(current, run)
		}
	}
	for index := len(current.Runs) - 1; index >= 0; index-- {
		run := current.Runs[index]
		if run.Issue == issue && run.Status == scheduler.StatusResolvedExternally {
			return historicalRun(current, run)
		}
	}
	return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("issue #%d has no incomplete leased Run or applicable Historical Resolution", issue)
}

func historicalRun(current state.State, run scheduler.Run) (scheduler.Run, scheduler.Lease, error) {
	for _, lease := range current.Leases {
		if lease.RunID == run.RunID {
			return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("Historical Run %s still has old Lease %s", run.RunID, lease.LeaseID)
		}
	}
	return run, scheduler.Lease{}, nil
}
