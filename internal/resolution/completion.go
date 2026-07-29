package resolution

import (
	"context"
	"errors"
	"fmt"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

// RecoveredCompletionPolicy routes Completion discovered after Recovery through
// the same complete, fail-closed retirement service used by lifecycle commands.
func RecoveredCompletionPolicy(runID string) retirement.Policy {
	return retirement.Policy{
		Operation: "Recovered Completion",
		SelectRun: func(current state.State) (scheduler.Run, scheduler.Lease, error) {
			var selected scheduler.Run
			for _, run := range current.Runs {
				if run.RunID == runID {
					selected = run
					break
				}
			}
			if selected.RunID == "" {
				return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("Run %q was not found", runID)
			}
			for _, lease := range current.Leases {
				if lease.RunID == selected.RunID && lease.Issue == selected.Issue {
					return selected, lease, nil
				}
			}
			if selected.Status == scheduler.StatusMerged {
				return selected, scheduler.Lease{}, nil
			}
			return scheduler.Run{}, scheduler.Lease{}, fmt.Errorf("Run %q no longer owns its Lease", runID)
		},
		ValidateSnapshot: func(snapshot retirement.Snapshot) error {
			return validateRecoveredCompletionSnapshot(snapshot, retirement.PullRequest{})
		},
		ValidateMergedCompletionSnapshot: validateRecoveredCompletionSnapshot,
		EligibleStatuses: []scheduler.Status{
			scheduler.StatusFailed, scheduler.StatusNeedsHuman, scheduler.StatusSuspended,
			scheduler.StatusWaitingForMerge, scheduler.StatusResolvingExternally, scheduler.StatusMerged,
		},
		CanTransition:                   scheduler.CanTransition,
		Explanation:                     func(scheduler.Run) string { return "Recovered Completion" },
		ExplanationAction:               "explain Recovered Completion",
		Labels:                          retirement.LabelOutcome{Remove: []string{"in-progress", "ready-for-agent"}},
		ProgressStatus:                  scheduler.StatusResolvingExternally,
		TerminalStatus:                  scheduler.StatusMerged,
		AllowMergedCompletion:           true,
		RetireMergedCompletionArtifacts: true,
		VerifyHistoricalOnly:            true,
	}
}

func validateRecoveredCompletionSnapshot(snapshot retirement.Snapshot, merged retirement.PullRequest) error {
	if snapshot.Issue.Open || len(snapshot.PullRequests) != 1 || snapshot.PullRequests[0].State != retirement.PullRequestMerged || snapshot.Run.PullRequest != "" && snapshot.PullRequests[0].URL != snapshot.Run.PullRequest {
		return errors.New("Recovered Completion requires one merged expected pull request and a closed issue")
	}
	pullCommit := merged.Commit
	if pullCommit == "" {
		pullCommit = snapshot.PullRequests[0].Commit
	}
	if snapshot.RemoteBranch.Present && snapshot.RemoteBranch.Commit != pullCommit || snapshot.LocalBranch.Present && snapshot.LocalBranch.Commit != pullCommit || snapshot.Worktree.Present && snapshot.Worktree.Commit != pullCommit {
		return errors.New("Recovered Completion artifact commit identity does not match the merged pull request head")
	}
	if snapshot.Run.Status == scheduler.StatusMerged && (snapshot.RemoteBranch.Present || snapshot.LocalBranch.Present || snapshot.Worktree.Present || snapshot.Session.Present) {
		return errors.New("historical Recovered Completion still has active owned artifacts")
	}
	return nil
}

// RetireRecoveredCompletion performs complete retirement with a freshly
// inspected plan. Each successful external stage is durable and rerunnable.
func (r *AutomaticReconciler) RetireRecoveredCompletion(ctx context.Context, run scheduler.Run) (bool, error) {
	if !run.RecoveredRetirementRequired && run.RecoveryCount == 0 {
		return false, nil
	}
	module, err := retirement.New(r.config, RecoveredCompletionPolicy(run.RunID))
	if err != nil {
		return true, err
	}
	plan, err := module.Inspect(ctx)
	if err != nil {
		return true, err
	}
	if err := module.Validate(plan); err != nil {
		return true, err
	}
	if err := module.Retire(ctx, plan); err != nil {
		return true, err
	}
	return true, nil
}
