package retirement

import (
	"context"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

// The functions in this file expose focused verification seams for tests and
// diagnostics. Lifecycle callers should use Module so mutation ordering,
// revalidation, and postcondition verification cannot be bypassed.

func InspectWorkerAbsent(run scheduler.Run) error { return inspectWorkerAbsent(run) }

func AbsentWorkerSummary(run scheduler.Run) string { return absentWorkerSummary(run) }

func ValidateOwnedPaths(run scheduler.Run, stateDir, repositoryRoot, defaultBranch string) error {
	return validateOwnedPaths(run, stateDir, repositoryRoot, defaultBranch)
}

func InspectOriginRepository(ctx context.Context, gitExecutable, repositoryRoot string) (string, error) {
	return inspectOriginRepository(ctx, gitExecutable, repositoryRoot)
}

func InspectRemoteBranch(ctx context.Context, gitExecutable, repositoryRoot, branch string) (Branch, error) {
	return inspectRemoteBranch(ctx, gitExecutable, repositoryRoot, branch)
}

func InspectLocalResources(ctx context.Context, gitExecutable, repositoryRoot, commonDirectory string, run scheduler.Run) (Branch, Worktree, error) {
	return inspectLocalResources(ctx, gitExecutable, repositoryRoot, commonDirectory, run)
}

func InspectSession(run scheduler.Run, stateDirectory string) (Session, error) {
	return inspectSession(run, stateDirectory)
}

func VerifyFinalState(current state.State, expected scheduler.Run, policy Policy) error {
	return (Service{policy: policy}).verifyFinalState(current, expected)
}
