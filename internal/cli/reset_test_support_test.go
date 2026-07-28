package cli

import (
	"context"
	"os"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/reset"
	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

type resetStateStore = retirement.StateStore

type resetExecutor struct {
	store           resetStateStore
	github          ghadapter.Client
	issue           int
	repositoryRoot  string
	commonDirectory string
	stateDirectory  string
	gitExecutable   string
	syncPath        func(string) error
}

func (e resetExecutor) module() retirement.Module {
	module, err := retirement.New(retirement.Config{
		Store: e.store, GitHub: e.github, RepositoryRoot: e.repositoryRoot,
		CommonDirectory: e.commonDirectory, StateDirectory: e.stateDirectory,
		GitExecutable: e.gitExecutable, SyncPath: e.syncPath,
	}, reset.Policy(e.issue))
	if err != nil {
		panic(err)
	}
	return module
}

func (e resetExecutor) inspect(ctx context.Context) (reset.Plan, error) {
	return e.module().Inspect(ctx)
}

func (e resetExecutor) apply(ctx context.Context, plan reset.Plan) error {
	return e.module().Retire(ctx, plan)
}

func syncFilesystemPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func inspectSession(run scheduler.Run, stateDirectory string) (reset.Session, error) {
	return retirement.InspectSession(run, stateDirectory)
}

func inspectRemoteBranch(ctx context.Context, executable, root, branch string) (reset.Branch, error) {
	return retirement.InspectRemoteBranch(ctx, executable, root, branch)
}

func validateOwnedPaths(run scheduler.Run, stateDirectory, root, defaultBranch string) error {
	return retirement.ValidateOwnedPaths(run, stateDirectory, root, defaultBranch)
}

func inspectOriginRepository(ctx context.Context, executable, root string) (string, error) {
	return retirement.InspectOriginRepository(ctx, executable, root)
}

func inspectLocalResources(ctx context.Context, executable, root, common string, run scheduler.Run) (reset.Branch, reset.Worktree, error) {
	return retirement.InspectLocalResources(ctx, executable, root, common, run)
}

func inspectWorkerAbsent(run scheduler.Run) error { return retirement.InspectWorkerAbsent(run) }

func absentWorkerSummary(run scheduler.Run) string { return retirement.AbsentWorkerSummary(run) }

func verifyResetFinalState(current state.State, expected scheduler.Run) error {
	return retirement.VerifyFinalState(current, expected, reset.Policy(expected.Issue))
}

func resetCommentMarker(runID string) string { return reset.CommentMarker(runID) }
