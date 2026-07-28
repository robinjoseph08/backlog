package retirement

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/processidentity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

type StateStore interface {
	Preview() (state.State, bool, error)
	Save(state.State) error
}

// Config provides the external adapters and repository identity used by a
// retirement Service. Lifecycle callers configure these once and do not invoke
// individual artifact operations.
type Config struct {
	Store           StateStore
	GitHub          ghadapter.Client
	RepositoryRoot  string
	CommonDirectory string
	StateDirectory  string
	GitExecutable   string
	SyncPath        func(string) error
}

// Module is the complete owned Run retirement interface used by lifecycle
// callers.
type Module interface {
	Inspect(context.Context) (Plan, error)
	Validate(Plan) error
	Retire(context.Context, Plan) error
}

type Service struct {
	store           StateStore
	github          ghadapter.Client
	repositoryRoot  string
	commonDirectory string
	stateDirectory  string
	gitExecutable   string
	syncPath        func(string) error
	policy          Policy
}

// New constructs one authoritative owned Run retirement module.
func New(config Config, policy Policy) (Module, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	if config.Store == nil || config.RepositoryRoot == "" || config.CommonDirectory == "" || config.StateDirectory == "" || config.GitExecutable == "" {
		return nil, errors.New("owned Run retirement configuration is incomplete")
	}
	return &Service{
		store: config.Store, github: config.GitHub, repositoryRoot: config.RepositoryRoot,
		commonDirectory: config.CommonDirectory, stateDirectory: config.StateDirectory,
		gitExecutable: config.GitExecutable, syncPath: config.SyncPath, policy: policy,
	}, nil
}

// Inspect conclusively verifies Worker absence and every owned resource, then
// returns the remaining ordered retirement actions.
func (e Service) Inspect(ctx context.Context) (Plan, error) {
	return e.inspect(ctx)
}

// Validate checks mutation-only eligibility and final-state requirements. It
// remains separate from Inspect so dry-run preserves its read-only plan behavior.
func (e Service) Validate(plan Plan) error {
	return e.validateMutation(plan)
}

func (e Service) inspect(ctx context.Context) (Plan, error) {
	current, _, err := e.store.Preview()
	if err != nil {
		return Plan{}, err
	}
	run, lease, err := e.policy.SelectRun(current)
	if err != nil {
		return Plan{}, err
	}
	if err := inspectWorkerAbsent(run); err != nil {
		return Plan{}, err
	}
	repository, err := e.github.Repository(ctx)
	if err != nil {
		return Plan{}, err
	}
	if current.Repo == "" || current.Repo != repository.Slug {
		return Plan{}, fmt.Errorf("Run state belongs to %q, not repository %q", current.Repo, repository.Slug)
	}
	if current.DefaultBranch == "" || current.DefaultBranch != repository.DefaultBranch {
		return Plan{}, fmt.Errorf("Run state default branch %q does not match repository default branch %q", current.DefaultBranch, repository.DefaultBranch)
	}
	originRepository, err := inspectOriginRepository(ctx, e.gitExecutable, e.repositoryRoot)
	if err != nil {
		return Plan{}, err
	}
	if !strings.EqualFold(originRepository, repository.Slug) {
		return Plan{}, fmt.Errorf("Git origin belongs to %q, not repository %q", originRepository, repository.Slug)
	}
	if err := validateOwnedPaths(run, e.stateDirectory, e.repositoryRoot, repository.DefaultBranch); err != nil {
		return Plan{}, err
	}
	issueResource, pullResources, err := e.github.OwnedRunResources(ctx, repository.Slug, run.Issue, run.Branch)
	if err != nil {
		return Plan{}, err
	}
	if e.policy.RequireClosureReason {
		closure, closureErr := e.github.IssueClosure(ctx, repository.Slug, run.Issue)
		if closureErr != nil {
			return Plan{}, closureErr
		}
		if closure.Open {
			issueResource.State = "open"
		} else {
			issueResource.State = "closed"
		}
		issueResource.ClosureReason = closure.Reason
	}
	remoteBranch, err := inspectRemoteBranch(ctx, e.gitExecutable, e.repositoryRoot, run.Branch)
	if err != nil {
		return Plan{}, err
	}
	localBranch, localWorktree, err := inspectLocalResources(ctx, e.gitExecutable, e.repositoryRoot, e.commonDirectory, run)
	if err != nil {
		return Plan{}, err
	}
	session, err := inspectSession(run, e.stateDirectory)
	if err != nil {
		return Plan{}, err
	}
	snapshot := Snapshot{
		Repository: repository.Slug,
		Run:        run, Lease: lease,
		Issue: Issue{
			Number: issueResource.Number, URL: issueResource.URL, Open: issueResource.State == "open",
			ClosureReason: issueResource.ClosureReason, Labels: issueResource.Labels,
		},
		RemoteBranch: remoteBranch, LocalBranch: localBranch, Worktree: localWorktree, Session: session,
		WorkerSummary: absentWorkerSummary(run),
	}
	for _, pull := range pullResources {
		commented := false
		for _, comment := range pull.Comments {
			if comment == e.policy.Explanation(run) {
				commented = true
				break
			}
		}
		snapshot.PullRequests = append(snapshot.PullRequests, PullRequest{
			Number: pull.Number, URL: pull.URL, Branch: pull.Branch, Commit: pull.Commit,
			State: PullRequestState(pull.State), AutoMergeArmed: pull.AutoMergeArmed, Explained: commented,
		})
	}
	if err := validateInspectedGitHubIdentity(snapshot); err != nil {
		return Plan{}, err
	}
	return Build(e.policy, snapshot)
}

func validateInspectedGitHubIdentity(snapshot Snapshot) error {
	if !snapshot.RemoteBranch.Present {
		return nil
	}
	for _, pull := range snapshot.PullRequests {
		if pull.State == PullRequestOpen && pull.Commit != snapshot.RemoteBranch.Commit {
			return fmt.Errorf("pull request #%d expected commit %s does not match owned remote branch %s at %s", pull.Number, pull.Commit, snapshot.RemoteBranch.Name, snapshot.RemoteBranch.Commit)
		}
	}
	return nil
}

func (e Service) validateMutation(plan Plan) error {
	if plan.Snapshot.Run.Status == e.policy.TerminalStatus {
		if err := e.verifyOwnedFinalState(plan.Snapshot); err != nil {
			return fmt.Errorf("historical %s Run has incomplete final state: %w", e.policy.TerminalStatus, err)
		}
	}
	status := plan.Snapshot.Run.Status
	if !e.policy.statusEligible(status) {
		return fmt.Errorf("Run status %s is not eligible for %s", status, e.policy.Operation)
	}
	if status == scheduler.StatusWaitingForMerge {
		if plan.Snapshot.Run.PullRequest == "" {
			return errors.New("waiting-for-merge Run has no recorded pull request")
		}
		for _, pull := range plan.Snapshot.PullRequests {
			if pull.URL == plan.Snapshot.Run.PullRequest {
				return nil
			}
		}
		return fmt.Errorf("waiting-for-merge Run pull request %s was not freshly verified", plan.Snapshot.Run.PullRequest)
	}
	return nil
}

// PlansEqual compares the operator-visible approved plans.
func PlansEqual(left, right Plan) bool {
	var leftText, rightText bytes.Buffer
	printPlan(&leftText, left)
	printPlan(&rightText, right)
	return leftText.String() == rightText.String()
}

// Retire executes the complete ordered retirement, revalidating immediately
// before each mutation and verifying every postcondition before returning.
func (e Service) Retire(ctx context.Context, approved Plan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.policy.MarkProgressBeforeMutation && len(approved.Actions) != 0 && approved.Actions[0].kind == actionMarkProgress {
		if err := e.markProgress(approved.Snapshot); err != nil {
			return err
		}
		approved.Snapshot.Run.Status = e.policy.ProgressStatus
		approved.Actions = append([]Action(nil), approved.Actions[1:]...)
	}
	return e.apply(ctx, approved)
}

func (e Service) apply(ctx context.Context, approved Plan) error {
	plan, err := e.inspect(ctx)
	if err != nil {
		return err
	}
	if err := e.validateMutation(plan); err != nil {
		return err
	}
	if completed, err := e.completeMergedPlanWithContinuity(ctx, approved.Snapshot, plan); completed {
		return err
	}
	if !executablePlansEqual(approved, plan) {
		return fmt.Errorf("%s Plan changed after confirmation; rerun %s to review the current plan", e.policy.Operation, e.policy.Operation)
	}

	for {
		plan, err = e.inspect(ctx)
		if err != nil {
			return err
		}
		if err := e.validateMutation(plan); err != nil {
			return err
		}
		if err := e.verifyGitHubIdentityContinuity(approved.Snapshot, plan.Snapshot); err != nil {
			return err
		}

		if len(plan.Actions) == 0 {
			return e.finalize(ctx, plan)
		}
		action := plan.Actions[0]
		switch action.kind {
		case actionMarkProgress:
			if plan.Snapshot.Run.Status == scheduler.StatusWaitingForMerge && !e.policy.MarkProgressBeforeMutation {
				if err := verifyWaitingForMergeDisarmed(plan); err != nil {
					return err
				}
			}
			if err := e.markProgress(plan.Snapshot); err != nil {
				return err
			}
			continue
		case actionDisablePullRequestAutoMerge:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "disabling pull request auto-merge")
			if err != nil || completed {
				return err
			}
			pull, found := pullRequestByNumber(before.Snapshot.PullRequests, action.pullRequest)
			if !found || !pull.AutoMergeArmed {
				return fmt.Errorf("pull request #%d is not ready for auto-merge disablement", action.pullRequest)
			}
			if err := e.github.DisablePullRequestAutoMerge(ctx, before.Snapshot.Repository, pull.Number); err != nil {
				return e.reconcileMutationFailure(ctx, approved, fmt.Errorf("disable auto-merge for pull request #%d: %w", pull.Number, err))
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			updated, found := pullRequestByNumber(after.Snapshot.PullRequests, pull.Number)
			if !found || updated.State != PullRequestOpen || updated.AutoMergeArmed {
				return fmt.Errorf("pull request #%d was not freshly verified open, unmerged, and auto-merge unarmed", pull.Number)
			}
		case actionExplainPullRequest:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "commenting on pull request")
			if err != nil || completed {
				return err
			}
			pull, found := pullRequestByNumber(before.Snapshot.PullRequests, action.pullRequest)
			if !found || pull.AutoMergeArmed || pull.Explained {
				return fmt.Errorf("pull request #%d is not ready for the %s explanation", action.pullRequest, e.policy.Operation)
			}
			if err := e.github.CommentOnPullRequest(ctx, before.Snapshot.Repository, pull.Number, e.policy.Explanation(before.Snapshot.Run)); err != nil {
				return e.reconcileMutationFailure(ctx, approved, fmt.Errorf("%s on pull request #%d: %w", e.policy.ExplanationAction, pull.Number, err))
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			updated, found := pullRequestByNumber(after.Snapshot.PullRequests, pull.Number)
			if !found || updated.State == PullRequestMerged || updated.AutoMergeArmed || !updated.Explained {
				return fmt.Errorf("pull request #%d %s explanation did not satisfy its verified postcondition", pull.Number, e.policy.Operation)
			}
		case actionClosePullRequest:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "closing pull request")
			if err != nil || completed {
				return err
			}
			pull, found := pullRequestByNumber(before.Snapshot.PullRequests, action.pullRequest)
			if !found || pull.AutoMergeArmed || !pull.Explained {
				return fmt.Errorf("pull request #%d is not ready for safe closure", action.pullRequest)
			}
			if err := e.github.ClosePullRequest(ctx, before.Snapshot.Repository, pull.Number); err != nil {
				return e.reconcileMutationFailure(ctx, approved, fmt.Errorf("close unmerged pull request #%d: %w", pull.Number, err))
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			updated, found := pullRequestByNumber(after.Snapshot.PullRequests, pull.Number)
			if !found || updated.State != PullRequestClosed || updated.AutoMergeArmed {
				return fmt.Errorf("pull request #%d was not freshly verified closed and unmerged with auto-merge unarmed", pull.Number)
			}
		case actionDeleteRemoteBranch:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "deleting remote branch")
			if err != nil || completed {
				return err
			}
			branch := before.Snapshot.RemoteBranch
			if err := deleteRemoteBranch(ctx, e.gitExecutable, e.repositoryRoot, branch); err != nil {
				return err
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			if after.Snapshot.RemoteBranch.Present {
				return fmt.Errorf("owned remote branch %s is still present after deletion", branch.Name)
			}
		case actionRemoveLocalWorktree:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "removing local worktree")
			if err != nil || completed {
				return err
			}
			worktree := before.Snapshot.Worktree
			if !before.Snapshot.LocalBranch.Present || before.Snapshot.LocalBranch.Name != worktree.Branch || before.Snapshot.LocalBranch.Commit != worktree.Commit {
				return fmt.Errorf("local branch commit and worktree association changed immediately before removing %s", worktree.Path)
			}
			if err := removeLocalWorktree(ctx, e.gitExecutable, e.repositoryRoot, worktree); err != nil {
				return err
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			if after.Snapshot.Worktree.Present {
				return fmt.Errorf("owned local worktree %s is still present after removal", worktree.Path)
			}
		case actionDeleteLocalBranch:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "deleting local branch")
			if err != nil || completed {
				return err
			}
			branch := before.Snapshot.LocalBranch
			if before.Snapshot.Worktree.Present {
				return fmt.Errorf("owned local branch %s remains assigned to worktree %s", branch.Name, before.Snapshot.Worktree.Path)
			}
			if err := deleteLocalBranch(ctx, e.gitExecutable, e.repositoryRoot, branch); err != nil {
				return err
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			if after.Snapshot.LocalBranch.Present {
				return fmt.Errorf("owned local branch %s is still present after deletion", branch.Name)
			}
		case actionArchiveSession:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "archiving Pi session")
			if err != nil || completed {
				return err
			}
			session := before.Snapshot.Session
			if err := archiveSession(before.Snapshot.Run, session, e.stateDirectory, e.filesystemSync); err != nil {
				return err
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			if after.Snapshot.Session.Present || !after.Snapshot.Session.Archived {
				return fmt.Errorf("Pi session %s was not verified in its non-resumable historical archive", session.ID)
			}
		case actionRemoveIssueLabel:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "removing issue label "+action.label)
			if err != nil || completed {
				return err
			}
			if err := e.github.RemoveIssueLabel(ctx, before.Snapshot.Repository, before.Snapshot.Run.Issue, action.label); err != nil {
				return fmt.Errorf("remove issue label %s: %w", action.label, err)
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			if err := verifyLabelMutation(before.Snapshot.Issue.Labels, after.Snapshot.Issue.Labels, "", action.label); err != nil {
				return err
			}
		case actionAddIssueLabel:
			before, completed, err := e.revalidateAction(ctx, plan, approved, action, "adding issue label "+action.label)
			if err != nil || completed {
				return err
			}
			if err := e.github.AddIssueLabel(ctx, before.Snapshot.Repository, before.Snapshot.Run.Issue, action.label); err != nil {
				return fmt.Errorf("add issue label %s: %w", action.label, err)
			}
			after, completed, err := e.inspectMutationPostcondition(ctx, approved)
			if err != nil || completed {
				return err
			}
			if err := verifyLabelMutation(before.Snapshot.Issue.Labels, after.Snapshot.Issue.Labels, action.label, ""); err != nil {
				return err
			}
		case actionFinalize:
			return e.finalize(ctx, plan)
		case actionFinalizeCompletion:
			return e.finalizeCompletion(ctx, plan, action.pullRequest)
		default:
			return fmt.Errorf("%s Plan contains an unknown action", e.policy.Operation)
		}
	}
}

func (e Service) revalidatePlan(ctx context.Context, current, approved Plan, action string) (Plan, bool, error) {
	fresh, err := e.inspect(ctx)
	if err != nil {
		return Plan{}, false, err
	}
	if err := e.validateMutation(fresh); err != nil {
		return Plan{}, false, err
	}
	if completed, err := e.completeMergedPlanWithContinuity(ctx, approved.Snapshot, fresh); completed {
		return Plan{}, true, err
	}
	if err := e.verifyGitHubIdentityContinuity(approved.Snapshot, fresh.Snapshot); err != nil {
		return Plan{}, false, err
	}
	if !executablePlansEqual(current, fresh) {
		return Plan{}, false, fmt.Errorf("%s Plan changed immediately before %s", e.policy.Operation, action)
	}
	return fresh, false, nil
}

func (e Service) revalidateAction(ctx context.Context, current, approved Plan, action Action, description string) (Plan, bool, error) {
	fresh, completed, err := e.revalidatePlan(ctx, current, approved, description)
	if err != nil || completed {
		return Plan{}, completed, err
	}
	if len(fresh.Actions) == 0 || fresh.Actions[0] != action {
		return Plan{}, false, fmt.Errorf("%s Plan no longer authorizes %s as its next action", e.policy.Operation, description)
	}
	return fresh, false, nil
}

func executablePlansEqual(left, right Plan) bool {
	if !PlansEqual(left, right) || len(left.Actions) != len(right.Actions) {
		return false
	}
	for index := range left.Actions {
		if left.Actions[index] != right.Actions[index] {
			return false
		}
	}
	return true
}

func (e Service) verifyGitHubIdentityContinuity(expected, actual Snapshot) error {
	if expected.Repository == "" || expected.Repository != actual.Repository {
		return fmt.Errorf("repository identity changed while %s Run artifacts", e.policy.ProgressStatus)
	}
	if expected.Run.RunID != actual.Run.RunID || expected.Lease != actual.Lease {
		return fmt.Errorf("Run or Lease identity changed while %s Run artifacts", e.policy.ProgressStatus)
	}
	if expected.Issue.Number != actual.Issue.Number || expected.Issue.URL != actual.Issue.URL || expected.Issue.Open != actual.Issue.Open || expected.Issue.ClosureReason != actual.Issue.ClosureReason {
		return fmt.Errorf("issue identity, state, or closure reason changed while %s Run artifacts", e.policy.ProgressStatus)
	}
	expectedPulls := make(map[int]PullRequest, len(expected.PullRequests))
	for _, pull := range expected.PullRequests {
		expectedPulls[pull.Number] = pull
	}
	if len(expectedPulls) != len(actual.PullRequests) {
		return fmt.Errorf("pull request identity set changed while %s GitHub artifacts", e.policy.ProgressStatus)
	}
	for _, pull := range actual.PullRequests {
		owned, found := expectedPulls[pull.Number]
		if !found || pull.URL != owned.URL || pull.Branch != owned.Branch || pull.Commit != owned.Commit {
			return fmt.Errorf("pull request #%d branch or expected commit identity changed while %s", pull.Number, e.policy.ProgressStatus)
		}
	}
	if err := verifyBranchIdentityContinuity("remote", expected.RemoteBranch, actual.RemoteBranch, expected.Run.RunID, e.policy.ProgressStatus); err != nil {
		return err
	}
	if err := verifyBranchIdentityContinuity("local", expected.LocalBranch, actual.LocalBranch, expected.Run.RunID, e.policy.ProgressStatus); err != nil {
		return err
	}
	if actual.Worktree.Present {
		if !expected.Worktree.Present || actual.Worktree != expected.Worktree {
			return fmt.Errorf("owned local worktree identity changed while %s Run %s", e.policy.ProgressStatus, expected.Run.RunID)
		}
	} else if actual.Worktree.Path != expected.Worktree.Path || actual.Worktree.Branch != expected.Worktree.Branch {
		return fmt.Errorf("owned local worktree assignment changed while %s Run %s", e.policy.ProgressStatus, expected.Run.RunID)
	}
	if actual.Session.Present {
		if !expected.Session.Present || actual.Session.ID != expected.Session.ID || actual.Session.Dir != expected.Session.Dir || actual.Session.ArchiveDir != expected.Session.ArchiveDir {
			return fmt.Errorf("active Pi session identity changed while %s Run %s", e.policy.ProgressStatus, expected.Run.RunID)
		}
	}
	if actual.Session.Archived {
		if (!expected.Session.Present && !expected.Session.Archived) || actual.Session.ID != expected.Session.ID || actual.Session.Dir != expected.Session.Dir || actual.Session.ArchiveDir != expected.Session.ArchiveDir {
			return fmt.Errorf("historical Pi session identity changed while %s Run %s", e.policy.ProgressStatus, expected.Run.RunID)
		}
	} else if expected.Session.Archived || expected.Session.Present && !actual.Session.Present {
		return fmt.Errorf("Pi session archive disappeared while %s Run %s", e.policy.ProgressStatus, expected.Run.RunID)
	}
	return nil
}

func verifyBranchIdentityContinuity(location string, expected, actual Branch, runID string, progress scheduler.Status) error {
	if actual.Present {
		if !expected.Present || actual.Name != expected.Name || actual.Commit != expected.Commit {
			return fmt.Errorf("owned %s branch identity changed while %s Run %s", location, progress, runID)
		}
	} else if actual.Name != expected.Name {
		return fmt.Errorf("owned %s branch name changed while %s Run %s", location, progress, runID)
	}
	return nil
}

func pullRequestByNumber(pulls []PullRequest, number int) (PullRequest, bool) {
	for _, pull := range pulls {
		if pull.Number == number {
			return pull, true
		}
	}
	return PullRequest{}, false
}

func verifyWaitingForMergeDisarmed(plan Plan) error {
	pull, found := pullRequestByURL(plan.Snapshot.PullRequests, plan.Snapshot.Run.PullRequest)
	if !found || pull.State == PullRequestMerged || pull.AutoMergeArmed {
		return errors.New("waiting-for-merge Run was not freshly verified unmerged with auto-merge unarmed")
	}
	return nil
}

func pullRequestByURL(pulls []PullRequest, target string) (PullRequest, bool) {
	for _, pull := range pulls {
		if pull.URL == target {
			return pull, true
		}
	}
	return PullRequest{}, false
}

func deleteRemoteBranch(ctx context.Context, gitExecutable, repositoryRoot string, branch Branch) error {
	ref := "refs/heads/" + branch.Name
	lease := "--force-with-lease=" + ref + ":" + branch.Commit
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "push", "origin", lease, ":"+ref)
	if err != nil {
		return fmt.Errorf("delete owned remote branch %s at %s: %w", branch.Name, branch.Commit, err)
	}
	if exit != 0 {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = fmt.Sprintf("git exited %d", exit)
		}
		return fmt.Errorf("delete owned remote branch %s at expected commit %s: %s", branch.Name, branch.Commit, message)
	}
	return nil
}

func removeLocalWorktree(ctx context.Context, gitExecutable, repositoryRoot string, worktree Worktree) error {
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "worktree", "remove", "--force", worktree.Path)
	if err != nil {
		return fmt.Errorf("remove owned local worktree %s: %w", worktree.Path, err)
	}
	if exit != 0 {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = fmt.Sprintf("git exited %d", exit)
		}
		return fmt.Errorf("remove owned local worktree %s for %s at %s: %s", worktree.Path, worktree.Branch, worktree.Commit, message)
	}
	return nil
}

func deleteLocalBranch(ctx context.Context, gitExecutable, repositoryRoot string, branch Branch) error {
	ref := "refs/heads/" + branch.Name
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "update-ref", "-d", ref, branch.Commit)
	if err != nil {
		return fmt.Errorf("delete owned local branch %s at %s: %w", branch.Name, branch.Commit, err)
	}
	if exit != 0 {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = fmt.Sprintf("git exited %d", exit)
		}
		return fmt.Errorf("delete owned local branch %s at expected commit %s: %s", branch.Name, branch.Commit, message)
	}
	return nil
}

func archiveSession(run scheduler.Run, session Session, stateDirectory string, syncPath func(string) error) error {
	if !session.Present || session.Archived || session.Dir == "" || session.ArchiveDir == "" {
		return errors.New("Pi session is not ready for atomic archival")
	}
	archiveParent := filepath.Dir(session.ArchiveDir)
	if err := rejectSymlinkedManagedParents(stateDirectory, session.Dir); err != nil {
		return fmt.Errorf("active Pi session ownership changed before archival: %w", err)
	}
	if err := rejectSymlinkedManagedParents(stateDirectory, session.ArchiveDir); err != nil {
		return fmt.Errorf("historical Pi session archive ownership changed before archival: %w", err)
	}
	if err := os.MkdirAll(archiveParent, 0o700); err != nil {
		return fmt.Errorf("create Pi session archive: %w", err)
	}
	if err := rejectSymlinkedManagedParents(stateDirectory, session.ArchiveDir); err != nil {
		return fmt.Errorf("historical Pi session archive ownership changed before archival: %w", err)
	}
	if _, err := os.Lstat(session.ArchiveDir); err == nil {
		return fmt.Errorf("historical Pi session archive %s already exists", session.ArchiveDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect historical Pi session archive: %w", err)
	}
	if _, present, err := inspectSessionDirectory(session.Dir, run); err != nil {
		return fmt.Errorf("active Pi session identity changed immediately before archival: %w", err)
	} else if !present {
		return errors.New("active Pi session disappeared immediately before archival")
	}
	if err := os.Rename(session.Dir, session.ArchiveDir); err != nil {
		return fmt.Errorf("atomically archive Pi session %s: %w", session.ID, err)
	}
	if _, archived, err := inspectSessionDirectory(session.ArchiveDir, run); err != nil || !archived {
		if restoreErr := os.Rename(session.ArchiveDir, session.Dir); restoreErr != nil {
			return fmt.Errorf("verify archived Pi session identity: %v; restore active session: %w", err, restoreErr)
		}
		if err != nil {
			return fmt.Errorf("refuse Pi session archival after source identity changed: %w", err)
		}
		return errors.New("refuse Pi session archival because the renamed source disappeared")
	}
	return syncArchivedSession(session, stateDirectory, syncPath)
}

func syncArchivedSession(session Session, stateDirectory string, syncPath func(string) error) error {
	archivePaths := make([]string, 0)
	if err := filepath.WalkDir(session.ArchiveDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("historical Pi session archive path %s is not a regular file or directory", path)
		}
		archivePaths = append(archivePaths, path)
		return nil
	}); err != nil {
		return fmt.Errorf("inspect historical Pi session archive for durability: %w", err)
	}
	for index := len(archivePaths) - 1; index >= 0; index-- {
		if err := syncPath(archivePaths[index]); err != nil {
			return fmt.Errorf("sync historical Pi session archive payload %s: %w", archivePaths[index], err)
		}
	}

	parents := []struct {
		description string
		path        string
	}{
		{description: "historical Pi session archive", path: filepath.Dir(session.ArchiveDir)},
		{description: "historical Pi session parent", path: filepath.Dir(filepath.Dir(session.ArchiveDir))},
		{description: "state directory", path: stateDirectory},
		{description: "active Pi session directory after archival", path: filepath.Dir(session.Dir)},
	}
	seen := make(map[string]bool, len(parents))
	for _, directory := range parents {
		cleaned := filepath.Clean(directory.path)
		if seen[cleaned] {
			continue
		}
		seen[cleaned] = true
		if err := syncPath(cleaned); err != nil {
			return fmt.Errorf("sync %s: %w", directory.description, err)
		}
	}
	return nil
}

func (e Service) filesystemSync(path string) error {
	if e.syncPath != nil {
		return e.syncPath(path)
	}
	return syncFilesystemPath(path)
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

func normalizedLabelSet(labels []string) map[string]bool {
	result := make(map[string]bool, len(labels))
	for _, label := range labels {
		result[foldLabel(label)] = true
	}
	return result
}

func verifyLabelMutation(before, after []string, added, removed string) error {
	expected := normalizedLabelSet(before)
	if added != "" {
		expected[foldLabel(added)] = true
	}
	if removed != "" {
		delete(expected, foldLabel(removed))
	}
	actual := normalizedLabelSet(after)
	if len(expected) == len(actual) {
		matches := true
		for label := range expected {
			if !actual[label] {
				matches = false
				break
			}
		}
		if matches {
			return nil
		}
	}
	return fmt.Errorf("issue label mutation did not satisfy its verified postcondition: labels changed from %s to %s", formatLabels(before), formatLabels(after))
}

func (e Service) inspectMutationPostcondition(ctx context.Context, approved Plan) (Plan, bool, error) {
	after, err := e.inspect(ctx)
	if err != nil {
		return Plan{}, false, err
	}
	if completed, err := e.completeMergedPlanWithContinuity(ctx, approved.Snapshot, after); completed {
		return Plan{}, true, err
	}
	if err := e.verifyGitHubIdentityContinuity(approved.Snapshot, after.Snapshot); err != nil {
		return Plan{}, false, err
	}
	return after, false, nil
}

func (e Service) reconcileMutationFailure(ctx context.Context, approved Plan, mutationErr error) error {
	if completed, completionErr := e.completeLateMerge(ctx, approved); completed {
		return completionErr
	}
	return mutationErr
}

func (e Service) completeLateMerge(ctx context.Context, approved Plan) (bool, error) {
	fresh, err := e.inspect(ctx)
	if err != nil {
		return false, nil
	}
	return e.completeMergedPlanWithContinuity(ctx, approved.Snapshot, fresh)
}

func (e Service) completeMergedPlanWithContinuity(ctx context.Context, approved Snapshot, plan Plan) (bool, error) {
	if !e.policy.AllowMergedCompletion || plan.TerminalState != scheduler.StatusMerged {
		return false, nil
	}
	if err := e.verifyGitHubIdentityContinuity(approved, plan.Snapshot); err != nil {
		return true, err
	}
	return e.completeMergedPlan(ctx, plan)
}

func (e Service) completeMergedPlan(ctx context.Context, plan Plan) (bool, error) {
	if !e.policy.AllowMergedCompletion || plan.TerminalState != scheduler.StatusMerged {
		return false, nil
	}
	if len(plan.Actions) != 1 || plan.Actions[0].kind != actionFinalizeCompletion {
		return true, errors.New("merged expected pull request did not produce an executable Completion plan")
	}
	return true, e.finalizeCompletion(ctx, plan, plan.Actions[0].pullRequest)
}

func (e Service) markProgress(expected Snapshot) error {
	current, _, err := e.store.Preview()
	if err != nil {
		return err
	}
	run, lease, err := e.policy.SelectRun(current)
	if err != nil {
		return err
	}
	if run.RunID != expected.Run.RunID || run.Issue != expected.Run.Issue || run.Status != expected.Run.Status || lease != expected.Lease {
		return errors.New("Run or Lease identity changed before recording retirement progress")
	}
	if run.Status == e.policy.TerminalStatus || run.Status == e.policy.ProgressStatus {
		return nil
	}
	if !e.policy.CanTransition(run.Status, e.policy.ProgressStatus) {
		return fmt.Errorf("Run status %s cannot transition to %s", run.Status, e.policy.ProgressStatus)
	}
	for index := range current.Runs {
		if current.Runs[index].RunID == run.RunID {
			current.Runs[index].Status = e.policy.ProgressStatus
			current.Runs[index].UpdatedAt = time.Now().UTC()
			break
		}
	}
	if lease.LeaseID == "" {
		return fmt.Errorf("Run %s has no active Lease while entering %s", run.RunID, e.policy.ProgressStatus)
	}
	return e.store.Save(current)
}

func (e Service) finalizeCompletion(ctx context.Context, verified Plan, pullNumber int) error {
	fresh, err := e.inspect(ctx)
	if err != nil {
		return err
	}
	if !executablePlansEqual(verified, fresh) || len(fresh.Actions) != 1 || fresh.Actions[0].kind != actionFinalizeCompletion {
		return fmt.Errorf("%s Plan changed immediately before recording Completion", e.policy.Operation)
	}
	pull, found := pullRequestByNumber(fresh.Snapshot.PullRequests, pullNumber)
	if !found || pull.State != PullRequestMerged || fresh.Snapshot.Issue.Open {
		return errors.New("expected merged pull request and closed issue were not freshly verified for Completion")
	}
	current, _, err := e.store.Preview()
	if err != nil {
		return err
	}
	run, lease, err := e.policy.SelectRun(current)
	if err != nil {
		return err
	}
	if run.RunID != fresh.Snapshot.Run.RunID || lease != fresh.Snapshot.Lease || lease.LeaseID == "" {
		return errors.New("Run or Lease changed before recording Completion")
	}
	if !scheduler.CanTransition(run.Status, scheduler.StatusMerged) {
		return fmt.Errorf("Run status %s cannot transition to merged Completion", run.Status)
	}
	now := time.Now().UTC()
	for index := range current.Runs {
		if current.Runs[index].RunID == run.RunID {
			current.Runs[index].Status = scheduler.StatusMerged
			current.Runs[index].PullRequest = pull.URL
			current.Runs[index].CompletedAt = &now
			current.Runs[index].UpdatedAt = now
			current.Runs[index].PID = 0
			current.Runs[index].ProcessIdentity = ""
			current.Runs[index].WorkerLogOpen = false
			current.Runs[index].CleanupPending = fresh.Snapshot.Worktree.Present || fresh.Snapshot.LocalBranch.Present
			current.Runs[index].Error = ""
			break
		}
	}
	for index := range current.Leases {
		if current.Leases[index] == lease {
			current.Leases = append(current.Leases[:index], current.Leases[index+1:]...)
			break
		}
	}
	if err := e.store.Save(current); err != nil {
		return fmt.Errorf("atomically record Completion and release Lease: %w", err)
	}
	persisted, _, err := e.store.Preview()
	if err != nil {
		return err
	}
	for _, lease := range persisted.Leases {
		if lease.RunID == run.RunID {
			return fmt.Errorf("Completion Run %s still has Lease %s", run.RunID, lease.LeaseID)
		}
	}
	for _, persistedRun := range persisted.Runs {
		if persistedRun.RunID == run.RunID && persistedRun.Status == scheduler.StatusMerged && persistedRun.PullRequest == pull.URL && persistedRun.CompletedAt != nil {
			return nil
		}
	}
	return fmt.Errorf("Completion for Run %s was not durably recorded", run.RunID)
}

func (e Service) finalize(ctx context.Context, verified Plan) error {
	if verified.Snapshot.Session.Archived {
		if err := syncArchivedSession(verified.Snapshot.Session, e.stateDirectory, e.filesystemSync); err != nil {
			return fmt.Errorf("verify durable Pi session archive: %w", err)
		}
	}
	fresh, err := e.inspect(ctx)
	if err != nil {
		return err
	}
	if completed, err := e.completeMergedPlanWithContinuity(ctx, verified.Snapshot, fresh); completed {
		return err
	}
	if err := e.verifyGitHubIdentityContinuity(verified.Snapshot, fresh.Snapshot); err != nil {
		return err
	}
	if !executablePlansEqual(verified, fresh) {
		return fmt.Errorf("%s Plan changed immediately before finalization", e.policy.Operation)
	}
	verified = fresh
	if !e.policy.labelsSatisfied(verified.Snapshot.Issue.Labels) {
		return errors.New("managed issue label postconditions were not verified")
	}
	if err := e.validateMutation(verified); err != nil {
		return err
	}
	if err := e.verifyOwnedFinalState(verified.Snapshot); err != nil {
		return err
	}
	current, _, err := e.store.Preview()
	if err != nil {
		return err
	}
	run, lease, err := e.policy.SelectRun(current)
	if err != nil {
		return err
	}
	if run.RunID != verified.Snapshot.Run.RunID {
		return fmt.Errorf("active Run changed from %s to %s before finalization", verified.Snapshot.Run.RunID, run.RunID)
	}
	if run.Status == e.policy.TerminalStatus {
		return e.verifyFinalState(current, run)
	}
	if lease != verified.Snapshot.Lease {
		return fmt.Errorf("Lease for Run %s changed before finalization", run.RunID)
	}
	if !e.policy.CanTransition(run.Status, e.policy.TerminalStatus) {
		return fmt.Errorf("Run status %s cannot transition to %s", run.Status, e.policy.TerminalStatus)
	}
	now := time.Now().UTC()
	finalized := run
	finalized.Status = e.policy.TerminalStatus
	finalized.WorkerLogOpen = false
	finalized.UpdatedAt = now
	finalized.CompletedAt = &now
	if e.policy.RecordMissingLogWarn {
		finalized.DiagnosticWarning = missingLogWarning(run)
	}
	if e.policy.FinalizeMetadata != nil {
		e.policy.FinalizeMetadata(&finalized, verified.Snapshot, now)
	}
	for index := range current.Runs {
		if current.Runs[index].RunID == run.RunID {
			current.Runs[index] = finalized
			break
		}
	}
	for index := range current.Leases {
		if current.Leases[index] == lease {
			current.Leases = append(current.Leases[:index], current.Leases[index+1:]...)
			break
		}
	}
	if err := e.store.Save(current); err != nil {
		return fmt.Errorf("atomically mark Run %s and release Lease: %w", e.policy.TerminalStatus, err)
	}
	persisted, _, err := e.store.Preview()
	if err != nil {
		return fmt.Errorf("verify finalized %s state: %w", e.policy.Operation, err)
	}
	return e.verifyFinalState(persisted, finalized)
}

func missingLogWarning(run scheduler.Run) string {
	var missing []string
	for _, log := range []struct {
		name string
		path string
	}{{"Worker JSONL log", run.LogPath}, {"Worker standard-error log", run.StderrPath}} {
		if log.path == "" {
			continue
		}
		info, err := os.Lstat(log.path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			missing = append(missing, fmt.Sprintf("%s %s", log.name, log.path))
		}
	}
	if len(missing) == 0 {
		return ""
	}
	return "Recorded diagnostics unavailable at External Resolution: " + strings.Join(missing, "; ")
}

func (e Service) verifyOwnedFinalState(snapshot Snapshot) error {
	for _, pull := range snapshot.PullRequests {
		if pull.State != PullRequestClosed || pull.AutoMergeArmed {
			return fmt.Errorf("pull request #%d final state is not verified closed, unmerged, and auto-merge unarmed", pull.Number)
		}
		if e.policy.RequireClosedExplanation && !pull.Explained {
			return fmt.Errorf("pull request #%d final state is missing its verified %s explanation", pull.Number, e.policy.Operation)
		}
	}
	if snapshot.RemoteBranch.Present {
		return fmt.Errorf("owned remote branch %s remains present at %s", snapshot.RemoteBranch.Name, snapshot.RemoteBranch.Commit)
	}
	if snapshot.Worktree.Present {
		return fmt.Errorf("owned local worktree %s remains present", snapshot.Worktree.Path)
	}
	if snapshot.LocalBranch.Present {
		return fmt.Errorf("owned local branch %s remains present at %s", snapshot.LocalBranch.Name, snapshot.LocalBranch.Commit)
	}
	if snapshot.Session.Present {
		return fmt.Errorf("active Pi session %s remains resumable in %s", snapshot.Session.ID, snapshot.Session.Dir)
	}
	if e.policy.RequireDurableLogs {
		return verifyDurableRunLogs(snapshot.Run)
	}
	return nil
}

func verifyDurableRunLogs(run scheduler.Run) error {
	for _, log := range []struct {
		description string
		path        string
	}{
		{description: "Worker JSONL log", path: run.LogPath},
		{description: "Worker standard-error log", path: run.StderrPath},
	} {
		description, path := log.description, log.path
		if path == "" {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("verify durable %s for Run %s at %s: %w", description, run.RunID, path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("durable %s for Run %s at %s is not a regular file", description, run.RunID, path)
		}
	}
	return nil
}

func (e Service) verifyFinalState(current state.State, expected scheduler.Run) error {
	if e.policy.RequireDurableLogs {
		if err := verifyDurableRunLogs(expected); err != nil {
			return err
		}
	}
	found := false
	for _, run := range current.Runs {
		if run.RunID == expected.RunID {
			found = true
			if run.Status != e.policy.TerminalStatus {
				return fmt.Errorf("Run %s final status is %s, not %s", expected.RunID, run.Status, e.policy.TerminalStatus)
			}
			if run.WorkerLogOpen {
				return fmt.Errorf("Run %s Worker-log-open marker remains open after %s", expected.RunID, e.policy.Operation)
			}
			metadata := run
			metadata.Status = expected.Status
			metadata.UpdatedAt = expected.UpdatedAt
			metadata.CompletedAt = expected.CompletedAt
			if !reflect.DeepEqual(metadata, expected) {
				return fmt.Errorf("historical metadata for Run %s changed during finalization", expected.RunID)
			}
		}
	}
	if !found {
		return fmt.Errorf("historical Run %s is absent after %s", expected.RunID, e.policy.Operation)
	}
	for _, lease := range current.Leases {
		if lease.RunID == expected.RunID {
			return fmt.Errorf("old Lease %s for %s Run %s is still active", lease.LeaseID, e.policy.TerminalStatus, expected.RunID)
		}
	}
	return nil
}

func inspectWorkerAbsent(run scheduler.Run) error {
	if run.ResumePending {
		return errors.New("replacement Worker launch is pending; Worker absence is uncertain")
	}
	if run.ProcessIdentity == "" {
		if run.PID == 0 {
			return nil
		}
		return fmt.Errorf("Run %s has incomplete Worker identity", run.RunID)
	}
	pid, err := processidentity.PID(run.ProcessIdentity)
	if err != nil {
		if run.PID != 0 {
			return fmt.Errorf("Worker PID %d has uncertain identity %q: %w", run.PID, run.ProcessIdentity, err)
		}
		return fmt.Errorf("Run %s has uncertain retained Worker identity: %w", run.RunID, err)
	}
	if run.PID != 0 && run.PID != pid {
		return fmt.Errorf("Run %s has contradictory Worker identity: recorded PID %d does not match process identity PID %d", run.RunID, run.PID, pid)
	}
	processAlive, err := processidentity.Alive(pid)
	if err != nil {
		return fmt.Errorf("verify Worker PID %d: %w", pid, err)
	}
	groupAlive, err := processidentity.Alive(-pid)
	if err != nil {
		return fmt.Errorf("verify Worker process group %d: %w", pid, err)
	}
	if !processAlive && !groupAlive {
		return nil
	}
	if !processAlive || !groupAlive {
		return fmt.Errorf("Worker PID/process-group liveness is uncertain for Run %s", run.RunID)
	}
	identity, err := processidentity.Start(pid)
	if err != nil {
		return fmt.Errorf("verify live Worker identity: %w", err)
	}
	if identity != run.ProcessIdentity {
		return fmt.Errorf("Worker PID %d is live with uncertain identity %q instead of %q", pid, identity, run.ProcessIdentity)
	}
	return fmt.Errorf("Worker for Run %s is live at PID %d", run.RunID, pid)
}

func absentWorkerSummary(run scheduler.Run) string {
	pid := run.PID
	if pid == 0 && run.ProcessIdentity != "" {
		pid, _ = processidentity.PID(run.ProcessIdentity)
	}
	if pid == 0 {
		return "absent (no recorded PID)"
	}
	return fmt.Sprintf("absent (recorded PID and process group %d)", pid)
}

func validateOwnedPaths(run scheduler.Run, stateDir, repositoryRoot, defaultBranch string) error {
	if !isManagedPathComponent(run.RunID) {
		return fmt.Errorf("Run ID %q is not a safe managed path component", run.RunID)
	}
	if (run.Branch == "") != (run.Worktree == "") {
		return fmt.Errorf("Run %s has incomplete branch/worktree ownership", run.RunID)
	}
	if run.Branch != "" {
		manager := worktree.Manager{RepositoryDir: repositoryRoot, WorktreesDir: filepath.Join(stateDir, "worktrees"), DefaultBranch: defaultBranch}
		expected, err := manager.Plan(run.Issue, run.RunID)
		if err != nil {
			return err
		}
		if run.Branch != expected.Branch || filepath.Clean(run.Worktree) != filepath.Clean(expected.Path) {
			return fmt.Errorf("Run %s branch/worktree identity is not Backlog-owned", run.RunID)
		}
		if err := rejectSymlinkedManagedParents(stateDir, run.Worktree); err != nil {
			return fmt.Errorf("Run %s worktree ownership is uncertain: %w", run.RunID, err)
		}
	}
	if run.WorkerMode == scheduler.WorkerModeRPC {
		expectedSessionDir := filepath.Join(stateDir, "sessions", run.RunID)
		if run.SessionID != "backlog-"+run.RunID || filepath.Clean(run.SessionDir) != filepath.Clean(expectedSessionDir) {
			return fmt.Errorf("Run %s Pi session identity is not Backlog-owned", run.RunID)
		}
		if err := rejectSymlinkedManagedParents(stateDir, run.SessionDir); err != nil {
			return fmt.Errorf("Run %s Pi session ownership is uncertain: %w", run.RunID, err)
		}
		archiveDir := filepath.Join(stateDir, "history", "sessions", run.RunID)
		if err := rejectSymlinkedManagedParents(stateDir, archiveDir); err != nil {
			return fmt.Errorf("Run %s Pi session archive ownership is uncertain: %w", run.RunID, err)
		}
	} else if run.SessionID != "" || run.SessionDir != "" || run.Continuation != nil {
		return fmt.Errorf("print-mode Run %s has uncertain Pi session identity", run.RunID)
	}
	return nil
}

func isManagedPathComponent(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value
}

func rejectSymlinkedManagedParents(root, target string) error {
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path %s is outside managed state directory %s", target, root)
	}
	current := filepath.Clean(root)
	components := strings.Split(relative, string(filepath.Separator))
	for _, component := range components[:len(components)-1] {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("inspect managed path component %s: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("managed path component %s is a symlink", current)
		}
		if !info.IsDir() {
			return fmt.Errorf("managed path component %s is not a directory", current)
		}
	}
	return nil
}

func inspectOriginRepository(ctx context.Context, gitExecutable, repositoryRoot string) (string, error) {
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "remote", "get-url", "origin")
	if err != nil {
		return "", fmt.Errorf("inspect Git origin: %w", err)
	}
	if exit != 0 {
		return "", fmt.Errorf("inspect Git origin: git exited %d; identity is unknown", exit)
	}
	remote := strings.TrimSpace(string(output))
	if strings.Contains(remote, "\n") {
		return "", errors.New("inspect Git origin: git returned multiple URLs")
	}
	host, repository, ok := parseGitRemote(remote)
	if !ok || !strings.EqualFold(host, "github.com") {
		return "", fmt.Errorf("inspect Git origin: unsupported or unknown GitHub URL %q", remote)
	}
	return repository, nil
}

func parseGitRemote(remote string) (string, string, bool) {
	var host, path string
	if !strings.Contains(remote, "://") {
		userHost, value, found := strings.Cut(remote, ":")
		if !found || strings.Contains(userHost, "/") {
			return "", "", false
		}
		host = userHost
		if _, value, found := strings.Cut(host, "@"); found {
			host = value
		}
		path = value
	} else {
		parsed, err := url.Parse(remote)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "ssh") || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return "", "", false
		}
		host, path = parsed.Hostname(), parsed.Path
	}
	parts := strings.Split(strings.Trim(strings.TrimSuffix(path, ".git"), "/"), "/")
	if host == "" || len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return host, parts[0] + "/" + parts[1], true
}

func inspectRemoteBranch(ctx context.Context, gitExecutable, repositoryRoot, branch string) (Branch, error) {
	if branch == "" {
		return Branch{}, nil
	}
	ref := "refs/heads/" + branch
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "ls-remote", "--exit-code", "--heads", "origin", ref)
	if err != nil {
		return Branch{}, fmt.Errorf("inspect remote branch %s: %w", branch, err)
	}
	if exit == 2 && len(bytes.TrimSpace(output)) == 0 {
		return Branch{Name: branch}, nil
	}
	if exit != 0 {
		return Branch{}, fmt.Errorf("inspect remote branch %s: git exited %d; state is unknown", branch, exit)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 2 || fields[1] != ref || !validObjectID(fields[0]) {
		return Branch{}, fmt.Errorf("inspect remote branch %s: unknown ls-remote output", branch)
	}
	return Branch{Name: branch, Commit: fields[0], Present: true}, nil
}

func inspectLocalResources(ctx context.Context, gitExecutable, repositoryRoot, commonDirectory string, run scheduler.Run) (Branch, Worktree, error) {
	if run.Branch == "" {
		return Branch{}, Worktree{}, nil
	}
	ref := "refs/heads/" + run.Branch
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "for-each-ref", "--format=%(objectname)", ref)
	if err != nil {
		return Branch{}, Worktree{}, fmt.Errorf("inspect local branch %s: %w", run.Branch, err)
	}
	if exit != 0 {
		return Branch{}, Worktree{}, fmt.Errorf("inspect local branch %s: git exited %d with unknown output", run.Branch, exit)
	}
	local := Branch{Name: run.Branch}
	commit := strings.TrimSpace(string(output))
	if commit != "" {
		if strings.Contains(commit, "\n") || !validObjectID(commit) {
			return Branch{}, Worktree{}, fmt.Errorf("inspect local branch %s: unknown object identity", run.Branch)
		}
		local.Commit, local.Present = commit, true
	}

	output, exit, err = runGitInspection(ctx, gitExecutable, repositoryRoot, "worktree", "list", "--porcelain", "-z")
	if err != nil || exit != 0 {
		if err == nil {
			err = fmt.Errorf("git exited %d", exit)
		}
		return Branch{}, Worktree{}, fmt.Errorf("inspect local worktrees: %w", err)
	}
	entries, err := parseWorktrees(output)
	if err != nil {
		return Branch{}, Worktree{}, err
	}
	expectedPath := canonicalPath(run.Worktree)
	var registered *gitWorktree
	for index := range entries {
		entry := &entries[index]
		if entry.Branch == ref && canonicalPath(entry.Path) != expectedPath {
			return Branch{}, Worktree{}, fmt.Errorf("Run branch %s is assigned to unexpected worktree %s", run.Branch, entry.Path)
		}
		if canonicalPath(entry.Path) == expectedPath {
			if registered != nil {
				return Branch{}, Worktree{}, fmt.Errorf("Run worktree %s has duplicate Git registrations", run.Worktree)
			}
			registered = entry
		}
	}
	info, statErr := os.Lstat(run.Worktree)
	filesystemPresent := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return Branch{}, Worktree{}, fmt.Errorf("inspect worktree path %s: %w", run.Worktree, statErr)
	}
	if filesystemPresent && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return Branch{}, Worktree{}, fmt.Errorf("worktree path %s has unknown filesystem identity", run.Worktree)
	}
	if registered == nil {
		if filesystemPresent {
			return Branch{}, Worktree{}, fmt.Errorf("worktree path %s exists without the expected Git registration", run.Worktree)
		}
		return local, Worktree{Path: run.Worktree, Branch: run.Branch}, nil
	}
	if registered.Branch != ref || !validObjectID(registered.Commit) {
		return Branch{}, Worktree{}, fmt.Errorf("worktree %s has unknown branch or commit identity", run.Worktree)
	}
	if !local.Present || local.Commit != registered.Commit {
		return Branch{}, Worktree{}, fmt.Errorf("worktree %s commit does not match owned local branch", run.Worktree)
	}
	if err := verifyRegisteredWorktree(ctx, gitExecutable, run, commonDirectory, registered.Commit); err != nil {
		return Branch{}, Worktree{}, err
	}
	return local, Worktree{Path: run.Worktree, Branch: run.Branch, Commit: registered.Commit, Present: true}, nil
}

func verifyRegisteredWorktree(ctx context.Context, gitExecutable string, run scheduler.Run, commonDirectory, commit string) error {
	checks := []struct {
		args []string
		want string
	}{
		{args: []string{"rev-parse", "--show-toplevel"}, want: canonicalPath(run.Worktree)},
		{args: []string{"rev-parse", "--path-format=absolute", "--git-common-dir"}, want: canonicalPath(commonDirectory)},
		{args: []string{"symbolic-ref", "--quiet", "HEAD"}, want: "refs/heads/" + run.Branch},
		{args: []string{"rev-parse", "--verify", "HEAD"}, want: commit},
	}
	for _, check := range checks {
		output, exit, err := runGitInspection(ctx, gitExecutable, run.Worktree, check.args...)
		if err != nil || exit != 0 {
			if err == nil {
				err = fmt.Errorf("git exited %d", exit)
			}
			return fmt.Errorf("verify worktree %s identity: %w", run.Worktree, err)
		}
		got := strings.TrimSpace(string(output))
		if check.args[1] == "--show-toplevel" || check.args[1] == "--path-format=absolute" {
			got = canonicalPath(got)
		}
		if got != check.want {
			return fmt.Errorf("worktree %s has mismatched Git identity", run.Worktree)
		}
	}
	return nil
}

type gitWorktree struct {
	Path   string
	Commit string
	Branch string
}

func parseWorktrees(output []byte) ([]gitWorktree, error) {
	var result []gitWorktree
	var current gitWorktree
	for _, raw := range bytes.Split(output, []byte{0}) {
		line := string(raw)
		if line == "" {
			if current.Path != "" {
				result = append(result, current)
				current = gitWorktree{}
			}
			continue
		}
		key, value, found := strings.Cut(line, " ")
		if !found {
			if line == "bare" || line == "detached" || line == "locked" || line == "prunable" {
				continue
			}
			return nil, fmt.Errorf("inspect local worktrees: unknown porcelain field %q", line)
		}
		switch key {
		case "worktree":
			if current.Path != "" {
				return nil, errors.New("inspect local worktrees: duplicate path field")
			}
			current.Path = value
		case "HEAD":
			current.Commit = value
		case "branch":
			current.Branch = value
		case "locked", "prunable":
		default:
			return nil, fmt.Errorf("inspect local worktrees: unknown porcelain field %q", key)
		}
	}
	if current.Path != "" {
		result = append(result, current)
	}
	return result, nil
}

func canonicalPath(path string) string {
	absolute, err := filepath.Abs(path)
	if err == nil {
		path = absolute
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	return filepath.Clean(path)
}

func runGitInspection(ctx context.Context, executable, repositoryRoot string, args ...string) ([]byte, int, error) {
	commandArgs := append([]string{"-C", repositoryRoot}, args...)
	command := exec.CommandContext(ctx, executable, commandArgs...)
	output, err := command.CombinedOutput()
	if err == nil {
		return output, 0, nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return output, -1, contextErr
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return output, exitError.ExitCode(), nil
	}
	return output, -1, err
}

func validObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func inspectSession(run scheduler.Run, stateDirectory string) (Session, error) {
	if run.WorkerMode != scheduler.WorkerModeRPC {
		return Session{}, nil
	}
	result := Session{
		ID: run.SessionID, Dir: run.SessionDir,
		ArchiveDir: filepath.Join(stateDirectory, "history", "sessions", run.RunID),
	}
	activeFiles, active, err := inspectSessionDirectory(run.SessionDir, run)
	if err != nil {
		return Session{}, err
	}
	_, archived, err := inspectSessionDirectory(result.ArchiveDir, run)
	if err != nil {
		return Session{}, fmt.Errorf("inspect historical Pi session archive: %w", err)
	}
	if active && archived {
		return Session{}, fmt.Errorf("Pi session %s exists in both active and historical storage", run.SessionID)
	}
	if active && run.Continuation != nil {
		found := false
		for _, path := range activeFiles {
			if filepath.Clean(path) == filepath.Clean(run.Continuation.SessionFile) {
				found = true
				break
			}
		}
		if !found {
			return Session{}, fmt.Errorf("Pi continuation file %s is not present in the owned session", run.Continuation.SessionFile)
		}
	}
	result.Present, result.Archived = active, archived
	return result, nil
}

func inspectSessionDirectory(directory string, run scheduler.Run) ([]string, bool, error) {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inspect Pi session directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, false, fmt.Errorf("Pi session path %s has unknown filesystem identity", directory)
	}
	files := make([]string, 0)
	err = filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == directory {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("Pi session directory contains symlink %s", path)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || filepath.Ext(path) != ".jsonl" {
			return fmt.Errorf("Pi session directory contains unknown resource %s", path)
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("inspect Pi session directory: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, false, fmt.Errorf("Pi session directory %s contains no identity-bearing session files", directory)
	}
	for _, path := range files {
		if err := verifySessionHeader(path, run); err != nil {
			return nil, false, err
		}
	}
	return files, true, nil
}

func verifySessionHeader(path string, run scheduler.Run) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open Pi session file %s: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read Pi session file %s: %w", path, err)
		}
		return fmt.Errorf("Pi session file %s has no identity header", path)
	}
	var header struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		CWD  string `json:"cwd"`
	}
	if !json.Valid(scanner.Bytes()) || json.Unmarshal(scanner.Bytes(), &header) != nil || header.Type != "session" || header.ID != run.SessionID || filepath.Clean(header.CWD) != filepath.Clean(run.Worktree) {
		return fmt.Errorf("Pi session file %s identity does not match Run %s", path, run.RunID)
	}
	return nil
}

// WritePlan prints the complete operator-visible retirement plan.
func WritePlan(writer io.Writer, plan Plan) {
	printPlan(writer, plan)
}

func printPlan(writer io.Writer, plan Plan) {
	snapshot := plan.Snapshot
	fmt.Fprintf(writer, "%s Plan for issue #%d\n", plan.Operation, snapshot.Run.Issue)
	fmt.Fprintf(writer, "Run: %s (%s)\n", snapshot.Run.RunID, snapshot.Run.Status)
	if snapshot.Lease.LeaseID == "" {
		fmt.Fprintf(writer, "Lease: absent (Run already %s)\n", plan.TerminalState)
	} else {
		fmt.Fprintf(writer, "Lease: %s\n", snapshot.Lease.LeaseID)
	}
	issueState := "closed"
	if snapshot.Issue.Open {
		issueState = "open"
	}
	if snapshot.Issue.ClosureReason != "" {
		issueState += "; closure reason: " + snapshot.Issue.ClosureReason
	}
	fmt.Fprintf(writer, "Issue: %s (%s; labels: %s)\n", snapshot.Issue.URL, issueState, formatLabels(snapshot.Issue.Labels))
	fmt.Fprintf(writer, "Worker: %s\n", snapshot.WorkerSummary)
	printBranchResource(writer, "Remote branch", snapshot.RemoteBranch)
	printBranchResource(writer, "Local branch", snapshot.LocalBranch)
	if snapshot.Worktree.Present {
		fmt.Fprintf(writer, "Local worktree: %s (%s at %s)\n", snapshot.Worktree.Path, snapshot.Worktree.Branch, snapshot.Worktree.Commit)
	} else if snapshot.Worktree.Path != "" {
		fmt.Fprintf(writer, "Local worktree: %s (absent)\n", snapshot.Worktree.Path)
	} else {
		fmt.Fprintln(writer, "Local worktree: absent (not assigned)")
	}
	if snapshot.Session.Present {
		fmt.Fprintf(writer, "Pi session: %s in %s (active; archive: %s)\n", snapshot.Session.ID, snapshot.Session.Dir, snapshot.Session.ArchiveDir)
	} else if snapshot.Session.Archived {
		fmt.Fprintf(writer, "Pi session: %s in %s (archived; active storage absent)\n", snapshot.Session.ID, snapshot.Session.ArchiveDir)
	} else if snapshot.Session.ID != "" {
		fmt.Fprintf(writer, "Pi session: %s in %s (absent; archive absent)\n", snapshot.Session.ID, snapshot.Session.Dir)
	} else {
		fmt.Fprintln(writer, "Pi session: absent (not assigned)")
	}
	if len(snapshot.PullRequests) == 0 && snapshot.Run.Branch != "" {
		fmt.Fprintf(writer, "Pull requests for branch %s: absent\n", snapshot.Run.Branch)
	} else if len(snapshot.PullRequests) == 0 {
		fmt.Fprintln(writer, "Pull requests: absent (no Run branch assigned)")
	} else {
		for _, pull := range snapshot.PullRequests {
			autoMerge := "unarmed"
			if pull.AutoMergeArmed {
				autoMerge = "armed"
			}
			fmt.Fprintf(writer, "Pull request: #%d %s (%s; %s at %s; auto-merge %s)\n", pull.Number, pull.URL, pull.State, pull.Branch, pull.Commit, autoMerge)
		}
	}
	fmt.Fprintln(writer, "Required actions:")
	if len(plan.Actions) == 0 {
		fmt.Fprintln(writer, "  None.")
	}
	for index, action := range plan.Actions {
		fmt.Fprintf(writer, "  %d. %s\n", index+1, action.String())
	}
}

func printBranchResource(writer io.Writer, name string, branch Branch) {
	if branch.Present {
		fmt.Fprintf(writer, "%s: %s at %s\n", name, branch.Name, branch.Commit)
	} else if branch.Name != "" {
		fmt.Fprintf(writer, "%s: %s (absent)\n", name, branch.Name)
	} else {
		fmt.Fprintf(writer, "%s: absent (not assigned)\n", name)
	}
}

func formatLabels(labels []string) string {
	if len(labels) == 0 {
		return "none"
	}
	values := append([]string(nil), labels...)
	sort.Strings(values)
	return strings.Join(values, ", ")
}
