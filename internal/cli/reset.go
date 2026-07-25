package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/reset"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worktree"
	"golang.org/x/term"
)

func resetCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return resetCommandWithInput(ctx, args, os.Stdin, inputIsInteractive(os.Stdin), stdout, stderr)
}

func resetCommandWithInput(ctx context.Context, args []string, stdin io.Reader, interactive bool, stdout, stderr io.Writer) error {
	output := &resetOutputWriter{writer: stdout}
	stdout = output
	flags := flag.NewFlagSet("reset", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: backlog reset <issue-number> [flags]")
		fmt.Fprintln(stderr, "")
		fmt.Fprintln(stderr, "Reset abandons an incomplete Run after verifying ownership, retires its")
		fmt.Fprintln(stderr, "GitHub, local Git, and active Pi session artifacts, restores managed")
		fmt.Fprintln(stderr, "labels, preserves logs and history, then releases the Lease.")
		fmt.Fprintln(stderr, "Dry-run only inspects and prints remaining actions. Mutating Reset is")
		fmt.Fprintln(stderr, "idempotent, and partial progress can be resumed by running Reset again.")
		fmt.Fprintln(stderr, "The deprecated retry command is an alias for this command.")
		fmt.Fprintln(stderr, "")
		flags.PrintDefaults()
		fmt.Fprintln(stderr, "")
		fmt.Fprintln(stderr, "Exit statuses: 0 success or interactive cancellation; 1 refusal or failure.")
	}
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the Run")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable")
	ghExecutable := flags.String("gh", "gh", "gh executable")
	dryRun := flags.Bool("dry-run", false, "print the current Reset Plan without mutation")
	yes := flags.Bool("yes", false, "confirm Reset without an interactive prompt")
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return flags.Parse([]string{arg})
		}
	}
	issueArg, flagArgs, err := splitResetArguments(args)
	if err != nil {
		return err
	}
	if err := flags.Parse(flagArgs); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected reset arguments: %s", strings.Join(flags.Args(), " "))
	}
	issueNumber, err := strconv.Atoi(issueArg)
	if err != nil || issueNumber <= 0 {
		return fmt.Errorf("invalid issue number %q", issueArg)
	}
	if !*dryRun && !*yes && !interactive {
		return errors.New("non-interactive Reset requires --yes")
	}

	absoluteRepo, err := filepath.Abs(*repoDir)
	if err != nil {
		return err
	}
	repositoryRoot, err := gitRepositoryRoot(ctx, *gitExecutable, absoluteRepo)
	if err != nil {
		return err
	}
	commonDirectory, err := gitCommonDirectory(ctx, *gitExecutable, repositoryRoot)
	if err != nil {
		return err
	}
	lock, err := acquireResetReadLock(commonDirectory)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()

	resolvedState, err := repositoryStateDirectory(commonDirectory, repositoryRoot, *stateDir)
	if err != nil {
		return err
	}
	executor := resetExecutor{
		store:  state.FileStore{Path: filepath.Join(resolvedState, "state.json")},
		github: ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot},
		issue:  issueNumber, repositoryRoot: repositoryRoot, commonDirectory: commonDirectory,
		stateDirectory: resolvedState, gitExecutable: *gitExecutable,
	}
	plan, err := executor.inspect(ctx)
	if err != nil {
		return err
	}
	printResetPlan(stdout, plan)
	if err := output.Err(); err != nil {
		return err
	}
	if *dryRun {
		fmt.Fprintln(stdout, "Dry-run: no changes made.")
		return output.Err()
	}
	if err := validateOwnedGitHubMutation(plan); err != nil {
		return err
	}
	if err := validateResetMutationStatus(plan); err != nil {
		return err
	}

	if *yes {
		fresh, err := executor.inspect(ctx)
		if err != nil {
			return err
		}
		if err := validateOwnedGitHubMutation(fresh); err != nil {
			return err
		}
		if err := validateResetMutationStatus(fresh); err != nil {
			return err
		}
		if !resetPlansEqual(plan, fresh) {
			fmt.Fprintln(stdout, "Reset Plan changed after confirmation; using the current plan:")
			printResetPlan(stdout, fresh)
		}
		if err := output.Err(); err != nil {
			return err
		}
		plan = fresh
	} else {
		reader := bufio.NewReader(stdin)
		for {
			confirmed, err := confirmReset(ctx, reader, stdout)
			if err != nil {
				return err
			}
			if !confirmed {
				fmt.Fprintln(stdout, "Reset cancelled; no changes made.")
				return output.Err()
			}
			fresh, err := executor.inspect(ctx)
			if err != nil {
				return err
			}
			if err := validateOwnedGitHubMutation(fresh); err != nil {
				return err
			}
			if err := validateResetMutationStatus(fresh); err != nil {
				return err
			}
			if resetPlansEqual(plan, fresh) {
				plan = fresh
				break
			}
			fmt.Fprintln(stdout, "Reset Plan changed after confirmation; confirm the current plan again:")
			printResetPlan(stdout, fresh)
			if err := output.Err(); err != nil {
				return err
			}
			plan = fresh
		}
	}

	if err := output.Err(); err != nil {
		return err
	}
	if err := ensureResetStateBinding(commonDirectory, resolvedState); err != nil {
		return err
	}
	if err := executor.apply(ctx, plan); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Reset complete for issue #%d. No replacement Run was created.\n", issueNumber)
	return output.Err()
}

func ensureResetStateBinding(commonDirectory, stateDirectory string) error {
	_, exists, err := readStateDirectoryBinding(commonDirectory)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return bindStateDirectory(commonDirectory, stateDirectory)
}

func inputIsInteractive(file *os.File) bool {
	return term.IsTerminal(int(file.Fd()))
}

func confirmReset(ctx context.Context, reader *bufio.Reader, stdout io.Writer) (bool, error) {
	if _, err := fmt.Fprint(stdout, "Proceed with Reset? [y/N] "); err != nil {
		return false, fmt.Errorf("print Reset confirmation: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	type result struct {
		line string
		err  error
	}
	resultChannel := make(chan result, 1)
	go func() {
		line, err := reader.ReadString('\n')
		resultChannel <- result{line: line, err: err}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case read := <-resultChannel:
		if errors.Is(read.err, io.EOF) {
			return false, nil
		}
		if read.err != nil {
			return false, fmt.Errorf("read Reset confirmation: %w", read.err)
		}
		answer := strings.ToLower(strings.TrimSpace(read.line))
		return answer == "y" || answer == "yes", nil
	}
}

type resetOutputWriter struct {
	writer io.Writer
	err    error
}

func (w *resetOutputWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	written, err := w.writer.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = fmt.Errorf("write Reset output: %w", err)
	}
	return written, err
}

func (w *resetOutputWriter) Err() error {
	return w.err
}

type resetStateStore interface {
	Preview() (state.State, bool, error)
	Save(state.State) error
}

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

func (e resetExecutor) inspect(ctx context.Context) (reset.Plan, error) {
	current, _, err := e.store.Preview()
	if err != nil {
		return reset.Plan{}, err
	}
	run, lease, err := resetRun(current, e.issue)
	if err != nil {
		return reset.Plan{}, err
	}
	if err := inspectWorkerAbsent(run); err != nil {
		return reset.Plan{}, err
	}
	repository, err := e.github.Repository(ctx)
	if err != nil {
		return reset.Plan{}, err
	}
	if current.Repo == "" || current.Repo != repository.Slug {
		return reset.Plan{}, fmt.Errorf("Run state belongs to %q, not repository %q", current.Repo, repository.Slug)
	}
	if current.DefaultBranch == "" || current.DefaultBranch != repository.DefaultBranch {
		return reset.Plan{}, fmt.Errorf("Run state default branch %q does not match repository default branch %q", current.DefaultBranch, repository.DefaultBranch)
	}
	originRepository, err := inspectOriginRepository(ctx, e.gitExecutable, e.repositoryRoot)
	if err != nil {
		return reset.Plan{}, err
	}
	if !strings.EqualFold(originRepository, repository.Slug) {
		return reset.Plan{}, fmt.Errorf("Git origin belongs to %q, not repository %q", originRepository, repository.Slug)
	}
	if err := validateOwnedPaths(run, e.stateDirectory, e.repositoryRoot, repository.DefaultBranch); err != nil {
		return reset.Plan{}, err
	}
	issueResource, pullResources, err := e.github.ResetResources(ctx, repository.Slug, e.issue, run.Branch)
	if err != nil {
		return reset.Plan{}, err
	}
	remoteBranch, err := inspectRemoteBranch(ctx, e.gitExecutable, e.repositoryRoot, run.Branch)
	if err != nil {
		return reset.Plan{}, err
	}
	localBranch, localWorktree, err := inspectLocalResources(ctx, e.gitExecutable, e.repositoryRoot, e.commonDirectory, run)
	if err != nil {
		return reset.Plan{}, err
	}
	session, err := inspectSession(run, e.stateDirectory)
	if err != nil {
		return reset.Plan{}, err
	}
	snapshot := reset.Snapshot{
		Run: run, Lease: lease,
		Issue:        reset.Issue{Number: issueResource.Number, URL: issueResource.URL, Open: issueResource.State == "open", Labels: issueResource.Labels},
		RemoteBranch: remoteBranch, LocalBranch: localBranch, Worktree: localWorktree, Session: session,
		WorkerSummary: absentWorkerSummary(run),
	}
	for _, pull := range pullResources {
		commented := false
		for _, comment := range pull.Comments {
			if comment == resetComment(run) {
				commented = true
				break
			}
		}
		snapshot.PullRequests = append(snapshot.PullRequests, reset.PullRequest{
			Number: pull.Number, URL: pull.URL, Branch: pull.Branch, Commit: pull.Commit,
			State: reset.PullRequestState(pull.State), AutoMergeArmed: pull.AutoMergeArmed, ResetCommented: commented,
		})
	}
	if err := validateInspectedGitHubIdentity(snapshot); err != nil {
		return reset.Plan{}, err
	}
	return reset.Build(snapshot)
}

func validateInspectedGitHubIdentity(snapshot reset.Snapshot) error {
	if !snapshot.RemoteBranch.Present {
		return nil
	}
	for _, pull := range snapshot.PullRequests {
		if pull.State == reset.PullRequestOpen && pull.Commit != snapshot.RemoteBranch.Commit {
			return fmt.Errorf("pull request #%d expected commit %s does not match owned remote branch %s at %s", pull.Number, pull.Commit, snapshot.RemoteBranch.Name, snapshot.RemoteBranch.Commit)
		}
	}
	return nil
}

func validateOwnedGitHubMutation(plan reset.Plan) error {
	if plan.Snapshot.Run.Status == scheduler.StatusReset {
		if err := verifyOwnedFinalState(plan.Snapshot); err != nil {
			return fmt.Errorf("historical reset Run has incomplete final state: %w", err)
		}
	}
	return nil
}

func validateResetMutationStatus(plan reset.Plan) error {
	status := plan.Snapshot.Run.Status
	switch status {
	case scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
		scheduler.StatusFailed, scheduler.StatusSuspended, scheduler.StatusNeedsHuman,
		scheduler.StatusResetting, scheduler.StatusReset:
		return nil
	case scheduler.StatusWaitingForMerge:
		if plan.Snapshot.Run.PullRequest == "" {
			return errors.New("waiting-for-merge Run has no recorded pull request")
		}
		for _, pull := range plan.Snapshot.PullRequests {
			if pull.URL == plan.Snapshot.Run.PullRequest {
				return nil
			}
		}
		return fmt.Errorf("waiting-for-merge Run pull request %s was not freshly verified", plan.Snapshot.Run.PullRequest)
	default:
		return fmt.Errorf("Run status %s is not eligible for Reset", status)
	}
}

func resetCommentMarker(runID string) string {
	return "<!-- backlog-reset:" + runID + " -->"
}

func resetComment(run scheduler.Run) string {
	return fmt.Sprintf("%s\nBacklog is resetting Run %s for issue #%d. This pull request is being closed as part of abandoning the incomplete Run.", resetCommentMarker(run.RunID), run.RunID, run.Issue)
}

func resetPlansEqual(left, right reset.Plan) bool {
	var leftText, rightText bytes.Buffer
	printResetPlan(&leftText, left)
	printResetPlan(&rightText, right)
	return leftText.String() == rightText.String()
}

func (e resetExecutor) apply(ctx context.Context, approved reset.Plan) error {
	plan, err := e.inspect(ctx)
	if err != nil {
		return err
	}
	if err := validateOwnedGitHubMutation(plan); err != nil {
		return err
	}
	if err := validateResetMutationStatus(plan); err != nil {
		return err
	}
	if !resetPlansEqual(approved, plan) {
		return errors.New("Reset Plan changed after confirmation; rerun Reset to review the current plan")
	}

	for {
		plan, err = e.inspect(ctx)
		if err != nil {
			return err
		}
		if err := validateOwnedGitHubMutation(plan); err != nil {
			return err
		}
		if err := validateResetMutationStatus(plan); err != nil {
			return err
		}
		if err := verifyGitHubIdentityContinuity(approved.Snapshot, plan.Snapshot); err != nil {
			return err
		}

		pull, hasOpenPull := reset.NextPullRequestForReset(plan.Snapshot)
		labels := normalizedLabelSet(plan.Snapshot.Issue.Labels)
		needsProgress := hasOpenPull || plan.Snapshot.RemoteBranch.Present || plan.Snapshot.LocalBranch.Present ||
			plan.Snapshot.Worktree.Present || plan.Snapshot.Session.Present || labels["in-progress"] || !labels["ready-for-agent"]
		if plan.Snapshot.Run.Status == scheduler.StatusWaitingForMerge && (!hasOpenPull || !pull.AutoMergeArmed) {
			if err := verifyWaitingForMergeDisarmed(plan); err != nil {
				return err
			}
			if err := e.markResetting(); err != nil {
				return err
			}
			continue
		}
		if needsProgress && plan.Snapshot.Run.Status != scheduler.StatusResetting && plan.Snapshot.Run.Status != scheduler.StatusWaitingForMerge && plan.Snapshot.Run.Status != scheduler.StatusReset {
			if err := e.markResetting(); err != nil {
				return err
			}
			continue
		}

		switch {
		case hasOpenPull && pull.AutoMergeArmed:
			before, err := e.revalidatePlan(ctx, plan, approved, "disabling pull request auto-merge")
			if err != nil {
				return err
			}
			pull, _ = pullRequestByNumber(before.Snapshot.PullRequests, pull.Number)
			repository, err := e.repositorySlug()
			if err != nil {
				return err
			}
			if err := e.github.DisablePullRequestAutoMerge(ctx, repository, pull.Number); err != nil {
				return fmt.Errorf("disable auto-merge for pull request #%d: %w", pull.Number, err)
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if err := verifyGitHubIdentityContinuity(approved.Snapshot, after.Snapshot); err != nil {
				return err
			}
			updated, found := pullRequestByNumber(after.Snapshot.PullRequests, pull.Number)
			if !found || updated.State != reset.PullRequestOpen || updated.AutoMergeArmed {
				return fmt.Errorf("pull request #%d was not freshly verified open, unmerged, and auto-merge unarmed", pull.Number)
			}
			if before.Snapshot.Run.Status == scheduler.StatusWaitingForMerge {
				if err := e.markResetting(); err != nil {
					return err
				}
			}
		case hasOpenPull && !pull.ResetCommented:
			before, err := e.revalidatePlan(ctx, plan, approved, "commenting on pull request")
			if err != nil {
				return err
			}
			pull, _ = pullRequestByNumber(before.Snapshot.PullRequests, pull.Number)
			if pull.AutoMergeArmed {
				return fmt.Errorf("pull request #%d auto-merge was rearmed before the Reset explanation", pull.Number)
			}
			repository, err := e.repositorySlug()
			if err != nil {
				return err
			}
			if err := e.github.CommentOnPullRequest(ctx, repository, pull.Number, resetComment(before.Snapshot.Run)); err != nil {
				return fmt.Errorf("explain Reset on pull request #%d: %w", pull.Number, err)
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if err := verifyGitHubIdentityContinuity(approved.Snapshot, after.Snapshot); err != nil {
				return err
			}
			updated, found := pullRequestByNumber(after.Snapshot.PullRequests, pull.Number)
			if !found || updated.State != reset.PullRequestOpen || updated.AutoMergeArmed || !updated.ResetCommented {
				return fmt.Errorf("pull request #%d Reset explanation did not satisfy its verified postcondition", pull.Number)
			}
		case hasOpenPull:
			before, err := e.revalidatePlan(ctx, plan, approved, "closing pull request")
			if err != nil {
				return err
			}
			pull, _ = pullRequestByNumber(before.Snapshot.PullRequests, pull.Number)
			if pull.AutoMergeArmed || !pull.ResetCommented {
				return fmt.Errorf("pull request #%d is not ready for safe closure", pull.Number)
			}
			repository, err := e.repositorySlug()
			if err != nil {
				return err
			}
			if err := e.github.ClosePullRequest(ctx, repository, pull.Number); err != nil {
				return fmt.Errorf("close unmerged pull request #%d: %w", pull.Number, err)
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if err := verifyGitHubIdentityContinuity(approved.Snapshot, after.Snapshot); err != nil {
				return err
			}
			updated, found := pullRequestByNumber(after.Snapshot.PullRequests, pull.Number)
			if !found || updated.State != reset.PullRequestClosed || updated.AutoMergeArmed {
				return fmt.Errorf("pull request #%d was not freshly verified closed and unmerged with auto-merge unarmed", pull.Number)
			}
		case plan.Snapshot.RemoteBranch.Present:
			before, err := e.revalidatePlan(ctx, plan, approved, "deleting remote branch")
			if err != nil {
				return err
			}
			branch := before.Snapshot.RemoteBranch
			if err := deleteRemoteBranch(ctx, e.gitExecutable, e.repositoryRoot, branch); err != nil {
				return err
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if after.Snapshot.RemoteBranch.Present {
				return fmt.Errorf("owned remote branch %s is still present after deletion", branch.Name)
			}
		case plan.Snapshot.Worktree.Present:
			before, err := e.revalidatePlan(ctx, plan, approved, "removing local worktree")
			if err != nil {
				return err
			}
			worktree := before.Snapshot.Worktree
			if !before.Snapshot.LocalBranch.Present || before.Snapshot.LocalBranch.Name != worktree.Branch || before.Snapshot.LocalBranch.Commit != worktree.Commit {
				return fmt.Errorf("local branch commit and worktree association changed immediately before removing %s", worktree.Path)
			}
			if err := removeLocalWorktree(ctx, e.gitExecutable, e.repositoryRoot, worktree); err != nil {
				return err
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if after.Snapshot.Worktree.Present {
				return fmt.Errorf("owned local worktree %s is still present after removal", worktree.Path)
			}
		case plan.Snapshot.LocalBranch.Present:
			before, err := e.revalidatePlan(ctx, plan, approved, "deleting local branch")
			if err != nil {
				return err
			}
			branch := before.Snapshot.LocalBranch
			if before.Snapshot.Worktree.Present {
				return fmt.Errorf("owned local branch %s remains assigned to worktree %s", branch.Name, before.Snapshot.Worktree.Path)
			}
			if err := deleteLocalBranch(ctx, e.gitExecutable, e.repositoryRoot, branch); err != nil {
				return err
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if after.Snapshot.LocalBranch.Present {
				return fmt.Errorf("owned local branch %s is still present after deletion", branch.Name)
			}
		case plan.Snapshot.Session.Present:
			before, err := e.revalidatePlan(ctx, plan, approved, "archiving Pi session")
			if err != nil {
				return err
			}
			session := before.Snapshot.Session
			if err := archiveSession(session, e.stateDirectory, e.filesystemSync); err != nil {
				return err
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if after.Snapshot.Session.Present || !after.Snapshot.Session.Archived {
				return fmt.Errorf("Pi session %s was not verified in its non-resumable historical archive", session.ID)
			}
		case labels["in-progress"]:
			before, err := e.revalidatePlan(ctx, plan, approved, "removing issue label in-progress")
			if err != nil {
				return err
			}
			repository, err := e.repositorySlug()
			if err != nil {
				return err
			}
			if err := e.github.RemoveIssueLabel(ctx, repository, e.issue, "in-progress"); err != nil {
				return fmt.Errorf("remove issue label in-progress: %w", err)
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if err := verifyLabelMutation(before.Snapshot.Issue.Labels, after.Snapshot.Issue.Labels, "", "in-progress"); err != nil {
				return err
			}
		case !labels["ready-for-agent"]:
			before, err := e.revalidatePlan(ctx, plan, approved, "adding issue label ready-for-agent")
			if err != nil {
				return err
			}
			repository, err := e.repositorySlug()
			if err != nil {
				return err
			}
			if err := e.github.AddIssueLabel(ctx, repository, e.issue, "ready-for-agent"); err != nil {
				return fmt.Errorf("add issue label ready-for-agent: %w", err)
			}
			after, err := e.inspect(ctx)
			if err != nil {
				return err
			}
			if err := verifyLabelMutation(before.Snapshot.Issue.Labels, after.Snapshot.Issue.Labels, "ready-for-agent", ""); err != nil {
				return err
			}
		default:
			return e.finalize(ctx, plan)
		}
	}
}

func (e resetExecutor) revalidatePlan(ctx context.Context, current, approved reset.Plan, action string) (reset.Plan, error) {
	fresh, err := e.inspect(ctx)
	if err != nil {
		return reset.Plan{}, err
	}
	if err := validateOwnedGitHubMutation(fresh); err != nil {
		return reset.Plan{}, err
	}
	if err := validateResetMutationStatus(fresh); err != nil {
		return reset.Plan{}, err
	}
	if err := verifyGitHubIdentityContinuity(approved.Snapshot, fresh.Snapshot); err != nil {
		return reset.Plan{}, err
	}
	if !resetPlansEqual(current, fresh) {
		return reset.Plan{}, fmt.Errorf("Reset Plan changed immediately before %s", action)
	}
	return fresh, nil
}

func verifyGitHubIdentityContinuity(expected, actual reset.Snapshot) error {
	if expected.Run.RunID != actual.Run.RunID || expected.Lease != actual.Lease {
		return fmt.Errorf("Run or Lease identity changed while resetting Run artifacts")
	}
	expectedPulls := make(map[int]reset.PullRequest, len(expected.PullRequests))
	for _, pull := range expected.PullRequests {
		expectedPulls[pull.Number] = pull
	}
	if len(expectedPulls) != len(actual.PullRequests) {
		return errors.New("pull request identity set changed while resetting GitHub artifacts")
	}
	for _, pull := range actual.PullRequests {
		owned, found := expectedPulls[pull.Number]
		if !found || pull.URL != owned.URL || pull.Branch != owned.Branch || pull.Commit != owned.Commit {
			return fmt.Errorf("pull request #%d branch or expected commit identity changed while resetting", pull.Number)
		}
	}
	if err := verifyBranchIdentityContinuity("remote", expected.RemoteBranch, actual.RemoteBranch, expected.Run.RunID); err != nil {
		return err
	}
	if err := verifyBranchIdentityContinuity("local", expected.LocalBranch, actual.LocalBranch, expected.Run.RunID); err != nil {
		return err
	}
	if actual.Worktree.Present {
		if !expected.Worktree.Present || actual.Worktree != expected.Worktree {
			return fmt.Errorf("owned local worktree identity changed while resetting Run %s", expected.Run.RunID)
		}
	} else if actual.Worktree.Path != expected.Worktree.Path || actual.Worktree.Branch != expected.Worktree.Branch {
		return fmt.Errorf("owned local worktree assignment changed while resetting Run %s", expected.Run.RunID)
	}
	if actual.Session.Present {
		if !expected.Session.Present || actual.Session.ID != expected.Session.ID || actual.Session.Dir != expected.Session.Dir || actual.Session.ArchiveDir != expected.Session.ArchiveDir {
			return fmt.Errorf("active Pi session identity changed while resetting Run %s", expected.Run.RunID)
		}
	}
	if actual.Session.Archived {
		if (!expected.Session.Present && !expected.Session.Archived) || actual.Session.ID != expected.Session.ID || actual.Session.Dir != expected.Session.Dir || actual.Session.ArchiveDir != expected.Session.ArchiveDir {
			return fmt.Errorf("historical Pi session identity changed while resetting Run %s", expected.Run.RunID)
		}
	} else if expected.Session.Archived || expected.Session.Present && !actual.Session.Present {
		return fmt.Errorf("Pi session archive disappeared while resetting Run %s", expected.Run.RunID)
	}
	return nil
}

func verifyBranchIdentityContinuity(location string, expected, actual reset.Branch, runID string) error {
	if actual.Present {
		if !expected.Present || actual.Name != expected.Name || actual.Commit != expected.Commit {
			return fmt.Errorf("owned %s branch identity changed while resetting Run %s", location, runID)
		}
	} else if actual.Name != expected.Name {
		return fmt.Errorf("owned %s branch name changed while resetting Run %s", location, runID)
	}
	return nil
}

func pullRequestByNumber(pulls []reset.PullRequest, number int) (reset.PullRequest, bool) {
	for _, pull := range pulls {
		if pull.Number == number {
			return pull, true
		}
	}
	return reset.PullRequest{}, false
}

func verifyWaitingForMergeDisarmed(plan reset.Plan) error {
	pull, found := pullRequestByURL(plan.Snapshot.PullRequests, plan.Snapshot.Run.PullRequest)
	if !found || pull.State == reset.PullRequestMerged || pull.AutoMergeArmed {
		return errors.New("waiting-for-merge Run was not freshly verified unmerged with auto-merge unarmed")
	}
	return nil
}

func pullRequestByURL(pulls []reset.PullRequest, target string) (reset.PullRequest, bool) {
	for _, pull := range pulls {
		if pull.URL == target {
			return pull, true
		}
	}
	return reset.PullRequest{}, false
}

func deleteRemoteBranch(ctx context.Context, gitExecutable, repositoryRoot string, branch reset.Branch) error {
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

func removeLocalWorktree(ctx context.Context, gitExecutable, repositoryRoot string, worktree reset.Worktree) error {
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

func deleteLocalBranch(ctx context.Context, gitExecutable, repositoryRoot string, branch reset.Branch) error {
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

func archiveSession(session reset.Session, stateDirectory string, syncPath func(string) error) error {
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
	if err := os.Rename(session.Dir, session.ArchiveDir); err != nil {
		return fmt.Errorf("atomically archive Pi session %s: %w", session.ID, err)
	}
	return syncArchivedSession(session, stateDirectory, syncPath)
}

func syncArchivedSession(session reset.Session, stateDirectory string, syncPath func(string) error) error {
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

func (e resetExecutor) filesystemSync(path string) error {
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
		result[strings.ToLower(label)] = true
	}
	return result
}

func verifyLabelMutation(before, after []string, added, removed string) error {
	expected := normalizedLabelSet(before)
	if added != "" {
		expected[strings.ToLower(added)] = true
	}
	if removed != "" {
		delete(expected, strings.ToLower(removed))
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

func (e resetExecutor) repositorySlug() (string, error) {
	current, _, err := e.store.Preview()
	if err != nil {
		return "", err
	}
	if current.Repo == "" {
		return "", errors.New("Run state has no repository identity")
	}
	return current.Repo, nil
}

func (e resetExecutor) markResetting() error {
	current, _, err := e.store.Preview()
	if err != nil {
		return err
	}
	run, lease, err := resetRun(current, e.issue)
	if err != nil {
		return err
	}
	if run.Status == scheduler.StatusReset || run.Status == scheduler.StatusResetting {
		return nil
	}
	if !scheduler.CanTransition(run.Status, scheduler.StatusResetting) {
		return fmt.Errorf("Run status %s cannot transition to resetting", run.Status)
	}
	for index := range current.Runs {
		if current.Runs[index].RunID == run.RunID {
			current.Runs[index].Status = scheduler.StatusResetting
			current.Runs[index].UpdatedAt = time.Now().UTC()
			break
		}
	}
	if lease.LeaseID == "" {
		return fmt.Errorf("Run %s has no active Lease while entering resetting", run.RunID)
	}
	return e.store.Save(current)
}

func (e resetExecutor) finalize(ctx context.Context, verified reset.Plan) error {
	fresh, err := e.inspect(ctx)
	if err != nil {
		return err
	}
	if !resetPlansEqual(verified, fresh) {
		return errors.New("Reset Plan changed immediately before finalization")
	}
	verified = fresh
	labels := normalizedLabelSet(verified.Snapshot.Issue.Labels)
	if labels["in-progress"] || !labels["ready-for-agent"] {
		return errors.New("managed issue label postconditions were not verified")
	}
	if err := validateOwnedGitHubMutation(verified); err != nil {
		return err
	}
	if err := verifyOwnedFinalState(verified.Snapshot); err != nil {
		return err
	}
	if verified.Snapshot.Session.Archived {
		if err := syncArchivedSession(verified.Snapshot.Session, e.stateDirectory, e.filesystemSync); err != nil {
			return fmt.Errorf("verify durable Pi session archive: %w", err)
		}
	}
	current, _, err := e.store.Preview()
	if err != nil {
		return err
	}
	run, lease, err := resetRun(current, e.issue)
	if err != nil {
		return err
	}
	if run.RunID != verified.Snapshot.Run.RunID {
		return fmt.Errorf("active Run changed from %s to %s before finalization", verified.Snapshot.Run.RunID, run.RunID)
	}
	if run.Status == scheduler.StatusReset {
		return verifyResetFinalState(current, run)
	}
	if lease != verified.Snapshot.Lease {
		return fmt.Errorf("Lease for Run %s changed before finalization", run.RunID)
	}
	if !scheduler.CanTransition(run.Status, scheduler.StatusReset) {
		return fmt.Errorf("Run status %s cannot transition to reset", run.Status)
	}
	now := time.Now().UTC()
	for index := range current.Runs {
		if current.Runs[index].RunID == run.RunID {
			current.Runs[index].Status = scheduler.StatusReset
			current.Runs[index].WorkerLogOpen = false
			current.Runs[index].UpdatedAt = now
			current.Runs[index].CompletedAt = &now
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
		return fmt.Errorf("atomically mark Run reset and release Lease: %w", err)
	}
	persisted, _, err := e.store.Preview()
	if err != nil {
		return fmt.Errorf("verify finalized Reset state: %w", err)
	}
	return verifyResetFinalState(persisted, run)
}

func verifyOwnedFinalState(snapshot reset.Snapshot) error {
	for _, pull := range snapshot.PullRequests {
		if pull.State != reset.PullRequestClosed || pull.AutoMergeArmed {
			return fmt.Errorf("pull request #%d final state is not verified closed, unmerged, and auto-merge unarmed", pull.Number)
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
	return verifyDurableRunLogs(snapshot.Run)
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

func verifyResetFinalState(current state.State, expected scheduler.Run) error {
	if err := verifyDurableRunLogs(expected); err != nil {
		return err
	}
	found := false
	for _, run := range current.Runs {
		if run.RunID == expected.RunID {
			found = true
			if run.Status != scheduler.StatusReset {
				return fmt.Errorf("Run %s final status is %s, not reset", expected.RunID, run.Status)
			}
			metadata := run
			metadata.Status = expected.Status
			metadata.WorkerLogOpen = expected.WorkerLogOpen
			metadata.UpdatedAt = expected.UpdatedAt
			metadata.CompletedAt = expected.CompletedAt
			if !reflect.DeepEqual(metadata, expected) {
				return fmt.Errorf("historical metadata for Run %s changed during finalization", expected.RunID)
			}
		}
	}
	if !found {
		return fmt.Errorf("historical Run %s is absent after Reset", expected.RunID)
	}
	for _, lease := range current.Leases {
		if lease.RunID == expected.RunID {
			return fmt.Errorf("old Lease %s for reset Run %s is still active", lease.LeaseID, expected.RunID)
		}
	}
	return nil
}

func splitResetArguments(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("usage: backlog reset <issue-number> [flags]")
	}
	if !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:], nil
	}
	for index, value := range args {
		if !strings.HasPrefix(value, "-") && (index == 0 || !resetFlagTakesValue(args[index-1])) {
			remaining := append([]string{}, args[:index]...)
			remaining = append(remaining, args[index+1:]...)
			return value, remaining, nil
		}
	}
	return "", nil, errors.New("reset requires an issue number")
}

func resetFlagTakesValue(name string) bool {
	if strings.Contains(name, "=") {
		return false
	}
	name = strings.TrimLeft(name, "-")
	return name == "repo-dir" || name == "state-dir" || name == "git" || name == "gh"
}

type resetReadLock struct {
	locks []*state.Lock
}

func acquireResetReadLock(commonDirectory string) (*resetReadLock, error) {
	coordination, err := state.AcquireReadOnlyLock(commonDirectory)
	if err != nil {
		return nil, err
	}
	result := &resetReadLock{locks: []*state.Lock{coordination}}
	for _, name := range []string{legacyLockFile, lockFile} {
		lock, exists, err := state.AcquireExistingReadOnlyLock(filepath.Join(commonDirectory, name))
		if err != nil {
			_ = result.Release()
			return nil, err
		}
		if exists {
			result.locks = append(result.locks, lock)
		}
	}
	return result, nil
}

func (l *resetReadLock) Release() error {
	var result error
	for index := len(l.locks) - 1; index >= 0; index-- {
		result = errors.Join(result, l.locks[index].Release())
	}
	return result
}

func resetRun(current state.State, issue int) (scheduler.Run, scheduler.Lease, error) {
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

func inspectWorkerAbsent(run scheduler.Run) error {
	if run.ResumePending {
		return errors.New("replacement Worker launch is pending; Worker absence is uncertain")
	}
	pid := run.PID
	if pid == 0 {
		if run.ProcessIdentity == "" {
			return nil
		}
		var err error
		pid, err = processIdentityPID(run.ProcessIdentity)
		if err != nil {
			return fmt.Errorf("Run %s has uncertain retained Worker identity: %w", run.RunID, err)
		}
	}
	if pid < 0 || run.ProcessIdentity == "" {
		return fmt.Errorf("Run %s has incomplete Worker identity", run.RunID)
	}
	processAlive, err := signalZero(pid)
	if err != nil {
		return fmt.Errorf("verify Worker PID %d: %w", pid, err)
	}
	groupAlive, err := signalZero(-pid)
	if err != nil {
		return fmt.Errorf("verify Worker process group %d: %w", pid, err)
	}
	if !processAlive && !groupAlive {
		return nil
	}
	if !processAlive || !groupAlive {
		return fmt.Errorf("Worker PID/process-group liveness is uncertain for Run %s", run.RunID)
	}
	identity, err := resetPIDIdentity(pid)
	if err != nil {
		return fmt.Errorf("verify live Worker identity: %w", err)
	}
	if identity != run.ProcessIdentity {
		return fmt.Errorf("Worker PID %d is live with uncertain identity %q instead of %q", pid, identity, run.ProcessIdentity)
	}
	return fmt.Errorf("Worker for Run %s is live at PID %d", run.RunID, pid)
}

func processIdentityPID(identity string) (int, error) {
	value, started, found := strings.Cut(identity, ":")
	pid, err := strconv.Atoi(value)
	if !found || err != nil || pid <= 0 || strings.TrimSpace(started) == "" {
		return 0, errors.New("invalid process identity")
	}
	return pid, nil
}

func absentWorkerSummary(run scheduler.Run) string {
	pid := run.PID
	if pid == 0 && run.ProcessIdentity != "" {
		pid, _ = processIdentityPID(run.ProcessIdentity)
	}
	if pid == 0 {
		return "absent (no recorded PID)"
	}
	return fmt.Sprintf("absent (recorded PID and process group %d)", pid)
}

func signalZero(pid int) (bool, error) {
	err := syscall.Kill(pid, syscall.Signal(0))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	case errors.Is(err, syscall.EPERM):
		return false, errors.New("permission denied; liveness is unknown")
	default:
		return false, err
	}
}

func resetPIDIdentity(pid int) (string, error) {
	command := exec.Command("ps", "-p", fmt.Sprint(pid), "-o", "lstart=") // #nosec G204 -- validated numeric PID
	output, err := command.CombinedOutput()
	if err != nil {
		return "", err
	}
	started := strings.TrimSpace(string(output))
	if started == "" {
		return "", errors.New("empty process start identity")
	}
	return fmt.Sprintf("%d:%s", pid, started), nil
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

func inspectRemoteBranch(ctx context.Context, gitExecutable, repositoryRoot, branch string) (reset.Branch, error) {
	if branch == "" {
		return reset.Branch{}, nil
	}
	ref := "refs/heads/" + branch
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "ls-remote", "--exit-code", "--heads", "origin", ref)
	if err != nil {
		return reset.Branch{}, fmt.Errorf("inspect remote branch %s: %w", branch, err)
	}
	if exit == 2 && len(bytes.TrimSpace(output)) == 0 {
		return reset.Branch{Name: branch}, nil
	}
	if exit != 0 {
		return reset.Branch{}, fmt.Errorf("inspect remote branch %s: git exited %d; state is unknown", branch, exit)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 2 || fields[1] != ref || !validObjectID(fields[0]) {
		return reset.Branch{}, fmt.Errorf("inspect remote branch %s: unknown ls-remote output", branch)
	}
	return reset.Branch{Name: branch, Commit: fields[0], Present: true}, nil
}

func inspectLocalResources(ctx context.Context, gitExecutable, repositoryRoot, commonDirectory string, run scheduler.Run) (reset.Branch, reset.Worktree, error) {
	if run.Branch == "" {
		return reset.Branch{}, reset.Worktree{}, nil
	}
	ref := "refs/heads/" + run.Branch
	output, exit, err := runGitInspection(ctx, gitExecutable, repositoryRoot, "for-each-ref", "--format=%(objectname)", ref)
	if err != nil {
		return reset.Branch{}, reset.Worktree{}, fmt.Errorf("inspect local branch %s: %w", run.Branch, err)
	}
	if exit != 0 {
		return reset.Branch{}, reset.Worktree{}, fmt.Errorf("inspect local branch %s: git exited %d with unknown output", run.Branch, exit)
	}
	local := reset.Branch{Name: run.Branch}
	commit := strings.TrimSpace(string(output))
	if commit != "" {
		if strings.Contains(commit, "\n") || !validObjectID(commit) {
			return reset.Branch{}, reset.Worktree{}, fmt.Errorf("inspect local branch %s: unknown object identity", run.Branch)
		}
		local.Commit, local.Present = commit, true
	}

	output, exit, err = runGitInspection(ctx, gitExecutable, repositoryRoot, "worktree", "list", "--porcelain", "-z")
	if err != nil || exit != 0 {
		if err == nil {
			err = fmt.Errorf("git exited %d", exit)
		}
		return reset.Branch{}, reset.Worktree{}, fmt.Errorf("inspect local worktrees: %w", err)
	}
	entries, err := parseWorktrees(output)
	if err != nil {
		return reset.Branch{}, reset.Worktree{}, err
	}
	expectedPath := canonicalPath(run.Worktree)
	var registered *gitWorktree
	for index := range entries {
		entry := &entries[index]
		if entry.Branch == ref && canonicalPath(entry.Path) != expectedPath {
			return reset.Branch{}, reset.Worktree{}, fmt.Errorf("Run branch %s is assigned to unexpected worktree %s", run.Branch, entry.Path)
		}
		if canonicalPath(entry.Path) == expectedPath {
			if registered != nil {
				return reset.Branch{}, reset.Worktree{}, fmt.Errorf("Run worktree %s has duplicate Git registrations", run.Worktree)
			}
			registered = entry
		}
	}
	info, statErr := os.Lstat(run.Worktree)
	filesystemPresent := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return reset.Branch{}, reset.Worktree{}, fmt.Errorf("inspect worktree path %s: %w", run.Worktree, statErr)
	}
	if filesystemPresent && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return reset.Branch{}, reset.Worktree{}, fmt.Errorf("worktree path %s has unknown filesystem identity", run.Worktree)
	}
	if registered == nil {
		if filesystemPresent {
			return reset.Branch{}, reset.Worktree{}, fmt.Errorf("worktree path %s exists without the expected Git registration", run.Worktree)
		}
		return local, reset.Worktree{Path: run.Worktree, Branch: run.Branch}, nil
	}
	if registered.Branch != ref || !validObjectID(registered.Commit) {
		return reset.Branch{}, reset.Worktree{}, fmt.Errorf("worktree %s has unknown branch or commit identity", run.Worktree)
	}
	if !local.Present || local.Commit != registered.Commit {
		return reset.Branch{}, reset.Worktree{}, fmt.Errorf("worktree %s commit does not match owned local branch", run.Worktree)
	}
	if err := verifyRegisteredWorktree(ctx, gitExecutable, run, commonDirectory, registered.Commit); err != nil {
		return reset.Branch{}, reset.Worktree{}, err
	}
	return local, reset.Worktree{Path: run.Worktree, Branch: run.Branch, Commit: registered.Commit, Present: true}, nil
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

func inspectSession(run scheduler.Run, stateDirectory string) (reset.Session, error) {
	if run.WorkerMode != scheduler.WorkerModeRPC {
		return reset.Session{}, nil
	}
	result := reset.Session{
		ID: run.SessionID, Dir: run.SessionDir,
		ArchiveDir: filepath.Join(stateDirectory, "history", "sessions", run.RunID),
	}
	activeFiles, active, err := inspectSessionDirectory(run.SessionDir, run)
	if err != nil {
		return reset.Session{}, err
	}
	_, archived, err := inspectSessionDirectory(result.ArchiveDir, run)
	if err != nil {
		return reset.Session{}, fmt.Errorf("inspect historical Pi session archive: %w", err)
	}
	if active && archived {
		return reset.Session{}, fmt.Errorf("Pi session %s exists in both active and historical storage", run.SessionID)
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
			return reset.Session{}, fmt.Errorf("Pi continuation file %s is not present in the owned session", run.Continuation.SessionFile)
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

func printResetPlan(writer io.Writer, plan reset.Plan) {
	snapshot := plan.Snapshot
	fmt.Fprintf(writer, "Reset Plan for issue #%d\n", snapshot.Run.Issue)
	fmt.Fprintf(writer, "Run: %s (%s)\n", snapshot.Run.RunID, snapshot.Run.Status)
	if snapshot.Lease.LeaseID == "" {
		fmt.Fprintln(writer, "Lease: absent (Run already reset)")
	} else {
		fmt.Fprintf(writer, "Lease: %s\n", snapshot.Lease.LeaseID)
	}
	fmt.Fprintf(writer, "Issue: %s (open; labels: %s)\n", snapshot.Issue.URL, formatLabels(snapshot.Issue.Labels))
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
		fmt.Fprintf(writer, "  %d. %s\n", index+1, action)
	}
}

func printBranchResource(writer io.Writer, name string, branch reset.Branch) {
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
