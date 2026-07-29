package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/processidentity"
	"github.com/robinjoseph08/backlog/internal/recovery"
	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

func recoverCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return recoverCommandWithInput(ctx, args, os.Stdin, inputIsInteractive(os.Stdin), stdout, stderr)
}

func recoverCommandWithInput(ctx context.Context, args []string, stdin io.Reader, interactive bool, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("recover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: backlog recover <run-id|positive-issue-number> [flags]")
		fmt.Fprintln(stderr, "")
		fmt.Fprintln(stderr, "Recovery verifies an Intervention-required Run's retained Lease, absent Worker,")
		fmt.Fprintln(stderr, "branch, worktree, Pi session, durable leaf/hash, workflow checkpoint, issue labels,")
		fmt.Fprintln(stderr, "and expected-branch pull request state. It then establishes the same Run as")
		fmt.Fprintln(stderr, "Suspended so normal Resume can launch a replacement Worker after fresh checks.")
		fmt.Fprintln(stderr, "Dry-run is read-only. Interactive confirmation defaults to no.")
		fmt.Fprintln(stderr, "Non-interactive mutation requires --yes.")
		fmt.Fprintln(stderr, "")
		flags.PrintDefaults()
	}
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the Run")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable")
	ghExecutable := flags.String("gh", "gh", "gh executable")
	dryRun := flags.Bool("dry-run", false, "print the current Recovery Plan without mutation")
	yes := flags.Bool("yes", false, "confirm Recovery without an interactive prompt")
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return flags.Parse([]string{arg})
		}
	}
	selector, flagArgs, err := splitRecoverArguments(args)
	if err != nil {
		return err
	}
	if err := flags.Parse(flagArgs); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected recover arguments: %s", strings.Join(flags.Args(), " "))
	}
	if !*dryRun && !*yes && !interactive {
		return errors.New("non-interactive Recovery requires --yes")
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
		return fmt.Errorf("Recovery requires a conclusively absent Runner and Worker: %w", err)
	}
	defer func() { _ = lock.Release() }()
	resolvedState, err := repositoryStateDirectory(commonDirectory, repositoryRoot, *stateDir)
	if err != nil {
		return err
	}
	fileStore := state.FileStore{Path: filepath.Join(resolvedState, "state.json")}
	preview, _, err := fileStore.Preview()
	if err != nil {
		return err
	}
	if preview.Repo == "" || preview.DefaultBranch == "" {
		return errors.New("runner state has no repository identity")
	}
	var recoveryStore recovery.Store = fileStore
	if *dryRun {
		recoveryStore = readOnlyRecoveryStore{store: fileStore}
	}
	module, err := recovery.New(recovery.Config{
		Store:             recoveryStore,
		GitHub:            ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot},
		Worktrees:         &worktree.Manager{GitExecutable: *gitExecutable, RepositoryDir: repositoryRoot, WorktreesDir: filepath.Join(resolvedState, "worktrees"), DefaultBranch: preview.DefaultBranch},
		Git:               recoveryGitVerifier{executable: *gitExecutable, repositoryRoot: repositoryRoot},
		Repo:              preview.Repo,
		ProcessAlive:      processidentity.Alive,
		ProcessGroupAlive: func(pid int) (bool, error) { return processidentity.Alive(-pid) },
	})
	if err != nil {
		return err
	}
	plan, err := module.Inspect(ctx, selector)
	if err != nil {
		return err
	}
	if err := recovery.WritePlan(stdout, plan); err != nil {
		return fmt.Errorf("write Recovery Plan: %w", err)
	}
	if *dryRun {
		_, err := fmt.Fprintln(stdout, "Dry-run: no changes made.")
		return err
	}

	if *yes {
		fresh, err := module.Inspect(ctx, plan.Run.RunID)
		if err != nil {
			return err
		}
		if !recovery.PlansEqual(plan, fresh) {
			fmt.Fprintln(stdout, "Recovery Plan changed after confirmation; using the current verified plan:")
			if err := recovery.WritePlan(stdout, fresh); err != nil {
				return err
			}
		}
		plan = fresh
	} else {
		reader := bufio.NewReader(stdin)
		for {
			confirmed, err := confirmRecovery(ctx, reader, stdout)
			if err != nil {
				return err
			}
			if !confirmed {
				_, err := fmt.Fprintln(stdout, "Recovery cancelled; no changes made.")
				return err
			}
			fresh, err := module.Inspect(ctx, plan.Run.RunID)
			if err != nil {
				return err
			}
			if recovery.PlansEqual(plan, fresh) {
				plan = fresh
				break
			}
			fmt.Fprintln(stdout, "Recovery Plan changed after confirmation; confirm the current plan again:")
			if err := recovery.WritePlan(stdout, fresh); err != nil {
				return err
			}
			plan = fresh
		}
	}
	if err := ensureResetStateBinding(commonDirectory, resolvedState); err != nil {
		return err
	}
	if plan.Outcome == recovery.OutcomeCompletion {
		return retireRecoveredCompletion(ctx, stdout, fileStore, ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot}, repositoryRoot, commonDirectory, resolvedState, *gitExecutable, plan.Run.RunID)
	}
	result, err := module.Recover(ctx, plan)
	if err != nil {
		return err
	}
	switch result.Outcome {
	case recovery.OutcomeAlready:
		fmt.Fprintf(stdout, "Recovery complete: Run %s is Suspended with its existing Lease and verified continuation boundary.\n", result.Run.RunID)
	case recovery.OutcomeWaiting:
		fmt.Fprintf(stdout, "Recovery reconciled Run %s as waiting for merge at %s; no replacement Worker will launch.\n", result.Run.RunID, result.PullRequest)
	case recovery.OutcomeCompletion:
		return retireRecoveredCompletion(ctx, stdout, fileStore, ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot}, repositoryRoot, commonDirectory, resolvedState, *gitExecutable, result.Run.RunID)
	}
	return nil
}

func retireRecoveredCompletion(ctx context.Context, stdout io.Writer, store state.FileStore, github ghadapter.Client, repositoryRoot, commonDirectory, stateDirectory, gitExecutable, runID string) error {
	retire, err := retirement.New(retirement.Config{
		Store: store, GitHub: github, RepositoryRoot: repositoryRoot, CommonDirectory: commonDirectory,
		StateDirectory: stateDirectory, GitExecutable: gitExecutable,
	}, recoveredCompletionPolicy(runID))
	if err != nil {
		return err
	}
	plan, err := retire.Inspect(ctx)
	if err != nil {
		return err
	}
	if err := retire.Validate(plan); err != nil {
		return err
	}
	if err := retire.Retire(ctx, plan); err != nil {
		return err
	}
	return writeResolveOutcome(stdout, store, runID)
}

func recoveredCompletionPolicy(runID string) retirement.Policy {
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
			if snapshot.Issue.Open || len(snapshot.PullRequests) != 1 || snapshot.PullRequests[0].State != retirement.PullRequestMerged || snapshot.Run.PullRequest != "" && snapshot.PullRequests[0].URL != snapshot.Run.PullRequest {
				return errors.New("Recovered Completion requires one merged expected pull request and a closed issue")
			}
			pullCommit := snapshot.PullRequests[0].Commit
			if snapshot.RemoteBranch.Present && snapshot.RemoteBranch.Commit != pullCommit || snapshot.LocalBranch.Present && snapshot.LocalBranch.Commit != pullCommit || snapshot.Worktree.Present && snapshot.Worktree.Commit != pullCommit {
				return errors.New("Recovered Completion artifact commit identity does not match the merged pull request head")
			}
			if snapshot.Run.Status == scheduler.StatusMerged && (snapshot.RemoteBranch.Present || snapshot.LocalBranch.Present || snapshot.Worktree.Present || snapshot.Session.Present) {
				return errors.New("historical Recovered Completion still has active owned artifacts")
			}
			return nil
		},
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

type readOnlyRecoveryStore struct{ store state.FileStore }

func (s readOnlyRecoveryStore) Load() (state.State, error) {
	value, _, err := s.store.Preview()
	return value, err
}
func (readOnlyRecoveryStore) Save(state.State) error {
	return errors.New("read-only Recovery inspection attempted mutation")
}

type recoveryGitVerifier struct {
	executable     string
	repositoryRoot string
}

func (v recoveryGitVerifier) Verify(ctx context.Context, run scheduler.Run) (recovery.GitIdentity, error) {
	local, err := commandOutput(ctx, v.executable, "-C", run.Worktree, "rev-parse", "HEAD")
	if err != nil {
		return recovery.GitIdentity{}, err
	}
	remoteOutput, err := commandOutput(ctx, v.executable, "-C", v.repositoryRoot, "ls-remote", "--heads", "origin", "refs/heads/"+run.Branch)
	if err != nil {
		return recovery.GitIdentity{}, err
	}
	identity := recovery.GitIdentity{LocalCommit: local}
	if remoteOutput != "" {
		fields := strings.Fields(remoteOutput)
		if len(fields) != 2 || fields[1] != "refs/heads/"+run.Branch {
			return recovery.GitIdentity{}, errors.New("remote branch lookup returned malformed or ambiguous identity")
		}
		identity.RemotePresent = true
		identity.RemoteCommit = fields[0]
	}
	return identity, nil
}

func commandOutput(ctx context.Context, executable string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, executable, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		message := strings.TrimSpace(string(output))
		if message != "" {
			return "", errors.New(message)
		}
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func confirmRecovery(ctx context.Context, reader *bufio.Reader, output io.Writer) (bool, error) {
	if _, err := fmt.Fprint(output, "Proceed with Recovery? [y/N] "); err != nil {
		return false, err
	}
	return readConfirmation(ctx, reader, "Recovery")
}

func splitRecoverArguments(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("usage: backlog recover <run-id|positive-issue-number> [flags]")
	}
	if !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:], nil
	}
	for index, value := range args {
		if !strings.HasPrefix(value, "-") && (index == 0 || !recoverFlagTakesValue(args[index-1])) {
			remaining := append([]string{}, args[:index]...)
			remaining = append(remaining, args[index+1:]...)
			return value, remaining, nil
		}
	}
	return "", nil, errors.New("recover requires a Run ID or positive issue number")
}

func recoverFlagTakesValue(name string) bool {
	if strings.Contains(name, "=") {
		return false
	}
	name = strings.TrimLeft(name, "-")
	return name == "repo-dir" || name == "state-dir" || name == "git" || name == "gh"
}
