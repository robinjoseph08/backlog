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
	"github.com/robinjoseph08/backlog/internal/resolution"
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
		fmt.Fprintln(stderr, "Recovery first verifies an Intervention-required Run's retained Lease, absent Worker,")
		fmt.Fprintln(stderr, "issue, and expected-branch pull request state. Verified Completion and armed auto-merge")
		fmt.Fprintln(stderr, "outcomes are considered before suspension. Recovered Completion still requires")
		fmt.Fprintln(stderr, "one merged expected pull request, a closed issue, and matching artifact commits.")
		fmt.Fprintln(stderr, "A Run that can Resume must also pass branch, worktree, Pi session,")
		fmt.Fprintln(stderr, "durable leaf/hash, and workflow checkpoint checks before becoming Suspended.")
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
	githubClient := ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot}
	newCompletionRetire := func(runID string) (retirement.Module, error) {
		return retirement.New(retirement.Config{
			Store: fileStore, GitHub: githubClient, RepositoryRoot: repositoryRoot, CommonDirectory: commonDirectory,
			StateDirectory: resolvedState, GitExecutable: *gitExecutable,
		}, resolution.RecoveredCompletionPolicy(runID))
	}
	inspectPlan := func(target string) (recovery.Plan, *retirement.Plan, error) {
		plan, inspectErr := module.Inspect(ctx, target)
		if inspectErr != nil {
			return recovery.Plan{}, nil, inspectErr
		}
		if plan.Outcome != recovery.OutcomeCompletion {
			return plan, nil, nil
		}
		retire, inspectErr := newCompletionRetire(plan.Run.RunID)
		if inspectErr != nil {
			return recovery.Plan{}, nil, inspectErr
		}
		retirementPlan, inspectErr := retire.Inspect(ctx)
		if inspectErr != nil {
			return recovery.Plan{}, nil, inspectErr
		}
		if inspectErr = retire.Validate(retirementPlan); inspectErr != nil {
			return recovery.Plan{}, nil, inspectErr
		}
		return plan, &retirementPlan, nil
	}
	writePlan := func(plan recovery.Plan, retirementPlan *retirement.Plan) error {
		if writeErr := recovery.WritePlan(stdout, plan); writeErr != nil {
			return fmt.Errorf("write Recovery Plan: %w", writeErr)
		}
		if retirementPlan != nil {
			retirement.WritePlan(stdout, *retirementPlan)
		}
		return nil
	}
	plansEqual := func(left recovery.Plan, leftRetirement *retirement.Plan, right recovery.Plan, rightRetirement *retirement.Plan) bool {
		if !recovery.PlansEqual(left, right) || (leftRetirement == nil) != (rightRetirement == nil) {
			return false
		}
		return leftRetirement == nil || retirement.PlansEqual(*leftRetirement, *rightRetirement)
	}

	plan, retirementPlan, err := inspectPlan(selector)
	if err != nil {
		return err
	}
	if err := writePlan(plan, retirementPlan); err != nil {
		return err
	}
	if *dryRun {
		_, err := fmt.Fprintln(stdout, "Dry-run: no changes made.")
		return err
	}

	reader := bufio.NewReader(stdin)
	confirmCurrent := func(changedMessage string) error {
		for {
			if !*yes {
				confirmed, confirmErr := confirmRecovery(ctx, reader, stdout)
				if confirmErr != nil {
					return confirmErr
				}
				if !confirmed {
					return errRecoveryCancelled
				}
			}
			fresh, freshRetirement, inspectErr := inspectPlan(plan.Run.RunID)
			if inspectErr != nil {
				return inspectErr
			}
			if plansEqual(plan, retirementPlan, fresh, freshRetirement) {
				plan, retirementPlan = fresh, freshRetirement
				return nil
			}
			fmt.Fprintln(stdout, changedMessage)
			if writeErr := writePlan(fresh, freshRetirement); writeErr != nil {
				return writeErr
			}
			plan, retirementPlan = fresh, freshRetirement
			if *yes {
				return nil
			}
		}
	}
	if err := confirmCurrent("Recovery Plan changed after confirmation; confirm the current exact plan again:"); err != nil {
		if errors.Is(err, errRecoveryCancelled) {
			_, writeErr := fmt.Fprintln(stdout, "Recovery cancelled; no changes made.")
			return writeErr
		}
		return err
	}
	if err := ensureResetStateBinding(commonDirectory, resolvedState); err != nil {
		return err
	}
	if plan.Outcome == recovery.OutcomeCompletion {
		retire, err := newCompletionRetire(plan.Run.RunID)
		if err != nil {
			return err
		}
		return retireRecoveredCompletion(ctx, stdout, fileStore, retire, *retirementPlan, plan.Run.RunID)
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
		fresh, freshRetirement, inspectErr := inspectPlan(result.Run.RunID)
		if inspectErr != nil {
			return inspectErr
		}
		fmt.Fprintln(stdout, "Recovery Plan changed after confirmation; merged Completion now requires this exact retirement plan:")
		if err := writePlan(fresh, freshRetirement); err != nil {
			return err
		}
		plan, retirementPlan = fresh, freshRetirement
		if !*yes {
			if err := confirmCurrent("Recovery Plan changed again after confirmation; confirm the current exact plan again:"); err != nil {
				if errors.Is(err, errRecoveryCancelled) {
					_, writeErr := fmt.Fprintln(stdout, "Recovery cancelled; no changes made.")
					return writeErr
				}
				return err
			}
		}
		retire, err := newCompletionRetire(result.Run.RunID)
		if err != nil {
			return err
		}
		return retireRecoveredCompletion(ctx, stdout, fileStore, retire, *retirementPlan, result.Run.RunID)
	}
	return nil
}

var errRecoveryCancelled = errors.New("Recovery cancelled")

func retireRecoveredCompletion(ctx context.Context, stdout io.Writer, store state.FileStore, retire retirement.Module, plan retirement.Plan, runID string) error {
	if err := retire.Validate(plan); err != nil {
		return err
	}
	if err := retire.Retire(ctx, plan); err != nil {
		return err
	}
	return writeResolveOutcome(stdout, store, runID)
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
