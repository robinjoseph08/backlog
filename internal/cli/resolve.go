package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/resolution"
	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func resolveCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return resolveCommandWithInput(ctx, args, os.Stdin, inputIsInteractive(os.Stdin), stdout, stderr)
}

func resolveCommandWithInput(ctx context.Context, args []string, stdin io.Reader, interactive bool, stdout, stderr io.Writer) error {
	output := &resolveOutputWriter{writer: stdout}
	stdout = output
	flags := flag.NewFlagSet("resolve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: backlog resolve <run-id|positive-issue-number> [flags]")
		fmt.Fprintln(stderr, "")
		fmt.Fprintln(stderr, "Completion is preferred when GitHub verifies a closed issue and the Run's")
		fmt.Fprintln(stderr, "recorded pull request is merged. When the Run has no recorded pull request,")
		fmt.Fprintln(stderr, "exactly one merged pull request discovered from its expected branch may establish")
		fmt.Fprintln(stderr, "Completion. Recovered Runs require the stricter Recovered Completion path, and")
		fmt.Fprintln(stderr, "present branch and worktree commits must match the merged pull request head.")
		fmt.Fprintln(stderr, "Failed validation retains the Lease for operator intervention. Verified Completion")
		fmt.Fprintln(stderr, "retires owned branches, worktrees, active Pi sessions, and managed labels")
		fmt.Fprintln(stderr, "`in-progress` and `ready-for-agent` before recording the merged outcome and releasing the Lease.")
		fmt.Fprintln(stderr, "Multiple unrecorded merged pull requests")
		fmt.Fprintln(stderr, "are ambiguous and are refused with the Lease retained.")
		fmt.Fprintln(stderr, "Otherwise, recognize a supported GitHub issue closure as External Resolution")
		fmt.Fprintln(stderr, "of an incomplete leased Run. Safely retire owned unmerged pull requests, remote")
		fmt.Fprintln(stderr, "and local branches, worktrees, and active Pi sessions; remove managed active")
		fmt.Fprintln(stderr, "and Candidate labels; preserve diagnostics; then release the Lease.")
		fmt.Fprintln(stderr, "Historical merged Runs are also eligible for verified remaining Completion cleanup,")
		fmt.Fprintln(stderr, "including verification-only no-op reruns after cleanup. Their Completion metadata and absent Lease are preserved.")
		fmt.Fprintln(stderr, "Dry-run is read-only. Interactive confirmation defaults to no.")
		fmt.Fprintln(stderr, "Non-interactive mutation requires --yes.")
		fmt.Fprintln(stderr, "")
		flags.PrintDefaults()
	}
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the Run")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable")
	ghExecutable := flags.String("gh", "gh", "gh executable")
	dryRun := flags.Bool("dry-run", false, "print the current Resolution Plan without mutation")
	yes := flags.Bool("yes", false, "confirm External Resolution without an interactive prompt")
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return flags.Parse([]string{arg})
		}
	}
	selector, flagArgs, err := splitResolveArguments(args)
	if err != nil {
		return err
	}
	if err := flags.Parse(flagArgs); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected resolve arguments: %s", strings.Join(flags.Args(), " "))
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
		return fmt.Errorf("External Resolution refused while Runner owns repository coordination; the supervising Runner handles automatic reconciliation at startup, during watch polling, and after normal Worker settlement: %w", err)
	}
	defer func() { _ = lock.Release() }()
	if !*dryRun && !*yes && !interactive {
		return errors.New("non-interactive External Resolution requires --yes")
	}

	resolvedState, err := repositoryStateDirectory(commonDirectory, repositoryRoot, *stateDir)
	if err != nil {
		return err
	}
	store := state.FileStore{Path: filepath.Join(resolvedState, "state.json")}
	module, err := retirement.New(retirement.Config{
		Store:          store,
		GitHub:         ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot},
		RepositoryRoot: repositoryRoot, CommonDirectory: commonDirectory,
		StateDirectory: resolvedState, GitExecutable: *gitExecutable,
	}, resolution.Policy(selector))
	if err != nil {
		return err
	}
	plan, err := module.Inspect(ctx)
	if err != nil {
		return err
	}
	retirement.WritePlan(stdout, plan)
	if err := output.Err(); err != nil {
		return err
	}
	if *dryRun {
		_, err = fmt.Fprintln(stdout, "Dry-run: no changes made.")
		return err
	}
	if err := module.Validate(plan); err != nil {
		return err
	}
	if *yes {
		fresh, err := module.Inspect(ctx)
		if err != nil {
			return err
		}
		if err := module.Validate(fresh); err != nil {
			return err
		}
		if !retirement.PlansEqual(plan, fresh) {
			fmt.Fprintln(stdout, "Resolution Plan changed; using the current plan:")
			retirement.WritePlan(stdout, fresh)
		}
		plan = fresh
	} else {
		reader := bufio.NewReader(stdin)
		for {
			confirmed, err := confirmResolve(ctx, reader, stdout, plan.Operation)
			if err != nil {
				return err
			}
			if !confirmed {
				_, err := fmt.Fprintf(stdout, "%s cancelled; no changes made.\n", plan.Operation)
				return err
			}
			fresh, err := module.Inspect(ctx)
			if err != nil {
				return err
			}
			if err := module.Validate(fresh); err != nil {
				return err
			}
			if retirement.PlansEqual(plan, fresh) {
				plan = fresh
				break
			}
			fmt.Fprintln(stdout, "Resolution Plan changed; confirm the current plan again:")
			retirement.WritePlan(stdout, fresh)
			plan = fresh
		}
	}
	if err := output.Err(); err != nil {
		return err
	}
	if len(plan.Actions) != 0 && plan.Snapshot.Run.Status != scheduler.StatusResolvedExternally {
		if err := ensureResetStateBinding(commonDirectory, resolvedState); err != nil {
			return err
		}
	}
	if err := module.Retire(ctx, plan); err != nil {
		return err
	}
	return writeResolveOutcomeForOperation(stdout, store, plan.Snapshot.Run.RunID, plan.Snapshot.Run.Status == scheduler.StatusMerged)
}

func writeResolveOutcome(output io.Writer, store state.FileStore, runID string) error {
	return writeResolveOutcomeForOperation(output, store, runID, false)
}

func writeResolveOutcomeForOperation(output io.Writer, store state.FileStore, runID string, historicalCompletion bool) error {
	current, err := store.Load()
	if err != nil {
		return fmt.Errorf("read completed Resolution outcome: %w", err)
	}
	for _, run := range current.Runs {
		if run.RunID != runID {
			continue
		}
		switch run.Status {
		case scheduler.StatusMerged:
			if historicalCompletion {
				_, err = fmt.Fprintf(output, "Completion cleanup verified for Historical Run %s. Existing merged outcome %s was preserved.\n", run.RunID, run.PullRequest)
			} else {
				_, err = fmt.Fprintf(output, "Completion recorded for Run %s from merged expected pull request %s. No replacement Run was created.\n", run.RunID, run.PullRequest)
			}
		case scheduler.StatusResolvedExternally:
			_, err = fmt.Fprintf(output, "External Resolution complete for Run %s. No replacement Run was created.\n", run.RunID)
		default:
			return fmt.Errorf("Resolution for Run %s ended with unexpected status %s", run.RunID, run.Status)
		}
		return err
	}
	return fmt.Errorf("Resolution outcome for Run %s is absent", runID)
}

type resolveOutputWriter struct {
	writer io.Writer
	err    error
}

func (w *resolveOutputWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	written, err := w.writer.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = fmt.Errorf("write External Resolution output: %w", err)
	}
	return written, err
}

func (w *resolveOutputWriter) Err() error { return w.err }

func confirmResolve(ctx context.Context, reader *bufio.Reader, output io.Writer, operation string) (bool, error) {
	if _, err := fmt.Fprintf(output, "Proceed with %s? [y/N] ", operation); err != nil {
		return false, err
	}
	return readConfirmation(ctx, reader, operation)
}

func splitResolveArguments(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("usage: backlog resolve <run-id|positive-issue-number> [flags]")
	}
	if !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:], nil
	}
	for index, value := range args {
		if !strings.HasPrefix(value, "-") && (index == 0 || !resolveFlagTakesValue(args[index-1])) {
			remaining := append([]string{}, args[:index]...)
			remaining = append(remaining, args[index+1:]...)
			return value, remaining, nil
		}
	}
	return "", nil, errors.New("resolve requires a Run ID or positive issue number")
}

func resolveFlagTakesValue(name string) bool {
	if strings.Contains(name, "=") {
		return false
	}
	name = strings.TrimLeft(name, "-")
	return name == "repo-dir" || name == "state-dir" || name == "git" || name == "gh"
}
