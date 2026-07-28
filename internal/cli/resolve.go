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
		fmt.Fprintln(stderr, "Completion takes precedence when GitHub verifies a closed issue and the Run's")
		fmt.Fprintln(stderr, "recorded pull request is merged. When the Run has no recorded pull request,")
		fmt.Fprintln(stderr, "exactly one merged pull request discovered from its expected branch also")
		fmt.Fprintln(stderr, "establishes Completion. Multiple unrecorded merged pull requests are ambiguous;")
		fmt.Fprintln(stderr, "resolve refuses them, retains the Lease, and requires operator intervention.")
		fmt.Fprintln(stderr, "Otherwise, recognize a supported GitHub issue closure as External Resolution")
		fmt.Fprintln(stderr, "of an incomplete leased Run. Safely retire owned unmerged pull requests, remote")
		fmt.Fprintln(stderr, "and local branches, worktrees, and active Pi sessions; remove managed active")
		fmt.Fprintln(stderr, "and Candidate labels; preserve diagnostics; then release the Lease.")
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
	if !*dryRun && !*yes && !interactive {
		return errors.New("non-interactive External Resolution requires --yes")
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
		return fmt.Errorf("External Resolution refused while Runner owns repository coordination: %w", err)
	}
	defer func() { _ = lock.Release() }()

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
			confirmed, err := confirmResolve(ctx, reader, stdout)
			if err != nil {
				return err
			}
			if !confirmed {
				_, err := fmt.Fprintln(stdout, "External Resolution cancelled; no changes made.")
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
	if plan.Snapshot.Run.Status != scheduler.StatusResolvedExternally {
		if err := ensureResetStateBinding(commonDirectory, resolvedState); err != nil {
			return err
		}
	}
	if err := module.Retire(ctx, plan); err != nil {
		return err
	}
	return writeResolveOutcome(stdout, store, plan.Snapshot.Run.RunID)
}

func writeResolveOutcome(output io.Writer, store state.FileStore, runID string) error {
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
			_, err = fmt.Fprintf(output, "Completion recorded for Run %s from merged expected pull request %s. No replacement Run was created.\n", run.RunID, run.PullRequest)
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

func confirmResolve(ctx context.Context, reader *bufio.Reader, output io.Writer) (bool, error) {
	if _, err := fmt.Fprint(output, "Proceed with External Resolution? [y/N] "); err != nil {
		return false, err
	}
	return readConfirmation(ctx, reader, "External Resolution")
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
