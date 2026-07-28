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
	"strconv"
	"strings"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/reset"
	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/state"
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
		fmt.Fprintln(stderr, "Interactive confirmation defaults to no.")
		fmt.Fprintln(stderr, "Enter, EOF, and every non-affirmative response cancel without mutation.")
		fmt.Fprintln(stderr, "Non-interactive mutation requires --yes.")
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
	module, err := retirement.New(retirement.Config{
		Store:           state.FileStore{Path: filepath.Join(resolvedState, "state.json")},
		GitHub:          ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot},
		RepositoryRoot:  repositoryRoot,
		CommonDirectory: commonDirectory,
		StateDirectory:  resolvedState,
		GitExecutable:   *gitExecutable,
	}, reset.Policy(issueNumber))
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
		fmt.Fprintln(stdout, "Dry-run: no changes made.")
		return output.Err()
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
			fmt.Fprintln(stdout, "Reset Plan changed after confirmation; using the current plan:")
			retirement.WritePlan(stdout, fresh)
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
			fmt.Fprintln(stdout, "Reset Plan changed after confirmation; confirm the current plan again:")
			retirement.WritePlan(stdout, fresh)
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
	if err := module.Retire(ctx, plan); err != nil {
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
