package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/herdr"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

func Main(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return MainWithSignals(ctx, args, stdout, stderr, nil)
}

// MainWithSignals runs the CLI while preserving each delivered interrupt for
// the runner lifecycle instead of reducing all interrupts to one cancellation.
func MainWithSignals(ctx context.Context, args []string, stdout, stderr io.Writer, signals <-chan os.Signal) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}
	var err error
	switch args[0] {
	case "run":
		err = runCommand(ctx, args[1:], stdout, stderr, signals)
	case "status":
		commandCtx, stop := cancelContextOnSignal(ctx, signals)
		defer stop()
		err = statusCommand(commandCtx, args[1:], stdout, stderr)
	case "follow":
		commandCtx, stop := cancelContextOnSignal(ctx, signals)
		defer stop()
		err = followCommand(commandCtx, args[1:], stdout, stderr)
	case "acknowledge":
		commandCtx, stop := cancelContextOnSignal(ctx, signals)
		defer stop()
		err = acknowledgeCommand(commandCtx, args[1:], stdout, stderr)
	case "reset":
		commandCtx, stop := cancelContextOnSignal(ctx, signals)
		defer stop()
		err = resetCommand(commandCtx, args[1:], stdout, stderr)
	case "retry":
		commandCtx, stop := cancelContextOnSignal(ctx, signals)
		defer stop()
		fmt.Fprintln(stderr, "Warning: backlog retry is deprecated; use backlog reset.")
		err = resetCommand(commandCtx, args[1:], stdout, stderr)
	case "help", "--help", "-h":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		printUsage(stderr)
		return 2
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		var signalExit *runner.SignalExit
		if errors.As(err, &signalExit) {
			return signalExit.Code
		}
		var intervention *runner.InterventionRequired
		if errors.As(err, &intervention) {
			return 1
		}
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

const (
	setupSignalNone                int32 = 0
	setupSignalDrainAccepted       int32 = 1
	setupSignalInterruptSuspension int32 = 130
	setupSignalTermSuspension      int32 = 143
)

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer, signals <-chan os.Signal) (resultErr error) {
	setupCtx := ctx
	runnerSignals := signals
	var cancelSetup context.CancelFunc
	var relayDone chan struct{}
	var setupMu sync.Mutex
	setupActive := signals != nil
	setupSignalExit := setupSignalNone
	runnerEntered := false
	if signals != nil {
		setupCtx, cancelSetup = context.WithCancel(ctx)
		forwarded := make(chan os.Signal, 16)
		runnerSignals = forwarded
		relayDone = make(chan struct{})
		go func() {
			for {
				select {
				case signal, ok := <-signals:
					if !ok {
						return
					}
					select {
					case forwarded <- signal:
					case <-relayDone:
						return
					}
					setupMu.Lock()
					if setupActive {
						switch {
						case signal == syscall.SIGTERM && setupSignalExit != setupSignalInterruptSuspension && setupSignalExit != setupSignalTermSuspension:
							setupSignalExit = setupSignalTermSuspension
						case setupSignalExit == setupSignalNone:
							setupSignalExit = setupSignalDrainAccepted
						case setupSignalExit == setupSignalDrainAccepted:
							setupSignalExit = setupSignalInterruptSuspension
						}
						cancelSetup()
					}
					setupMu.Unlock()
				case <-relayDone:
					return
				}
			}
		}()
		defer func() {
			close(relayDone)
			cancelSetup()
			setupMu.Lock()
			setupExit := setupSignalExit
			entered := runnerEntered
			setupMu.Unlock()
			if entered {
				return
			}
			switch setupExit {
			case setupSignalDrainAccepted:
				fmt.Fprintln(stdout, "Drain: admission stopped during setup; 0 Workers remaining")
				fmt.Fprintln(stdout, "Drain complete: 0 Workers remaining; exiting successfully")
				resultErr = nil
			case setupSignalInterruptSuspension:
				fmt.Fprintln(stdout, "Suspension: repeated SIGINT accepted during setup; 0 Workers remaining")
				resultErr = &runner.SignalExit{Code: int(setupSignalInterruptSuspension)}
			case setupSignalTermSuspension:
				fmt.Fprintln(stdout, "Suspension: SIGTERM accepted during setup; 0 Workers remaining")
				resultErr = &runner.SignalExit{Code: int(setupSignalTermSuspension)}
			}
		}()
	}

	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	repoDir := flags.String("repo-dir", ".", "Git repository to drain")
	stateDir := flags.String("state-dir", "", "runner state directory outside the repository")
	maxWorkers := flags.Int("max-workers", 3, "maximum concurrent issue workers")
	poll := flags.Duration("poll", 30*time.Second, "GitHub and process reconciliation interval")
	maxWorkerAge := flags.Duration("max-worker-age", 7*24*time.Hour, "maximum age before a recovered PID requires human verification")
	watch := flags.Bool("watch", false, "keep waiting after the current runnable backlog is exhausted")
	approve := flags.Bool("approve", true, "trust project-local Pi resources in worker worktrees")
	ghExecutable := flags.String("gh", "gh", "gh executable")
	gitExecutable := flags.String("git", "git", "git executable")
	piExecutable := flags.String("pi", "pi", "pi executable")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("run takes no positional arguments")
	}
	absoluteRepo, err := filepath.Abs(*repoDir)
	if err != nil {
		return fmt.Errorf("resolve repository directory: %w", err)
	}
	repositoryRoot, err := gitRepositoryRoot(setupCtx, *gitExecutable, absoluteRepo)
	if err != nil {
		return err
	}
	commonDirectory, err := gitCommonDirectory(setupCtx, *gitExecutable, repositoryRoot)
	if err != nil {
		return err
	}
	resolvedStateDir, err := repositoryStateDirectory(commonDirectory, repositoryRoot, *stateDir)
	if err != nil {
		return err
	}
	lock, err := acquireRepositoryLock(commonDirectory)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()
	if err := bindStateDirectory(commonDirectory, resolvedStateDir); err != nil {
		return err
	}
	herdrReporter := herdr.FromEnvironment()
	if herdrReporter.Enabled() {
		_ = herdrReporter.Working("scheduling Runs")
		defer func() { _ = herdrReporter.Release() }()
	}

	github := &ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot}
	repository, err := github.Repository(setupCtx)
	if err != nil {
		return err
	}
	worktrees := &worktree.Manager{
		GitExecutable: *gitExecutable,
		RepositoryDir: repositoryRoot,
		WorktreesDir:  filepath.Join(resolvedStateDir, "worktrees"),
		DefaultBranch: repository.DefaultBranch,
	}
	supervisor := &worker.Supervisor{
		Executable: *piExecutable,
		LogsDir:    filepath.Join(resolvedStateDir, "logs"),
		Approve:    *approve,
	}
	store := state.FileStore{Path: filepath.Join(resolvedStateDir, "state.json")}
	summarySource := repositoryFollowSource{followStateSource: store, commonDirectory: commonDirectory}
	backlogRunner := &runner.Runner{
		Config: runner.Config{
			Repo: repository.Slug, DefaultBranch: repository.DefaultBranch,
			MaxConcurrentIssues: *maxWorkers, PollInterval: *poll, MaxWorkerAge: *maxWorkerAge, Watch: *watch,
			SessionsDir: filepath.Join(resolvedStateDir, "sessions"),
		},
		GitHub:    github,
		Store:     store,
		Worktrees: worktrees,
		Workers:   workerAdapter{supervisor: supervisor},
		Output:    stdout,
		Signals:   runnerSignals,
		FinalSummary: func(current state.State) error {
			return printRunFinalSummary(stdout, current, summarySource, time.Now())
		},
	}
	supervision, err := establishRunnerSupervision(commonDirectory)
	if err != nil {
		return err
	}
	defer func() { _ = supervision.Release() }()
	if signals != nil {
		setupMu.Lock()
		setupActive = false
		setupExit := setupSignalExit
		if setupExit == setupSignalNone {
			runnerEntered = true
		}
		setupMu.Unlock()
		if setupExit != setupSignalNone {
			return setupCtx.Err()
		}
	}
	return backlogRunner.Run(ctx)
}

func cancelContextOnSignal(ctx context.Context, signals <-chan os.Signal) (context.Context, func()) {
	if signals == nil {
		return ctx, func() {}
	}
	commandCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		select {
		case _, ok := <-signals:
			if ok {
				cancel()
			}
		case <-ctx.Done():
		case <-done:
		}
	}()
	return commandCtx, func() {
		close(done)
		cancel()
	}
}

type workerAdapter struct {
	supervisor *worker.Supervisor
}

func (a workerAdapter) Start(ctx context.Context, request worker.Request) (runner.WorkerProcess, error) {
	return a.supervisor.Start(ctx, request)
}

func (a workerAdapter) Release(runID string) error {
	return a.supervisor.Release(runID)
}

func statusCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the runner")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable used to identify the repository root")
	asJSON := flags.Bool("json", false, "print the complete state as JSON")
	showAll := flags.Bool("all", false, "print every persisted Run")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("status takes no positional arguments")
	}
	resolved, commonDirectory, err := resolveStateFromFlags(ctx, *repoDir, *stateDir, *gitExecutable)
	if err != nil {
		return err
	}
	store := state.FileStore{Path: filepath.Join(resolved, "state.json")}
	current, migrationRequired, err := store.Preview()
	if err != nil {
		return err
	}
	if migrationRequired {
		lock, err := acquireRepositoryLock(commonDirectory)
		if err != nil {
			return fmt.Errorf("migrate state for status: %w", err)
		}
		defer func() { _ = lock.Release() }()
		if err := bindStateDirectory(commonDirectory, resolved); err != nil {
			return err
		}
		current, err = store.Load()
		if err != nil {
			return err
		}
	}
	if *asJSON {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(current)
	}
	source := repositoryFollowSource{followStateSource: store, commonDirectory: commonDirectory}
	return printPlainStatusProjection(stdout, current, source, time.Now(), *showAll)
}

func resolveStateFromFlags(ctx context.Context, repoDir, stateDir, gitExecutable string) (string, string, error) {
	absoluteRepo, err := filepath.Abs(repoDir)
	if err != nil {
		return "", "", err
	}
	root, err := gitRepositoryRoot(ctx, gitExecutable, absoluteRepo)
	if err != nil {
		return "", "", err
	}
	common, err := gitCommonDirectory(ctx, gitExecutable, root)
	if err != nil {
		return "", "", err
	}
	resolved, err := repositoryStateDirectory(common, root, stateDir)
	return resolved, common, err
}

func repositoryStateDirectory(commonDirectory, repositoryRoot, override string) (string, error) {
	if bound, ok, err := readStateDirectoryBinding(commonDirectory); err != nil {
		return "", err
	} else if ok {
		if override != "" {
			requested, err := filepath.Abs(override)
			if err != nil {
				return "", err
			}
			if requested != bound {
				return "", fmt.Errorf("repository runner state is bound to %s, not %s", bound, requested)
			}
		}
		return bound, nil
	}
	return resolveStateDirectory(repositoryRoot, override)
}

func gitRepositoryRoot(ctx context.Context, executable, directory string) (string, error) {
	command := exec.CommandContext(ctx, executable, "-C", directory, "rev-parse", "--show-toplevel")
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message != "" {
			return "", fmt.Errorf("discover Git repository root: %s", message)
		}
		return "", fmt.Errorf("discover Git repository root: %w", err)
	}
	root := strings.TrimSpace(string(output))
	if root == "" {
		return "", errors.New("discover Git repository root: git returned an empty path")
	}
	return filepath.Abs(root)
}

func gitCommonDirectory(ctx context.Context, executable, repositoryRoot string) (string, error) {
	command := exec.CommandContext(ctx, executable, "-C", repositoryRoot, "rev-parse", "--git-common-dir")
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message != "" {
			return "", fmt.Errorf("discover Git common directory: %s", message)
		}
		return "", fmt.Errorf("discover Git common directory: %w", err)
	}
	common := strings.TrimSpace(string(output))
	if common == "" {
		return "", errors.New("discover Git common directory: git returned an empty path")
	}
	if !filepath.IsAbs(common) {
		common = filepath.Join(repositoryRoot, common)
	}
	return filepath.Abs(common)
}

const (
	lockFile                        = "backlog.lock"
	legacyLockFile                  = "pi-backlog-runner.lock"
	stateDirectoryBindingFile       = "backlog.state-dir"
	legacyStateDirectoryBindingFile = "pi-backlog-runner.state-dir"
)

type repositoryLock struct {
	coordination *state.Lock
	current      *state.Lock
	legacy       *state.Lock
}

func acquireRepositoryLock(commonDirectory string) (*repositoryLock, error) {
	if err := os.MkdirAll(commonDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("create Git common directory: %w", err)
	}
	coordination, err := state.AcquireReadOnlyLock(commonDirectory)
	if err != nil {
		return nil, err
	}
	legacy, err := state.AcquireLock(filepath.Join(commonDirectory, legacyLockFile))
	if err != nil {
		_ = coordination.Release()
		return nil, err
	}
	current, err := state.AcquireLock(filepath.Join(commonDirectory, lockFile))
	if err != nil {
		_ = legacy.Release()
		_ = coordination.Release()
		return nil, err
	}
	return &repositoryLock{coordination: coordination, current: current, legacy: legacy}, nil
}

func (l *repositoryLock) Release() error {
	return errors.Join(l.current.Release(), l.legacy.Release(), l.coordination.Release())
}

func bindStateDirectory(commonDirectory, stateDirectory string) error {
	absolute, err := filepath.Abs(stateDirectory)
	if err != nil {
		return err
	}
	if bound, ok, err := readStateDirectoryBinding(commonDirectory); err != nil {
		return err
	} else if ok && bound != absolute {
		return fmt.Errorf("repository runner state is already bound to %s; refusing alternate state directory %s", bound, absolute)
	}
	if err := writeStateDirectoryBinding(commonDirectory, absolute); err != nil {
		return fmt.Errorf("bind repository runner state directory: %w", err)
	}
	return nil
}

func writeStateDirectoryBinding(commonDirectory, stateDirectory string) error {
	// Write the legacy binding first so rolling back to the old executable cannot
	// split this repository across two state directories. If the second write is
	// interrupted, the new executable can also recover from the legacy binding.
	for _, name := range []string{legacyStateDirectoryBindingFile, stateDirectoryBindingFile} {
		if err := writeStateDirectoryBindingFile(commonDirectory, name, stateDirectory); err != nil {
			return err
		}
	}
	return nil
}

func writeStateDirectoryBindingFile(commonDirectory, name, stateDirectory string) error {
	temporary, err := os.CreateTemp(commonDirectory, ".backlog-state-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := io.WriteString(temporary, stateDirectory+"\n"); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, filepath.Join(commonDirectory, name)); err != nil {
		return err
	}
	directory, err := os.Open(commonDirectory)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		directory.Close()
		return err
	}
	return directory.Close()
}

func readStateDirectoryBinding(commonDirectory string) (string, bool, error) {
	current, currentExists, err := readStateDirectoryBindingFile(filepath.Join(commonDirectory, stateDirectoryBindingFile))
	if err != nil {
		return "", false, err
	}
	legacy, legacyExists, err := readStateDirectoryBindingFile(filepath.Join(commonDirectory, legacyStateDirectoryBindingFile))
	if err != nil {
		return "", false, err
	}
	if currentExists && legacyExists && current != legacy {
		return "", false, fmt.Errorf("repository runner state bindings disagree: %s and %s", current, legacy)
	}
	if currentExists {
		return current, true, nil
	}
	return legacy, legacyExists, nil
}

func readStateDirectoryBindingFile(path string) (string, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read repository runner state binding: %w", err)
	}
	bound := strings.TrimSpace(string(data))
	if bound == "" || !filepath.IsAbs(bound) {
		return "", false, fmt.Errorf("repository runner state binding is invalid: %q", bound)
	}
	return bound, true, nil
}

func resolveStateDirectory(repository, override string) (string, error) {
	if override != "" {
		return filepath.Abs(override)
	}
	return defaultStateDirectory(repository)
}

func defaultStateDirectory(repository string) (string, error) {
	absolute, err := filepath.Abs(repository)
	if err != nil {
		return "", err
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("find user cache directory: %w", err)
	}
	digest := sha256.Sum256([]byte(absolute))
	name := strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, filepath.Base(absolute))
	return filepath.Join(cache, "backlog", name+"-"+hex.EncodeToString(digest[:8])), nil
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func printUsage(writer io.Writer) {
	fmt.Fprintln(writer, "Usage:")
	fmt.Fprintln(writer, "  backlog run [flags]")
	fmt.Fprintln(writer, "  backlog status [--all] [--json] [flags]")
	fmt.Fprintln(writer, "  backlog follow <run-id|positive-issue-number> [--raw] [flags]")
	fmt.Fprintln(writer, "  backlog acknowledge <run-id|positive-issue-number>... [flags]")
	fmt.Fprintln(writer, "  backlog acknowledge --all [flags]")
	fmt.Fprintln(writer, "  backlog reset <issue-number> [--dry-run | --yes] [flags]")
	fmt.Fprintln(writer, "  backlog retry <issue-number> [--dry-run | --yes] [flags]  (deprecated alias for reset)")
	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "backlog run lifecycle:")
	fmt.Fprintln(writer, "  First SIGINT drains Owned Workers without admitting new Leases.")
	fmt.Fprintln(writer, "  Second SIGINT, or first SIGTERM, suspends resumable Runs with one bounded deadline.")
	fmt.Fprintln(writer, "  Third SIGINT force stops verified Worker groups; unverified continuation needs human review.")
	fmt.Fprintln(writer, "  Restarting run resumes verified Suspended Runs before admitting new Candidates.")
	fmt.Fprintln(writer, "  Reset retires verified artifacts, archives active Pi sessions, preserves logs and Run history,")
	fmt.Fprintln(writer, "  restores Candidate labels, and releases the Lease only after all postconditions pass.")
	fmt.Fprintln(writer, "  Acknowledge records presentation-only review of eligible Historical Run outcomes.")
	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "Exit statuses:")
	fmt.Fprintln(writer, "  0 success; 1 natural one-shot exhaustion with Intervention-required Runs, command refusal, or operational failure; 2 missing or unknown command.")
	fmt.Fprintln(writer, "  A completed first-SIGINT Drain exits 0; second-SIGINT suspension exits 130.")
	fmt.Fprintln(writer, "  SIGTERM suspension exits 143, including later force escalation.")
	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "Upgrade limits:")
	fmt.Fprintln(writer, "  Version 1 and version 2 state migrate to version 3; legacy print-mode Runs cannot Resume.")
	fmt.Fprintln(writer, "  State written by a newer unsupported version is refused.")
}
