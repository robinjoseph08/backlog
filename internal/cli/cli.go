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
	"strconv"
	"strings"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

func Main(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}
	var err error
	switch args[0] {
	case "run":
		err = runCommand(ctx, args[1:], stdout, stderr)
	case "status":
		err = statusCommand(ctx, args[1:], stdout, stderr)
	case "retry":
		err = retryCommand(ctx, args[1:], stdout, stderr)
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
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
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
	repositoryRoot, err := gitRepositoryRoot(ctx, *gitExecutable, absoluteRepo)
	if err != nil {
		return err
	}
	commonDirectory, err := gitCommonDirectory(ctx, *gitExecutable, repositoryRoot)
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
	defer lock.Release()
	if err := bindStateDirectory(commonDirectory, resolvedStateDir); err != nil {
		return err
	}

	github := &ghadapter.Client{Executable: *ghExecutable, Dir: repositoryRoot}
	repository, err := github.Repository(ctx)
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
	backlogRunner := &runner.Runner{
		Config: runner.Config{
			Repo: repository.Slug, DefaultBranch: repository.DefaultBranch,
			MaxConcurrentIssues: *maxWorkers, PollInterval: *poll, MaxWorkerAge: *maxWorkerAge, Watch: *watch,
		},
		GitHub:    github,
		Store:     state.FileStore{Path: filepath.Join(resolvedStateDir, "state.json")},
		Worktrees: worktrees,
		Workers:   workerAdapter{supervisor: supervisor},
		Output:    stdout,
	}
	return backlogRunner.Run(ctx)
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
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("status takes no positional arguments")
	}
	resolved, err := resolveStateFromFlags(ctx, *repoDir, *stateDir, *gitExecutable)
	if err != nil {
		return err
	}
	current, err := (state.FileStore{Path: filepath.Join(resolved, "state.json")}).Load()
	if err != nil {
		return err
	}
	if *asJSON {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(current)
	}
	fmt.Fprintf(stdout, "Repository: %s\n", valueOr(current.Repo, "not initialized"))
	fmt.Fprintf(stdout, "Runs: %d\n", len(current.Runs))
	fmt.Fprintf(stdout, "Active Leases: %d\n", len(current.Leases))
	for _, run := range current.Runs {
		fmt.Fprintf(stdout, "  #%d  %-17s  %s", run.Issue, run.Status, run.Branch)
		if run.Error != "" {
			fmt.Fprintf(stdout, "  (%s)", run.Error)
		}
		fmt.Fprintln(stdout)
	}
	return nil
}

func retryCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("retry", flag.ContinueOnError)
	flags.SetOutput(stderr)
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the runner")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable used to identify the repository root")
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return flags.Parse([]string{arg})
		}
	}
	issueArg, flagArgs, err := splitRetryArguments(args)
	if err != nil {
		return err
	}
	if err := flags.Parse(flagArgs); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected retry arguments: %s", strings.Join(flags.Args(), " "))
	}
	issue, err := strconv.Atoi(issueArg)
	if err != nil || issue <= 0 {
		return fmt.Errorf("invalid issue number %q", issueArg)
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
	resolved, err := repositoryStateDirectory(commonDirectory, repositoryRoot, *stateDir)
	if err != nil {
		return err
	}
	lock, err := acquireRepositoryLock(commonDirectory)
	if err != nil {
		return err
	}
	defer lock.Release()
	if err := bindStateDirectory(commonDirectory, resolved); err != nil {
		return err
	}
	store := state.FileStore{Path: filepath.Join(resolved, "state.json")}
	current, err := store.Load()
	if err != nil {
		return err
	}
	leaseIndex := -1
	var selected scheduler.Run
	for index, lease := range current.Leases {
		if lease.Issue != issue {
			continue
		}
		leaseIndex = index
		for _, run := range current.Runs {
			if run.RunID == lease.RunID && run.Issue == lease.Issue {
				selected = run
				break
			}
		}
		break
	}
	if leaseIndex < 0 {
		return fmt.Errorf("issue #%d has no intervention-required Run with an active Lease", issue)
	}
	if selected.RunID == "" {
		return fmt.Errorf("active Lease for issue #%d has an invalid Run reference", issue)
	}
	if selected.Status != scheduler.StatusFailed && selected.Status != scheduler.StatusNeedsHuman {
		return fmt.Errorf("issue #%d is %s; only failed or needs-human runs can be retried", issue, selected.Status)
	}
	current.Leases = append(current.Leases[:leaseIndex], current.Leases[leaseIndex+1:]...)
	retained := selected.Worktree
	if err := store.Save(current); err != nil {
		return err
	}
	if retained != "" {
		fmt.Fprintf(stdout, "Issue #%d can be scheduled again; retained prior worktree at %s\n", issue, retained)
	} else {
		fmt.Fprintf(stdout, "Issue #%d can be scheduled again\n", issue)
	}
	return nil
}

func splitRetryArguments(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("usage: backlog retry <issue-number> [flags]")
	}
	if !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:], nil
	}
	// Standard flag parsing supports flags before the issue number.
	for index, value := range args {
		if !strings.HasPrefix(value, "-") && (index == 0 || !flagTakesValue(args[index-1])) {
			remaining := append([]string{}, args[:index]...)
			remaining = append(remaining, args[index+1:]...)
			return value, remaining, nil
		}
	}
	return "", nil, errors.New("retry requires an issue number")
}

func flagTakesValue(name string) bool {
	if strings.Contains(name, "=") {
		return false
	}
	name = strings.TrimLeft(name, "-")
	return name == "repo-dir" || name == "state-dir" || name == "git"
}

func resolveStateFromFlags(ctx context.Context, repoDir, stateDir, gitExecutable string) (string, error) {
	absoluteRepo, err := filepath.Abs(repoDir)
	if err != nil {
		return "", err
	}
	root, err := gitRepositoryRoot(ctx, gitExecutable, absoluteRepo)
	if err != nil {
		return "", err
	}
	common, err := gitCommonDirectory(ctx, gitExecutable, root)
	if err != nil {
		return "", err
	}
	return repositoryStateDirectory(common, root, stateDir)
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
	current *state.Lock
	legacy  *state.Lock
}

func acquireRepositoryLock(commonDirectory string) (*repositoryLock, error) {
	legacy, err := state.AcquireLock(filepath.Join(commonDirectory, legacyLockFile))
	if err != nil {
		return nil, err
	}
	current, err := state.AcquireLock(filepath.Join(commonDirectory, lockFile))
	if err != nil {
		_ = legacy.Release()
		return nil, err
	}
	return &repositoryLock{current: current, legacy: legacy}, nil
}

func (l *repositoryLock) Release() error {
	return errors.Join(l.current.Release(), l.legacy.Release())
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
	fmt.Fprintln(writer, "  backlog status [flags]")
	fmt.Fprintln(writer, "  backlog retry <issue-number> [flags]")
}
