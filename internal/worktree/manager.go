package worktree

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type Assignment struct {
	Path   string
	Branch string
}

type Manager struct {
	GitExecutable string
	RepositoryDir string
	WorktreesDir  string
	DefaultBranch string

	fetchRetryWait func(context.Context, time.Duration) error
}

const fetchMaxAttempts = 3

var unsafeRunID = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func (m Manager) Plan(issue int, runID string) (Assignment, error) {
	if issue <= 0 {
		return Assignment{}, fmt.Errorf("invalid issue number %d", issue)
	}
	safeRunID := strings.Trim(unsafeRunID.ReplaceAllString(runID, "-"), "-.")
	if safeRunID == "" {
		return Assignment{}, fmt.Errorf("run id %q has no safe branch characters", runID)
	}
	if m.RepositoryDir == "" || m.WorktreesDir == "" || m.DefaultBranch == "" {
		return Assignment{}, fmt.Errorf("worktree manager is not fully configured")
	}
	return Assignment{
		Branch: fmt.Sprintf("agent/issue-%d-%s", issue, safeRunID),
		Path:   filepath.Join(m.WorktreesDir, fmt.Sprintf("issue-%d-%s", issue, safeRunID)),
	}, nil
}

func (m Manager) Prepare(ctx context.Context, assignment Assignment) error {
	if assignment.Path == "" || assignment.Branch == "" {
		return fmt.Errorf("worktree assignment is incomplete")
	}
	if err := os.MkdirAll(m.WorktreesDir, 0o700); err != nil {
		return fmt.Errorf("create worktree directory: %w", err)
	}
	if err := m.fetchBase(ctx); err != nil {
		return fmt.Errorf("fetch base branch: %w", err)
	}
	if err := m.git(ctx, "-C", m.RepositoryDir, "worktree", "add", "-b", assignment.Branch,
		assignment.Path, "origin/"+m.DefaultBranch); err != nil {
		return fmt.Errorf("create worktree: %w", err)
	}
	return nil
}

// Verify proves that the retained path is the expected Git worktree and is
// still checked out on the Run's exact branch.
func (m Manager) Verify(ctx context.Context, assignment Assignment) error {
	if assignment.Path == "" || assignment.Branch == "" {
		return fmt.Errorf("worktree assignment is incomplete")
	}
	info, err := os.Lstat(assignment.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("worktree %q is missing", assignment.Path)
		}
		return fmt.Errorf("inspect worktree path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("worktree %q is a symlink", assignment.Path)
	}
	if !info.IsDir() {
		return fmt.Errorf("worktree %q is not a directory", assignment.Path)
	}
	expectedPath, err := filepath.EvalSymlinks(assignment.Path)
	if err != nil {
		return fmt.Errorf("resolve expected worktree: %w", err)
	}
	root, err := m.gitOutput(ctx, "-C", assignment.Path, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("inspect worktree root: %w", err)
	}
	actualPath, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve actual worktree: %w", err)
	}
	if filepath.Clean(actualPath) != filepath.Clean(expectedPath) {
		return fmt.Errorf("Git worktree root %q does not match expected path %q", actualPath, expectedPath)
	}
	expectedCommon, err := m.gitOutput(ctx, "-C", m.RepositoryDir, "rev-parse", "--git-common-dir")
	if err != nil {
		return fmt.Errorf("inspect repository common directory: %w", err)
	}
	actualCommon, err := m.gitOutput(ctx, "-C", assignment.Path, "rev-parse", "--git-common-dir")
	if err != nil {
		return fmt.Errorf("inspect worktree common directory: %w", err)
	}
	expectedCommon, err = resolveGitDirectory(m.RepositoryDir, expectedCommon)
	if err != nil {
		return fmt.Errorf("resolve repository common directory: %w", err)
	}
	actualCommon, err = resolveGitDirectory(assignment.Path, actualCommon)
	if err != nil {
		return fmt.Errorf("resolve worktree common directory: %w", err)
	}
	if expectedCommon != actualCommon {
		return fmt.Errorf("worktree Git common directory %q does not match repository %q", actualCommon, expectedCommon)
	}
	branch, err := m.gitOutput(ctx, "-C", assignment.Path, "branch", "--show-current")
	if err != nil {
		return fmt.Errorf("inspect worktree branch: %w", err)
	}
	if branch != assignment.Branch {
		return fmt.Errorf("worktree branch %q does not match expected branch %q", branch, assignment.Branch)
	}
	return nil
}

func resolveGitDirectory(base, path string) (string, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(base, path)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func (m Manager) Cleanup(ctx context.Context, assignment Assignment) error {
	if assignment.Path == "" || assignment.Branch == "" {
		return fmt.Errorf("worktree assignment is incomplete")
	}
	if m.Exists(assignment) {
		if err := m.git(ctx, "-C", m.RepositoryDir, "worktree", "remove", "--force", assignment.Path); err != nil {
			return fmt.Errorf("remove worktree: %w", err)
		}
	}
	if err := m.git(ctx, "-C", m.RepositoryDir, "worktree", "prune"); err != nil {
		return fmt.Errorf("prune worktrees: %w", err)
	}
	exists, err := m.branchExists(ctx, assignment.Branch)
	if err != nil {
		return err
	}
	if exists {
		if err := m.git(ctx, "-C", m.RepositoryDir, "branch", "-D", assignment.Branch); err != nil {
			return fmt.Errorf("delete local branch: %w", err)
		}
	}
	return nil
}

func (m Manager) branchExists(ctx context.Context, branch string) (bool, error) {
	executable := m.GitExecutable
	if executable == "" {
		executable = "git"
	}
	args := []string{"-C", m.RepositoryDir, "show-ref", "--verify", "--quiet", "refs/heads/" + branch}
	command := exec.CommandContext(ctx, executable, args...)
	output, err := command.CombinedOutput()
	if err == nil {
		return true, nil
	}
	if exitError, ok := err.(*exec.ExitError); ok && exitError.ExitCode() == 1 {
		return false, nil
	}
	message := strings.TrimSpace(string(output))
	if message != "" {
		return false, fmt.Errorf("inspect local branch: %s", message)
	}
	return false, fmt.Errorf("inspect local branch: %w", err)
}

func (m Manager) Exists(assignment Assignment) bool {
	info, err := os.Stat(assignment.Path)
	return err == nil && info.IsDir()
}

func (m Manager) fetchBase(ctx context.Context) error {
	var fetchErr error
	for attempt := 1; attempt <= fetchMaxAttempts; attempt++ {
		fetchErr = m.git(ctx, "-C", m.RepositoryDir, "fetch", "origin", m.DefaultBranch)
		if fetchErr == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt == fetchMaxAttempts {
			break
		}
		wait := waitForFetchAttempt
		if m.fetchRetryWait != nil {
			wait = m.fetchRetryWait
		}
		if err := wait(ctx, defaultFetchRetryDelay(attempt)); err != nil {
			return err
		}
	}
	return fmt.Errorf("after %d attempts: %w", fetchMaxAttempts, fetchErr)
}

func defaultFetchRetryDelay(attempt int) time.Duration {
	return time.Second << (attempt - 1)
}

func waitForFetchAttempt(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m Manager) git(ctx context.Context, args ...string) error {
	_, err := m.gitOutput(ctx, args...)
	return err
}

func (m Manager) gitOutput(ctx context.Context, args ...string) (string, error) {
	executable := m.GitExecutable
	if executable == "" {
		executable = "git"
	}
	command := exec.CommandContext(ctx, executable, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message != "" {
			return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
		}
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output)), nil
}
