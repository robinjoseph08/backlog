package worktree

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
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
}

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
	if err := m.git(ctx, "-C", m.RepositoryDir, "fetch", "origin", m.DefaultBranch); err != nil {
		return fmt.Errorf("fetch base branch: %w", err)
	}
	if err := m.git(ctx, "-C", m.RepositoryDir, "worktree", "add", "-b", assignment.Branch,
		assignment.Path, "origin/"+m.DefaultBranch); err != nil {
		return fmt.Errorf("create worktree: %w", err)
	}
	return nil
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

func (m Manager) git(ctx context.Context, args ...string) error {
	executable := m.GitExecutable
	if executable == "" {
		executable = "git"
	}
	command := exec.CommandContext(ctx, executable, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message != "" {
			return fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
		}
		return fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return nil
}
