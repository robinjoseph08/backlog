package worktree

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagerCreatesAndCleansRealIsolatedWorktree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	repository := filepath.Join(root, "repository")
	runGit(t, "", "init", "--bare", remote)
	runGit(t, "", "init", "-b", "main", seed)
	runGit(t, seed, "config", "user.email", "runner@example.test")
	runGit(t, seed, "config", "user.name", "Runner Test")
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, seed, "add", "README.md")
	runGit(t, seed, "commit", "-m", "fixture")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "origin", "main")
	runGit(t, "", "--git-dir", remote, "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(t, "", "clone", remote, repository)

	manager := Manager{
		RepositoryDir: repository,
		WorktreesDir:  filepath.Join(root, "worktrees"),
		DefaultBranch: "main",
	}
	assignment, err := manager.Plan(42, "integration")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := manager.Prepare(context.Background(), assignment); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got := strings.TrimSpace(runGit(t, assignment.Path, "branch", "--show-current")); got != assignment.Branch {
		t.Fatalf("worktree branch = %q, want %q", got, assignment.Branch)
	}
	if _, err := os.Stat(filepath.Join(assignment.Path, "README.md")); err != nil {
		t.Fatalf("worktree did not start from remote main: %v", err)
	}
	if err := manager.Verify(context.Background(), assignment); err != nil {
		t.Fatalf("verify retained worktree: %v", err)
	}
	runGit(t, assignment.Path, "checkout", "-b", "changed-branch")
	if err := manager.Verify(context.Background(), assignment); err == nil || !strings.Contains(err.Error(), "does not match expected branch") {
		t.Fatalf("changed branch verification error = %v", err)
	}
	runGit(t, assignment.Path, "checkout", assignment.Branch)

	if err := manager.Cleanup(context.Background(), assignment); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Stat(assignment.Path); !os.IsNotExist(err) {
		t.Fatalf("worktree remains after cleanup, stat error = %v", err)
	}
	command := exec.Command("git", "-C", repository, "show-ref", "--verify", "--quiet", "refs/heads/"+assignment.Branch)
	if err := command.Run(); err == nil {
		t.Fatalf("local branch %q remains after cleanup", assignment.Branch)
	}
}

func runGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	if directory != "" {
		command.Dir = directory
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
