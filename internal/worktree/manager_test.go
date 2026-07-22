package worktree

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagerPreparesUniqueWorktreeFromLatestRemoteBase(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logPath := filepath.Join(root, "git.log")
	git := fakeGit(t, logPath)
	manager := Manager{
		GitExecutable: git,
		RepositoryDir: filepath.Join(root, "repo"),
		WorktreesDir:  filepath.Join(root, "worktrees"),
		DefaultBranch: "trunk",
	}

	got, err := manager.Plan(42, "20260702-abc")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := manager.Prepare(context.Background(), got); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got.Branch != "agent/issue-42-20260702-abc" {
		t.Fatalf("branch = %q", got.Branch)
	}
	if got.Path != filepath.Join(root, "worktrees", "issue-42-20260702-abc") {
		t.Fatalf("path = %q", got.Path)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	log := string(logData)
	want := []string{
		"-C " + manager.RepositoryDir + " fetch origin trunk",
		"-C " + manager.RepositoryDir + " worktree add -b " + got.Branch + " " + got.Path + " origin/trunk",
	}
	for _, command := range want {
		if !strings.Contains(log, command+"\n") {
			t.Fatalf("git log %q does not contain %q", log, command)
		}
	}
}

func TestManagerCleansMergedWorktreeAndBranch(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logPath := filepath.Join(root, "git.log")
	git := fakeGit(t, logPath)
	manager := Manager{GitExecutable: git, RepositoryDir: filepath.Join(root, "repo")}
	worktreePath := filepath.Join(root, "worktree")
	if err := os.Mkdir(worktreePath, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := manager.Cleanup(context.Background(), Assignment{Path: worktreePath, Branch: "agent/issue-42-run"}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	logData, _ := os.ReadFile(logPath)
	log := string(logData)
	for _, command := range []string{
		"-C " + manager.RepositoryDir + " worktree remove --force " + worktreePath,
		"-C " + manager.RepositoryDir + " worktree prune",
		"-C " + manager.RepositoryDir + " branch -D agent/issue-42-run",
	} {
		if !strings.Contains(log, command+"\n") {
			t.Fatalf("git log %q does not contain %q", log, command)
		}
	}
}

func fakeGit(t *testing.T, logPath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "git")
	script := "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$*\" >> " + shellQuote(logPath) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
