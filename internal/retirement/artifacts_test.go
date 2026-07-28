package retirement

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robinjoseph08/backlog/internal/scheduler"
)

func TestArchiveSessionUsesAtomicRename(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	sessionDir := filepath.Join(stateDir, "sessions", "run-atomic")
	archiveDir := filepath.Join(stateDir, "history", "sessions", "run-atomic")
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	if err := os.WriteFile(sessionFile, []byte("session\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(sessionFile)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{ID: "backlog-run-atomic", Dir: sessionDir, ArchiveDir: archiveDir, Present: true}
	synced := make(map[string]bool)
	if err := archiveSession(session, stateDir, func(path string) error {
		synced[filepath.Clean(path)] = true
		return syncFilesystemPath(path)
	}); err != nil {
		t.Fatal(err)
	}
	archiveFile := filepath.Join(archiveDir, "session.jsonl")
	after, err := os.Stat(archiveFile)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("session archival replaced the session file instead of atomically renaming it")
	}
	if !synced[archiveFile] || !synced[archiveDir] {
		t.Fatalf("archive payload syncs = %#v, want file and archive directory", synced)
	}
	if _, err := os.Stat(sessionFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active session survived archival: %v", err)
	}
}

func TestDeleteLocalBranchUsesExpectedCommit(t *testing.T) {
	t.Parallel()
	repository := filepath.Join(t.TempDir(), "repo")
	runRetirementGit(t, t.TempDir(), "init", "-b", "main", repository)
	runRetirementGit(t, repository, "config", "user.name", "Retirement Test")
	runRetirementGit(t, repository, "config", "user.email", "retirement@example.test")
	if err := os.WriteFile(filepath.Join(repository, "tracked"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runRetirementGit(t, repository, "add", "tracked")
	runRetirementGit(t, repository, "commit", "-m", "base")
	branchName := "agent/issue-42-local-cas"
	runRetirementGit(t, repository, "branch", branchName)
	expected := retirementGitOutput(t, repository, "rev-parse", branchName)
	runRetirementGit(t, repository, "commit", "--allow-empty", "-m", "advanced")
	advanced := retirementGitOutput(t, repository, "rev-parse", "HEAD")
	runRetirementGit(t, repository, "update-ref", "refs/heads/"+branchName, advanced)
	branch := Branch{Name: branchName, Commit: expected, Present: true}
	if err := deleteLocalBranch(context.Background(), "git", repository, branch); err == nil || !strings.Contains(err.Error(), "expected commit") {
		t.Fatalf("stale local branch deletion error = %v", err)
	}
	if got := retirementGitOutput(t, repository, "rev-parse", branchName); got != advanced {
		t.Fatalf("stale deletion changed branch to %s, want %s", got, advanced)
	}
	branch.Commit = advanced
	if err := deleteLocalBranch(context.Background(), "git", repository, branch); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "-C", repository, "show-ref", "--verify", "--quiet", "refs/heads/"+branchName)
	if err := command.Run(); err == nil {
		t.Fatal("expected-commit deletion left local branch present")
	}
}

func TestDeleteRemoteBranchUsesExpectedCommitLease(t *testing.T) {
	t.Parallel()
	calls := filepath.Join(t.TempDir(), "calls")
	git := writeRetirementExecutable(t, "#!/bin/sh\nprintf '%s\\n' \"$*\" > "+shellQuote(calls)+"\n")
	branch := Branch{Name: "agent/issue-42-run", Commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Present: true}
	if err := deleteRemoteBranch(context.Background(), git, t.TempDir(), branch); err != nil {
		t.Fatal(err)
	}
	call, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(call), "--force-with-lease=refs/heads/agent/issue-42-run:"+branch.Commit+" :refs/heads/agent/issue-42-run") {
		t.Fatalf("git call = %q", call)
	}

	failingGit := writeRetirementExecutable(t, "#!/bin/sh\necho stale lease >&2\nexit 1\n")
	if err := deleteRemoteBranch(context.Background(), failingGit, t.TempDir(), branch); err == nil || !strings.Contains(err.Error(), "expected commit") || !strings.Contains(err.Error(), "stale lease") {
		t.Fatalf("conditional deletion error = %v", err)
	}
}

func TestInspectSessionRefusesEmptyHistoricalArchive(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	archiveDir := filepath.Join(stateDir, "history", "sessions", "run-empty")
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		RunID: "run-empty", WorkerMode: scheduler.WorkerModeRPC,
		Worktree:  filepath.Join(stateDir, "worktrees", "run-empty"),
		SessionID: "backlog-run-empty", SessionDir: filepath.Join(stateDir, "sessions", "run-empty"),
	}
	if _, err := inspectSession(run, stateDir); err == nil || !strings.Contains(err.Error(), "no identity-bearing session files") {
		t.Fatalf("empty historical archive error = %v", err)
	}
}

func runRetirementGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func retirementGitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func writeRetirementExecutable(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "executable")
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
