package retirement

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robinjoseph08/backlog/internal/processidentity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
)

func TestInspectWorkerAbsentRefusesContradictoryRecordedPIDs(t *testing.T) {
	t.Parallel()
	identity, err := processidentity.Start(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{RunID: "contradictory-worker", PID: os.Getpid() + 1, ProcessIdentity: identity}
	if err := inspectWorkerAbsent(run); err == nil || !strings.Contains(err.Error(), "recorded PID") || !strings.Contains(err.Error(), "does not match process identity PID") {
		t.Fatalf("contradictory Worker identity error = %v", err)
	}
}

func TestArchiveSessionUsesAtomicRenameAndSyncsEveryDurabilityPath(t *testing.T) {
	t.Parallel()
	fixture := newArchivableSession(t)
	before, err := os.Stat(fixture.activeFile)
	if err != nil {
		t.Fatal(err)
	}
	synced := make(map[string]bool)
	if err := archiveSession(fixture.run, fixture.session, fixture.stateDir, func(path string) error {
		synced[filepath.Clean(path)] = true
		return syncFilesystemPath(path)
	}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(fixture.archiveFile)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("session archival replaced the session file instead of atomically renaming it")
	}
	for _, path := range []string{
		fixture.archiveFile,
		fixture.nestedArchiveFile,
		fixture.nestedArchiveDir,
		fixture.session.ArchiveDir,
		filepath.Dir(fixture.session.ArchiveDir),
		filepath.Dir(filepath.Dir(fixture.session.ArchiveDir)),
		fixture.stateDir,
		filepath.Dir(fixture.session.Dir),
	} {
		if !synced[filepath.Clean(path)] {
			t.Errorf("durability path %s was not synced; syncs = %#v", path, synced)
		}
	}
	if _, err := os.Stat(fixture.activeFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active session survived archival: %v", err)
	}
}

func TestArchiveSessionReportsEveryDurabilitySyncFailure(t *testing.T) {
	t.Parallel()
	boundaries := []struct {
		name   string
		target func(string, Session) string
	}{
		{name: "root payload", target: func(_ string, session Session) string { return filepath.Join(session.ArchiveDir, "session.jsonl") }},
		{name: "nested payload", target: func(_ string, session Session) string {
			return filepath.Join(session.ArchiveDir, "nested", "events.jsonl")
		}},
		{name: "nested directory", target: func(_ string, session Session) string { return filepath.Join(session.ArchiveDir, "nested") }},
		{name: "archive directory", target: func(_ string, session Session) string { return session.ArchiveDir }},
		{name: "archive parent", target: func(_ string, session Session) string { return filepath.Dir(session.ArchiveDir) }},
		{name: "history parent", target: func(_ string, session Session) string { return filepath.Dir(filepath.Dir(session.ArchiveDir)) }},
		{name: "state directory", target: func(stateDir string, _ Session) string { return stateDir }},
		{name: "active session parent", target: func(_ string, session Session) string { return filepath.Dir(session.Dir) }},
	}
	for _, boundary := range boundaries {
		t.Run(boundary.name, func(t *testing.T) {
			t.Parallel()
			fixture := newArchivableSession(t)
			target := filepath.Clean(boundary.target(fixture.stateDir, fixture.session))
			injected := "injected " + boundary.name + " sync failure"
			err := archiveSession(fixture.run, fixture.session, fixture.stateDir, func(path string) error {
				if filepath.Clean(path) == target {
					return errors.New(injected)
				}
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), injected) {
				t.Fatalf("durability sync error = %v, want %q", err, injected)
			}
			if _, err := os.Stat(fixture.activeFile); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("active session survived failed archive sync: %v", err)
			}
			if info, err := os.Stat(fixture.session.ArchiveDir); err != nil || !info.IsDir() {
				t.Fatalf("historical session missing after failed archive sync: %v", err)
			}
		})
	}
}

func TestArchiveSessionRefusesSourceReplacementAfterInspection(t *testing.T) {
	t.Parallel()
	fixture := newArchivableSession(t)
	if _, err := inspectSession(fixture.run, fixture.stateDir); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(fixture.session.Dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(fixture.session.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	replacement := `{"type":"session","id":"unrelated","cwd":"/unrelated"}` + "\n"
	if err := os.WriteFile(filepath.Join(fixture.session.Dir, "session.jsonl"), []byte(replacement), 0o600); err != nil {
		t.Fatal(err)
	}

	err := archiveSession(fixture.run, fixture.session, fixture.stateDir, syncFilesystemPath)
	if err == nil || !strings.Contains(err.Error(), "identity changed immediately before archival") {
		t.Fatalf("replacement session archival error = %v", err)
	}
	if info, statErr := os.Stat(fixture.session.Dir); statErr != nil || !info.IsDir() {
		t.Fatalf("replacement session was moved: %v", statErr)
	}
	if _, statErr := os.Stat(fixture.session.ArchiveDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("replacement session reached historical archive: %v", statErr)
	}
}

type archivableSessionFixture struct {
	stateDir, activeFile, archiveFile, nestedArchiveDir, nestedArchiveFile string
	run                                                                    scheduler.Run
	session                                                                Session
}

func newArchivableSession(t *testing.T) archivableSessionFixture {
	t.Helper()
	stateDir := t.TempDir()
	sessionDir := filepath.Join(stateDir, "sessions", "run-atomic")
	archiveDir := filepath.Join(stateDir, "history", "sessions", "run-atomic")
	nestedSessionDir := filepath.Join(sessionDir, "nested")
	if err := os.MkdirAll(nestedSessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		RunID: "run-atomic", WorkerMode: scheduler.WorkerModeRPC,
		Worktree:  filepath.Join(stateDir, "worktrees", "run-atomic"),
		SessionID: "backlog-run-atomic", SessionDir: sessionDir,
	}
	header := `{"type":"session","id":"` + run.SessionID + `","cwd":"` + run.Worktree + `"}` + "\n"
	activeFile := filepath.Join(sessionDir, "session.jsonl")
	if err := os.WriteFile(activeFile, []byte(header), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nestedSessionDir, "events.jsonl"), []byte(header), 0o600); err != nil {
		t.Fatal(err)
	}
	session := Session{ID: run.SessionID, Dir: sessionDir, ArchiveDir: archiveDir, Present: true}
	return archivableSessionFixture{
		stateDir: stateDir, run: run, session: session, activeFile: activeFile,
		archiveFile:       filepath.Join(archiveDir, "session.jsonl"),
		nestedArchiveDir:  filepath.Join(archiveDir, "nested"),
		nestedArchiveFile: filepath.Join(archiveDir, "nested", "events.jsonl"),
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
