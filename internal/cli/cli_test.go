package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestMainWithSignalsCancelsCommandsDuringRepositorySetup(t *testing.T) {
	for _, command := range []string{"run", "status"} {
		t.Run(command, func(t *testing.T) {
			root := t.TempDir()
			started := filepath.Join(root, "git-started")
			git := writeExecutable(t, `#!/bin/sh
set -eu
touch `+quote(started)+`
exec sleep 30
`)
			signals := make(chan os.Signal, 1)
			done := make(chan int, 1)
			var stdout, stderr bytes.Buffer
			go func() {
				done <- MainWithSignals(context.Background(), []string{command, "--git", git}, &stdout, &stderr, signals)
			}()
			waitForFile(t, started)
			signals <- os.Interrupt
			select {
			case exitCode := <-done:
				if exitCode != 1 {
					t.Fatalf("exit = %d, stderr = %q", exitCode, stderr.String())
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s did not stop after SIGINT during setup", command)
			}
		})
	}
}

func TestRunCommandDrainsIssueThroughFakeExecutables(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	stateDir := filepath.Join(root, "state")
	closedMarker := filepath.Join(root, "issue-42-closed")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if test -f `+quote(closedMarker)+`; then
      printf '%s\n' '[]'
    else
      printf '%s\n' '[{"number":42,"title":"Build it","createdAt":"2026-01-01T00:00:00Z","url":"https://github.com/acme/widgets/issues/42"}]'
    fi ;;
  "issue view 42 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":42,"title":"Build it","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/42","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/42/comments?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/42/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-42-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    head=$8
    printf '[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"MERGED","mergedAt":"2026-01-02T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"%s","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$head" ;;
  "issue view 42 --repo acme/widgets --json number,state,title,url")
    touch `+quote(closedMarker)+`
    printf '%s\n' '{"number":42,"state":"CLOSED","title":"Build it","url":"https://github.com/acme/widgets/issues/42"}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	gitLog := filepath.Join(root, "git.log")
	git := writeExecutable(t, `#!/bin/sh
set -eu
printf '%s\n' "$*" >> `+quote(gitLog)+`
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; fi
if [ "$3" = "worktree" ] && [ "$4" = "remove" ]; then rm -rf "$6"; fi
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)

	var stdout, stderr bytes.Buffer
	exitCode := Main(context.Background(), []string{
		"run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi,
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("exit = %d, stderr = %q", exitCode, stderr.String())
	}

	persisted, err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Load()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(persisted.Runs) != 1 || persisted.Runs[0].Status != scheduler.StatusMerged || len(persisted.Leases) != 0 {
		t.Fatalf("state Runs/Leases = %#v/%#v, want merged history without an active Lease", persisted.Runs, persisted.Leases)
	}
	if _, err := os.Stat(persisted.Runs[0].Worktree); !os.IsNotExist(err) {
		t.Fatalf("successful worktree still exists, stat error = %v", err)
	}
	if !strings.Contains(stdout.String(), "verified merged completion for issue #42") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestStatusPrintsIssueTitlesAndFallsBackToIssueNumbers(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{
			{Issue: 26, IssueTitle: "Show observable Run context in status", RunID: "new", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC, SessionID: "backlog-new", SessionDir: "/sessions/new"},
			{
				Issue: 7, RunID: "old-active", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
				SessionID: "backlog-old-active", SessionDir: "/sessions/old-active", PID: 700,
				ProcessIdentity: "identity-700", StartedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		Leases: []scheduler.Lease{{LeaseID: "old-active", Issue: 7, RunID: "old-active"}},
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if !strings.Contains(stdout.String(), "#26  Show observable Run context in status") {
		t.Fatalf("stdout = %q, want snapshotted issue title", stdout.String())
	}
	if !strings.Contains(stdout.String(), "#7  running") {
		t.Fatalf("stdout = %q, want old active Run issue-number fallback", stdout.String())
	}
}

func TestStatusPrintsMachineReadableState(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets",
		Runs: []scheduler.Run{{
			Issue: 26, IssueTitle: "Observable context", IssueURL: "https://github.com/acme/widgets/issues/26",
			RunID: "run-26", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC,
			SessionID: "backlog-run-26", SessionDir: "/sessions/run-26",
			LogPath: "/logs/run-26.jsonl", StderrPath: "/logs/run-26.stderr.log",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--repo-dir", repository, "--state-dir", stateDir, "--json"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	var got state.State
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("status JSON: %v", err)
	}
	if got.Repo != "acme/widgets" || len(got.Runs) != 1 {
		t.Fatalf("status state = %#v", got)
	}
	run := got.Runs[0]
	if run.Issue != 26 || run.IssueTitle != "Observable context" || run.IssueURL != "https://github.com/acme/widgets/issues/26" ||
		run.LogPath != "/logs/run-26.jsonl" || run.StderrPath != "/logs/run-26.stderr.log" {
		t.Fatalf("status Run metadata = %#v", run)
	}
}

func TestStatusDoesNotMigrateV1WhileRunnerLockIsHeld(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := t.TempDir()
	statePath := filepath.Join(stateDir, "state.json")
	legacy := `{"version":1,"paused":true,"runs":[{"issue":1,"runId":"failed","status":"failed"}]}`
	if err := os.WriteFile(statePath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	lock, err := state.AcquireLock(filepath.Join(repository, ".git", legacyLockFile))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr); exit != 1 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if !strings.Contains(stderr.String(), "runner already active") {
		t.Fatalf("stderr = %q, want active runner refusal", stderr.String())
	}
	persisted, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(persisted), `"version":1`) || !strings.Contains(string(persisted), `"paused":true`) {
		t.Fatalf("status migrated state despite active runner: %s", persisted)
	}
}

func writeExecutable(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake")
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func quote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }

func TestRepositoryRejectsAlternateStateDirectoryBinding(t *testing.T) {
	t.Parallel()

	common := t.TempDir()
	first := filepath.Join(t.TempDir(), "state-a")
	second := filepath.Join(t.TempDir(), "state-b")
	if err := bindStateDirectory(common, first); err != nil {
		t.Fatalf("bind first state directory: %v", err)
	}
	for _, name := range []string{legacyStateDirectoryBindingFile, stateDirectoryBindingFile} {
		bound, ok, err := readStateDirectoryBindingFile(filepath.Join(common, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !ok || bound != first {
			t.Fatalf("%s = %q, %t, want %q, true", name, bound, ok, first)
		}
	}
	if err := bindStateDirectory(common, second); err == nil {
		t.Fatal("alternate state directory binding succeeded")
	}
}

func TestRepositoryReadsLegacyStateDirectoryBinding(t *testing.T) {
	t.Parallel()

	common := t.TempDir()
	legacy := filepath.Join(t.TempDir(), "legacy-state")
	if err := os.WriteFile(filepath.Join(common, legacyStateDirectoryBindingFile), []byte(legacy+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := repositoryStateDirectory(common, t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got != legacy {
		t.Fatalf("state directory = %q, want legacy binding %q", got, legacy)
	}
}

func TestBindStateDirectoryRepairsSingleBinding(t *testing.T) {
	for _, existing := range []string{legacyStateDirectoryBindingFile, stateDirectoryBindingFile} {
		t.Run(existing, func(t *testing.T) {
			common := t.TempDir()
			stateDir := filepath.Join(t.TempDir(), "state")
			if err := os.WriteFile(filepath.Join(common, existing), []byte(stateDir+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := bindStateDirectory(common, stateDir); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{legacyStateDirectoryBindingFile, stateDirectoryBindingFile} {
				bound, ok, err := readStateDirectoryBindingFile(filepath.Join(common, name))
				if err != nil {
					t.Fatal(err)
				}
				if !ok || bound != stateDir {
					t.Fatalf("%s = %q, %t, want %q, true", name, bound, ok, stateDir)
				}
			}
		})
	}
}

func TestRepositoryRejectsDisagreeingStateBindings(t *testing.T) {
	common := t.TempDir()
	current := filepath.Join(t.TempDir(), "current")
	legacy := filepath.Join(t.TempDir(), "legacy")
	if err := os.WriteFile(filepath.Join(common, stateDirectoryBindingFile), []byte(current+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(common, legacyStateDirectoryBindingFile), []byte(legacy+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := repositoryStateDirectory(common, t.TempDir(), ""); err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("state resolution error = %v, want disagreement", err)
	}
}

func TestStateBindingWritesLegacyFileBeforeCurrentFile(t *testing.T) {
	common := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(filepath.Join(common, stateDirectoryBindingFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeStateDirectoryBinding(common, stateDir); err == nil {
		t.Fatal("state binding succeeded with current binding path blocked")
	}
	bound, ok, err := readStateDirectoryBindingFile(filepath.Join(common, legacyStateDirectoryBindingFile))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || bound != stateDir {
		t.Fatalf("legacy binding = %q, %t, want %q, true", bound, ok, stateDir)
	}
}

func TestRepositoryLockConflictsWithLegacyRunner(t *testing.T) {
	common := t.TempDir()
	legacy, err := state.AcquireLock(filepath.Join(common, legacyLockFile))
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Release()

	lock, err := acquireRepositoryLock(common)
	if err == nil {
		_ = lock.Release()
		t.Fatal("repository lock succeeded while legacy runner lock was held")
	}
	if !strings.Contains(err.Error(), "already active") {
		t.Fatalf("lock error = %q, want active runner error", err)
	}
}

func TestRepositoryLockReleasesLegacyLockWhenCurrentLockIsHeld(t *testing.T) {
	common := t.TempDir()
	current, err := state.AcquireLock(filepath.Join(common, lockFile))
	if err != nil {
		t.Fatal(err)
	}
	defer current.Release()

	if lock, err := acquireRepositoryLock(common); err == nil {
		_ = lock.Release()
		t.Fatal("repository lock succeeded while current runner lock was held")
	}
	legacy, err := state.AcquireLock(filepath.Join(common, legacyLockFile))
	if err != nil {
		t.Fatalf("legacy lock remained held after partial acquisition: %v", err)
	}
	_ = legacy.Release()
}

func TestCommandHelpExitsSuccessfully(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"run", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if !strings.Contains(stderr.String(), "max-workers") {
		t.Fatalf("help = %q, want run flags", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), []string{"follow", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("follow help exit = %d, stderr = %q", exit, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage: backlog follow <run-id> --raw [flags]") || strings.Contains(stderr.String(), "requires a Run ID") {
		t.Fatalf("follow help = %q", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), []string{"reset", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("reset help exit = %d, stderr = %q", exit, stderr.String())
	}
	if !strings.Contains(stderr.String(), "dry-run") || strings.Contains(stderr.String(), "requires an issue number") {
		t.Fatalf("reset help = %q", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), []string{"retry", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("retry help exit = %d, stderr = %q", exit, stderr.String())
	}
	if strings.Contains(stderr.String(), "requires an issue number") {
		t.Fatalf("retry help = %q", stderr.String())
	}
}

func TestUserFacingUsageUsesBacklogName(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("help exit = %d", exit)
	}
	if !strings.Contains(stdout.String(), "backlog run") || !strings.Contains(stdout.String(), "backlog follow <run-id> --raw") || strings.Contains(stdout.String(), "pi-backlog-runner") {
		t.Fatalf("help = %q", stdout.String())
	}

	stdout.Reset()
	if exit := Main(context.Background(), []string{"retry"}, &stdout, &stderr); exit != 1 {
		t.Fatalf("retry exit = %d, want 1", exit)
	}
	if !strings.Contains(stderr.String(), "Warning: backlog retry is deprecated") ||
		!strings.Contains(stderr.String(), "usage: backlog reset") || strings.Contains(stderr.String(), "pi-backlog-runner") {
		t.Fatalf("retry error = %q", stderr.String())
	}
}

func TestSplitResetArgumentsAcceptsFlagsBeforeIssue(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"-repo-dir", "/tmp/repo", "123"},
		{"--repo-dir=/tmp/repo", "123"},
		{"--state-dir=/tmp/state", "123"},
		{"--git=/tmp/git", "123"},
		{"--gh=/tmp/gh", "123"},
	} {
		issue, flags, err := splitResetArguments(args)
		if err != nil {
			t.Fatalf("split %q: %v", args, err)
		}
		if issue != "123" {
			t.Fatalf("split %q issue = %q, want 123", args, issue)
		}
		if len(flags) == 0 {
			t.Fatalf("split %q dropped flags", args)
		}
	}
}

func TestDefaultStateDirectoryIsStableAndOutsideRepository(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repo")
	first, err := defaultStateDirectory(repository)
	if err != nil {
		t.Fatal(err)
	}
	second, err := defaultStateDirectory(repository)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || strings.HasPrefix(first, repository+string(os.PathSeparator)) {
		t.Fatalf("state directories = %q, %q for repository %q", first, second, repository)
	}
	if namespace := filepath.Base(filepath.Dir(first)); namespace != "backlog" {
		t.Fatalf("state namespace = %q, want backlog", namespace)
	}
}
