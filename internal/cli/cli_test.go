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

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestRunCommandDrainsIssueThroughFakeExecutables(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	stateDir := filepath.Join(root, "state")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":42,"title":"Build it","createdAt":"2026-01-01T00:00:00Z","url":"https://github.com/acme/widgets/issues/42"}]' ;;
  "issue view 42 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":42,"title":"Build it","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/42","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/42/comments?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/42/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-42-"*" --json number,url,state,mergedAt,autoMergeRequest,isDraft")
    printf '%s\n' '[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"MERGED","mergedAt":"2026-01-02T00:00:00Z"}]' ;;
  "issue view 42 --repo acme/widgets --json state,title,url")
    printf '%s\n' '{"state":"CLOSED","title":"Build it","url":"https://github.com/acme/widgets/issues/42"}' ;;
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
printf '%s\n' '{"type":"session"}' '{"type":"agent_start"}' '{"type":"agent_settled"}'
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
	if len(persisted.Runs) != 1 || persisted.Runs[0].Status != scheduler.StatusMerged {
		t.Fatalf("state runs = %#v, want merged issue", persisted.Runs)
	}
	if _, err := os.Stat(persisted.Runs[0].Worktree); !os.IsNotExist(err) {
		t.Fatalf("successful worktree still exists, stat error = %v", err)
	}
	if !strings.Contains(stdout.String(), "verified merged completion for issue #42") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRetryRemovesOnlyInterventionRequiredLease(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion,
		Runs:    []scheduler.Run{{Issue: 42, RunID: "old", Status: scheduler.StatusFailed, Worktree: "/retained"}},
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"retry", "42", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Runs) != 0 {
		t.Fatalf("runs = %#v, want lease removed", got.Runs)
	}
	if !strings.Contains(stdout.String(), "retained") {
		t.Fatalf("stdout = %q, want retained-worktree notice", stdout.String())
	}
}

func TestStatusPrintsMachineReadableState(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Repo: "acme/widgets"}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--state-dir", stateDir, "--json"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	var got state.State
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("status JSON: %v", err)
	}
	if got.Repo != "acme/widgets" {
		t.Fatalf("repo = %q", got.Repo)
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

func TestCommandHelpExitsSuccessfully(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"run", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if !strings.Contains(stderr.String(), "max-workers") {
		t.Fatalf("help = %q, want run flags", stderr.String())
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
}
