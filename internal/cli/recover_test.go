package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestRecoverHelpDescribesFailClosedLifecycle(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"recover", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	for _, want := range []string{
		"Usage: backlog recover <run-id|positive-issue-number> [flags]",
		"durable leaf/hash", "workflow checkpoint", "Suspended", "Dry-run is read-only",
		"Interactive confirmation defaults to no", "Non-interactive mutation requires --yes",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("help omitted %q: %q", want, stderr.String())
		}
	}
}

func TestCompiledRecoverDryRunAndIdempotentMutationPreserveRunIdentities(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	remote := filepath.Join(root, "remote.git")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", "-b", "main", repository)
	runGit(t, repository, "config", "user.name", "Recovery Test")
	runGit(t, repository, "config", "user.email", "recovery@example.test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "README.md")
	runGit(t, repository, "commit", "-m", "base")
	runGit(t, repository, "remote", "add", "origin", remote)
	runGit(t, repository, "push", "-u", "origin", "main")

	stateDir := filepath.Join(root, "state")
	runID := "recover-42"
	branch := "agent/issue-42-" + runID
	worktreePath := filepath.Join(stateDir, "worktrees", "issue-42-"+runID)
	if err := os.MkdirAll(filepath.Dir(worktreePath), 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "worktree", "add", "-b", branch, worktreePath, "HEAD")
	if err := os.WriteFile(filepath.Join(worktreePath, "work.txt"), []byte("unfinished\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worktreePath, "add", "work.txt")
	runGit(t, worktreePath, "commit", "-m", "unfinished")
	runGit(t, repository, "push", "origin", branch)

	sessionID := "backlog-" + runID
	sessionDir := filepath.Join(stateDir, "sessions", runID)
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	session := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":%q,\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"continue\"}}\n", sessionID, worktreePath)
	if err := os.WriteFile(sessionFile, []byte(session), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	started := time.Now().Add(-time.Hour).UTC()
	original := scheduler.Run{
		Issue: 42, RunID: runID, Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
		Branch: branch, Worktree: worktreePath, SessionName: "afk #42", SessionID: sessionID, SessionDir: sessionDir,
		LogPath: filepath.Join(stateDir, "logs", runID+".jsonl"), StderrPath: filepath.Join(stateDir, "logs", runID+".stderr.log"),
		Error: "validation failure retained for manual Recovery", FailureClass: scheduler.FailureValidation, StartedAt: started, UpdatedAt: started,
	}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{original}, Leases: []scheduler.Lease{{LeaseID: "lease-42", Issue: 42, RunID: runID}},
	}); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	binary := buildExecutable(t, root)
	command := func(arguments ...string) string {
		base := []string{"recover", runID, "--repo-dir", repository, "--state-dir", stateDir, "--gh", gh}
		cmd := exec.Command(binary, append(base, arguments...)...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("compiled recover %v: %v\n%s", arguments, err, output)
		}
		return string(output)
	}

	before := fileDigest(t, store.Path)
	output := command("--dry-run")
	for _, want := range []string{"Recovery Plan for Run " + runID, "Outcome: suspend", "afk stage afk-coordinator", "Dry-run: no changes made."} {
		if !strings.Contains(output, want) {
			t.Fatalf("dry-run omitted %q:\n%s", want, output)
		}
	}
	if after := fileDigest(t, store.Path); after != before {
		t.Fatalf("dry-run changed state: %x != %x", after, before)
	}

	output = command("--yes")
	if !strings.Contains(output, "Recovery complete: Run "+runID+" is Suspended") {
		t.Fatalf("mutation output = %q", output)
	}
	current, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	got := current.Runs[0]
	if got.Status != scheduler.StatusSuspended || got.RunID != original.RunID || got.Branch != original.Branch || got.Worktree != original.Worktree || got.SessionID != original.SessionID || got.LogPath != original.LogPath || got.StderrPath != original.StderrPath || got.Error != original.Error || got.Continuation == nil || got.RecoveryCount != 1 || len(current.Leases) != 1 {
		t.Fatalf("recovered state = %#v, leases = %#v", got, current.Leases)
	}
	output = command("--yes")
	if !strings.Contains(output, "Recovery complete") {
		t.Fatalf("idempotent output = %q", output)
	}
	current, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].RecoveryCount != 1 || len(current.Leases) != 1 {
		t.Fatalf("idempotent Recovery changed metadata: %#v", current)
	}
}
