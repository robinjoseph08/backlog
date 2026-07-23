package cli

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestCompiledExecutableMigratesV1StatusAndReconcilesStartup(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := filepath.Join(root, "backlog")
	build := exec.Command("go", "build", "-o", binary, "./cmd/backlog")
	build.Dir = filepath.Clean(filepath.Join("..", ".."))
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build compiled acceptance executable: %v\n%s", err, output)
	}

	stateDir := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, "state.json")
	worktreePath := filepath.Join(root, "retained-worktree")
	legacy := `{
  "version": 1,
  "repo": "acme/widgets",
  "defaultBranch": "main",
  "maxConcurrentIssues": 1,
  "paused": true,
  "runs": [
    {
      "issue": 7,
      "runId": "old-merged",
      "status": "merged",
      "branch": "agent/issue-7-old-merged",
      "worktree": "/retained/merged-worktree",
      "logPath": "/retained/merged.jsonl",
      "pullRequest": "https://example.test/pull/7",
      "error": "retained merged diagnostic",
      "startedAt": "2026-06-01T00:00:00Z",
      "updatedAt": "2026-06-02T00:00:00Z",
      "completedAt": "2026-06-03T00:00:00Z"
    },
    {
      "issue": 42,
      "runId": "legacy-running",
      "status": "running",
      "pid": 2147483646,
      "processIdentity": "2147483646:legacy",
      "branch": "agent/issue-42-legacy-running",
      "worktree": "` + worktreePath + `",
      "sessionName": "afk #42",
      "logPath": "/retained/legacy.jsonl",
      "stderrPath": "/retained/legacy.stderr.log",
      "pullRequest": "https://example.test/pull/42",
      "error": "legacy diagnostic",
      "startedAt": "2026-07-01T00:00:00Z",
      "updatedAt": "2026-07-02T00:00:00Z"
    }
  ]
}`
	if err := os.WriteFile(statePath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}

	statusCommand := exec.Command(binary, "status", "--repo-dir", repository, "--state-dir", stateDir, "--json")
	statusOutput, err := statusCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled status after upgrade: %v\n%s", err, statusOutput)
	}
	var upgraded state.State
	if err := json.Unmarshal(statusOutput, &upgraded); err != nil {
		t.Fatalf("decode compiled status: %v\n%s", err, statusOutput)
	}
	if upgraded.Version != state.CurrentVersion || len(upgraded.Runs) != 2 || len(upgraded.Leases) != 1 {
		t.Fatalf("upgraded status = %#v", upgraded)
	}
	if upgraded.Leases[0].RunID != "legacy-running" || upgraded.Runs[0].WorkerMode != scheduler.WorkerModePrint || upgraded.Runs[1].WorkerMode != scheduler.WorkerModePrint {
		t.Fatalf("upgraded worker and Lease metadata = %#v / %#v", upgraded.Runs, upgraded.Leases)
	}
	persistedAfterStatus, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(persistedAfterStatus), `"paused"`) || strings.Contains(string(persistedAfterStatus), `"continuation"`) {
		t.Fatalf("legacy paused state or implied continuation survived migration: %s", persistedAfterStatus)
	}

	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-42-legacy-running --json number,url,state,mergedAt,autoMergeRequest,isDraft")
    printf '%s\n' '[{"number":42,"url":"https://example.test/pull/42","state":"MERGED","mergedAt":"2026-07-03T00:00:00Z"}]' ;;
  "issue view 42 --repo acme/widgets --json state,title,url")
    printf '%s\n' '{"state":"CLOSED","title":"Migrated","url":"https://example.test/issues/42"}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "-C `+repository+` rev-parse --show-toplevel") printf '%s\n' `+quote(repository)+` ;;
  "-C `+repository+` rev-parse --git-common-dir") printf '%s\n' `+quote(filepath.Join(repository, ".git"))+` ;;
  "-C `+repository+` worktree prune") ;;
  "-C `+repository+` show-ref --verify --quiet refs/heads/agent/issue-42-legacy-running") exit 1 ;;
  *) echo "unexpected git: $*" >&2; exit 9 ;;
esac
`)
	pi := writeExecutable(t, "#!/bin/sh\necho 'legacy print-mode Run must not be resumed' >&2\nexit 9\n")

	runCommand := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	runOutput, err := runCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled startup reconciliation after upgrade: %v\n%s", err, runOutput)
	}
	final, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Runs) != 2 || len(final.Leases) != 0 {
		t.Fatalf("reconciled Runs/Leases = %#v/%#v", final.Runs, final.Leases)
	}
	if final.Runs[0].RunID != "old-merged" || final.Runs[0].Error != "retained merged diagnostic" {
		t.Fatalf("existing merged history changed: %#v", final.Runs[0])
	}
	reconciled := final.Runs[1]
	if reconciled.RunID != "legacy-running" || reconciled.Status != scheduler.StatusMerged || reconciled.WorkerMode != scheduler.WorkerModePrint ||
		reconciled.Branch != "agent/issue-42-legacy-running" || reconciled.Worktree != worktreePath || reconciled.SessionName != "afk #42" ||
		reconciled.LogPath != "/retained/legacy.jsonl" || reconciled.StderrPath != "/retained/legacy.stderr.log" || reconciled.PullRequest != "https://example.test/pull/42" {
		t.Fatalf("startup reconciliation lost migrated artifacts: %#v", reconciled)
	}
}
