package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestResetDryRunPrintsPlanWithoutChangingResources(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := filepath.Join(root, "state")
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs:   []scheduler.Run{{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "lease-42", Issue: 42, RunID: "run-42"}},
	}); err != nil {
		t.Fatal(err)
	}
	githubState := filepath.Join(root, "github.json")
	if err := os.WriteFile(githubState, []byte(`{"state":"OPEN","labels":["spec"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    state=$(jq -r .state `+quote(githubState)+`)
    labels=$(jq -c '[.labels[] | {name:.}]' `+quote(githubState)+`)
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"%s","labels":%s}\n' "$state" "$labels" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)

	beforeState := fileDigest(t, store.Path)
	beforeGit := gitSnapshot(t, repository)
	beforeGitEntries := directoryEntries(t, filepath.Join(repository, ".git"))
	beforeGitHub := fileDigest(t, githubState)
	beforeFilesystem := directoryEntries(t, root)

	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), []string{
		"reset", "42", "--dry-run", "--yes", "--repo-dir", repository, "--state-dir", stateDir, "--gh", gh,
	}, &stdout, &stderr)
	if exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "Reset Plan for issue #42") ||
		!strings.Contains(output, "add issue label ready-for-agent") ||
		!strings.Contains(output, "mark Run run-42 reset and release Lease lease-42") ||
		!strings.Contains(output, "Dry-run: no changes made.") {
		t.Fatalf("stdout = %q", output)
	}
	if strings.Contains(output, "delete remote branch") || strings.Contains(output, "retire Pi session") {
		t.Fatalf("plan included already-absent resources: %q", output)
	}
	if got := fileDigest(t, store.Path); got != beforeState {
		t.Fatalf("state changed: %x != %x", got, beforeState)
	}
	if got := gitSnapshot(t, repository); got != beforeGit {
		t.Fatalf("Git state changed: %q != %q", got, beforeGit)
	}
	if got := directoryEntries(t, filepath.Join(repository, ".git")); strings.Join(got, "\n") != strings.Join(beforeGitEntries, "\n") {
		t.Fatalf("Git filesystem entries changed: %v != %v", got, beforeGitEntries)
	}
	if got := fileDigest(t, githubState); got != beforeGitHub {
		t.Fatalf("GitHub state changed: %x != %x", got, beforeGitHub)
	}
	if got := directoryEntries(t, root); strings.Join(got, "\n") != strings.Join(beforeFilesystem, "\n") {
		t.Fatalf("filesystem resources changed: %v != %v", got, beforeFilesystem)
	}
}

func TestResetDryRunInspectsEveryOwnedResourceWithoutMutation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	remote := filepath.Join(root, "remote.git")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", "-b", "main", repository)
	runGit(t, repository, "config", "user.name", "Reset Test")
	runGit(t, repository, "config", "user.email", "reset@example.test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "README.md")
	runGit(t, repository, "commit", "-m", "base")
	runGit(t, repository, "remote", "add", "origin", remote)
	runGit(t, repository, "push", "-u", "origin", "main")

	stateDir := filepath.Join(root, "state")
	runID := "run-rich"
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
	sessionPath := filepath.Join(sessionDir, "session.jsonl")
	sessionHeader := `{"type":"session","id":"` + sessionID + `","cwd":"` + worktreePath + `"}` + "\n"
	if err := os.WriteFile(sessionPath, []byte(sessionHeader), 0o600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, "state.json")
	store := state.FileStore{Path: statePath}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 42, RunID: runID, Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
			Branch: branch, Worktree: worktreePath, SessionID: sessionID, SessionDir: sessionDir,
			PullRequest: "https://github.com/acme/widgets/pull/99",
		}},
		Leases: []scheduler.Lease{{LeaseID: "lease-rich", Issue: 42, RunID: runID}},
	}); err != nil {
		t.Fatal(err)
	}
	githubState := filepath.Join(root, "github-rich.json")
	if err := os.WriteFile(githubState, []byte(`{"issue":"OPEN","labels":["in-progress","spec"],"pr":"OPEN","autoMerge":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner")
    printf '%s\n' '[{"number":99,"url":"https://github.com/acme/widgets/pull/99","state":"OPEN","mergedAt":null,"autoMergeRequest":{"mergeMethod":"SQUASH"},"isDraft":false,"headRefName":"`+branch+`","headRepositoryOwner":{"login":"acme"}}]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)

	binary := buildExecutable(t, root)
	beforeState := fileDigest(t, statePath)
	beforeSession := fileDigest(t, sessionPath)
	beforeGitHub := fileDigest(t, githubState)
	beforeRefs := gitSnapshot(t, repository)
	beforeRemoteRefs := gitSnapshot(t, remote)
	beforeWorktrees := gitOutput(t, repository, "worktree", "list", "--porcelain")

	command := exec.Command(binary, "reset", "42", "--dry-run", "--repo-dir", repository, "--state-dir", stateDir, "--gh", gh)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled reset: %v\n%s", err, output)
	}
	planOutput := string(output)
	for _, required := range []string{
		"disable auto-merge for pull request #99", "close unmerged pull request #99", "delete remote branch " + branch,
		"remove local worktree " + worktreePath, "delete local branch " + branch, "retire Pi session " + sessionID,
		"remove issue label in-progress", "add issue label ready-for-agent", "mark Run " + runID + " reset and release Lease lease-rich",
	} {
		if !strings.Contains(planOutput, required) {
			t.Fatalf("plan omitted %q:\n%s", required, planOutput)
		}
	}
	if fileDigest(t, statePath) != beforeState || fileDigest(t, sessionPath) != beforeSession || fileDigest(t, githubState) != beforeGitHub {
		t.Fatal("dry-run changed state, session, or GitHub resources")
	}
	if gitSnapshot(t, repository) != beforeRefs || gitSnapshot(t, remote) != beforeRemoteRefs || gitOutput(t, repository, "worktree", "list", "--porcelain") != beforeWorktrees {
		t.Fatal("dry-run changed local or remote Git resources")
	}
	if _, err := os.Stat(worktreePath); err != nil {
		t.Fatalf("dry-run changed filesystem worktree: %v", err)
	}
}

func TestResetDryRunRefusesUnknownGitHubStateWithoutChangingState(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 9, RunID: "run-9", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "lease-9", Issue: 9, RunID: "run-9"}},
	}); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  *) echo 'GitHub unavailable' >&2; exit 1 ;;
esac
`)
	before := fileDigest(t, store.Path)
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"reset", "9", "--dry-run", "--repo-dir", repository, "--state-dir", stateDir, "--gh", gh}, &stdout, &stderr); exit == 0 {
		t.Fatalf("unknown GitHub state was accepted: %q", stdout.String())
	}
	if got := fileDigest(t, store.Path); got != before {
		t.Fatal("refused dry-run changed state")
	}
}

func TestResetInspectionDistinguishesAbsentFromUnknownResources(t *testing.T) {
	t.Parallel()

	run := scheduler.Run{
		Issue: 4, RunID: "run-4", WorkerMode: scheduler.WorkerModeRPC,
		Branch: "agent/issue-4-run-4", Worktree: filepath.Join(t.TempDir(), "worktree"),
		SessionID: "backlog-run-4", SessionDir: filepath.Join(t.TempDir(), "missing-session"),
	}
	session, err := inspectSession(run)
	if err != nil || session.Present {
		t.Fatalf("absent session = %#v, %v", session, err)
	}
	if err := os.MkdirAll(run.SessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run.SessionDir, "unknown.txt"), []byte("unknown"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectSession(run); err == nil || !strings.Contains(err.Error(), "unknown resource") {
		t.Fatalf("unknown session error = %v", err)
	}

	absentGit := writeExecutable(t, "#!/bin/sh\nexit 2\n")
	branch, err := inspectRemoteBranch(context.Background(), absentGit, t.TempDir(), run.Branch)
	if err != nil || branch.Present {
		t.Fatalf("absent remote branch = %#v, %v", branch, err)
	}
	unknownGit := writeExecutable(t, "#!/bin/sh\necho uncertain >&2\nexit 2\n")
	if _, err := inspectRemoteBranch(context.Background(), unknownGit, t.TempDir(), run.Branch); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown remote branch error = %v", err)
	}
}

func TestResetDryRunRefusesLiveWorker(t *testing.T) {
	t.Parallel()

	run := scheduler.Run{Issue: 1, RunID: "live", PID: os.Getpid(), ProcessIdentity: "not-this-process"}
	if err := inspectWorkerAbsent(run); err == nil || (!strings.Contains(err.Error(), "uncertain identity") && !strings.Contains(err.Error(), "liveness is uncertain")) {
		t.Fatalf("live or uncertain Worker error = %v", err)
	}
}

func fileDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(data)
}

func gitSnapshot(t *testing.T, repository string) string {
	t.Helper()
	return gitOutput(t, repository, "for-each-ref", "--format=%(refname) %(objectname)")
}

func gitOutput(t *testing.T, repository string, args ...string) string {
	t.Helper()
	commandArgs := append([]string{"-C", repository}, args...)
	command := exec.Command("git", commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(commandArgs, " "), err, output)
	}
	return string(output)
}

func runGit(t *testing.T, repository string, args ...string) {
	t.Helper()
	_ = gitOutput(t, repository, args...)
}

func directoryEntries(t *testing.T, root string) []string {
	t.Helper()
	var entries []string
	err := filepath.WalkDir(root, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative != "." {
			entries = append(entries, relative)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
