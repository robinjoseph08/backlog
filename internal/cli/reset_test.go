package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestResetHelpShowsRequiredDryRunUsage(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"reset", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage: backlog reset <issue-number> --dry-run [flags]") ||
		!strings.Contains(stderr.String(), "accepted for dry-run compatibility; has no effect") {
		t.Fatalf("help = %q", stderr.String())
	}
}

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

	git := githubGit(t)
	beforeState := fileDigest(t, store.Path)
	beforeGit := gitSnapshot(t, repository)
	beforeGitEntries := directoryEntries(t, filepath.Join(repository, ".git"))
	beforeGitHub := fileDigest(t, githubState)
	beforeFilesystem := directoryEntries(t, root)

	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), []string{
		"reset", "42", "--dry-run", "--yes", "--repo-dir", repository, "--state-dir", stateDir, "--git", git, "--gh", gh,
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
	if err := os.WriteFile(filepath.Join(worktreePath, "work.txt"), []byte("unfinished and dirty\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(worktreePath, "untracked"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktreePath, "untracked", "sentinel.txt"), []byte("preserve me\n"), 0o600); err != nil {
		t.Fatal(err)
	}

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
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":99,"url":"https://github.com/acme/widgets/pull/99","state":"OPEN","mergedAt":null,"autoMergeRequest":{"mergeMethod":"SQUASH"},"isDraft":false,"headRefName":"`+branch+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)

	binary := buildExecutable(t, root)
	git := githubGit(t)
	beforeState := fileDigest(t, statePath)
	beforeSession := fileDigest(t, sessionPath)
	beforeGitHub := fileDigest(t, githubState)
	beforeRefs := gitSnapshot(t, repository)
	beforeRemoteRefs := gitSnapshot(t, remote)
	beforeWorktrees := gitOutput(t, repository, "worktree", "list", "--porcelain")
	beforeWorktreeFilesystem := filesystemSnapshot(t, worktreePath)

	command := exec.Command(binary, "reset", "42", "--dry-run", "--repo-dir", repository, "--state-dir", stateDir, "--git", git, "--gh", gh)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled reset: %v\n%s", err, output)
	}
	planOutput := string(output)
	wantActions := "Required actions:\n" +
		"  1. disable auto-merge for pull request #99 (https://github.com/acme/widgets/pull/99)\n" +
		"  2. close unmerged pull request #99 (https://github.com/acme/widgets/pull/99)\n" +
		"  3. delete remote branch " + branch + " at " + strings.TrimSpace(gitOutput(t, repository, "rev-parse", "refs/remotes/origin/"+branch)) + "\n" +
		"  4. remove local worktree " + worktreePath + " for " + branch + " at " + strings.TrimSpace(gitOutput(t, repository, "rev-parse", branch)) + "\n" +
		"  5. delete local branch " + branch + " at " + strings.TrimSpace(gitOutput(t, repository, "rev-parse", branch)) + "\n" +
		"  6. retire Pi session " + sessionID + " in " + sessionDir + "\n" +
		"  7. remove issue label in-progress from https://github.com/acme/widgets/issues/42\n" +
		"  8. add issue label ready-for-agent to https://github.com/acme/widgets/issues/42\n" +
		"  9. mark Run " + runID + " reset and release Lease lease-rich\n" +
		"Dry-run: no changes made.\n"
	if !strings.Contains(planOutput, wantActions) {
		t.Fatalf("actions block =\n%s\nwant block =\n%s", planOutput, wantActions)
	}
	if fileDigest(t, statePath) != beforeState || fileDigest(t, sessionPath) != beforeSession || fileDigest(t, githubState) != beforeGitHub {
		t.Fatal("dry-run changed state, session, or GitHub resources")
	}
	if gitSnapshot(t, repository) != beforeRefs || gitSnapshot(t, remote) != beforeRemoteRefs || gitOutput(t, repository, "worktree", "list", "--porcelain") != beforeWorktrees {
		t.Fatal("dry-run changed local or remote Git resources")
	}
	if got := filesystemSnapshot(t, worktreePath); got != beforeWorktreeFilesystem {
		t.Fatalf("dry-run changed worktree filesystem:\n%s\nwant:\n%s", got, beforeWorktreeFilesystem)
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
	git := githubGit(t)
	before := fileDigest(t, store.Path)
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"reset", "9", "--dry-run", "--repo-dir", repository, "--state-dir", stateDir, "--git", git, "--gh", gh}, &stdout, &stderr); exit == 0 {
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

func TestInspectOriginRepositoryAcceptsOwnedGitHubURLsAndRefusesUnknownRemotes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		remote string
		want   string
		ok     bool
	}{
		{name: "SSH", remote: "git@github.com:acme/widgets.git", want: "acme/widgets", ok: true},
		{name: "HTTPS", remote: "https://github.com/acme/widgets.git", want: "acme/widgets", ok: true},
		{name: "wrong host", remote: "git@example.test:acme/widgets.git"},
		{name: "local path", remote: "/tmp/widgets.git"},
		{name: "extra path", remote: "https://github.com/acme/extra/widgets.git"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			git := writeExecutable(t, "#!/bin/sh\nprintf '%s\\n' "+quote(test.remote)+"\n")
			got, err := inspectOriginRepository(context.Background(), git, t.TempDir())
			if test.ok && (err != nil || got != test.want) {
				t.Fatalf("repository = %q, error = %v", got, err)
			}
			if !test.ok && err == nil {
				t.Fatalf("unknown remote %q produced %q", test.remote, got)
			}
		})
	}
}

func TestInspectLocalResourcesRefusesUnknownAndReplacedWorktrees(t *testing.T) {
	t.Parallel()

	run := scheduler.Run{RunID: "run-4", Branch: "agent/issue-4-run-4", Worktree: filepath.Join(t.TempDir(), "worktree")}
	unknownGit := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" show-ref --verify --hash refs/heads/agent/issue-4-run-4") exit 2 ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac
`)
	if _, _, err := inspectLocalResources(context.Background(), unknownGit, t.TempDir(), t.TempDir(), run); err == nil || !strings.Contains(err.Error(), "unknown output") {
		t.Fatalf("unknown local branch error = %v", err)
	}

	repository := filepath.Join(t.TempDir(), "repo")
	runGit(t, t.TempDir(), "init", "-b", "main", repository)
	runGit(t, repository, "config", "user.name", "Reset Test")
	runGit(t, repository, "config", "user.email", "reset@example.test")
	if err := os.WriteFile(filepath.Join(repository, "tracked"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked")
	runGit(t, repository, "commit", "-m", "base")
	worktreePath := filepath.Join(t.TempDir(), "owned")
	branch := "agent/issue-4-run-4"
	runGit(t, repository, "worktree", "add", "-b", branch, worktreePath, "HEAD")
	if err := os.RemoveAll(worktreePath); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(worktreePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktreePath, "foreign"), []byte("not a worktree\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	commonDirectory, err := gitCommonDirectory(context.Background(), "git", repository)
	if err != nil {
		t.Fatal(err)
	}
	run = scheduler.Run{RunID: "run-4", Branch: branch, Worktree: worktreePath}
	if _, _, err := inspectLocalResources(context.Background(), "git", repository, commonDirectory, run); err == nil || !strings.Contains(err.Error(), "verify worktree") {
		t.Fatalf("replaced worktree error = %v", err)
	}
}

func TestInspectSessionRefusesMismatchedIdentityAndMissingContinuation(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		header string
	}{
		{name: "wrong session ID", header: `{"type":"session","id":"other","cwd":"WORKTREE"}`},
		{name: "wrong worktree", header: `{"type":"session","id":"backlog-run-4","cwd":"/other"}`},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			worktree := filepath.Join(root, "worktree")
			sessionDir := filepath.Join(root, "session")
			if err := os.MkdirAll(sessionDir, 0o700); err != nil {
				t.Fatal(err)
			}
			header := strings.ReplaceAll(test.header, "WORKTREE", worktree)
			if err := os.WriteFile(filepath.Join(sessionDir, "session.jsonl"), []byte(header+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			run := scheduler.Run{RunID: "run-4", WorkerMode: scheduler.WorkerModeRPC, Worktree: worktree, SessionID: "backlog-run-4", SessionDir: sessionDir}
			if _, err := inspectSession(run); err == nil || !strings.Contains(err.Error(), "identity does not match") {
				t.Fatalf("error = %v", err)
			}
		})
	}

	root := t.TempDir()
	sessionDir := filepath.Join(root, "session")
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(root, "worktree")
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	if err := os.WriteFile(sessionFile, []byte(`{"type":"session","id":"backlog-run-4","cwd":"`+worktree+`"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		RunID: "run-4", WorkerMode: scheduler.WorkerModeRPC, Worktree: worktree, SessionID: "backlog-run-4", SessionDir: sessionDir,
		Continuation: &scheduler.ContinuationBoundary{SessionFile: filepath.Join(sessionDir, "missing.jsonl")},
	}
	if _, err := inspectSession(run); err == nil || !strings.Contains(err.Error(), "continuation file") {
		t.Fatalf("error = %v", err)
	}
}

func TestResetDryRunRefusesLiveWorker(t *testing.T) {
	t.Parallel()

	run := scheduler.Run{Issue: 1, RunID: "live", PID: os.Getpid(), ProcessIdentity: "not-this-process"}
	if err := inspectWorkerAbsent(run); err == nil || (!strings.Contains(err.Error(), "uncertain identity") && !strings.Contains(err.Error(), "liveness is uncertain")) {
		t.Fatalf("live or uncertain Worker error = %v", err)
	}
}

func TestResetCommandRefusesLiveWorkerBeforeGitHubInspection(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	identity, err := resetPIDIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 42, RunID: "run-live", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint,
			PID: os.Getpid(), ProcessIdentity: identity,
		}},
		Leases: []scheduler.Lease{{LeaseID: "lease-live", Issue: 42, RunID: "run-live"}},
	}); err != nil {
		t.Fatal(err)
	}
	githubCalled := filepath.Join(t.TempDir(), "called")
	gh := writeExecutable(t, "#!/bin/sh\ntouch "+quote(githubCalled)+"\nexit 9\n")
	before := fileDigest(t, store.Path)
	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), []string{
		"reset", "42", "--dry-run", "--repo-dir", repository, "--state-dir", stateDir, "--gh", gh,
	}, &stdout, &stderr)
	if exit == 0 || (!strings.Contains(stderr.String(), "live") && !strings.Contains(stderr.String(), "liveness is uncertain")) {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("unsafe plan printed: %q", stdout.String())
	}
	if _, err := os.Stat(githubCalled); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GitHub was inspected before Worker refusal: %v", err)
	}
	if got := fileDigest(t, store.Path); got != before {
		t.Fatal("Worker refusal changed state")
	}
}

func TestResetReadLockCoordinatesWithRepositoryRunnerLock(t *testing.T) {
	commonDirectory := t.TempDir()
	runnerLock, err := acquireRepositoryLock(commonDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if resetLock, err := acquireResetReadLock(commonDirectory); err == nil {
		_ = resetLock.Release()
		t.Fatal("Reset acquired coordination lock while runner lock was held")
	}
	if err := runnerLock.Release(); err != nil {
		t.Fatal(err)
	}

	resetLock, err := acquireResetReadLock(commonDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if runnerLock, err := acquireRepositoryLock(commonDirectory); err == nil {
		_ = runnerLock.Release()
		t.Fatal("runner acquired coordination lock while Reset lock was held")
	}
	if err := resetLock.Release(); err != nil {
		t.Fatal(err)
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

func githubGit(t *testing.T) string {
	t.Helper()
	return writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" remote get-url origin") printf '%s\n' 'git@github.com:acme/widgets.git' ;;
  *) exec git "$@" ;;
esac
`)
}

func filesystemSnapshot(t *testing.T, root string) string {
	t.Helper()
	var snapshot bytes.Buffer
	err := filepath.WalkDir(root, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(&snapshot, "%s %s", relative, info.Mode())
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(&snapshot, " -> %s", target)
		case info.Mode().IsRegular():
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(&snapshot, " %x", sha256.Sum256(content))
		}
		snapshot.WriteByte('\n')
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.String()
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
