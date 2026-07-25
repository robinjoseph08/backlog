package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/reset"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestResetHelpShowsMutationAndDryRunFlags(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"reset", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	for _, text := range []string{
		"Usage: backlog reset <issue-number> [flags]",
		"confirm Reset without an interactive prompt",
		"retires its",
		"active Pi session artifacts",
		"preserves logs and history",
		"idempotent",
		"deprecated retry command",
		"Exit statuses: 0 success or interactive cancellation; 1 refusal or failure.",
	} {
		if !strings.Contains(stderr.String(), text) {
			t.Fatalf("help omitted %q: %q", text, stderr.String())
		}
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
	if strings.Contains(output, "delete remote branch") || strings.Contains(output, "archive Pi session") {
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

func TestCompiledResetCoversFullOwnedArtifactLifecycle(t *testing.T) {
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
	logsDir := filepath.Join(stateDir, "logs")
	if err := os.MkdirAll(logsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logsDir, runID+".jsonl")
	stderrPath := filepath.Join(logsDir, runID+".stderr.log")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_settled\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stderrPath, []byte("preserved diagnostic log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, "state.json")
	store := state.FileStore{Path: statePath}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 42, IssueTitle: "Rich Reset", IssueURL: "https://github.com/acme/widgets/issues/42",
			RunID: runID, Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
			Branch: branch, Worktree: worktreePath, SessionID: sessionID, SessionDir: sessionDir,
			LogPath: logPath, StderrPath: stderrPath, PullRequest: "https://github.com/acme/widgets/pull/99", Error: "preserved diagnostic",
		}},
		Leases: []scheduler.Lease{{LeaseID: "lease-rich", Issue: 42, RunID: runID}},
	}); err != nil {
		t.Fatal(err)
	}
	githubState := filepath.Join(root, "github-rich.json")
	head := strings.TrimSpace(gitOutput(t, repository, "rev-parse", "refs/remotes/origin/"+branch))
	githubInitial := fmt.Sprintf(`{"issue":"OPEN","labels":["in-progress","spec"],"pr":"OPEN","autoMerge":true,"comments":[],"head":%q}`, head)
	if err := os.WriteFile(githubState, []byte(githubInitial), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(githubState)+`
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    issue=$(jq -r .issue "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"%s","labels":%s}\n' "$issue" "$labels" ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    jq -c --arg branch `+quote(branch)+` '[{number:99,url:"https://github.com/acme/widgets/pull/99",state:.pr,mergedAt:null,autoMergeRequest:(if .autoMerge then {mergeMethod:"SQUASH"} else null end),isDraft:false,headRefName:$branch,headRefOid:.head,headRepositoryOwner:{login:"acme"},headRepository:{nameWithOwner:"acme/widgets"}}]' "$state" ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/99/comments?per_page=100 --paginate --slurp")
    jq -c '[.comments | map({body:.})]' "$state" ;;
  "pr merge 99 --repo acme/widgets --disable-auto")
    temporary="$state.tmp"; jq '.autoMerge=false' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  pr\ comment\ 99\ --repo\ acme/widgets\ --body\ *)
    body=''; for value in "$@"; do body=$value; done
    temporary="$state.tmp"; jq --arg body "$body" '.comments += [$body]' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "pr close 99 --repo acme/widgets")
    temporary="$state.tmp"; jq '.pr="CLOSED"' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary="$state.tmp"; jq '.labels = [.labels[] | select(. != "in-progress")]' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --add-label ready-for-agent")
    temporary="$state.tmp"; jq '.labels += ["ready-for-agent"] | .labels |= unique' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)

	binary := buildExecutable(t, root)
	git := githubGit(t)
	beforeState := fileDigest(t, statePath)
	beforeSession := fileDigest(t, sessionPath)
	beforeGitHub := fileDigest(t, githubState)
	beforeLog := fileDigest(t, logPath)
	beforeStderr := fileDigest(t, stderrPath)
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
		"  1. mark Run " + runID + " resetting while retaining Lease lease-rich\n" +
		"  2. disable auto-merge for pull request #99 (https://github.com/acme/widgets/pull/99)\n" +
		"  3. explain Reset on pull request #99 (https://github.com/acme/widgets/pull/99)\n" +
		"  4. close unmerged pull request #99 (https://github.com/acme/widgets/pull/99)\n" +
		"  5. delete remote branch " + branch + " at " + strings.TrimSpace(gitOutput(t, repository, "rev-parse", "refs/remotes/origin/"+branch)) + "\n" +
		"  6. remove local worktree " + worktreePath + " for " + branch + " at " + strings.TrimSpace(gitOutput(t, repository, "rev-parse", branch)) + "\n" +
		"  7. delete local branch " + branch + " at " + strings.TrimSpace(gitOutput(t, repository, "rev-parse", branch)) + "\n" +
		"  8. archive Pi session " + sessionID + " from " + sessionDir + " to " + filepath.Join(stateDir, "history", "sessions", runID) + "\n" +
		"  9. remove issue label in-progress from https://github.com/acme/widgets/issues/42\n" +
		"  10. add issue label ready-for-agent to https://github.com/acme/widgets/issues/42\n" +
		"  11. mark Run " + runID + " reset and release Lease lease-rich\n" +
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

	mutation := exec.Command(binary, "reset", "42", "--yes", "--repo-dir", repository, "--state-dir", stateDir, "--git", git, "--gh", gh)
	mutationOutput, mutationErr := mutation.CombinedOutput()
	if mutationErr != nil {
		t.Fatalf("compiled full Reset: %v\n%s", mutationErr, mutationOutput)
	}
	if !strings.Contains(string(mutationOutput), "Reset complete for issue #42") {
		t.Fatalf("mutation output = %q", mutationOutput)
	}
	current, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || len(current.Leases) != 0 || current.Runs[0].Status != scheduler.StatusReset {
		t.Fatalf("final historical state = %#v", current)
	}
	historical := current.Runs[0]
	if historical.IssueTitle != "Rich Reset" || historical.IssueURL != "https://github.com/acme/widgets/issues/42" ||
		historical.LogPath != logPath || historical.StderrPath != stderrPath || historical.Error != "preserved diagnostic" || historical.PullRequest != "https://github.com/acme/widgets/pull/99" {
		t.Fatalf("historical metadata changed: %#v", historical)
	}
	if fileDigest(t, logPath) != beforeLog || fileDigest(t, stderrPath) != beforeStderr {
		t.Fatal("Worker logs changed during finalization")
	}
	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree survived Reset: %v", err)
	}
	if output, err := exec.Command("git", "-C", repository, "show-ref", "--verify", "--quiet", "refs/heads/"+branch).CombinedOutput(); err == nil {
		t.Fatalf("local branch survived Reset: %s", output)
	}
	if output, err := exec.Command("git", "--git-dir", remote, "show-ref", "--verify", "--quiet", "refs/heads/"+branch).CombinedOutput(); err == nil {
		t.Fatalf("remote branch survived Reset: %s", output)
	}
	archivePath := filepath.Join(stateDir, "history", "sessions", runID, "session.jsonl")
	if _, err := os.Stat(sessionPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active Pi session remains resumable: %v", err)
	}
	if fileDigest(t, archivePath) != beforeSession {
		t.Fatal("historical Pi session archive changed")
	}
	var githubFinal struct {
		Labels    []string `json:"labels"`
		PR        string   `json:"pr"`
		AutoMerge bool     `json:"autoMerge"`
		Comments  []string `json:"comments"`
	}
	githubData, err := os.ReadFile(githubState)
	if err != nil || json.Unmarshal(githubData, &githubFinal) != nil {
		t.Fatalf("read final GitHub fixture: %v", err)
	}
	sort.Strings(githubFinal.Labels)
	if githubFinal.PR != "CLOSED" || githubFinal.AutoMerge || len(githubFinal.Comments) != 1 || strings.Join(githubFinal.Labels, ",") != "ready-for-agent,spec" {
		t.Fatalf("final GitHub state = %#v", githubFinal)
	}

	rerun := exec.Command(binary, "reset", "42", "--yes", "--repo-dir", repository, "--state-dir", stateDir, "--git", git, "--gh", gh)
	rerunOutput, err := rerun.CombinedOutput()
	if err != nil || !strings.Contains(string(rerunOutput), "Required actions:\n  None.") {
		t.Fatalf("idempotent compiled Reset: %v\n%s", err, rerunOutput)
	}
}

type localArtifactResetFixture struct {
	repository string
	stateDir   string
	store      state.FileStore
	branch     string
	worktree   string
	sessionDir string
	archiveDir string
	github     string
	git        string
}

func newLocalArtifactResetFixture(t *testing.T, failLabelOnce bool) localArtifactResetFixture {
	t.Helper()
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	remote := filepath.Join(root, "remote.git")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", "-b", "main", repository)
	runGit(t, repository, "config", "user.name", "Reset Test")
	runGit(t, repository, "config", "user.email", "reset@example.test")
	if err := os.WriteFile(filepath.Join(repository, "tracked"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked")
	runGit(t, repository, "commit", "-m", "base")
	runGit(t, repository, "remote", "add", "origin", remote)
	runGit(t, repository, "push", "-u", "origin", "main")

	runID := "run-local"
	branch := "agent/issue-42-" + runID
	stateDir := filepath.Join(root, "state")
	worktreePath := filepath.Join(stateDir, "worktrees", "issue-42-"+runID)
	if err := os.MkdirAll(filepath.Dir(worktreePath), 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "worktree", "add", "-b", branch, worktreePath, "HEAD")
	if err := os.WriteFile(filepath.Join(worktreePath, "unfinished"), []byte("unfinished\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worktreePath, "add", "unfinished")
	runGit(t, worktreePath, "commit", "-m", "unfinished")

	sessionDir := filepath.Join(stateDir, "sessions", runID)
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sessionID := "backlog-" + runID
	if err := os.WriteFile(filepath.Join(sessionDir, "session.jsonl"), []byte(fmt.Sprintf("{\"type\":\"session\",\"id\":%q,\"cwd\":%q}\n", sessionID, worktreePath)), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 42, RunID: runID, Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
			Branch: branch, Worktree: worktreePath, SessionID: sessionID, SessionDir: sessionDir,
		}},
		Leases: []scheduler.Lease{{LeaseID: "lease-local", Issue: 42, RunID: runID}},
	}); err != nil {
		t.Fatal(err)
	}
	githubState := filepath.Join(root, "labels.json")
	if err := os.WriteFile(githubState, []byte(`{"labels":["spec"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	failed := filepath.Join(root, "label-failed")
	failValue := "false"
	if failLabelOnce {
		failValue = "true"
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(githubState)+`
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":%s}\n' "$labels" ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository") printf '%s\n' '[]' ;;
  "issue edit 42 --repo acme/widgets --add-label ready-for-agent")
    if `+failValue+` && [ ! -e `+quote(failed)+` ]; then touch `+quote(failed)+`; echo 'failure after session archival' >&2; exit 1; fi
    temporary="$state.tmp"; jq '.labels += ["ready-for-agent"] | .labels |= unique' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	return localArtifactResetFixture{
		repository: repository, stateDir: stateDir, store: store, branch: branch, worktree: worktreePath,
		sessionDir: sessionDir, archiveDir: filepath.Join(stateDir, "history", "sessions", runID), github: gh, git: githubGit(t),
	}
}

func (f localArtifactResetFixture) args(git string, extra ...string) []string {
	arguments := []string{"reset", "42", "--repo-dir", f.repository, "--state-dir", f.stateDir, "--git", git, "--gh", f.github}
	return append(arguments, extra...)
}

func TestResetRerunsAfterEachLocalArtifactRetirementBoundary(t *testing.T) {
	for _, test := range []struct {
		name            string
		failLabel       bool
		wrapGit         func(*testing.T, localArtifactResetFixture) string
		completedAbsent []string
		remaining       []string
	}{
		{
			name: "worktree removal", completedAbsent: []string{"remove local worktree"}, remaining: []string{"delete local branch", "archive Pi session"},
			wrapGit: func(t *testing.T, fixture localArtifactResetFixture) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" worktree remove --force "*)
    if [ ! -e `+quote(failed)+` ]; then touch `+quote(failed)+`; `+quote(fixture.git)+` "$@"; echo 'failure after worktree removal' >&2; exit 1; fi ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
			},
		},
		{
			name: "branch deletion", completedAbsent: []string{"remove local worktree", "delete local branch"}, remaining: []string{"archive Pi session"},
			wrapGit: func(t *testing.T, fixture localArtifactResetFixture) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" update-ref -d refs/heads/`+fixture.branch+` "*)
    if [ ! -e `+quote(failed)+` ]; then touch `+quote(failed)+`; `+quote(fixture.git)+` "$@"; echo 'failure after branch deletion' >&2; exit 1; fi ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
			},
		},
		{
			name: "session archival", failLabel: true,
			completedAbsent: []string{"remove local worktree", "delete local branch", "archive Pi session"},
			remaining:       []string{"add issue label ready-for-agent"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLocalArtifactResetFixture(t, test.failLabel)
			git := fixture.git
			if test.wrapGit != nil {
				git = test.wrapGit(t, fixture)
			}
			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), fixture.args(git, "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "failure after") {
				t.Fatalf("first exit = %d, stderr = %q", exit, stderr.String())
			}
			partial, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if partial.Runs[0].Status != scheduler.StatusResetting || len(partial.Leases) != 1 {
				t.Fatalf("partial Reset released ownership: %#v", partial)
			}

			stdout.Reset()
			stderr.Reset()
			if exit := Main(context.Background(), fixture.args(fixture.git, "--dry-run"), &stdout, &stderr); exit != 0 {
				t.Fatalf("partial plan exit = %d, stderr = %q", exit, stderr.String())
			}
			plan := stdout.String()
			for _, absent := range test.completedAbsent {
				if strings.Contains(plan, absent) {
					t.Fatalf("partial plan repeated completed %s action:\n%s", absent, plan)
				}
			}
			for _, remaining := range test.remaining {
				if !strings.Contains(plan, remaining) {
					t.Fatalf("partial plan omitted remaining %s action:\n%s", remaining, plan)
				}
			}

			stdout.Reset()
			stderr.Reset()
			if exit := Main(context.Background(), fixture.args(fixture.git, "--yes"), &stdout, &stderr); exit != 0 {
				t.Fatalf("rerun exit = %d, stderr = %q", exit, stderr.String())
			}
			final, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if final.Runs[0].Status != scheduler.StatusReset || len(final.Leases) != 0 {
				t.Fatalf("rerun final state = %#v", final)
			}
			if _, err := os.Stat(fixture.worktree); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("worktree remains: %v", err)
			}
			if _, err := os.Stat(fixture.sessionDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("active session remains: %v", err)
			}
			if info, err := os.Stat(fixture.archiveDir); err != nil || !info.IsDir() {
				t.Fatalf("session archive missing: %v", err)
			}
		})
	}
}

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
	session := reset.Session{ID: "backlog-run-atomic", Dir: sessionDir, ArchiveDir: archiveDir, Present: true}
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

func TestResetRerunsAfterSessionArchiveSyncFailure(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	commonDirectory, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	syncCalls := 0
	executor := resetExecutor{
		store: fixture.store, github: ghadapter.Client{Executable: fixture.github, Dir: fixture.repository}, issue: 42,
		repositoryRoot: fixture.repository, commonDirectory: commonDirectory, stateDirectory: fixture.stateDir, gitExecutable: fixture.git,
		syncPath: func(path string) error {
			syncCalls++
			if syncCalls == 1 {
				return errors.New("injected directory sync failure")
			}
			return syncFilesystemPath(path)
		},
	}
	approved, err := executor.inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.apply(context.Background(), approved); err == nil || !strings.Contains(err.Error(), "injected directory sync failure") {
		t.Fatalf("archive sync error = %v", err)
	}
	if _, err := os.Stat(fixture.sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active session remains after rename: %v", err)
	}
	if info, err := os.Stat(fixture.archiveDir); err != nil || !info.IsDir() {
		t.Fatalf("archive missing after sync failure: %v", err)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
		t.Fatalf("sync failure released ownership: %#v", current)
	}

	executor.syncPath = nil
	approved, err = executor.inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.apply(context.Background(), approved); err != nil {
		t.Fatalf("rerun after archive sync failure: %v", err)
	}
	current, err = fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusReset || len(current.Leases) != 0 {
		t.Fatalf("rerun final state = %#v", current)
	}
}

func TestResetRevalidatesWorktreeImmediatelyBeforeRemoval(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	countPath := filepath.Join(t.TempDir(), "worktree-inspections")
	git := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" worktree list --porcelain -z")
    count=0; if [ -f `+quote(countPath)+` ]; then count=$(cat `+quote(countPath)+`); fi
    count=$((count + 1)); printf '%s' "$count" > `+quote(countPath)+`
    if [ "$count" -eq 5 ]; then `+quote(fixture.git)+` -C `+quote(fixture.worktree)+` checkout -b foreign >/dev/null 2>&1; fi ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
	commonDirectory, err := gitCommonDirectory(context.Background(), git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	executor := resetExecutor{
		store: fixture.store, github: ghadapter.Client{Executable: fixture.github, Dir: fixture.repository}, issue: 42,
		repositoryRoot: fixture.repository, commonDirectory: commonDirectory, stateDirectory: fixture.stateDir, gitExecutable: git,
	}
	approved, err := executor.inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.apply(context.Background(), approved); err == nil || !strings.Contains(err.Error(), "unknown branch or commit identity") {
		t.Fatalf("immediate worktree revalidation error = %v", err)
	}
	if _, err := os.Stat(fixture.worktree); err != nil {
		t.Fatalf("reassigned worktree was removed: %v", err)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
		t.Fatalf("revalidation failure released ownership: %#v", current)
	}
}

func TestResetStopsForChangedReassignedAndUnknownLocalStateWithLeaseRetained(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, localArtifactResetFixture) string
		want   string
	}{
		{
			name: "changed commit",
			mutate: func(t *testing.T, fixture localArtifactResetFixture) string {
				if err := os.WriteFile(filepath.Join(fixture.worktree, "changed"), []byte("changed\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				runGit(t, fixture.worktree, "add", "changed")
				runGit(t, fixture.worktree, "commit", "-m", "changed identity")
				return fixture.git
			},
			want: "Plan changed",
		},
		{
			name: "reassigned worktree",
			mutate: func(t *testing.T, fixture localArtifactResetFixture) string {
				runGit(t, fixture.worktree, "checkout", "-b", "foreign")
				return fixture.git
			},
			want: "unknown branch or commit identity",
		},
		{
			name: "unknown branch inspection",
			mutate: func(t *testing.T, fixture localArtifactResetFixture) string {
				return writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" for-each-ref --format=%(objectname) refs/heads/`+fixture.branch+`") echo uncertain >&2; exit 2 ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
			},
			want: "unknown output",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLocalArtifactResetFixture(t, false)
			commonDirectory, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
			if err != nil {
				t.Fatal(err)
			}
			executor := resetExecutor{
				store: fixture.store, github: ghadapter.Client{Executable: fixture.github, Dir: fixture.repository}, issue: 42,
				repositoryRoot: fixture.repository, commonDirectory: commonDirectory, stateDirectory: fixture.stateDir, gitExecutable: fixture.git,
			}
			approved, err := executor.inspect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			executor.gitExecutable = test.mutate(t, fixture)
			err = executor.apply(context.Background(), approved)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("changed local state error = %v", err)
			}
			current, loadErr := fixture.store.Load()
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if current.Runs[0].Status != scheduler.StatusFailed || len(current.Leases) != 1 {
				t.Fatalf("local identity refusal changed ownership: %#v", current)
			}
		})
	}
}

func TestDeleteLocalBranchUsesExpectedCommit(t *testing.T) {
	t.Parallel()
	repository := filepath.Join(t.TempDir(), "repo")
	runGit(t, t.TempDir(), "init", "-b", "main", repository)
	runGit(t, repository, "config", "user.name", "Reset Test")
	runGit(t, repository, "config", "user.email", "reset@example.test")
	if err := os.WriteFile(filepath.Join(repository, "tracked"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked")
	runGit(t, repository, "commit", "-m", "base")
	branchName := "agent/issue-42-local-cas"
	runGit(t, repository, "branch", branchName)
	expected := strings.TrimSpace(gitOutput(t, repository, "rev-parse", branchName))
	runGit(t, repository, "commit", "--allow-empty", "-m", "advanced")
	advanced := strings.TrimSpace(gitOutput(t, repository, "rev-parse", "HEAD"))
	runGit(t, repository, "update-ref", "refs/heads/"+branchName, advanced)
	branch := reset.Branch{Name: branchName, Commit: expected, Present: true}
	if err := deleteLocalBranch(context.Background(), "git", repository, branch); err == nil || !strings.Contains(err.Error(), "expected commit") {
		t.Fatalf("stale local branch deletion error = %v", err)
	}
	if got := strings.TrimSpace(gitOutput(t, repository, "rev-parse", branchName)); got != advanced {
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
	session, err := inspectSession(run, t.TempDir())
	if err != nil || session.Present {
		t.Fatalf("absent session = %#v, %v", session, err)
	}
	if err := os.MkdirAll(run.SessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run.SessionDir, "unknown.txt"), []byte("unknown"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectSession(run, t.TempDir()); err == nil || !strings.Contains(err.Error(), "unknown resource") {
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

func TestResetRefusesSymlinkedManagedArtifactParents(t *testing.T) {
	for _, test := range []struct {
		name      string
		link      string
		configure func(scheduler.Run, string) scheduler.Run
	}{
		{
			name: "worktrees", link: "worktrees",
			configure: func(run scheduler.Run, stateDir string) scheduler.Run {
				run.WorkerMode = scheduler.WorkerModePrint
				run.Branch = "agent/issue-42-run-links"
				run.Worktree = filepath.Join(stateDir, "worktrees", "issue-42-run-links")
				return run
			},
		},
		{
			name: "active sessions", link: "sessions",
			configure: func(run scheduler.Run, stateDir string) scheduler.Run {
				run.WorkerMode = scheduler.WorkerModeRPC
				run.SessionID = "backlog-run-links"
				run.SessionDir = filepath.Join(stateDir, "sessions", "run-links")
				return run
			},
		},
		{
			name: "historical sessions", link: "history",
			configure: func(run scheduler.Run, stateDir string) scheduler.Run {
				run.WorkerMode = scheduler.WorkerModeRPC
				run.SessionID = "backlog-run-links"
				run.SessionDir = filepath.Join(stateDir, "sessions", "run-links")
				return run
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stateDir := t.TempDir()
			repository := t.TempDir()
			run := test.configure(scheduler.Run{Issue: 42, RunID: "run-links"}, stateDir)
			if err := os.Symlink(t.TempDir(), filepath.Join(stateDir, test.link)); err != nil {
				t.Fatal(err)
			}
			if err := validateOwnedPaths(run, stateDir, repository, "main"); err == nil || !strings.Contains(err.Error(), "is a symlink") {
				t.Fatalf("symlinked %s ownership error = %v", test.name, err)
			}
		})
	}
}

func TestResetRefusesRunIDsThatEscapeManagedPaths(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	run := scheduler.Run{
		Issue: 42, RunID: "../logs", WorkerMode: scheduler.WorkerModeRPC,
		SessionID: "backlog-../logs", SessionDir: filepath.Join(stateDir, "sessions", "../logs"),
	}
	if err := validateOwnedPaths(run, stateDir, t.TempDir(), "main"); err == nil || !strings.Contains(err.Error(), "safe managed path component") {
		t.Fatalf("unsafe Run id error = %v", err)
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
  *" for-each-ref --format=%(objectname) refs/heads/agent/issue-4-run-4") exit 2 ;;
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
			if _, err := inspectSession(run, root); err == nil || !strings.Contains(err.Error(), "identity does not match") {
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
	if _, err := inspectSession(run, root); err == nil || !strings.Contains(err.Error(), "continuation file") {
		t.Fatalf("error = %v", err)
	}
}

func TestInspectSessionRefusesMismatchedHistoricalArchive(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	worktree := filepath.Join(stateDir, "worktrees", "run-4")
	archiveDir := filepath.Join(stateDir, "history", "sessions", "run-4")
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, "session.jsonl"), []byte(`{"type":"session","id":"other","cwd":"`+worktree+`"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		RunID: "run-4", WorkerMode: scheduler.WorkerModeRPC, Worktree: worktree,
		SessionID: "backlog-run-4", SessionDir: filepath.Join(stateDir, "sessions", "run-4"),
	}
	if _, err := inspectSession(run, stateDir); err == nil || !strings.Contains(err.Error(), "identity does not match") {
		t.Fatalf("mismatched historical archive error = %v", err)
	}
}

func TestResetDryRunRefusesLiveWorker(t *testing.T) {
	t.Parallel()

	run := scheduler.Run{Issue: 1, RunID: "live", PID: os.Getpid(), ProcessIdentity: "not-this-process"}
	if err := inspectWorkerAbsent(run); err == nil || (!strings.Contains(err.Error(), "uncertain identity") && !strings.Contains(err.Error(), "liveness is uncertain")) {
		t.Fatalf("live or uncertain Worker error = %v", err)
	}
}

func TestResetInspectionRefusesPendingReplacementWorker(t *testing.T) {
	t.Parallel()

	run := scheduler.Run{Issue: 1, RunID: "pending", ResumePending: true}
	if err := inspectWorkerAbsent(run); err == nil || !strings.Contains(err.Error(), "absence is uncertain") {
		t.Fatalf("pending replacement Worker error = %v", err)
	}
}

func TestResetInspectionAcceptsRetainedIdentityAfterWorkerExit(t *testing.T) {
	t.Parallel()

	worker := exec.Command("sleep", "10")
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	identity, err := pidStartIdentity(worker.Process.Pid)
	if err != nil {
		_ = worker.Process.Kill()
		_ = worker.Wait()
		t.Fatal(err)
	}
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Wait(); err == nil {
		t.Fatal("killed Worker exited successfully")
	}
	run := scheduler.Run{Issue: 1, RunID: "stopped", ProcessIdentity: identity}
	if err := inspectWorkerAbsent(run); err != nil {
		t.Fatalf("retained stopped Worker identity was refused: %v", err)
	}
	if summary := absentWorkerSummary(run); !strings.Contains(summary, fmt.Sprint(worker.Process.Pid)) {
		t.Fatalf("summary = %q", summary)
	}
}

func TestResetCommandRefusesLiveWorkerBeforeGitHubInspection(t *testing.T) {
	t.Parallel()

	repository := filepath.Join(t.TempDir(), "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	identity, err := pidStartIdentity(os.Getpid())
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

type artifactFreeResetFixture struct {
	repository  string
	stateDir    string
	logPath     string
	stderrPath  string
	store       state.FileStore
	githubState string
	git         string
	gh          string
}

func newArtifactFreeResetFixture(t *testing.T, labels []string) artifactFreeResetFixture {
	t.Helper()
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := filepath.Join(root, "state")
	logPath := filepath.Join(stateDir, "logs", "run-42.jsonl")
	stderrPath := filepath.Join(stateDir, "logs", "run-42.stderr.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("preserved Worker log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stderrPath, []byte("preserved Worker diagnostics\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint,
			LogPath: logPath, StderrPath: stderrPath, Error: "preserved diagnostic",
		}},
		Leases: []scheduler.Lease{{LeaseID: "lease-42", Issue: 42, RunID: "run-42"}},
	}); err != nil {
		t.Fatal(err)
	}
	encodedLabels, err := json.Marshal(labels)
	if err != nil {
		t.Fatal(err)
	}
	githubState := filepath.Join(root, "github.json")
	if err := os.WriteFile(githubState, []byte(`{"labels":`+string(encodedLabels)+`}`), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' `+quote(githubState)+`)
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":%s}\n' "$labels" ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary=`+quote(githubState)+`.tmp
    jq '.labels = [.labels[] | select(ascii_downcase != "in-progress")]' `+quote(githubState)+` > "$temporary"
    mv "$temporary" `+quote(githubState)+` ;;
  "issue edit 42 --repo acme/widgets --add-label ready-for-agent")
    temporary=`+quote(githubState)+`.tmp
    jq 'if any(.labels[]; ascii_downcase == "ready-for-agent") then . else .labels += ["ready-for-agent"] end' `+quote(githubState)+` > "$temporary"
    mv "$temporary" `+quote(githubState)+` ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	return artifactFreeResetFixture{
		repository: repository, stateDir: stateDir, logPath: logPath, stderrPath: stderrPath, store: store, githubState: githubState,
		git: githubGit(t), gh: gh,
	}
}

func (f artifactFreeResetFixture) args(command string, extra ...string) []string {
	args := []string{command, "42", "--repo-dir", f.repository, "--state-dir", f.stateDir, "--git", f.git, "--gh", f.gh}
	return append(args, extra...)
}

func (f artifactFreeResetFixture) resetArgs() []string {
	return []string{"42", "--repo-dir", f.repository, "--state-dir", f.stateDir, "--git", f.git, "--gh", f.gh}
}

func (f artifactFreeResetFixture) labels(t *testing.T) []string {
	t.Helper()
	var value struct {
		Labels []string `json:"labels"`
	}
	data, err := os.ReadFile(f.githubState)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	sort.Strings(value.Labels)
	return value.Labels
}

func TestCompiledResetFinalizesArtifactFreeRun(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	binary := buildExecutable(t, t.TempDir())
	arguments := fixture.args("reset", "--yes")
	command := exec.Command(binary, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled Reset: %v\n%s", err, output)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusReset || len(current.Leases) != 0 ||
		strings.Join(fixture.labels(t), ",") != "ready-for-agent,spec" {
		t.Fatalf("compiled Reset state = %#v, labels = %v", current, fixture.labels(t))
	}
	if !strings.Contains(string(output), "No replacement Run was created") {
		t.Fatalf("compiled Reset output = %q", output)
	}
}

func TestResetMutationRequiresYesWhenNonInteractive(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	beforeState := fileDigest(t, fixture.store.Path)
	beforeGitHub := fileDigest(t, fixture.githubState)
	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), fixture.args("reset"), &stdout, &stderr)
	if exit == 0 || !strings.Contains(stderr.String(), "requires --yes") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
		t.Fatal("non-interactive refusal changed state")
	}
}

func TestResetRefusesMutationWhenPlanOrPromptCannotBePrinted(t *testing.T) {
	for _, test := range []struct {
		name        string
		interactive bool
		input       string
		failOn      string
	}{
		{name: "initial plan", interactive: true, input: "yes\n", failOn: "Reset Plan"},
		{name: "interactive prompt", interactive: true, input: "yes\n", failOn: "Proceed with Reset"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
			beforeState := fileDigest(t, fixture.store.Path)
			beforeGitHub := fileDigest(t, fixture.githubState)
			stdout := &failOnTextWriter{text: test.failOn}
			var stderr bytes.Buffer
			err := resetCommandWithInput(context.Background(), fixture.resetArgs(), strings.NewReader(test.input), test.interactive, stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), "output") && !strings.Contains(err.Error(), "confirmation") {
				t.Fatalf("error = %v", err)
			}
			if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
				t.Fatal("output failure allowed Reset mutation")
			}
		})
	}
}

func TestResetConfirmationStopsWaitingWhenContextIsCancelled(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	prompted := make(chan struct{})
	stdout := writerFunc(func(data []byte) (int, error) {
		select {
		case <-prompted:
		default:
			close(prompted)
		}
		return len(data), nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := confirmReset(ctx, bufio.NewReader(reader), stdout)
		done <- err
	}()
	<-prompted
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("confirmation error = %v", err)
	}
}

func TestResetInteractiveDefaultNoInputsMakeNoChanges(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
	}{
		{name: "Enter", input: "\n"},
		{name: "EOF"},
		{name: "non-affirmative", input: "no\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
			beforeState := fileDigest(t, fixture.store.Path)
			beforeGitHub := fileDigest(t, fixture.githubState)
			var stdout, stderr bytes.Buffer
			err := resetCommandWithInput(context.Background(), fixture.resetArgs(), strings.NewReader(test.input), true, &stdout, &stderr)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(stdout.String(), "Reset cancelled; no changes made.") {
				t.Fatalf("stdout = %q", stdout.String())
			}
			if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
				t.Fatal("cancelled Reset changed state")
			}
		})
	}
}

func TestResetFinalizesArtifactFreeEligibleRunStatuses(t *testing.T) {
	for _, status := range []scheduler.Status{
		scheduler.StatusClaimed,
		scheduler.StatusWorktreeReady,
		scheduler.StatusRunning,
		scheduler.StatusNeedsHuman,
	} {
		t.Run(string(status), func(t *testing.T) {
			fixture := newArtifactFreeResetFixture(t, []string{"ready-for-agent", "spec"})
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			current.Runs[0].Status = status
			if status == scheduler.StatusRunning {
				worker := exec.Command("sleep", "10")
				if err := worker.Start(); err != nil {
					t.Fatal(err)
				}
				identity, err := pidStartIdentity(worker.Process.Pid)
				if err != nil {
					_ = worker.Process.Kill()
					_ = worker.Wait()
					t.Fatal(err)
				}
				if err := worker.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err := worker.Wait(); err == nil {
					t.Fatal("killed Worker exited successfully")
				}
				current.Runs[0].PID = worker.Process.Pid
				current.Runs[0].ProcessIdentity = identity
				current.Runs[0].StartedAt = time.Now().UTC()
			}
			if err := fixture.store.Save(current); err != nil {
				t.Fatal(err)
			}

			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
				t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
			}
			final, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if final.Runs[0].Status != scheduler.StatusReset || len(final.Leases) != 0 {
				t.Fatalf("final state = %#v", final)
			}
		})
	}
}

func TestResetFinalizationPersistsRunAndLeaseTogether(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"ready-for-agent", "spec"})
	commonDirectory, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingResetStore{resetStateStore: fixture.store}
	executor := resetExecutor{
		store: store, github: ghadapter.Client{Executable: fixture.gh, Dir: fixture.repository}, issue: 42,
		repositoryRoot: fixture.repository, commonDirectory: commonDirectory, stateDirectory: fixture.stateDir, gitExecutable: fixture.git,
	}
	approved, err := executor.inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.apply(context.Background(), approved); err != nil {
		t.Fatal(err)
	}
	if len(store.saves) != 1 {
		t.Fatalf("state saves = %d, want one atomic finalization", len(store.saves))
	}
	persisted := store.saves[0]
	if len(persisted.Runs) != 1 || persisted.Runs[0].Status != scheduler.StatusReset || len(persisted.Leases) != 0 {
		t.Fatalf("atomic finalization snapshot = %#v", persisted)
	}
}

func TestResetFinalizationRejectsHistoricalMetadataChanges(t *testing.T) {
	t.Parallel()
	verifiedAt := time.Now().UTC()
	expected := scheduler.Run{
		Issue: 42, RunID: "run-metadata", Status: scheduler.StatusResetting, WorkerMode: scheduler.WorkerModeRPC,
		PID: 123, ProcessIdentity: "123:start", Branch: "agent/issue-42-run-metadata", Worktree: "/worktree",
		SessionID: "backlog-run-metadata", SessionDir: "/sessions/run-metadata",
		Continuation: &scheduler.ContinuationBoundary{SessionID: "backlog-run-metadata", SessionFile: "/sessions/run-metadata/session.jsonl", Worktree: "/worktree", LeafID: "leaf", EntryCount: 1, SHA256: strings.Repeat("a", 64), VerifiedAt: verifiedAt},
	}
	for _, test := range []struct {
		name   string
		mutate func(*scheduler.Run)
	}{
		{name: "branch", mutate: func(run *scheduler.Run) { run.Branch = "other" }},
		{name: "process identity", mutate: func(run *scheduler.Run) { run.ProcessIdentity = "123:other" }},
		{name: "session directory", mutate: func(run *scheduler.Run) { run.SessionDir = "/other" }},
		{name: "continuation", mutate: func(run *scheduler.Run) { run.Continuation.SHA256 = strings.Repeat("b", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual := expected
			boundary := *expected.Continuation
			actual.Continuation = &boundary
			actual.Status = scheduler.StatusReset
			now := verifiedAt.Add(time.Minute)
			actual.UpdatedAt = now
			actual.CompletedAt = &now
			test.mutate(&actual)
			if err := verifyResetFinalState(state.State{Runs: []scheduler.Run{actual}}, expected); err == nil || !strings.Contains(err.Error(), "historical metadata") {
				t.Fatalf("metadata mismatch error = %v", err)
			}
		})
	}
}

func TestResetFinalizationRequiresDurableRecordedLogs(t *testing.T) {
	for _, test := range []struct {
		name        string
		stderr      bool
		replacement string
	}{
		{name: "missing JSONL", replacement: "missing"},
		{name: "symlinked JSONL", replacement: "symlink"},
		{name: "directory JSONL", replacement: "directory"},
		{name: "missing stderr", stderr: true, replacement: "missing"},
		{name: "symlinked stderr", stderr: true, replacement: "symlink"},
		{name: "directory stderr", stderr: true, replacement: "directory"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newArtifactFreeResetFixture(t, []string{"ready-for-agent", "spec"})
			path := fixture.logPath
			want := "Worker JSONL log"
			if test.stderr {
				path = fixture.stderrPath
				want = "Worker standard-error log"
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			switch test.replacement {
			case "missing":
			case "symlink":
				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, []byte("replacement\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unknown replacement %q", test.replacement)
			}
			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 1 {
				t.Fatalf("exit = %d, want 1; stderr = %q", exit, stderr.String())
			}
			if !strings.Contains(stderr.String(), want) {
				t.Fatalf("stderr = %q", stderr.String())
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if current.Runs[0].Status == scheduler.StatusReset || len(current.Leases) != 1 {
				t.Fatalf("invalid log released ownership: %#v", current)
			}
		})
	}
}

func TestResetConvergesEveryManagedLabelCombinationAndPreservesHistory(t *testing.T) {
	for _, labels := range [][]string{
		{"spec"},
		{"in-progress", "spec"},
		{"ready-for-agent", "spec"},
		{"in-progress", "ready-for-agent", "spec"},
	} {
		name := strings.Join(labels, "+")
		t.Run(name, func(t *testing.T) {
			fixture := newArtifactFreeResetFixture(t, labels)
			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
				t.Fatalf("exit = %d, stderr = %q\nstdout = %q", exit, stderr.String(), stdout.String())
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusReset || len(current.Leases) != 0 {
				t.Fatalf("final Runs/Leases = %#v/%#v", current.Runs, current.Leases)
			}
			if current.Runs[0].LogPath != fixture.logPath || current.Runs[0].Error != "preserved diagnostic" {
				t.Fatalf("historical metadata changed: %#v", current.Runs[0])
			}
			if got := strings.Join(fixture.labels(t), ","); got != "ready-for-agent,spec" {
				t.Fatalf("labels = %q", got)
			}
			if len(current.Runs) != 1 || !strings.Contains(stdout.String(), "No replacement Run was created") {
				t.Fatalf("Reset created or implied a replacement Run: %#v, %q", current.Runs, stdout.String())
			}
		})
	}
}

func TestResetRepeatsOnlyAfterVerifyingOldLeaseAbsent(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"ready-for-agent", "spec"})
	for attempt := 1; attempt <= 2; attempt++ {
		var stdout, stderr bytes.Buffer
		if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
			t.Fatalf("attempt %d exit = %d, stderr = %q", attempt, exit, stderr.String())
		}
		if attempt == 2 && !strings.Contains(stdout.String(), "Lease: absent (Run already reset)") {
			t.Fatalf("repeat output = %q", stdout.String())
		}
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || len(current.Leases) != 0 || current.Runs[0].Status != scheduler.StatusReset {
		t.Fatalf("repeated Reset state = %#v", current)
	}
}

func TestResetRepairsManagedLabelDriftForAlreadyResetRun(t *testing.T) {
	fixture := newArtifactFreeResetFixture(t, []string{"ready-for-agent", "spec"})
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
		t.Fatalf("initial exit = %d, stderr = %q", exit, stderr.String())
	}
	if err := os.WriteFile(fixture.githubState, []byte(`{"labels":["in-progress","spec"]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	stdout.Reset()
	stderr.Reset()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if exit := Main(ctx, fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
		t.Fatalf("repair exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusReset || len(current.Leases) != 0 || strings.Join(fixture.labels(t), ",") != "ready-for-agent,spec" {
		t.Fatalf("repaired state = %#v, labels = %v", current, fixture.labels(t))
	}
	if strings.Contains(stdout.String(), "mark Run run-42 resetting") {
		t.Fatalf("already reset Run planned an invalid transition: %q", stdout.String())
	}
}

func TestRetryMatchesResetNonMutationPaths(t *testing.T) {
	for _, test := range []struct {
		name  string
		extra []string
	}{
		{name: "non-interactive mutation refusal"},
		{name: "dry-run", extra: []string{"--dry-run"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
			var resetOut, resetErr, retryOut, retryErr bytes.Buffer
			resetExit := Main(context.Background(), fixture.args("reset", test.extra...), &resetOut, &resetErr)
			retryExit := Main(context.Background(), fixture.args("retry", test.extra...), &retryOut, &retryErr)
			warning := "Warning: backlog retry is deprecated; use backlog reset.\n"
			if resetExit != retryExit || resetOut.String() != retryOut.String() || strings.TrimPrefix(retryErr.String(), warning) != resetErr.String() {
				t.Fatalf("reset/retry differ: exits %d/%d, stdout %q/%q, stderr %q/%q", resetExit, retryExit, resetOut.String(), retryOut.String(), resetErr.String(), retryErr.String())
			}
			if !strings.HasPrefix(retryErr.String(), warning) {
				t.Fatalf("retry warning = %q", retryErr.String())
			}
		})
	}
}

func TestRetryUsesResetMutationPathWithDeprecationWarning(t *testing.T) {
	t.Parallel()
	resetFixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	retryFixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	var resetOut, resetErr, retryOut, retryErr bytes.Buffer
	resetExit := Main(context.Background(), resetFixture.args("reset", "--yes"), &resetOut, &resetErr)
	retryExit := Main(context.Background(), retryFixture.args("retry", "--yes"), &retryOut, &retryErr)
	if resetExit != retryExit || resetExit != 0 {
		t.Fatalf("reset/retry exits = %d/%d; errors = %q/%q", resetExit, retryExit, resetErr.String(), retryErr.String())
	}
	if resetOut.String() != retryOut.String() {
		t.Fatalf("reset output = %q, retry output = %q", resetOut.String(), retryOut.String())
	}
	if resetErr.Len() != 0 || retryErr.String() != "Warning: backlog retry is deprecated; use backlog reset.\n" {
		t.Fatalf("reset/retry stderr = %q/%q", resetErr.String(), retryErr.String())
	}
	resetState, _ := resetFixture.store.Load()
	retryState, _ := retryFixture.store.Load()
	if resetState.Runs[0].Status != retryState.Runs[0].Status || len(resetState.Leases) != len(retryState.Leases) ||
		strings.Join(resetFixture.labels(t), ",") != strings.Join(retryFixture.labels(t), ",") {
		t.Fatalf("reset/retry mutations differ: %#v / %#v", resetState, retryState)
	}
}

func TestResetStopsWhenHumanWorkflowLabelAppearsAfterFirstMutation(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	underlyingGH := fixture.gh
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
if [ "$*" = "issue edit 42 --repo acme/widgets --remove-label in-progress" ]; then
  `+quote(underlyingGH)+` "$@"
  temporary=`+quote(fixture.githubState)+`.tmp
  jq '.labels += ["needs-info"]' `+quote(fixture.githubState)+` > "$temporary"
  mv "$temporary" `+quote(fixture.githubState)+`
  exit 0
fi
exec `+quote(underlyingGH)+` "$@"
`)
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "human workflow label") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
		t.Fatalf("human-label race released ownership: %#v", current)
	}
	if got := strings.Join(fixture.labels(t), ","); got != "needs-info,spec" {
		t.Fatalf("labels after refusal = %q", got)
	}
}

func TestResetStopsWhenHumanWorkflowLabelAppearsAfterConfirmation(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	views := filepath.Join(t.TempDir(), "views")
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    if test -f `+quote(views)+`; then labels='[{"name":"needs-info"},{"name":"spec"}]'; else touch `+quote(views)+`; labels='[{"name":"in-progress"},{"name":"spec"}]'; fi
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":%s}\n' "$labels" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	before := fileDigest(t, fixture.store.Path)
	var stdout, stderr bytes.Buffer
	err := resetCommandWithInput(context.Background(), fixture.resetArgs(), strings.NewReader("yes\n"), true, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "human workflow label") {
		t.Fatalf("error = %v", err)
	}
	if fileDigest(t, fixture.store.Path) != before {
		t.Fatal("human workflow label refusal changed state")
	}
}

func TestResetChangedInteractivePlanRequiresConfirmationAgain(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	views := filepath.Join(t.TempDir(), "views")
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    if test -f `+quote(views)+`; then labels='[{"name":"in-progress"},{"name":"ready-for-agent"},{"name":"spec"}]'; else touch `+quote(views)+`; labels='[{"name":"in-progress"},{"name":"spec"}]'; fi
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":%s}\n' "$labels" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	before := fileDigest(t, fixture.store.Path)
	var stdout, stderr bytes.Buffer
	if err := resetCommandWithInput(context.Background(), fixture.resetArgs(), strings.NewReader("yes\nno\n"), true, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if strings.Count(stdout.String(), "Reset Plan for issue #42") != 2 || !strings.Contains(stdout.String(), "confirm the current plan again") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if fileDigest(t, fixture.store.Path) != before {
		t.Fatal("second confirmation refusal changed state")
	}
}

func TestResetYesPrintsChangedPlanAndContinues(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	underlyingGH := fixture.gh
	viewed := filepath.Join(t.TempDir(), "viewed")
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    if test -f `+quote(viewed)+`; then
      temporary=`+quote(fixture.githubState)+`.tmp
      jq 'if any(.labels[]; . == "ready-for-agent") then . else .labels += ["ready-for-agent"] end' `+quote(fixture.githubState)+` > "$temporary"
      mv "$temporary" `+quote(fixture.githubState)+`
    else
      touch `+quote(viewed)+`
    fi ;;
esac
exec `+quote(underlyingGH)+` "$@"
`)
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	if strings.Count(stdout.String(), "Reset Plan for issue #42") != 2 || !strings.Contains(stdout.String(), "using the current plan") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestResetYesRefusesMutationWhenChangedPlanCannotBePrinted(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	underlyingGH := fixture.gh
	viewed := filepath.Join(t.TempDir(), "viewed")
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    if test -f `+quote(viewed)+`; then
      temporary=`+quote(fixture.githubState)+`.tmp
      jq 'if any(.labels[]; . == "ready-for-agent") then . else .labels += ["ready-for-agent"] end' `+quote(fixture.githubState)+` > "$temporary"
      mv "$temporary" `+quote(fixture.githubState)+`
    else
      touch `+quote(viewed)+`
    fi ;;
esac
exec `+quote(underlyingGH)+` "$@"
`)
	before := fileDigest(t, fixture.store.Path)
	stdout := &failOnTextWriter{text: "using the current plan"}
	var stderr bytes.Buffer
	args := append(fixture.resetArgs(), "--yes")
	err := resetCommandWithInput(context.Background(), args, strings.NewReader(""), false, stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "output") {
		t.Fatalf("error = %v", err)
	}
	if fileDigest(t, fixture.store.Path) != before {
		t.Fatal("changed-plan output failure allowed Reset mutation")
	}
}

func TestResetRejectsPlanChangeAfterInteractiveReconfirmation(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	views := filepath.Join(t.TempDir(), "views")
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    count=0
    if test -f `+quote(views)+`; then count=$(cat `+quote(views)+`); fi
    count=$((count + 1))
    printf '%s\n' "$count" > `+quote(views)+`
    if test "$count" -ge 3; then labels='[{"name":"in-progress"},{"name":"ready-for-agent"},{"name":"spec"}]'; else labels='[{"name":"in-progress"},{"name":"spec"}]'; fi
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":%s}\n' "$labels" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	before := fileDigest(t, fixture.store.Path)
	var stdout, stderr bytes.Buffer
	err := resetCommandWithInput(context.Background(), fixture.resetArgs(), strings.NewReader("yes\n"), true, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "changed after confirmation") {
		t.Fatalf("error = %v", err)
	}
	if fileDigest(t, fixture.store.Path) != before {
		t.Fatal("late plan change mutated Run state")
	}
}

func TestResetRerunsAfterPartialLabelProgress(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	underlyingGH := fixture.gh
	failedOnce := filepath.Join(t.TempDir(), "failed-once")
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
if [ "$*" = "issue edit 42 --repo acme/widgets --add-label ready-for-agent" ] && [ ! -f `+quote(failedOnce)+` ]; then
  touch `+quote(failedOnce)+`
  echo "temporary label failure" >&2
  exit 1
fi
exec `+quote(underlyingGH)+` "$@"
`)
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "temporary label failure") {
		t.Fatalf("first exit = %d, stderr = %q", exit, stderr.String())
	}
	partial, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if partial.Runs[0].Status != scheduler.StatusResetting || len(partial.Leases) != 1 || strings.Join(fixture.labels(t), ",") != "spec" {
		t.Fatalf("partial Reset = %#v, labels = %v", partial, fixture.labels(t))
	}

	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
		t.Fatalf("rerun exit = %d, stderr = %q", exit, stderr.String())
	}
	final, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if final.Runs[0].Status != scheduler.StatusReset || len(final.Leases) != 0 || strings.Join(fixture.labels(t), ",") != "ready-for-agent,spec" {
		t.Fatalf("rerun final state = %#v, labels = %v", final, fixture.labels(t))
	}
}

func TestResetVerifiesUnrelatedLabelsSurviveMutation(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	underlyingGH := fixture.gh
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
if [ "$*" = "issue edit 42 --repo acme/widgets --remove-label in-progress" ]; then
  `+quote(underlyingGH)+` "$@"
  temporary=`+quote(fixture.githubState)+`.tmp
  jq '.labels = [.labels[] | select(. != "spec")]' `+quote(fixture.githubState)+` > "$temporary"
  mv "$temporary" `+quote(fixture.githubState)+`
  exit 0
fi
exec `+quote(underlyingGH)+` "$@"
`)
	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr)
	if exit == 0 || !strings.Contains(stderr.String(), "did not satisfy its verified postcondition") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
		t.Fatalf("unrelated-label loss released ownership: %#v", current)
	}
}

func TestResetVerifiesAddedLabelMutationBeforeFinalization(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"spec"})
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"spec"}]}' ;;
  "issue edit 42 --repo acme/widgets --add-label ready-for-agent") : ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr)
	if exit == 0 || !strings.Contains(stderr.String(), "did not satisfy its verified postcondition") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
		t.Fatalf("failed verification released ownership: %#v", current)
	}
}

func TestResetVerifiesLabelMutationBeforeFinalization(t *testing.T) {
	t.Parallel()
	fixture := newArtifactFreeResetFixture(t, []string{"in-progress", "spec"})
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress") : ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr)
	if exit == 0 || !strings.Contains(stderr.String(), "did not satisfy its verified postcondition") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
		t.Fatalf("failed verification released ownership: %#v", current)
	}
}

type githubArtifactResetFixture struct {
	artifactFreeResetFixture
	branch      string
	remote      string
	githubCalls string
}

func newGitHubArtifactResetFixture(t *testing.T, status scheduler.Status, failClose, mergeOnDisable, advanceOnClose bool) githubArtifactResetFixture {
	t.Helper()
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	remote := filepath.Join(root, "remote.git")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", "-b", "main", repository)
	runGit(t, repository, "config", "user.name", "Reset Test")
	runGit(t, repository, "config", "user.email", "reset@example.test")
	if err := os.WriteFile(filepath.Join(repository, "tracked"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked")
	runGit(t, repository, "commit", "-m", "base")
	runGit(t, repository, "remote", "add", "origin", remote)
	runGit(t, repository, "push", "-u", "origin", "main")

	runID := "run-github"
	branch := "agent/issue-42-" + runID
	runGit(t, repository, "checkout", "-b", branch)
	if err := os.WriteFile(filepath.Join(repository, "owned"), []byte("owned\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "owned")
	runGit(t, repository, "commit", "-m", "owned")
	head := strings.TrimSpace(gitOutput(t, repository, "rev-parse", "HEAD"))
	runGit(t, repository, "push", "origin", branch)
	runGit(t, repository, "checkout", "main")
	runGit(t, repository, "branch", "-D", branch)

	stateDir := filepath.Join(root, "state")
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 42, RunID: runID, Status: status, WorkerMode: scheduler.WorkerModePrint,
			Branch: branch, Worktree: filepath.Join(stateDir, "worktrees", "issue-42-"+runID),
			PullRequest: "https://github.com/acme/widgets/pull/99",
		}},
		Leases: []scheduler.Lease{{LeaseID: "lease-github", Issue: 42, RunID: runID}},
	}); err != nil {
		t.Fatal(err)
	}
	githubState := filepath.Join(root, "github.json")
	encoded := fmt.Sprintf(`{"pr":"OPEN","merged":false,"auto":true,"comments":[],"head":%q,"labels":["ready-for-agent"],"failClose":%t,"failAction":"","noopAction":"","mergeOnDisable":%t,"advanceOnClose":%t}`, head, failClose, mergeOnDisable, advanceOnClose)
	if err := os.WriteFile(githubState, []byte(encoded), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(root, "github-calls")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(githubState)+`
calls=`+quote(calls)+`
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":%s}\n' "$labels" ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    jq -c --arg branch `+quote(branch)+` '[{number:99,url:"https://github.com/acme/widgets/pull/99",state:.pr,mergedAt:(if .merged then "2026-01-01T00:00:00Z" else null end),autoMergeRequest:(if .auto then {mergeMethod:"SQUASH"} else null end),isDraft:false,headRefName:$branch,headRefOid:.head,headRepositoryOwner:{login:"acme"},headRepository:{nameWithOwner:"acme/widgets"}}]' "$state" ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/99/comments?per_page=100 --paginate --slurp")
    jq -c '[.comments | map({body:.})]' "$state" ;;
  "pr merge 99 --repo acme/widgets --disable-auto")
    printf '%s\n' disable >> "$calls"
    if [ "$(jq -r .failAction "$state")" = disable ]; then
      temporary="$state.tmp"
      jq '.failAction=""' "$state" > "$temporary"
      mv "$temporary" "$state"
      echo 'temporary disable failure' >&2
      exit 1
    fi
    if [ "$(jq -r .noopAction "$state")" = disable ]; then exit 0; fi
    temporary="$state.tmp"
    jq 'if .mergeOnDisable then .auto=false | .pr="MERGED" | .merged=true else .auto=false end' "$state" > "$temporary"
    mv "$temporary" "$state" ;;
  pr\ comment\ 99\ --repo\ acme/widgets\ --body\ *)
    printf '%s\n' comment >> "$calls"
    if [ "$(jq -r .failAction "$state")" = comment ]; then
      temporary="$state.tmp"
      jq '.failAction=""' "$state" > "$temporary"
      mv "$temporary" "$state"
      echo 'temporary comment failure' >&2
      exit 1
    fi
    if [ "$(jq -r .noopAction "$state")" = comment ]; then exit 0; fi
    body=''
    for value in "$@"; do body=$value; done
    temporary="$state.tmp"
    jq --arg body "$body" '.comments += [$body]' "$state" > "$temporary"
    mv "$temporary" "$state" ;;
  "pr close 99 --repo acme/widgets")
    printf '%s\n' close >> "$calls"
    if [ "$(jq -r .failAction "$state")" = close ]; then
      temporary="$state.tmp"
      jq '.failAction=""' "$state" > "$temporary"
      mv "$temporary" "$state"
      echo 'temporary close failure' >&2
      exit 1
    fi
    if [ "$(jq -r .noopAction "$state")" = close ]; then exit 0; fi
    if [ "$(jq -r .failClose "$state")" = true ]; then
      temporary="$state.tmp"
      jq '.failClose=false' "$state" > "$temporary"
      mv "$temporary" "$state"
      echo 'temporary close failure' >&2
      exit 1
    fi
    if [ "$(jq -r .advanceOnClose "$state")" = true ]; then
      git -C `+quote(repository)+` commit --allow-empty -m race >/dev/null
      git -C `+quote(repository)+` push --force origin HEAD:refs/heads/`+branch+` >/dev/null 2>&1
      replacement=$(git -C `+quote(repository)+` rev-parse HEAD)
      temporary="$state.tmp"
      jq --arg replacement "$replacement" '.head=$replacement | .advanceOnClose=false' "$state" > "$temporary"
      mv "$temporary" "$state"
    fi
    temporary="$state.tmp"
    jq '.pr="CLOSED"' "$state" > "$temporary"
    mv "$temporary" "$state" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	return githubArtifactResetFixture{
		artifactFreeResetFixture: artifactFreeResetFixture{
			repository: repository, stateDir: stateDir, store: store, githubState: githubState,
			git: githubGit(t), gh: gh,
		},
		branch: branch, remote: remote, githubCalls: calls,
	}
}

func (f githubArtifactResetFixture) githubStateValue(t *testing.T) struct {
	PR       string   `json:"pr"`
	Merged   bool     `json:"merged"`
	Auto     bool     `json:"auto"`
	Comments []string `json:"comments"`
} {
	t.Helper()
	var value struct {
		PR       string   `json:"pr"`
		Merged   bool     `json:"merged"`
		Auto     bool     `json:"auto"`
		Comments []string `json:"comments"`
	}
	data, err := os.ReadFile(f.githubState)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (f githubArtifactResetFixture) updateGitHubState(t *testing.T, filter string) {
	t.Helper()
	command := exec.Command("jq", filter, f.githubState)
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	temporary := f.githubState + ".tmp"
	if err := os.WriteFile(temporary, output, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, f.githubState); err != nil {
		t.Fatal(err)
	}
}

func TestResetRerunsOnlyRemainingGitHubArtifactActionsAfterPartialFailure(t *testing.T) {
	t.Parallel()
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, true, false, false)

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "temporary close failure") {
		t.Fatalf("first exit = %d, stderr = %q", exit, stderr.String())
	}
	partial, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	github := fixture.githubStateValue(t)
	if partial.Runs[0].Status != scheduler.StatusResetting || len(partial.Leases) != 1 || github.PR != "OPEN" || github.Auto || len(github.Comments) != 1 {
		t.Fatalf("partial state = %#v, GitHub = %#v", partial, github)
	}
	comment := github.Comments[0]
	if !strings.Contains(comment, resetCommentMarker("run-github")) || !strings.Contains(comment, "Run run-github") ||
		!strings.Contains(comment, "issue #42") || !strings.Contains(comment, "being closed as part of abandoning") {
		t.Fatalf("Reset explanation = %q", comment)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || !branch.Present {
		t.Fatalf("remote branch after partial failure = %#v, %v", branch, err)
	}

	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
		t.Fatalf("rerun exit = %d, stderr = %q", exit, stderr.String())
	}
	if strings.Contains(stdout.String(), "disable auto-merge") || strings.Contains(stdout.String(), "explain Reset") {
		t.Fatalf("rerun planned completed actions: %q", stdout.String())
	}
	final, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	github = fixture.githubStateValue(t)
	if final.Runs[0].Status != scheduler.StatusReset || len(final.Leases) != 0 || github.PR != "CLOSED" || github.Merged || github.Auto {
		t.Fatalf("final state = %#v, GitHub = %#v", final, github)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("final remote branch = %#v, %v", branch, err)
	}
	calls, err := os.ReadFile(fixture.githubCalls)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(calls); strings.Count(got, "disable\n") != 1 || strings.Count(got, "comment\n") != 1 || strings.Count(got, "close\n") != 2 {
		t.Fatalf("GitHub calls = %q", got)
	}
}

func TestResetRerunsAfterEveryRemainingGitHubArtifactFailure(t *testing.T) {
	for _, action := range []string{"disable", "comment", "branch"} {
		t.Run(action, func(t *testing.T) {
			fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
			if action == "branch" {
				underlyingGit := fixture.git
				failed := filepath.Join(t.TempDir(), "failed")
				fixture.git = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" push origin --force-with-lease="*)
    if [ ! -e `+quote(failed)+` ]; then
      touch `+quote(failed)+`
      echo 'temporary branch deletion failure' >&2
      exit 1
    fi ;;
esac
exec `+quote(underlyingGit)+` "$@"
`)
			} else {
				fixture.updateGitHubState(t, `.failAction="`+action+`"`)
			}

			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "temporary") {
				t.Fatalf("first exit = %d, stderr = %q", exit, stderr.String())
			}
			partial, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if partial.Runs[0].Status != scheduler.StatusResetting || len(partial.Leases) != 1 {
				t.Fatalf("partial failure released ownership: %#v", partial)
			}

			stdout.Reset()
			stderr.Reset()
			if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
				t.Fatalf("rerun exit = %d, stderr = %q", exit, stderr.String())
			}
			final, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if final.Runs[0].Status != scheduler.StatusReset || len(final.Leases) != 0 {
				t.Fatalf("rerun final state = %#v", final)
			}
			calls, err := os.ReadFile(fixture.githubCalls)
			if err != nil {
				t.Fatal(err)
			}
			got := string(calls)
			switch action {
			case "disable":
				if strings.Count(got, "disable\n") != 2 || strings.Count(got, "comment\n") != 1 || strings.Count(got, "close\n") != 1 {
					t.Fatalf("GitHub calls = %q", got)
				}
			case "comment":
				if strings.Count(got, "disable\n") != 1 || strings.Count(got, "comment\n") != 2 || strings.Count(got, "close\n") != 1 {
					t.Fatalf("GitHub calls = %q", got)
				}
			case "branch":
				if got != "disable\ncomment\nclose\n" || strings.Contains(stdout.String(), "pull request") {
					t.Fatalf("GitHub calls = %q, rerun plan = %q", got, stdout.String())
				}
			}
		})
	}
}

func TestResetRejectsSuccessfulGitHubArtifactCommandsWithoutPostconditions(t *testing.T) {
	for _, action := range []string{"disable", "comment", "close", "branch"} {
		t.Run(action, func(t *testing.T) {
			fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
			if action == "branch" {
				underlyingGit := fixture.git
				fixture.git = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" push origin --force-with-lease="*) exit 0 ;;
esac
exec `+quote(underlyingGit)+` "$@"
`)
			} else {
				fixture.updateGitHubState(t, `.noopAction="`+action+`"`)
			}

			wantError := map[string]string{
				"disable": "freshly verified open, unmerged, and auto-merge unarmed",
				"comment": "did not satisfy its verified postcondition",
				"close":   "freshly verified closed and unmerged",
				"branch":  "still present after deletion",
			}[action]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var stdout, stderr bytes.Buffer
			if exit := Main(ctx, fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), wantError) {
				t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
				t.Fatalf("missing postcondition released ownership: %#v", current)
			}
			calls, err := os.ReadFile(fixture.githubCalls)
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := map[string]string{
				"disable": "disable\n",
				"comment": "disable\ncomment\n",
				"close":   "disable\ncomment\nclose\n",
				"branch":  "disable\ncomment\nclose\n",
			}[action]
			if string(calls) != wantCalls {
				t.Fatalf("GitHub calls = %q, want %q", calls, wantCalls)
			}
		})
	}
}

func TestResetWaitingForMergeResumesFromAlreadyDisarmedOrClosedPullRequest(t *testing.T) {
	for _, closed := range []bool{false, true} {
		name := "open"
		if closed {
			name = "closed"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, false, false)
			filter := `.auto=false`
			if closed {
				filter += ` | .pr="CLOSED"`
			}
			fixture.updateGitHubState(t, filter)

			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
				t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if current.Runs[0].Status != scheduler.StatusReset || len(current.Leases) != 0 {
				t.Fatalf("final state = %#v", current)
			}
			calls, err := os.ReadFile(fixture.githubCalls)
			if closed {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("closed pull request caused GitHub mutation: %v", err)
				}
			} else if err != nil || string(calls) != "comment\nclose\n" {
				t.Fatalf("already-disarmed GitHub calls = %q, %v", calls, err)
			}
		})
	}
}

func TestResetRejectsInitialPullRequestAndRemoteCommitMismatch(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
	fixture.updateGitHubState(t, `.head="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`)

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "does not match owned remote branch") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusFailed || len(current.Leases) != 1 {
		t.Fatalf("identity mismatch changed ownership: %#v", current)
	}
	if _, err := os.Stat(fixture.githubCalls); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("identity mismatch caused mutation: %v", err)
	}
}

func TestResetWaitingForMergeDisablesAutoMergeBeforeResetting(t *testing.T) {
	t.Parallel()
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, false, false)
	autoMergeArmedWhenResetting := true
	store := &recordingResetStore{resetStateStore: fixture.store}
	store.onSave = func(current state.State) {
		if current.Runs[0].Status == scheduler.StatusResetting {
			autoMergeArmedWhenResetting = fixture.githubStateValue(t).Auto
		}
	}
	commonDirectory, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	executor := resetExecutor{
		store: store, github: ghadapter.Client{Executable: fixture.gh, Dir: fixture.repository}, issue: 42,
		repositoryRoot: fixture.repository, commonDirectory: commonDirectory, stateDirectory: fixture.stateDir, gitExecutable: fixture.git,
	}
	approved, err := executor.inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.apply(context.Background(), approved); err != nil {
		t.Fatal(err)
	}
	if len(store.saves) < 2 || store.saves[0].Runs[0].Status != scheduler.StatusResetting || autoMergeArmedWhenResetting {
		t.Fatalf("persisted states = %#v, auto-merge armed when resetting = %t", store.saves, autoMergeArmedWhenResetting)
	}
	github := fixture.githubStateValue(t)
	if github.Auto || github.Merged || github.PR != "CLOSED" {
		t.Fatalf("final GitHub state = %#v", github)
	}
	calls, err := os.ReadFile(fixture.githubCalls)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(calls), "disable\ncomment\nclose\n") {
		t.Fatalf("GitHub action order = %q", calls)
	}
}

func TestResetWaitingForMergeAbortsMergedRaceBeforeResetting(t *testing.T) {
	t.Parallel()
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, true, false)
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "merged") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusWaitingForMerge || len(current.Leases) != 1 {
		t.Fatalf("merge race released or advanced ownership: %#v", current)
	}
	calls, err := os.ReadFile(fixture.githubCalls)
	if err != nil {
		t.Fatal(err)
	}
	if string(calls) != "disable\n" {
		t.Fatalf("actions after merge race = %q", calls)
	}
}

func TestResetAcceptsAlreadyClosedPullRequestAndAbsentOwnedRemoteBranch(t *testing.T) {
	t.Parallel()
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
	temporary := fixture.githubState + ".tmp"
	command := exec.Command("jq", `.pr="CLOSED" | .auto=false`, fixture.githubState)
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(temporary, output, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, fixture.githubState); err != nil {
		t.Fatal(err)
	}
	runGit(t, fixture.repository, "push", "origin", "--delete", fixture.branch)

	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusReset || len(current.Leases) != 0 {
		t.Fatalf("final state = %#v", current)
	}
	if _, err := os.Stat(fixture.githubCalls); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("idempotent GitHub state caused a mutation: %v", err)
	}
}

func TestDeleteRemoteBranchUsesExpectedCommitLease(t *testing.T) {
	t.Parallel()
	calls := filepath.Join(t.TempDir(), "calls")
	git := writeExecutable(t, "#!/bin/sh\nprintf '%s\\n' \"$*\" > "+quote(calls)+"\n")
	branch := reset.Branch{Name: "agent/issue-42-run", Commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Present: true}
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

	failingGit := writeExecutable(t, "#!/bin/sh\necho stale lease >&2\nexit 1\n")
	if err := deleteRemoteBranch(context.Background(), failingGit, t.TempDir(), branch); err == nil || !strings.Contains(err.Error(), "expected commit") || !strings.Contains(err.Error(), "stale lease") {
		t.Fatalf("conditional deletion error = %v", err)
	}
}

func TestResetAbortsBranchCommitRaceWithLeaseRetained(t *testing.T) {
	t.Parallel()
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, true)
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), fixture.args("reset", "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "expected commit identity changed") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResetting || len(current.Leases) != 1 {
		t.Fatalf("branch race released ownership: %#v", current)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || !branch.Present {
		t.Fatalf("raced remote branch was deleted: %#v, %v", branch, err)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(data []byte) (int, error) {
	return f(data)
}

type failOnTextWriter struct {
	text string
}

func (w *failOnTextWriter) Write(data []byte) (int, error) {
	if strings.Contains(string(data), w.text) {
		return 0, errors.New("injected output failure")
	}
	return len(data), nil
}

type recordingResetStore struct {
	resetStateStore
	saves  []state.State
	onSave func(state.State)
}

func (s *recordingResetStore) Save(current state.State) error {
	s.saves = append(s.saves, current)
	if s.onSave != nil {
		s.onSave(current)
	}
	return s.resetStateStore.Save(current)
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
