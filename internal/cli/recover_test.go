package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestRecoverConfirmationDefaultsNegativeAndAcceptsExplicitYes(t *testing.T) {
	for _, test := range []struct {
		name, input string
		want        bool
	}{
		{name: "enter", input: "\n"},
		{name: "EOF", input: ""},
		{name: "yes", input: "yes\n", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			got, err := confirmRecovery(context.Background(), bufio.NewReader(strings.NewReader(test.input)), &output)
			if err != nil || got != test.want || !strings.Contains(output.String(), "[y/N]") {
				t.Fatalf("confirmation = %t, %v, output=%q", got, err, output.String())
			}
		})
	}
	var stdout, stderr bytes.Buffer
	err := recoverCommandWithInput(context.Background(), []string{"run-1"}, strings.NewReader(""), false, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "requires --yes") {
		t.Fatalf("noninteractive refusal = %v", err)
	}
}

type mutatingRecoveryInput struct {
	reader *strings.Reader
	mutate func() error
	err    error
}

func (r *mutatingRecoveryInput) Read(buffer []byte) (int, error) {
	if r.mutate != nil {
		r.err = r.mutate()
		r.mutate = nil
		if r.err != nil {
			return 0, r.err
		}
	}
	return r.reader.Read(buffer)
}

func TestCompiledRecoverLateCompletionRetiresArtifactsLabelsAndLeaseIdempotently(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	runGit(t, fixture.worktree, "push", "origin", fixture.branch)
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	run := &current.Runs[0]
	run.PullRequest = "https://github.com/acme/widgets/pull/99"
	run.WorkerGeneration = 1
	run.StoppedWorkerGeneration = 1
	stopped := time.Now().Add(-time.Minute).UTC()
	run.WorkerStoppedAt = &stopped
	run.Error = "retained diagnostic before late Completion"
	sessionFile := filepath.Join(run.SessionDir, "session.jsonl")
	session := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":%q,\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"/skill:afk 42\"}}\n", run.SessionID, run.Worktree)
	if err := os.WriteFile(sessionFile, []byte(session), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := fmt.Sprintf("{\"version\":1,\"workflow\":\"afk\",\"stage\":\"afk-coordinator\",\"issue\":42,\"runId\":%q,\"sessionId\":%q,\"worktree\":%q}\n", run.RunID, run.SessionID, run.Worktree)
	if err := os.WriteFile(filepath.Join(run.SessionDir, "backlog-afk-checkpoint-v1.json"), []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.githubState, []byte(`{"labels":["in-progress","spec"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	merged := filepath.Join(t.TempDir(), "merged")
	head := strings.TrimSpace(gitOutput(t, fixture.worktree, "rev-parse", "HEAD"))
	gh := writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(fixture.githubState)+`
merged=`+quote(merged)+`
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    status=OPEN; if [ -f "$merged" ]; then status=CLOSED; fi
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"%s","labels":%s}\n' "$status" "$labels" ;;
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    status=OPEN; mergedAt=null
    if [ -f "$merged" ]; then status=MERGED; mergedAt='"2026-07-29T00:00:00Z"'; else touch "$merged"; fi
    printf '[{"number":99,"url":"https://github.com/acme/widgets/pull/99","state":"%s","mergedAt":%s,"autoMergeRequest":null,"isDraft":false,"headRefName":"`+fixture.branch+`","headRefOid":"`+head+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$status" "$mergedAt" ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/99/comments?per_page=100 --paginate --slurp") printf '%s\n' '[[]]' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "in-progress"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "ready-for-agent"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	binary := buildExecutable(t, t.TempDir())
	arguments := []string{"recover", run.RunID, "--yes", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git, "--gh", gh}
	command := func() string {
		output, commandErr := exec.Command(binary, arguments...).CombinedOutput()
		if commandErr != nil {
			t.Fatalf("compiled late Completion Recovery: %v\n%s", commandErr, output)
		}
		return string(output)
	}
	output := command()
	if !strings.Contains(output, "Recovery Plan changed after confirmation") || !strings.Contains(output, "Completion recorded for Run "+run.RunID+" from merged expected pull request "+run.PullRequest) {
		t.Fatalf("late Completion output:\n%s", output)
	}
	persisted, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	got := persisted.Runs[0]
	if got.Status != scheduler.StatusMerged || got.PullRequest != run.PullRequest || got.CompletedAt == nil || got.CleanupPending || len(persisted.Leases) != 0 {
		t.Fatalf("late Completion state = %#v", persisted)
	}
	if branch, inspectErr := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); inspectErr != nil || branch.Present {
		t.Fatalf("remote branch after Completion = %#v, %v", branch, inspectErr)
	}
	if _, statErr := os.Stat(fixture.worktree); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("worktree after Completion = %v", statErr)
	}
	if _, statErr := os.Stat(fixture.sessionDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("active session after Completion = %v", statErr)
	}
	if _, statErr := os.Stat(fixture.archiveDir); statErr != nil {
		t.Fatalf("historical session after Completion = %v", statErr)
	}
	var githubState struct {
		Labels []string `json:"labels"`
	}
	encoded, err := os.ReadFile(fixture.githubState)
	if err != nil || json.Unmarshal(encoded, &githubState) != nil || strings.Join(githubState.Labels, ",") != "spec" {
		t.Fatalf("managed labels after Completion = %s, %v", encoded, err)
	}
	beforeRerun := fileDigest(t, fixture.store.Path)
	if rerun := command(); !strings.Contains(rerun, "Completion recorded for Run "+run.RunID) {
		t.Fatalf("idempotent Completion output:\n%s", rerun)
	}
	if afterRerun := fileDigest(t, fixture.store.Path); afterRerun != beforeRerun {
		t.Fatalf("idempotent Completion changed state: %x != %x", afterRerun, beforeRerun)
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
	session := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":%q,\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"/skill:afk 42\"}}\n", sessionID, worktreePath)
	if err := os.WriteFile(sessionFile, []byte(session), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := fmt.Sprintf("{\"version\":1,\"workflow\":\"afk\",\"stage\":\"afk-coordinator\",\"issue\":42,\"runId\":%q,\"sessionId\":%q,\"worktree\":%q}\n", runID, sessionID, worktreePath)
	if err := os.WriteFile(filepath.Join(sessionDir, "backlog-afk-checkpoint-v1.json"), []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	started := time.Now().Add(-time.Hour).UTC()
	original := scheduler.Run{
		Issue: 42, RunID: runID, Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
		WorkerGeneration: 1, StoppedWorkerGeneration: 1, WorkerStoppedAt: &started,
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

	directArgs := []string{runID, "--repo-dir", repository, "--state-dir", stateDir, "--gh", gh}
	var directOutput, directError bytes.Buffer
	if err := recoverCommandWithInput(context.Background(), directArgs, strings.NewReader("\n"), true, &directOutput, &directError); err != nil {
		t.Fatalf("interactive cancellation: %v", err)
	}
	if !strings.Contains(directOutput.String(), "Recovery cancelled; no changes made") || fileDigest(t, store.Path) != before {
		t.Fatalf("interactive default-no mutated Recovery: %q", directOutput.String())
	}

	unsafeSession := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":%q,\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call-1\"}]}}\n", sessionID, worktreePath)
	if err := os.WriteFile(sessionFile, []byte(unsafeSession), 0o600); err != nil {
		t.Fatal(err)
	}
	directOutput.Reset()
	if err := recoverCommandWithInput(context.Background(), append(directArgs, "--dry-run"), strings.NewReader(""), false, &directOutput, &directError); err == nil || !strings.Contains(err.Error(), "durable results") {
		t.Fatalf("unsafe command evidence = %v", err)
	}
	if current, err := store.Load(); err != nil || len(current.Leases) != 1 || current.Runs[0].Status != scheduler.StatusFailed {
		t.Fatalf("unsafe command changed ownership: %#v, %v", current, err)
	}
	if _, err := os.Stat(worktreePath); err != nil {
		t.Fatalf("unsafe command removed worktree: %v", err)
	}
	if err := os.WriteFile(sessionFile, []byte(session), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := ensureResetStateBinding(filepath.Join(repository, ".git"), stateDir); err != nil {
		t.Fatal(err)
	}
	partialInput := &mutatingRecoveryInput{reader: strings.NewReader("yes\n"), mutate: func() error { return os.Chmod(stateDir, 0o500) }}
	directOutput.Reset()
	err := recoverCommandWithInput(context.Background(), directArgs, partialInput, true, &directOutput, &directError)
	if restoreErr := os.Chmod(stateDir, 0o700); restoreErr != nil {
		t.Fatal(restoreErr)
	}
	if err == nil || !strings.Contains(err.Error(), "persist Recovery transition") {
		t.Fatalf("partial Recovery failure = %v", err)
	}
	if current, loadErr := store.Load(); loadErr != nil || len(current.Leases) != 1 || current.Runs[0].Status != scheduler.StatusFailed {
		t.Fatalf("partial Recovery changed ownership: %#v, %v", current, loadErr)
	}

	changedInput := &mutatingRecoveryInput{reader: strings.NewReader("yes\nyes\n"), mutate: func() error {
		file, err := os.OpenFile(sessionFile, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, writeErr := fmt.Fprintln(file, `{"type":"message","id":"leaf-2","parentId":"leaf","message":{"role":"assistant","content":[{"type":"text","text":"checkpoint"}]}}`)
		return errors.Join(writeErr, file.Close())
	}}
	directOutput.Reset()
	if err := recoverCommandWithInput(context.Background(), directArgs, changedInput, true, &directOutput, &directError); err != nil {
		t.Fatalf("changed-plan interactive Recovery rerun: %v", err)
	}
	if !strings.Contains(directOutput.String(), "Recovery Plan changed after confirmation; confirm the current plan again") || strings.Count(directOutput.String(), "Proceed with Recovery? [y/N]") != 2 {
		t.Fatalf("changed plan was not reconfirmed: %q", directOutput.String())
	}

	output = command("--yes")
	if !strings.Contains(output, "Recovery complete: Run "+runID+" is Suspended") {
		t.Fatalf("idempotent mutation output = %q", output)
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
