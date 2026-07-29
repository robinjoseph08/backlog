package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"golang.org/x/term"
)

func TestCompiledExecutableUsesLifecycleExitForSignalDuringSetup(t *testing.T) {
	binary := buildExecutable(t, t.TempDir())
	for _, test := range []struct {
		name       string
		signal     os.Signal
		wantExit   int
		wantOutput string
	}{
		{name: "SIGINT", signal: os.Interrupt, wantExit: 0, wantOutput: "Drain: admission stopped during setup; 0 Workers remaining"},
		{name: "SIGTERM", signal: syscall.SIGTERM, wantExit: 143, wantOutput: "Suspension: SIGTERM accepted during setup; 0 Workers remaining"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			repository := filepath.Join(root, "repo")
			if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
				t.Fatalf("git init: %v\n%s", err, output)
			}
			started := filepath.Join(root, "setup-started")
			git := writeExecutable(t, "#!/bin/sh\ntouch "+quote(started)+"\nexec sleep 30\n")
			command := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", filepath.Join(root, "state"), "--git", git)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForFile(t, started)
			if err := command.Process.Signal(test.signal); err != nil {
				t.Fatal(err)
			}
			err := command.Wait()
			if test.wantExit == 0 {
				if err != nil {
					t.Fatalf("compiled setup signal exit: %v, output = %q", err, output.String())
				}
			} else {
				var exitError *exec.ExitError
				if !errors.As(err, &exitError) || exitError.ExitCode() != test.wantExit {
					t.Fatalf("compiled setup signal exit: %v, output = %q, want %d", err, output.String(), test.wantExit)
				}
			}
			if !strings.Contains(output.String(), test.wantOutput) {
				t.Fatalf("compiled setup signal output = %q, want %q", output.String(), test.wantOutput)
			}
		})
	}
}

func TestCompiledExecutableDrainSucceedsWhenMalformedRPCRequiresAttention(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)
	stateDir := filepath.Join(root, "state")
	workerStarted := filepath.Join(root, "worker-started")
	workerRelease := filepath.Join(root, "worker-release")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":35,"title":"Protocol failure","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/35"}]' ;;
  "issue view 35 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":35,"title":"Protocol failure","body":"","state":"OPEN","url":"https://example.test/issues/35","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/35/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/35/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-35-"*)
    printf '%s\n' '[]' ;;
  "issue view 35 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":35,"state":"OPEN","title":"Protocol failure","url":"https://github.com/acme/widgets/issues/35"}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
IFS= read -r prompt
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
touch `+quote(workerStarted)+`
while ! test -f `+quote(workerRelease)+`; do sleep 0.01; done
trap 'exit 0' TERM INT
printf '%s\n' 'malformed Pi RPC JSON'
while :; do sleep 0.1; done
`)
	command := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 20)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	waitForFile(t, workerStarted)
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	for line := range lines {
		if strings.Contains(line, "Drain: admission stopped; 1 Worker remaining") {
			break
		}
	}
	if err := os.WriteFile(workerRelease, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("compiled Drain with Attention Required: %v, stderr = %q", err, stderr.String())
	}
	current, err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusFailed || len(current.Leases) != 1 {
		t.Fatalf("state after compiled malformed RPC Drain = %#v", current)
	}
}

func TestCompiledExecutableSuspendsOnSecondSIGINTWithoutAdmittingAnotherLease(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)

	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	workerStarted := filepath.Join(root, "worker-started")
	suspensionRelease := filepath.Join(root, "suspension-release")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":31,"title":"First","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/31"},{"number":32,"title":"Later","createdAt":"2026-01-02T00:00:00Z","url":"https://example.test/issues/32"}]' ;;
  "issue view 31 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":31,"title":"First","body":"","state":"OPEN","url":"https://example.test/issues/31","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "issue view 32 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":32,"title":"Later","body":"","state":"OPEN","url":"https://example.test/issues/32","createdAt":"2026-01-02T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/31/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/31/dependencies/blocked_by?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/32/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/32/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-31-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository"|\
  "pr list --repo acme/widgets --state all --head agent/issue-32-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[]' ;;
  "issue view 31 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":31,"state":"OPEN","title":"First","url":"https://github.com/acme/widgets/issues/31"}' ;;
  "issue view 32 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":32,"state":"OPEN","title":"Later","url":"https://github.com/acme/widgets/issues/32"}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "remove" ]; then rm -rf "$6"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
session_dir= session_id=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir) session_dir=$2; shift 2 ;;
    --session-id) session_id=$2; shift 2 ;;
    *) shift ;;
  esac
done
worktree=$(pwd)
IFS= read -r prompt
touch `+quote(workerStarted)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
IFS= read -r abort
while ! test -f `+quote(suspensionRelease)+`; do sleep 0.01; done
session_file="$session_dir/session.jsonl"
printf '{"type":"session","version":3,"id":"%s","cwd":"%s"}\n' "$session_id" "$worktree" > "$session_file"
printf '%s\n' '{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}' >> "$session_file"
printf '%s\n' '{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
IFS= read -r state
printf '{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":"%s","sessionId":"%s"}}\n' "$session_file" "$session_id"
IFS= read -r entries
printf '%s\n' '{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":[{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}],"leafId":"leaf"}}'
IFS= read -r final_state
printf '{"id":"backlog-suspend-final-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":"%s","sessionId":"%s"}}\n' "$session_file" "$session_id"
while IFS= read -r ignored; do :; done
`)

	command := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "2", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	waitForFile(t, workerStarted)
	var beforeSignal state.State
	for deadline := time.Now().Add(2 * time.Second); ; {
		beforeSignal, err = (state.FileStore{Path: statePath}).Load()
		if err == nil && len(beforeSignal.Leases) == 2 && len(beforeSignal.Runs) == 2 && beforeSignal.Runs[0].PID != 0 && beforeSignal.Runs[1].PID != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state before SIGINT = %#v, err = %v", beforeSignal, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send SIGINT: %v", err)
	}
	var outputLines []string
	for {
		line, ok := <-lines
		if !ok {
			t.Fatalf("process exited before reporting Drain, output = %q, stderr = %q", outputLines, stderr.String())
		}
		outputLines = append(outputLines, line)
		if strings.Contains(line, "Drain: admission stopped; 2 Workers remaining; next SIGINT will be recorded as a suspension request") {
			break
		}
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send repeated SIGINT: %v", err)
	}
	for {
		line, ok := <-lines
		if !ok {
			t.Fatalf("process exited before reporting repeated SIGINT, output = %q, stderr = %q", outputLines, stderr.String())
		}
		outputLines = append(outputLines, line)
		if strings.Contains(line, "Drain: additional interrupt recorded as a suspension request; 2 Workers remaining") {
			break
		}
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		current, loadErr := (state.FileStore{Path: statePath}).Load()
		if loadErr == nil && len(current.Runs) == 2 && current.Runs[0].SuspendingAt != nil && current.Runs[1].SuspendingAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state while suspending = %#v, err = %v", current, loadErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	statusOutput, statusErr := exec.Command(binary, "status", "--repo-dir", repository, "--state-dir", stateDir, "--git", git).CombinedOutput()
	if statusErr != nil || strings.Count(string(statusOutput), "suspending") != 2 {
		t.Fatalf("compiled status while suspending: %v, output = %q", statusErr, statusOutput)
	}
	if err := os.WriteFile(suspensionRelease, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitCode() != 130 {
			t.Fatalf("compiled second-SIGINT run: %v, stderr = %q", err, stderr.String())
		}
	} else {
		t.Fatal("compiled second-SIGINT run exited zero, want 130")
	}
	for line := range lines {
		outputLines = append(outputLines, line)
	}
	current, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 2 || len(current.Leases) != 2 {
		t.Fatalf("persisted state after second SIGINT = %#v", current)
	}
	for _, run := range current.Runs {
		if run.Status != scheduler.StatusSuspended || run.PID != 0 || run.Continuation == nil || run.SuspendingAt != nil || run.SuspendedAt == nil {
			t.Fatalf("persisted Run after second SIGINT = %#v", run)
		}
	}
	output := strings.Join(outputLines, "\n")
	if !strings.Contains(output, "Suspension complete: 0 Workers remaining") {
		t.Fatalf("suspension output = %q", output)
	}
}

func TestCompiledExecutableRestartResumesSuspendedRunBeforeNewCandidate(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)
	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	sessionDir := filepath.Join(stateDir, "sessions", "run-91")
	worktreePath := filepath.Join(stateDir, "worktrees", "issue-91-run-91")
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(worktreePath, 0o700); err != nil {
		t.Fatal(err)
	}
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	sessionContent := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":\"session-91\",\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"continue\"}}\n", worktreePath)
	if err := os.WriteFile(sessionFile, []byte(sessionContent), 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(sessionContent))
	persisted := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 91, RunID: "run-91", Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
			Branch: "agent/issue-91-run-91", Worktree: worktreePath, SessionName: "afk #91", SessionID: "session-91", SessionDir: sessionDir,
			Continuation: &scheduler.ContinuationBoundary{
				SessionID: "session-91", SessionFile: sessionFile, Worktree: worktreePath, LeafID: "leaf", EntryCount: 1,
				SHA256: hex.EncodeToString(hash[:]), VerifiedAt: time.Now(),
			},
			StartedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now(),
		}},
		Leases: []scheduler.Lease{{LeaseID: "lease-91", Issue: 91, RunID: "run-91"}},
	}
	if err := (state.FileStore{Path: statePath}).Save(persisted); err != nil {
		t.Fatal(err)
	}

	orderPath := filepath.Join(root, "worker-order")
	resumedDone := filepath.Join(root, "resumed-done")
	candidateDone := filepath.Join(root, "candidate-done")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if test -f `+quote(candidateDone)+`; then printf '%s\n' '[]'; else printf '%s\n' '[{"number":92,"title":"new","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/92"}]'; fi ;;
  "issue view 92 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":92,"title":"new","body":"","state":"OPEN","url":"https://example.test/issues/92","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/92/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/92/dependencies/blocked_by?per_page=100 --paginate --slurp") printf '%s\n' '[[]]' ;;
  "issue view 91 --repo acme/widgets --json number,url,state,labels") printf '%s\n' '{"number":91,"url":"https://github.com/acme/widgets/issues/91","state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-91-run-91 --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    if test -f `+quote(resumedDone)+`; then printf '%s\n' '[{"number":191,"url":"https://github.com/acme/widgets/pull/191","state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-91-run-91","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]'; else printf '%s\n' '[]'; fi ;;
  "issue view 91 --repo acme/widgets --json number,state,title,url")
    if test -f `+quote(resumedDone)+`; then printf '%s\n' '{"number":91,"state":"CLOSED","url":"https://github.com/acme/widgets/issues/91"}'; else printf '%s\n' '{"number":91,"state":"OPEN","url":"https://github.com/acme/widgets/issues/91"}'; fi ;;
  "pr list --repo acme/widgets --state all --head agent/issue-92-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    head=$8
    printf '[{"number":192,"url":"https://github.com/acme/widgets/pull/192","state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"%s","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$head" ;;
  "issue view 92 --repo acme/widgets --json number,state,title,url") touch `+quote(candidateDone)+`; printf '%s\n' '{"number":92,"state":"CLOSED","url":"https://github.com/acme/widgets/issues/92"}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then
  if [ "$2" = `+quote(worktreePath)+` ]; then printf '%s\n' `+quote(worktreePath)+`; else printf '%s\n' `+quote(repository)+`; fi
  exit 0
fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "branch" ] && [ "$4" = "--show-current" ]; then printf '%s\n' 'agent/issue-91-run-91'; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "remove" ]; then rm -rf "$6"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
session_id=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-id) session_id=$2; shift 2 ;;
    --session) test "$2" = `+quote(sessionFile)+`; session_id=session-91; shift 2 ;;
    *) shift ;;
  esac
done
IFS= read -r prompt
case "$session_id" in
  session-91)
    printf '%s\n' "$session_id" >> `+quote(orderPath)+`
    printf '%s\n' "$prompt" | grep -q 'Reassess the repository and GitHub state'
    touch `+quote(resumedDone)+` ;;
  backlog-*)
    test -f `+quote(resumedDone)+`
    printf '%s\n' "$session_id" >> `+quote(orderPath)+`
    printf '%s\n' "$prompt" | grep -q '/skill:afk 92' ;;
  *) exit 9 ;;
esac
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)

	command := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("restart executable: %v\n%s", err, output)
	}
	order, err := os.ReadFile(orderPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Fields(string(order))
	if len(lines) != 2 || lines[0] != "session-91" || !strings.HasPrefix(lines[1], "backlog-") {
		t.Fatalf("Worker order = %q, want resumed session before Candidate", order)
	}
	final, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Runs) != 2 || final.Runs[0].RunID != "run-91" || final.Runs[0].Status != scheduler.StatusMerged || final.Runs[1].Status != scheduler.StatusMerged || len(final.Leases) != 0 {
		t.Fatalf("final restarted state = %#v", final)
	}
}

func TestCompiledExecutableSuspendsDirectlyOnSIGTERM(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)
	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	workerStarted := filepath.Join(root, "worker-started")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":33,"title":"Terminate","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/33"}]' ;;
  "issue view 33 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":33,"title":"Terminate","body":"","state":"OPEN","url":"https://example.test/issues/33","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/33/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/33/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-33-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[]' ;;
  "issue view 33 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":33,"state":"OPEN","title":"Terminate","url":"https://github.com/acme/widgets/issues/33"}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
session_dir= session_id=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir) session_dir=$2; shift 2 ;;
    --session-id) session_id=$2; shift 2 ;;
    *) shift ;;
  esac
done
worktree=$(pwd)
IFS= read -r prompt
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
touch `+quote(workerStarted)+`
IFS= read -r abort
session_file="$session_dir/session.jsonl"
printf '{"type":"session","version":3,"id":"%s","cwd":"%s"}\n' "$session_id" "$worktree" > "$session_file"
printf '%s\n' '{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}' >> "$session_file"
printf '%s\n' '{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
IFS= read -r state
printf '{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":"%s","sessionId":"%s"}}\n' "$session_file" "$session_id"
IFS= read -r entries
printf '%s\n' '{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":[{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}],"leafId":"leaf"}}'
IFS= read -r final_state
printf '{"id":"backlog-suspend-final-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":"%s","sessionId":"%s"}}\n' "$session_file" "$session_id"
while IFS= read -r ignored; do :; done
`)

	command := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, workerStarted)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	if err := command.Wait(); err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitCode() != 143 {
			t.Fatalf("compiled SIGTERM run: %v, output = %q", err, output.String())
		}
	} else {
		t.Fatal("compiled SIGTERM run exited zero, want 143")
	}
	current, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusSuspended || current.Runs[0].Continuation == nil ||
		current.Runs[0].PID != 0 || len(current.Leases) != 1 {
		t.Fatalf("persisted state after SIGTERM = %#v", current)
	}
	if strings.Contains(output.String(), "Drain:") {
		t.Fatalf("SIGTERM unexpectedly entered Drain: %q", output.String())
	}
}

func TestCompiledExecutableThirdSIGINTForceStopsOnlyItsWorkerGroup(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)
	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	workerStarted := filepath.Join(root, "worker-started")
	abortReceived := filepath.Join(root, "abort-received")
	workerPIDPath := filepath.Join(root, "worker.pid")

	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":33,"title":"Hung Worker","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/33"}]' ;;
  "issue view 33 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":33,"title":"Hung Worker","body":"","state":"OPEN","url":"https://example.test/issues/33","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/33/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/33/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
printf '%s\n' "$$" > `+quote(workerPIDPath)+`
IFS= read -r prompt
sh -c 'trap "" TERM; while :; do sleep 1; done' &
touch `+quote(workerStarted)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
IFS= read -r abort
touch `+quote(abortReceived)+`
trap '' TERM
while :; do sleep 1; done
`)

	unrelated := exec.Command("sleep", "30")
	unrelated.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := unrelated.Start(); err != nil {
		t.Fatalf("start unrelated process: %v", err)
	}
	defer func() {
		_ = syscall.Kill(-unrelated.Process.Pid, syscall.SIGKILL)
		_ = unrelated.Wait()
	}()

	command := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	waitForFile(t, workerStarted)
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send first SIGINT: %v", err)
	}
	for {
		line, ok := <-lines
		if !ok {
			t.Fatalf("process exited before Drain: %s", stderr.String())
		}
		if strings.Contains(line, "Drain: admission stopped") {
			break
		}
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send second SIGINT: %v", err)
	}
	waitForFile(t, abortReceived)
	started := time.Now()
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send third SIGINT: %v", err)
	}
	if err := command.Wait(); err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitCode() != 130 {
			t.Fatalf("compiled third-SIGINT run: %v, stderr = %q", err, stderr.String())
		}
	} else {
		t.Fatal("compiled third-SIGINT run exited zero, want 130")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("third SIGINT did not bypass the 60-second deadline: %s", elapsed)
	}

	current, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusNeedsHuman || current.Runs[0].Continuation != nil || current.Runs[0].PID != 0 || len(current.Leases) != 1 {
		t.Fatalf("persisted state after force stop = %#v", current)
	}
	pidData, err := os.ReadFile(workerPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	var workerPID int
	if _, err := fmt.Sscan(strings.TrimSpace(string(pidData)), &workerPID); err != nil {
		t.Fatalf("parse Worker PID: %v", err)
	}
	if err := syscall.Kill(-workerPID, syscall.Signal(0)); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("Worker process group survived third SIGINT: %v", err)
	}
	if err := unrelated.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unrelated process was signaled: %v", err)
	}
}

func TestCompiledExecutableFollowsNormalizedRunnerActivityConcurrentlyAndCtrlCIsPassive(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)
	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	workerStarted := filepath.Join(root, "worker-started")
	emitActivity := filepath.Join(root, "emit-activity")
	emitSubagentChange := filepath.Join(root, "emit-subagent-change")
	completeActivity := filepath.Join(root, "complete-activity")
	finishWorker := filepath.Join(root, "finish-worker")
	closedMarker := filepath.Join(root, "issue-closed")

	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if test -f `+quote(closedMarker)+`; then printf '%s\n' '[]'; else printf '%s\n' '[{"number":27,"title":"Follow me","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/27"}]'; fi ;;
  "issue view 27 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":27,"title":"Follow me","body":"","state":"OPEN","url":"https://example.test/issues/27","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/27/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/27/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-27-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '[{"number":27,"url":"https://github.com/acme/widgets/pull/27","state":"MERGED","mergedAt":"2026-07-27T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"%s","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$8" ;;
  "issue view 27 --repo acme/widgets --json number,state,title,url")
    touch `+quote(closedMarker)+`
    printf '%s\n' '{"number":27,"state":"CLOSED","title":"Follow me","url":"https://github.com/acme/widgets/issues/27"}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "remove" ]; then rm -rf "$6"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
IFS= read -r prompt
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}'
touch `+quote(workerStarted)+`
while [ ! -f `+quote(emitActivity)+` ]; do sleep 0.01; done
printf '%s\n' \
  '{"type":"agent_start"}' \
  '{"type":"turn_start"}' \
  '{"type":"message_start","message":{"role":"assistant"}}' \
  '{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"private live reasoning"}}' \
  '{"type":"tool_execution_start","toolCallId":"tool-live","toolName":"bash","args":{"command":"private tool arguments"}}' \
  '{"type":"tool_execution_update","toolCallId":"tool-live","toolName":"bash","partialResult":{"output":"private tool result","durationMs":10,"spinnerFrame":1}}' \
  '{"type":"tool_execution_end","toolCallId":"tool-live","toolName":"bash","result":{"content":"private full tool result"},"isError":false}' \
  '{"type":"tool_execution_start","toolCallId":"subagent-live","toolName":"Agent","args":{"prompt":"private Subagent prompt"}}' \
  '{"type":"tool_execution_update","toolCallId":"subagent-live","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"private Subagent output"}],"details":{"description":"Review live Activity","status":"running","activity":"reviewing","turnCount":1,"toolUses":2,"tokens":"1.2k tokens","durationMs":20,"spinnerFrame":2}}}'
while [ ! -f `+quote(emitSubagentChange)+` ]; do sleep 0.01; done
printf '%s\n' '{"type":"tool_execution_update","toolCallId":"subagent-live","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"changed private Subagent output"}],"details":{"description":"Review live Activity","status":"running","activity":"testing","turnCount":1,"toolUses":2,"tokens":"1.2k tokens","durationMs":30,"spinnerFrame":3}}}'
while [ ! -f `+quote(completeActivity)+` ]; do sleep 0.01; done
printf '%s\n' \
  '{"type":"tool_execution_end","toolCallId":"subagent-live","toolName":"Agent","result":{"content":[{"type":"text","text":"private full Subagent result"}],"details":{"description":"Review live Activity","status":"completed","activity":"reviewing","turnCount":1,"toolUses":2,"tokens":"1.2k tokens","durationMs":30}},"isError":false}' \
  '{"type":"message_end","message":{"role":"assistant","content":[{"type":"thinking","thinking":"private final reasoning"},{"type":"text","text":"Live normalized Activity observed"}],"usage":{"totalTokens":73}}}'
while [ ! -f `+quote(finishWorker)+` ]; do sleep 0.01; done
printf '%s\n' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)

	runnerCommand := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	var runnerOutput bytes.Buffer
	runnerCommand.Stdout = &runnerOutput
	runnerCommand.Stderr = &runnerOutput
	if err := runnerCommand.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if runnerCommand.ProcessState == nil {
			_ = runnerCommand.Process.Kill()
			_ = runnerCommand.Wait()
		}
	}()
	waitForFile(t, workerStarted)
	active, err := (state.FileStore{Path: statePath}).Load()
	if err != nil || len(active.Runs) != 1 || active.Runs[0].Status != scheduler.StatusRunning {
		t.Fatalf("active state = %#v, err = %v", active, err)
	}
	run := active.Runs[0]
	workerPID := run.PID

	firstFollower := exec.Command(binary, "follow", run.RunID, "--repo-dir", repository, "--state-dir", stateDir)
	firstStdout, err := firstFollower.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var firstStderr bytes.Buffer
	firstFollower.Stderr = &firstStderr
	if err := firstFollower.Start(); err != nil {
		t.Fatal(err)
	}
	firstLines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(firstStdout)
		for scanner.Scan() {
			firstLines <- scanner.Text()
		}
		close(firstLines)
	}()
	var firstOutputLines []string
	waitForFollowLine := func(lines <-chan string, outputLines *[]string, diagnostics *bytes.Buffer, want string) {
		t.Helper()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatalf("normalized follower exited before %q, output = %q, stderr = %q", want, *outputLines, diagnostics.String())
				}
				*outputLines = append(*outputLines, line)
				if strings.Contains(line, want) {
					return
				}
			case <-timer.C:
				t.Fatalf("normalized follower did not emit %q, output = %q, stderr = %q", want, *outputLines, diagnostics.String())
			}
		}
	}
	waitForFollowLine(firstLines, &firstOutputLines, &firstStderr, "Run Activity (latest 20)")

	duplicateContext, cancelDuplicate := context.WithTimeout(context.Background(), 2*time.Second)
	duplicate := exec.CommandContext(duplicateContext, runnerCommand.Path, runnerCommand.Args[1:]...)
	output, duplicateErr := duplicate.CombinedOutput()
	cancelDuplicate()
	var duplicateExit *exec.ExitError
	if errors.Is(duplicateContext.Err(), context.DeadlineExceeded) {
		t.Fatalf("duplicate Runner did not promptly honor repository scheduling coordination: %q", output)
	}
	if !errors.As(duplicateErr, &duplicateExit) || duplicateExit.ExitCode() != 1 || !strings.Contains(string(output), "runner already active") {
		t.Fatalf("duplicate Runner coordination refusal = %v, output = %q", duplicateErr, output)
	}
	if err := os.WriteFile(emitActivity, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitForFollowLine(firstLines, &firstOutputLines, &firstStderr, "Worker started")
	waitForFollowLine(firstLines, &firstOutputLines, &firstStderr, "Review live Activity")
	if err := os.WriteFile(emitSubagentChange, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitForFollowLine(firstLines, &firstOutputLines, &firstStderr, "activity: testing")
	if err := os.WriteFile(completeActivity, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitForFollowLine(firstLines, &firstOutputLines, &firstStderr, "Assistant response completed: Live normalized Activity observed")
	if err := firstFollower.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("interrupt follower: %v", err)
	}
	if err := firstFollower.Wait(); err != nil {
		t.Fatalf("interrupted follower: %v, stderr = %q", err, firstStderr.String())
	}
	if firstStderr.Len() != 0 {
		t.Fatalf("live normalized follower diagnostics = %q", firstStderr.String())
	}
	for line := range firstLines {
		firstOutputLines = append(firstOutputLines, line)
	}
	firstOutput := strings.Join(firstOutputLines, "\n")
	if !strings.Contains(firstOutput, "Runner supervision: SUPERVISED\n") ||
		!strings.Contains(firstOutput, "Worker liveness: alive (PID") {
		t.Fatalf("compiled Runner was not reported as supervising its live Worker: %q", firstOutput)
	}
	stillActive, err := (state.FileStore{Path: statePath}).Load()
	if err != nil || len(stillActive.Runs) != 1 || stillActive.Runs[0].Status != scheduler.StatusRunning || stillActive.Runs[0].PID != workerPID {
		t.Fatalf("Ctrl-C changed the active Run or Worker: state = %#v, err = %v", stillActive, err)
	}
	if err := runnerCommand.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("runner stopped after follower Ctrl-C: %v", err)
	}

	secondFollower := exec.Command(binary, "follow", run.RunID, "--repo-dir", repository, "--state-dir", stateDir)
	secondStdout, err := secondFollower.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var secondDiagnostics bytes.Buffer
	secondFollower.Stderr = &secondDiagnostics
	if err := secondFollower.Start(); err != nil {
		t.Fatal(err)
	}
	secondLines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(secondStdout)
		for scanner.Scan() {
			secondLines <- scanner.Text()
		}
		close(secondLines)
	}()
	var secondOutputLines []string
	waitForFollowLine(secondLines, &secondOutputLines, &secondDiagnostics, "State: running")
	if err := os.WriteFile(finishWorker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runnerCommand.Wait(); err != nil {
		t.Fatalf("runner completion: %v\n%s", err, runnerOutput.String())
	}
	if supervised, err := runnerSupervised(filepath.Join(repository, ".git")); err != nil || supervised {
		t.Fatalf("Runner supervision after compiled Runner exit = %t, %v", supervised, err)
	}
	if strings.Contains(runnerOutput.String(), "Drain:") || strings.Contains(runnerOutput.String(), "Suspension:") {
		t.Fatalf("follower Ctrl-C affected Runner lifecycle:\n%s", runnerOutput.String())
	}
	if err := secondFollower.Wait(); err != nil {
		t.Fatalf("terminal follower: %v\n%s", err, strings.Join(secondOutputLines, "\n"))
	}
	for line := range secondLines {
		secondOutputLines = append(secondOutputLines, line)
	}
	secondOutput := strings.Join(secondOutputLines, "\n")
	final, err := (state.FileStore{Path: statePath}).Load()
	if err != nil || len(final.Runs) != 1 || final.Runs[0].Status != scheduler.StatusMerged || len(final.Leases) != 0 {
		t.Fatalf("final state = %#v, err = %v", final, err)
	}
	combinedFollowOutput := firstOutput + "\n" + secondOutput
	for _, want := range []string{
		"Run: " + run.RunID, "Worker started", "Worker turn started", "Model streaming",
		"Tool bash started", "Tool bash output changed", "Tool bash completed",
		"Review live Activity", "Assistant response completed: Live normalized Activity observed",
		"Completed Worker tokens: 73", "Run state changed to merged", "Terminal Run summary:",
	} {
		if !strings.Contains(combinedFollowOutput, want) {
			t.Fatalf("normalized Follow output missing %q:\n%s", want, combinedFollowOutput)
		}
	}
	for _, private := range []string{
		"private live reasoning", "private final reasoning", "private tool arguments", "private tool result",
		"private full tool result", "private Subagent prompt", "private Subagent output", "changed private Subagent output", "private full Subagent result",
	} {
		if strings.Contains(combinedFollowOutput, private) {
			t.Fatalf("normalized Follow output exposed %q:\n%s", private, combinedFollowOutput)
		}
	}
	if strings.Contains(combinedFollowOutput, `{"type":`) {
		t.Fatalf("normalized Follow emitted raw Worker JSONL:\n%s", combinedFollowOutput)
	}
	if secondDiagnostics.Len() != 0 {
		t.Fatalf("terminal normalized follower diagnostics = %q", secondDiagnostics.String())
	}
}

func TestCompiledPseudoTerminalDashboardHandlesResizeNavigationActivityAndStagedSignals(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)
	stateDir := filepath.Join(root, "state")
	workerStarted := filepath.Join(root, "worker-started")
	abortReceived := filepath.Join(root, "abort-received")

	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":73,"title":"Terminal teardown","createdAt":"2026-01-01T00:00:00Z","url":"https://github.com/acme/widgets/issues/73"}]' ;;
  "issue view 73 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":73,"title":"Terminal teardown","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/73","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/73/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/73/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
trap '' TERM
IFS= read -r prompt
printf '%s\n' \
  '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' \
  '{"type":"agent_start"}' \
  '{"type":"turn_start"}' \
  '{"type":"tool_execution_start","toolCallId":"tool-live","toolName":"bash","args":{"command":"private"}}'
touch `+quote(workerStarted)+`
IFS= read -r abort
touch `+quote(abortReceived)+`
while :; do sleep 1; done
`)

	command := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi)
	terminal, childTerminal, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	defer childTerminal.Close()
	if err := pty.Setsize(terminal, &pty.Winsize{Rows: 28, Cols: 100}); err != nil {
		t.Fatal(err)
	}
	initialTerminalState, err := term.GetState(int(terminal.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	command.Stdin, command.Stdout, command.Stderr = childTerminal, childTerminal, childTerminal
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if err := childTerminal.Close(); err != nil {
		t.Fatal(err)
	}
	var output synchronizedBuffer
	readDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&output, terminal)
		close(readDone)
	}()
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()

	waitForFile(t, workerStarted)
	waitForPseudoTerminalScreen(t, &output, 100, 28, "Deepest operation: bash")
	if _, err := terminal.Write([]byte("G")); err != nil {
		t.Fatalf("navigate to dashboard end: %v", err)
	}
	waitForPseudoTerminalScreen(t, &output, 100, 28, "> Operational messages")
	if _, err := terminal.Write([]byte("g")); err != nil {
		t.Fatalf("navigate to dashboard start: %v", err)
	}
	waitForPseudoTerminalScreen(t, &output, 100, 28, "> Admission health")

	if err := pty.Setsize(terminal, &pty.Winsize{Rows: 10, Cols: 72}); err != nil {
		t.Fatalf("resize pseudo-terminal: %v", err)
	}
	if err := command.Process.Signal(syscall.SIGWINCH); err != nil {
		t.Fatalf("notify pseudo-terminal resize: %v", err)
	}
	waitForPseudoTerminalScreen(t, &output, 72, 10, "Backlog: acme/widgets")
	if err := pty.Setsize(terminal, &pty.Winsize{Rows: 28, Cols: 100}); err != nil {
		t.Fatalf("restore pseudo-terminal size: %v", err)
	}
	if err := command.Process.Signal(syscall.SIGWINCH); err != nil {
		t.Fatalf("notify restored pseudo-terminal size: %v", err)
	}

	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send first staged SIGINT: %v", err)
	}
	waitForBuffer(t, &output, "Draining")
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send second staged SIGINT: %v", err)
	}
	waitForFile(t, abortReceived)
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send force-stop SIGINT: %v", err)
	}

	err = command.Wait()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 130 {
		t.Fatalf("compiled pseudo-terminal exit = %v, want 130; output = %q", err, output.String())
	}
	restoredTerminalState, stateErr := term.GetState(int(terminal.Fd()))
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if !reflect.DeepEqual(restoredTerminalState, initialTerminalState) {
		t.Fatalf("compiled dashboard terminal state after force stop = %#v, want %#v", restoredTerminalState, initialTerminalState)
	}
	select {
	case <-readDone:
	case <-time.After(3 * time.Second):
		t.Fatal("pseudo-terminal output reader did not finish")
	}

	raw := output.String()
	for _, want := range []string{
		"\x1b[?1049h", "\x1b[?1049l", "\x1b[?25l", "\x1b[?25h",
		"Final aggregate summary", "Final outcome: Force stop finished with errors",
		"Completions produced (0)", "Attention Required (1)", "#73  Terminal teardown",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("compiled pseudo-terminal output missing %q: %q", want, raw)
		}
	}
	restore := strings.LastIndex(raw, "\x1b[?1049l")
	summary := strings.Index(raw, "Final aggregate summary")
	if restore < 0 || summary < restore {
		t.Fatalf("normal-screen summary preceded terminal restoration: %q", raw)
	}
}

func waitForPseudoTerminalScreen(t *testing.T, output *synchronizedBuffer, width, height int, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(terminalScreenText(output.String(), width, height), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("pseudo-terminal screen never contained %q:\n%s\nraw output: %q", want, terminalScreenText(output.String(), width, height), output.String())
}

func buildExecutable(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(root, "backlog")
	build := exec.Command("go", "build", "-o", binary, "./cmd/backlog")
	build.Dir = filepath.Clean(filepath.Join("..", ".."))
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build compiled acceptance executable: %v\n%s", err, output)
	}
	return binary
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("file %s was not created", path)
}

func TestCompiledExecutableRunsAFKThroughDurableRPCSettlement(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)

	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	finished := filepath.Join(root, "finished")
	piAlive := filepath.Join(root, "pi-alive")
	finishPi := filepath.Join(root, "finish-pi")
	reconciledAlive := filepath.Join(root, "reconciled-while-pi-alive")
	piArgs := filepath.Join(root, "pi.args")
	prompt := filepath.Join(root, "prompt.json")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if test -f `+quote(finished)+`; then printf '%s\n' '[]'; else printf '%s\n' '[{"number":5,"title":"RPC","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/5"}]'; fi ;;
  "issue view 5 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":5,"title":"RPC","body":"","state":"OPEN","url":"https://example.test/issues/5","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/5/comments?per_page=100 --paginate --slurp") printf '%s\n' '[[]]' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/5/dependencies/blocked_by?per_page=100 --paginate --slurp") printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-5-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    test -f `+quote(piAlive)+`
    head=$8
    printf '[{"number":5,"url":"https://github.com/acme/widgets/pull/5","state":"MERGED","mergedAt":"2026-07-22T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"%s","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$head" ;;
  "issue view 5 --repo acme/widgets --json number,state,title,url")
    test -f `+quote(piAlive)+`
    touch `+quote(reconciledAlive)+` `+quote(finished)+`
    printf '%s\n' '{"number":5,"state":"CLOSED","title":"RPC","url":"https://github.com/acme/widgets/issues/5"}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "remove" ]; then rm -rf "$6"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
if env | grep -q '^HERDR_'; then
  echo 'Pi Worker inherited the foreground Herdr pane environment' >&2
  exit 9
fi
if [ "$PWD" != "$(pwd)" ]; then
  echo 'Pi Worker PWD does not match its worktree' >&2
  exit 9
fi
printf '%s\n' "$*" > `+quote(piArgs)+`
touch `+quote(piAlive)+`
grep -q '"status": "running"' `+quote(statePath)+`
grep -q '"workerMode": "rpc"' `+quote(statePath)+`
grep -q '"issueTitle": "RPC"' `+quote(statePath)+`
grep -q '"issueUrl": "https://example.test/issues/5"' `+quote(statePath)+`
grep -q '"logPath": ".*\.jsonl"' `+quote(statePath)+`
grep -q '"stderrPath": ".*\.stderr\.log"' `+quote(statePath)+`
grep -q '"pid": '"$$" `+quote(statePath)+`
IFS= read -r command
printf '%s\n' "$command" > `+quote(prompt)+`
while [ ! -f `+quote(finishPi)+` ]; do sleep 0.01; done
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
test -f `+quote(reconciledAlive)+`
grep -q '"status": "merged"' `+quote(statePath)+`
rm -f `+quote(piAlive)+`
`)

	herdrSocket, herdrRequests, herdrDone := captureHerdrLifecycle(t)
	runArgs := []string{"run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi}
	herdrEnvironment := append(os.Environ(),
		"HERDR_ENV=1",
		"HERDR_SOCKET_PATH="+herdrSocket,
		"HERDR_PANE_ID=w1:p1",
		"HERDR_FUTURE_STATE=must-not-reach-worker",
	)
	command := exec.Command(binary, runArgs...)
	command.Env = herdrEnvironment
	var commandOutput bytes.Buffer
	command.Stdout = &commandOutput
	command.Stderr = &commandOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	workingRequest := waitForHerdrRequest(t, herdrRequests)
	if workingRequest.Method != "pane.report_agent" {
		t.Fatalf("first Herdr lifecycle request = %#v", workingRequest)
	}
	working := workingRequest.Params
	if working.PaneID != "w1:p1" || working.Source != "custom:backlog" || working.Agent != "backlog" || working.State != "working" || working.Message != "scheduling Runs" {
		t.Fatalf("Herdr working report = %#v", working)
	}
	waitForFile(t, piAlive)
	assertNoHerdrRequest(t, herdrRequests, "runner released its Herdr entry while its Worker was active")

	duplicate := exec.Command(binary, runArgs...)
	duplicate.Env = herdrEnvironment
	if output, err := duplicate.CombinedOutput(); err == nil {
		t.Fatalf("duplicate runner acquired the repository lock, output = %q", output)
	}
	assertNoHerdrRequest(t, herdrRequests, "lock-rejected duplicate changed the active runner's Herdr entry")

	if err := os.WriteFile(finishPi, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("compiled RPC run: %v\n%s", err, commandOutput.String())
	}
	releaseRequest := waitForHerdrRequest(t, herdrRequests)
	if releaseRequest.Method != "pane.release_agent" {
		t.Fatalf("final Herdr lifecycle request = %#v", releaseRequest)
	}
	if err := <-herdrDone; err != nil {
		t.Fatalf("capture Herdr lifecycle: %v", err)
	}
	current, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || len(current.Leases) != 0 {
		t.Fatalf("Runs/Leases = %#v/%#v", current.Runs, current.Leases)
	}
	run := current.Runs[0]
	if run.Status != scheduler.StatusMerged || run.WorkerMode != scheduler.WorkerModeRPC || run.SessionID != "backlog-"+run.RunID ||
		run.Issue != 5 || run.IssueTitle != "RPC" || run.IssueURL != "https://example.test/issues/5" ||
		run.LogPath != filepath.Join(stateDir, "logs", run.RunID+".jsonl") || run.StderrPath != filepath.Join(stateDir, "logs", run.RunID+".stderr.log") {
		t.Fatalf("persisted RPC Run = %#v", run)
	}
	wantSessionDir := filepath.Join(stateDir, "sessions", run.RunID)
	if run.SessionDir != wantSessionDir {
		t.Fatalf("session directory = %q, want %q", run.SessionDir, wantSessionDir)
	}
	if info, err := os.Stat(wantSessionDir); err != nil || !info.IsDir() {
		t.Fatalf("dedicated session storage missing: info=%v err=%v", info, err)
	}
	args, _ := os.ReadFile(piArgs)
	if !strings.Contains(string(args), "--mode rpc") || !strings.Contains(string(args), "--session-id "+run.SessionID) || !strings.Contains(string(args), "--session-dir "+run.SessionDir) {
		t.Fatalf("Pi RPC args = %q", args)
	}
	promptData, _ := os.ReadFile(prompt)
	if strings.TrimSpace(string(promptData)) != `{"id":"backlog-afk-prompt","type":"prompt","message":"/skill:afk 5"}` {
		t.Fatalf("AFK prompt = %q", promptData)
	}
	if _, err := os.Stat(piAlive); !os.IsNotExist(err) {
		t.Fatalf("Pi process did not shut down after persisted reconciliation: %v", err)
	}

	unavailableSocket := exec.Command(binary, runArgs...)
	unavailableSocket.Env = append(os.Environ(),
		"HERDR_ENV=1",
		"HERDR_SOCKET_PATH="+filepath.Join(root, "missing-herdr.sock"),
		"HERDR_PANE_ID=w1:p1",
	)
	if output, err := unavailableSocket.CombinedOutput(); err != nil {
		t.Fatalf("Herdr socket failure changed runner outcome: %v\n%s", err, output)
	}
}

func TestCompiledExecutablePersistsNewRunContextWhenWorkerSetupFails(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)

	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	piStarted := filepath.Join(root, "pi-started")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":6,"title":"Setup failure context","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/6"}]' ;;
  "issue view 6 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":6,"title":"Setup failure context","body":"","state":"OPEN","url":"https://example.test/issues/6","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/6/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/6/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then
  mkdir -p "$7"
  printf blocked > `+quote(filepath.Join(stateDir, "logs"))+`
  exit 0
fi
exit 0
`)
	pi := writeExecutable(t, `#!/bin/sh
touch `+quote(piStarted)+`
exit 9
`)

	output, err := exec.Command(binary, "run", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi).CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
		t.Fatalf("compiled natural exhaustion exit = %v, want 1\n%s", err, output)
	}
	for _, want := range []string{"Final aggregate summary", "Active (0)", "Attention Required (1)", "#6  Setup failure context  failed"} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("compiled final summary missing %q:\n%s", want, output)
		}
	}
	current, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || len(current.Leases) != 1 {
		t.Fatalf("Runs/Leases after setup failure = %#v/%#v", current.Runs, current.Leases)
	}
	run := current.Runs[0]
	if run.Issue != 6 || run.IssueTitle != "Setup failure context" || run.IssueURL != "https://example.test/issues/6" ||
		run.Status != scheduler.StatusFailed || !strings.Contains(run.Error, "create worker log directory") || run.LogPath != "" || run.StderrPath != "" {
		t.Fatalf("Run after Worker setup failure = %#v", run)
	}
	if _, err := os.Stat(piStarted); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Pi started despite setup failure: %v", err)
	}
}

func TestCompiledExecutablePreviewsV1StatusAndRunnerMigratesDuringStartup(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	binary := buildExecutable(t, root)

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
	if upgraded.Leases[0].RunID != "legacy-running" || upgraded.Runs[0].WorkerMode != scheduler.WorkerModePrint || upgraded.Runs[1].WorkerMode != scheduler.WorkerModePrint ||
		upgraded.Runs[0].IssueTitle != "" || upgraded.Runs[0].IssueURL != "" || upgraded.Runs[1].IssueTitle != "" || upgraded.Runs[1].IssueURL != "" {
		t.Fatalf("upgraded worker, Lease, and issue snapshot metadata = %#v / %#v", upgraded.Runs, upgraded.Leases)
	}
	persistedAfterStatus, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(persistedAfterStatus) != legacy {
		t.Fatalf("status persisted legacy migration:\n%s", persistedAfterStatus)
	}

	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-42-legacy-running --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":42,"url":"https://github.com/acme/widgets/pull/42","state":"MERGED","mergedAt":"2026-07-03T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-legacy-running","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "issue view 42 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":42,"state":"CLOSED","title":"Migrated","url":"https://github.com/acme/widgets/issues/42"}' ;;
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
	persistedAfterRun, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(persistedAfterRun), `"version": 4`) || strings.Contains(string(persistedAfterRun), `"paused"`) {
		t.Fatalf("Runner did not persist legacy migration:\n%s", persistedAfterRun)
	}
	final, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Runs) != 2 || len(final.Leases) != 0 {
		t.Fatalf("reconciled Runs/Leases = %#v/%#v", final.Runs, final.Leases)
	}
	if final.Runs[0].RunID != "old-merged" || final.Runs[0].Error != "retained merged diagnostic" ||
		final.Runs[0].IssueTitle != "" || final.Runs[0].IssueURL != "" {
		t.Fatalf("existing merged history changed or was backfilled: %#v", final.Runs[0])
	}
	reconciled := final.Runs[1]
	if reconciled.RunID != "legacy-running" || reconciled.Status != scheduler.StatusMerged || reconciled.WorkerMode != scheduler.WorkerModePrint ||
		reconciled.Branch != "agent/issue-42-legacy-running" || reconciled.Worktree != worktreePath || reconciled.SessionName != "afk #42" ||
		reconciled.LogPath != "/retained/legacy.jsonl" || reconciled.StderrPath != "/retained/legacy.stderr.log" || reconciled.PullRequest != "https://github.com/acme/widgets/pull/42" ||
		reconciled.IssueTitle != "" || reconciled.IssueURL != "" {
		t.Fatalf("startup reconciliation lost migrated artifacts: %#v", reconciled)
	}
}

type capturedHerdrRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params struct {
		PaneID  string `json:"pane_id"`
		Source  string `json:"source"`
		Agent   string `json:"agent"`
		State   string `json:"state"`
		Message string `json:"message"`
		Seq     uint64 `json:"seq"`
	} `json:"params"`
}

func captureHerdrLifecycle(t *testing.T) (string, <-chan capturedHerdrRequest, <-chan error) {
	t.Helper()
	directory, err := os.MkdirTemp("", "backlog-herdr-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })

	socketPath := filepath.Join(directory, "herdr.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })

	requests := make(chan capturedHerdrRequest, 2)
	done := make(chan error, 1)
	go func() {
		defer close(requests)
		for range 2 {
			connection, err := listener.AcceptUnix()
			if err != nil {
				done <- err
				return
			}
			var request capturedHerdrRequest
			if err := json.NewDecoder(connection).Decode(&request); err != nil {
				connection.Close()
				done <- err
				return
			}
			requests <- request
			response := fmt.Sprintf(`{"id":%q,"result":{"type":"ok"}}`+"\n", request.ID)
			if _, err := connection.Write([]byte(response)); err != nil {
				connection.Close()
				done <- err
				return
			}
			if err := connection.Close(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return socketPath, requests, done
}

func waitForHerdrRequest(t *testing.T, requests <-chan capturedHerdrRequest) capturedHerdrRequest {
	t.Helper()
	select {
	case request, ok := <-requests:
		if !ok {
			t.Fatal("fake Herdr server closed before receiving the expected request")
		}
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Herdr lifecycle request")
		return capturedHerdrRequest{}
	}
}

func assertNoHerdrRequest(t *testing.T, requests <-chan capturedHerdrRequest, message string) {
	t.Helper()
	select {
	case request, ok := <-requests:
		if !ok {
			t.Fatal("fake Herdr server closed unexpectedly")
		}
		t.Fatalf("%s: %#v", message, request)
	case <-time.After(100 * time.Millisecond):
	}
}
