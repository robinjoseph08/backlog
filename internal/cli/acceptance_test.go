package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

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
  "pr list --repo acme/widgets --state all --head agent/issue-31-"*" --json number,url,state,mergedAt,autoMergeRequest,isDraft")
    printf '%s\n' '[]' ;;
  "issue view 31 --repo acme/widgets --json state,title,url")
    printf '%s\n' '{"state":"OPEN","title":"First","url":"https://example.test/issues/31"}' ;;
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
session_file="$session_dir/session.jsonl"
printf '{"type":"session","version":3,"id":"%s","cwd":"%s"}\n' "$session_id" "$worktree" > "$session_file"
printf '%s\n' '{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}' >> "$session_file"
printf '%s\n' '{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
IFS= read -r state
printf '{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":"%s","sessionId":"%s"}}\n' "$session_file" "$session_id"
IFS= read -r entries
printf '%s\n' '{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":[{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}],"leafId":"leaf"}}'
while IFS= read -r ignored; do :; done
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
	lines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	waitForFile(t, workerStarted)
	if current, err := (state.FileStore{Path: statePath}).Load(); err != nil || len(current.Leases) != 1 || current.Leases[0].Issue != 31 {
		t.Fatalf("state before SIGINT = %#v, err = %v", current, err)
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
		if strings.Contains(line, "Drain: admission stopped; 1 Worker remaining; next SIGINT will be recorded as a suspension request") {
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
		if strings.Contains(line, "Drain: additional interrupt recorded as a suspension request; 1 Worker remaining") {
			break
		}
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
	if len(current.Runs) != 1 || current.Runs[0].Issue != 31 || current.Runs[0].Status != scheduler.StatusSuspended ||
		current.Runs[0].PID != 0 || current.Runs[0].Continuation == nil || len(current.Leases) != 1 {
		t.Fatalf("persisted state after second SIGINT = %#v", current)
	}
	output := strings.Join(outputLines, "\n")
	if !strings.Contains(output, "Suspension complete: 0 Workers remaining") {
		t.Fatalf("suspension output = %q", output)
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
  "pr list --repo acme/widgets --state all --head agent/issue-33-"*" --json number,url,state,mergedAt,autoMergeRequest,isDraft")
    printf '%s\n' '[]' ;;
  "issue view 33 --repo acme/widgets --json state,title,url")
    printf '%s\n' '{"state":"OPEN","title":"Terminate","url":"https://example.test/issues/33"}' ;;
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
	deadline := time.Now().Add(5 * time.Second)
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
  "pr list --repo acme/widgets --state all --head agent/issue-5-"*" --json number,url,state,mergedAt,autoMergeRequest,isDraft")
    test -f `+quote(piAlive)+`
    printf '%s\n' '[{"number":5,"url":"https://example.test/pull/5","state":"MERGED","mergedAt":"2026-07-22T00:00:00Z"}]' ;;
  "issue view 5 --repo acme/widgets --json state,title,url")
    test -f `+quote(piAlive)+`
    touch `+quote(reconciledAlive)+` `+quote(finished)+`
    printf '%s\n' '{"state":"CLOSED","title":"RPC","url":"https://example.test/issues/5"}' ;;
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
	if run.Status != scheduler.StatusMerged || run.WorkerMode != scheduler.WorkerModeRPC || run.SessionID != "backlog-"+run.RunID {
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

func TestCompiledExecutableMigratesV1StatusAndReconcilesStartup(t *testing.T) {
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
