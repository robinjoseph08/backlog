package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestProcessRejectsIncompleteRPCSessionAndUncreatableStorage(t *testing.T) {
	t.Parallel()

	supervisor := Supervisor{Executable: "/does/not/matter", LogsDir: t.TempDir()}
	if _, err := supervisor.Start(context.Background(), Request{Issue: 5}); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("incomplete request error = %v", err)
	}
	root := t.TempDir()
	blocked := filepath.Join(root, "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	req := request(5, "run-5", root, filepath.Join(blocked, "run-5"))
	if _, err := supervisor.Start(context.Background(), req); err == nil || !strings.Contains(err.Error(), "create Pi session directory") {
		t.Fatalf("session storage error = %v", err)
	}
}

func TestProcessStartsDeterministicPersistentRPCSessionInWorktree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	argsPath := filepath.Join(root, "args")
	cwdPath := filepath.Join(root, "cwd")
	inputPath := filepath.Join(root, "input")
	pi := fakePi(t, `
printf '%s\n' "$*" > `+shellQuote(argsPath)+`
pwd > `+shellQuote(cwdPath)+`
IFS= read -r command
printf '%s\n' "$command" > `+shellQuote(inputPath)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
echo 'diagnostic' >&2
`)
	worktree := filepath.Join(root, "worktree")
	sessionDir := filepath.Join(root, "sessions", "run-42")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs"), Approve: true}).Start(
		context.Background(), request(42, "run-42", worktree, sessionDir),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if process.PID() <= 0 {
		t.Fatalf("pid = %d, want positive", process.PID())
	}
	if got := process.command.Args[3]; got != "backlog-gate" {
		t.Fatalf("wrapper process name = %q, want backlog-gate", got)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	settled := process.Wait()
	if settled.Err != nil || !settled.Settled {
		t.Fatalf("wait = %#v", settled)
	}
	if _, err := os.Stat(filepath.Join(root, "args")); err != nil {
		t.Fatalf("Pi was not alive at settlement: %v", err)
	}
	result := process.Close()
	if result.Err != nil || result.ExitCode != 0 {
		t.Fatalf("close = %#v", result)
	}

	args, _ := os.ReadFile(argsPath)
	wantArgs := `--mode rpc --approve --name afk #42 --session-dir ` + sessionDir + ` --session-id backlog-run-42`
	if strings.TrimSpace(string(args)) != wantArgs {
		t.Fatalf("args = %q, want %q", strings.TrimSpace(string(args)), wantArgs)
	}
	input, _ := os.ReadFile(inputPath)
	if strings.TrimSpace(string(input)) != `{"id":"backlog-afk-prompt","type":"prompt","message":"/skill:afk 42"}` {
		t.Fatalf("RPC input = %q", input)
	}
	cwd, _ := os.ReadFile(cwdPath)
	if strings.TrimSpace(string(cwd)) != worktree {
		t.Fatalf("cwd = %q, want %q", strings.TrimSpace(string(cwd)), worktree)
	}
	if info, err := os.Stat(sessionDir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("session directory mode = %v, err = %v", info.Mode().Perm(), err)
	}
	stdout, err := os.ReadFile(result.LogPath)
	if err != nil || !strings.Contains(string(stdout), `"agent_settled"`) {
		t.Fatalf("stdout log = %q, err = %v", stdout, err)
	}
	stderr, err := os.ReadFile(result.StderrPath)
	if err != nil || strings.TrimSpace(string(stderr)) != "diagnostic" {
		t.Fatalf("stderr log = %q, err = %v", stderr, err)
	}
}

func TestProcessCannotSubmitPromptUntilReleased(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	marker := filepath.Join(root, "started")
	input := filepath.Join(root, "input")
	pi := fakePi(t, `
printf started > `+shellQuote(marker)+`
IFS= read -r command
printf '%s' "$command" > `+shellQuote(input)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(5, "run-5", root, filepath.Join(root, "sessions", "run-5")),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Pi ran before durable release, stat error = %v", err)
	}
	if _, err := os.Stat(input); !os.IsNotExist(err) {
		t.Fatalf("prompt was submitted before release, stat error = %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait: %#v", result)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatalf("close: %v", result.Err)
	}
	if _, err := os.Stat(input); err != nil {
		t.Fatalf("prompt was not submitted after release: %v", err)
	}
}

func TestProcessRPCValidationFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		output string
		want   string
	}{
		{"malformed", "not-json\\n", "malformed Pi RPC JSON"},
		{"truncated", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}`, "truncated Pi RPC JSON"},
		{"duplicate response", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n`, "duplicated Pi RPC prompt response"},
		{"mismatched response", `{"id":"wrong","type":"response","command":"prompt","success":true}\n`, "mismatched Pi RPC response"},
		{"invalid order", `{"type":"agent_settled"}\n`, "invalidly ordered Pi RPC agent_settled"},
		{"duplicate agent end", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"agent_end"}\n{"type":"agent_end"}\n`, "invalidly ordered Pi RPC agent_end"},
		{"unmatched turn end", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_end"}\n`, "invalidly ordered Pi RPC turn_end"},
		{"unsupported dialog", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"extension_ui_request","id":"ui-1","method":"confirm"}\n`, "unsupported interactive Pi RPC request"},
		{"unknown type", `{"type":"surprise"}\n`, "unknown Pi RPC message type"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			pi := fakePi(t, `
IFS= read -r command
printf '`+test.output+`'
`)
			process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
				context.Background(), request(7, "run-7", root, filepath.Join(root, "sessions", "run-7")),
			)
			if err != nil {
				t.Fatalf("start: %v", err)
			}
			if err := process.Release(); err != nil {
				t.Fatalf("release: %v", err)
			}
			result := process.Wait()
			if result.Err == nil || !strings.Contains(result.Err.Error(), test.want) {
				t.Fatalf("wait error = %v, want containing %q", result.Err, test.want)
			}
			_ = process.Abort()
			_ = process.Close()
		})
	}
}

func TestProcessAcceptsRetriesAndFireAndForgetExtensionUI(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' \
  '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' \
  '{"type":"extension_ui_request","id":"ui-1","method":"setTitle"}' \
  '{"type":"agent_start"}' \
  '{"type":"agent_end"}' \
  '{"type":"auto_retry_start"}' \
  '{"type":"auto_retry_end"}' \
  '{"type":"agent_start"}' \
  '{"type":"agent_end"}' \
  '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(11, "run-11", root, filepath.Join(root, "sessions", "run-11")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("valid retry stream = %#v", result)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatal(result.Err)
	}
}

func TestProcessAcceptsOnlyLFAsRecordBoundary(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true,"data":"line separator"}\r\n'
printf '%s\n' '{"type":"agent_start"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(10, "run-10", root, filepath.Join(root, "sessions", "run-10")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("strict LF parser rejected Unicode separator or CRLF: %#v", result)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatal(result.Err)
	}
}

func TestCloseWaitsForTheWorkerProcessGroupToExit(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	childDone := filepath.Join(root, "child-done")
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
(sleep 0.08; touch `+shellQuote(childDone)+`) </dev/null >/dev/null 2>&1 &
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: time.Second,
	}).Start(context.Background(), request(12, "run-12", root, filepath.Join(root, "sessions", "run-12")))
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatalf("close = %#v", result)
	}
	if _, err := os.Stat(childDone); err != nil {
		t.Fatalf("Close returned before a process-group child exited: %v", err)
	}
}

func TestAbortEscalatesForWorkerIgnoringTermination(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	childPIDPath := filepath.Join(root, "child.pid")
	pi := fakePi(t, `
IFS= read -r command
sh -c 'trap "" TERM; while :; do sleep 1; done' &
child=$!
printf '%s\n' "$child" > `+shellQuote(childPIDPath)+`
trap 'exit 0' TERM
wait "$child"
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 30 * time.Millisecond,
	}).Start(context.Background(), request(6, "run-6", root, filepath.Join(root, "sessions", "run-6")))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	var childData []byte
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		childData, _ = os.ReadFile(childPIDPath)
		if len(strings.TrimSpace(string(childData))) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	started := time.Now()
	if err := process.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	result := process.Close()
	if time.Since(started) > time.Second {
		t.Fatalf("abort took %s, want bounded escalation", time.Since(started))
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(string(childData)))
	if err != nil {
		t.Fatalf("parse child pid: %v", err)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if err := syscall.Kill(childPID, syscall.Signal(0)); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant pid %d survived process-group escalation; leader exit code %d", childPID, result.ExitCode)
}

func TestProcessExplicitlyRejectsProjectTrustWhenApprovalIsDisabled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	argsPath := filepath.Join(root, "args")
	pi := fakePi(t, `
printf '%s\n' "$*" > `+shellQuote(argsPath)+`
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs"), Approve: false}).Start(
		context.Background(), request(8, "run-8", root, filepath.Join(root, "sessions", "run-8")),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil {
		t.Fatalf("wait: %v", result.Err)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatalf("close: %v", result.Err)
	}
	args, _ := os.ReadFile(argsPath)
	if !strings.Contains(string(args), "--no-approve") || strings.Contains(string(args), " --approve") {
		t.Fatalf("args = %q, want --no-approve only", strings.TrimSpace(string(args)))
	}
}

func request(issue int, runID, worktree, sessionDir string) Request {
	return Request{
		Issue: issue, RunID: runID, Worktree: worktree, SessionName: "afk #" + strconv.Itoa(issue),
		SessionID: "backlog-" + runID, SessionDir: sessionDir,
	}
}

func fakePi(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pi")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
