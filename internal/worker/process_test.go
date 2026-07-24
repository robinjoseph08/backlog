package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestWorkersDoNotInheritHerdrPaneEnvironment(t *testing.T) {
	environment := []string{
		"PATH=/usr/bin",
		"PWD=/old/directory",
		"HERDR_ENV=1",
		"HERDR_SOCKET_PATH=/tmp/herdr.sock",
		"HERDR_PANE_ID=w1:p1",
		"HERDR_FUTURE_STATE=must-also-be-removed",
		"BACKLOG_TEST=preserved",
	}

	filtered := workerEnvironment(environment, "/worktree")
	if got := strings.Join(filtered, "\n"); got != "PATH=/usr/bin\nBACKLOG_TEST=preserved\nPWD=/worktree" {
		t.Fatalf("filtered environment = %q", got)
	}
}

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
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
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
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
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

func TestReleaseCreatesStartGateBeforeWritingPrompt(t *testing.T) {
	t.Parallel()

	gatePath := filepath.Join(t.TempDir(), "run.start")
	input := &gateCheckingWriteCloser{t: t, gatePath: gatePath}
	process := &Process{gatePath: gatePath, stdin: input, events: &rpcWriter{issue: 5}}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if !input.wrote {
		t.Fatal("AFK prompt was not written")
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
		{"wrong response command", `{"id":"backlog-afk-prompt","type":"response","command":"abort","success":true}\n`, "mismatched Pi RPC response"},
		{"missing response success", `{"id":"backlog-afk-prompt","type":"response","command":"prompt"}\n`, "Pi RPC prompt was rejected"},
		{"rejected response", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":false}\n`, "Pi RPC prompt was rejected"},
		{"invalid order", `{"type":"agent_settled"}\n`, "invalidly ordered Pi RPC agent_settled"},
		{"duplicate agent end", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_start"}\n{"type":"turn_end"}\n{"type":"agent_end"}\n{"type":"agent_end"}\n`, "invalidly ordered Pi RPC agent_end"},
		{"unmatched turn end", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_end"}\n`, "invalidly ordered Pi RPC turn_end"},
		{"unsupported dialog", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"extension_ui_request","id":"ui-1","method":"confirm"}\n`, "unsupported interactive Pi RPC request"},
		{"duplicate retry attempt", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_start"}\n{"type":"turn_end"}\n{"type":"agent_end"}\n{"type":"auto_retry_start","attempt":1}\n{"type":"auto_retry_start","attempt":1}\n`, "invalidly ordered Pi RPC auto_retry_start"},
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
  '{"type":"turn_start"}' \
  '{"type":"message_start"}' \
  '{"type":"message_update"}' \
  '{"type":"message_end"}' \
  '{"type":"tool_execution_start","toolCallId":"tool-1"}' \
  '{"type":"tool_execution_start","toolCallId":"tool-2"}' \
  '{"type":"tool_execution_update","toolCallId":"tool-2"}' \
  '{"type":"tool_execution_end","toolCallId":"tool-1"}' \
  '{"type":"tool_execution_end","toolCallId":"tool-2"}' \
  '{"type":"queue_update"}' \
  '{"type":"turn_end"}' \
  '{"type":"agent_end"}' \
  '{"type":"auto_retry_start","attempt":1}' \
  '{"type":"agent_start"}' \
  '{"type":"turn_start"}' \
  '{"type":"message_start"}' \
  '{"type":"message_end"}' \
  '{"type":"turn_end"}' \
  '{"type":"agent_end"}' \
  '{"type":"auto_retry_start","attempt":2}' \
  '{"type":"agent_start"}' \
  '{"type":"turn_start"}' \
  '{"type":"message_start"}' \
  '{"type":"message_end"}' \
  '{"type":"auto_retry_end","attempt":2}' \
  '{"type":"turn_end"}' \
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

func TestCloseReportsProtocolFailureAfterSettlement(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
sleep 0.05
printf '%s\n' '{"type":"surprise"}'
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(13, "run-13", root, filepath.Join(root, "sessions", "run-13")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}
	if result := process.Close(); result.Err == nil || !strings.Contains(result.Err.Error(), "followed agent_settled") {
		t.Fatalf("close error = %v, want post-settlement protocol failure", result.Err)
	}
}

func TestProcessAcceptsOnlyLFAsRecordBoundary(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true,"data":"line separator"}\r\n'
printf '%s\n' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
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
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
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

func TestCloseEscalatesWhenWorkerIgnoresInputClosure(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
trap '' TERM
while :; do sleep 1; done
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 30 * time.Millisecond,
	}).Start(context.Background(), request(14, "run-14", root, filepath.Join(root, "sessions", "run-14")))
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}
	started := time.Now()
	result := process.Close()
	if time.Since(started) > time.Second {
		t.Fatalf("Close took %s, want bounded escalation", time.Since(started))
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), "did not exit after input closed") {
		t.Fatalf("close error = %v, want graceful-exit timeout", result.Err)
	}
	if err := syscall.Kill(-process.PID(), syscall.Signal(0)); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("Worker process group survived Close escalation: %v", err)
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

func TestProcessSuspendVerifiesAndSyncsContinuationBoundary(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "worktree")
	sessionDir := filepath.Join(root, "sessions")
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	started := filepath.Join(root, "started")
	if err := os.MkdirAll(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	header := `{"type":"session","version":3,"id":"backlog-run-50","timestamp":"2026-07-23T00:00:00Z","cwd":` + strconv.Quote(worktree) + `}`
	entries := []string{
		`{"type":"message","id":"user","parentId":null,"timestamp":"2026-07-23T00:00:01Z","message":{"role":"user","content":"work"}}`,
		`{"type":"message","id":"assistant","parentId":"user","timestamp":"2026-07-23T00:00:02Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"tool-1","name":"bash","arguments":{}}],"stopReason":"toolUse"}}`,
		`{"type":"message","id":"result","parentId":"assistant","timestamp":"2026-07-23T00:00:03Z","message":{"role":"toolResult","toolCallId":"tool-1","toolName":"bash","content":[{"type":"text","text":"done"}],"isError":false}}`,
	}
	entriesJSON := "[" + strings.Join(entries, ",") + "]"
	pi := fakePi(t, `
IFS= read -r prompt
touch `+shellQuote(started)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"tool_execution_start","toolCallId":"tool-1"}'
IFS= read -r abort
printf '%s\n' `+shellQuote(header)+` `+shellQuote(entries[0])+` `+shellQuote(entries[1])+` `+shellQuote(entries[2])+` > `+shellQuote(sessionFile)+`
printf '%s\n' '{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}' '{"type":"tool_execution_end","toolCallId":"tool-1"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
IFS= read -r state
printf '%s\n' '{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":`+strings.ReplaceAll(strconv.Quote(sessionFile), `'`, `\'`)+`,"sessionId":"backlog-run-50"}}'
IFS= read -r get_entries
printf '%s\n' '{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":`+entriesJSON+`,"leafId":"result"}}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(50, "run-50", worktree, sessionDir),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	waitForPath(t, started)
	boundary, err := process.Suspend(context.Background(), ContinuationRequest{
		SessionID: "backlog-run-50", SessionDir: sessionDir, Worktree: worktree,
	})
	if err != nil {
		t.Fatalf("suspend: %v", err)
	}
	expectedHash := sha256.Sum256([]byte(header + "\n" + strings.Join(entries, "\n") + "\n"))
	if boundary.SessionFile != sessionFile || boundary.LeafID != "result" || boundary.EntryCount != 3 || boundary.SHA256 != hex.EncodeToString(expectedHash[:]) {
		t.Fatalf("boundary = %#v", boundary)
	}
	if result := process.CloseContext(context.Background(), nil); !result.GroupExited || result.Err != nil {
		t.Fatalf("close = %#v", result)
	}
}

func TestProcessSuspendRequiresCorrelatedAbortResponseAndCompleteToolTail(t *testing.T) {
	tests := []struct {
		name      string
		abortLine string
		want      string
	}{
		{name: "mismatched abort response", abortLine: `{"id":"wrong","type":"response","command":"abort","success":true}`, want: "unexpected or mismatched"},
		{name: "rejected correlated abort", abortLine: `{"id":"backlog-suspend-abort","type":"response","command":"abort","success":false,"error":"not active"}`, want: "not active"},
		{name: "missing tool result", abortLine: `{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}`, want: "without durable results"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			worktree := filepath.Join(root, "worktree")
			sessionDir := filepath.Join(root, "sessions")
			sessionFile := filepath.Join(sessionDir, "session.jsonl")
			started := filepath.Join(root, "started")
			if err := os.MkdirAll(worktree, 0o700); err != nil {
				t.Fatal(err)
			}
			header := `{"type":"session","version":3,"id":"backlog-run-51","cwd":` + strconv.Quote(worktree) + `}`
			entry := `{"type":"message","id":"assistant","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"tool-1"}]}}`
			pi := fakePi(t, `
IFS= read -r prompt
touch `+shellQuote(started)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
IFS= read -r abort
printf '%s\n' `+shellQuote(header)+` `+shellQuote(entry)+` > `+shellQuote(sessionFile)+`
printf '%s\n' `+shellQuote(test.abortLine)+` '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
IFS= read -r state
printf '%s\n' '{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":`+strings.ReplaceAll(strconv.Quote(sessionFile), `'`, `\'`)+`,"sessionId":"backlog-run-51"}}'
IFS= read -r entries
printf '%s\n' '{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":[`+entry+`],"leafId":"assistant"}}'
while IFS= read -r ignored; do :; done
`)
			process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
				context.Background(), request(51, "run-51", worktree, sessionDir),
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := process.Release(); err != nil {
				t.Fatal(err)
			}
			waitForPath(t, started)
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			_, suspendErr := process.Suspend(ctx, ContinuationRequest{SessionID: "backlog-run-51", SessionDir: sessionDir, Worktree: worktree})
			cancel()
			if suspendErr == nil || !strings.Contains(suspendErr.Error(), test.want) {
				t.Fatalf("suspend error = %v, want %q", suspendErr, test.want)
			}
			_ = process.Abort()
			_ = process.Close()
		})
	}
}

func TestProcessSuspendRejectsIncompleteIdleStateAndWrongSession(t *testing.T) {
	tests := []struct {
		name        string
		stateData   func(string) string
		settleEvent string
		want        string
	}{
		{name: "missing streaming field", stateData: func(path string) string {
			return `{"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "omitted required idle fields"},
		{name: "null compaction field", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":null,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "omitted required idle fields"},
		{name: "streaming", stateData: func(path string) string {
			return `{"isStreaming":true,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "not idle"},
		{name: "compacting", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":true,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "not idle"},
		{name: "pending messages", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":1,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "not idle"},
		{name: "wrong session", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"another-session"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "does not match"},
		{name: "missing settlement", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, want: "deadline exceeded"},
		{name: "open tool activity", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, settleEvent: `{"type":"tool_execution_start","toolCallId":"open"}` + "\n" + `{"type":"agent_settled"}`, want: "tool_execution_start"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			worktree := filepath.Join(root, "worktree")
			sessionDir := filepath.Join(root, "sessions")
			sessionFile := filepath.Join(sessionDir, "session.jsonl")
			started := filepath.Join(root, "started")
			if err := os.MkdirAll(worktree, 0o700); err != nil {
				t.Fatal(err)
			}
			header := `{"type":"session","id":"backlog-run-52","cwd":` + strconv.Quote(worktree) + `}`
			entry := `{"type":"message","id":"user","parentId":null,"message":{"role":"user","content":"work"}}`
			stateResponse := `{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":` + test.stateData(sessionFile) + `}`
			settleCommands := ""
			for _, event := range strings.Split(test.settleEvent, "\n") {
				if event != "" {
					settleCommands += "printf '%s\\n' " + shellQuote(event) + "\n"
				}
			}
			pi := fakePi(t, `
IFS= read -r prompt
touch `+shellQuote(started)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
IFS= read -r abort
printf '%s\n' `+shellQuote(header)+` `+shellQuote(entry)+` > `+shellQuote(sessionFile)+`
printf '%s\n' '{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}' '{"type":"turn_end"}' '{"type":"agent_end"}'
`+settleCommands+`
IFS= read -r state
printf '%s\n' `+shellQuote(stateResponse)+`
IFS= read -r entries
printf '%s\n' '{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":[`+entry+`],"leafId":"user"}}'
while IFS= read -r ignored; do :; done
`)
			process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(context.Background(), request(52, "run-52", worktree, sessionDir))
			if err != nil {
				t.Fatal(err)
			}
			if err := process.Release(); err != nil {
				t.Fatal(err)
			}
			waitForPath(t, started)
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			_, suspendErr := process.Suspend(ctx, ContinuationRequest{SessionID: "backlog-run-52", SessionDir: sessionDir, Worktree: worktree})
			cancel()
			if suspendErr == nil || !strings.Contains(suspendErr.Error(), test.want) {
				t.Fatalf("suspend error = %v, want %q", suspendErr, test.want)
			}
			_ = process.Abort()
			_ = process.Close()
		})
	}
}

func TestVerifySessionBoundaryRejectsPathIdentityAndEntryMismatches(t *testing.T) {
	root := t.TempDir()
	sessionDir := filepath.Join(root, "sessions")
	outsideDir := filepath.Join(root, "outside")
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outsideDir, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(outsideDir, "session.jsonl")
	if err := os.WriteFile(outside, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifySessionPath(sessionDir, outside); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("outside path error = %v", err)
	}
	link := filepath.Join(sessionDir, "linked.jsonl")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if err := verifySessionPath(sessionDir, link); err == nil || !strings.Contains(err.Error(), "resolved") {
		t.Fatalf("symlink path error = %v", err)
	}

	worktree := filepath.Join(root, "worktree")
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	header := `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`
	diskEntry := `{"type":"message","id":"leaf","parentId":null,"value":9007199254740992,"message":{"role":"user"}}`
	if err := os.WriteFile(sessionFile, []byte(header+"\n"+diskEntry+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree}
	syncCalls := []string{}
	syncFile := func(file *os.File) error {
		syncCalls = append(syncCalls, file.Name())
		return nil
	}
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(diskEntry)}, "leaf", syncFile); err != nil {
		t.Fatalf("valid boundary: %v", err)
	}
	if len(syncCalls) != 2 || syncCalls[0] != sessionFile || syncCalls[1] != sessionDir {
		t.Fatalf("sync calls = %v, want file then directory", syncCalls)
	}
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(strings.Replace(diskEntry, "9007199254740992", "9007199254740993", 1))}, "leaf", syncFile); err == nil || !strings.Contains(err.Error(), "not synchronized") {
		t.Fatalf("large-number mismatch error = %v", err)
	}
	duplicate := strings.Replace(diskEntry, `"value":9007199254740992`, `"value":9007199254740992,"value":9007199254740992`, 1)
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(duplicate)}, "leaf", syncFile); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("duplicate-key error = %v", err)
	}
	syncFailure := errors.New("sync failed")
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(diskEntry)}, "leaf", func(*os.File) error { return syncFailure }); !errors.Is(err, syncFailure) {
		t.Fatalf("sync failure = %v", err)
	}
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("path %s was not created", path)
}

func TestProcessExplicitlyRejectsProjectTrustWhenApprovalIsDisabled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	argsPath := filepath.Join(root, "args")
	pi := fakePi(t, `
printf '%s\n' "$*" > `+shellQuote(argsPath)+`
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
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

type gateCheckingWriteCloser struct {
	t        *testing.T
	gatePath string
	wrote    bool
}

func (w *gateCheckingWriteCloser) Write(data []byte) (int, error) {
	w.t.Helper()
	if _, err := os.Stat(w.gatePath); err != nil {
		w.t.Fatalf("prompt written before start gate existed: %v", err)
	}
	w.wrote = true
	return len(data), nil
}

func (*gateCheckingWriteCloser) Close() error { return nil }

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
