package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
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
	logPath, stderrPath := process.LogPaths()
	if logPath != filepath.Join(root, "logs", "run-42.jsonl") || stderrPath != filepath.Join(root, "logs", "run-42.stderr.log") {
		t.Fatalf("startup log identities = %q/%q", logPath, stderrPath)
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
	if result.Err != nil || result.ExitCode != 0 || !result.LogClosed {
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
		{"case-variant response identity", `{"id":"wrong","ID":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n`, "non-canonical key"},
		{"lone case-variant response identity", `{"ID":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n`, "non-canonical key"},
		{"Unicode response success alias", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":false,"\u017Fuccess":true}\n`, "non-canonical key"},
		{"lone Unicode response success alias", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","\u017Fuccess":true}\n`, "non-canonical key"},
		{"wrong response command", `{"id":"backlog-afk-prompt","type":"response","command":"abort","success":true}\n`, "mismatched Pi RPC response"},
		{"missing response success", `{"id":"backlog-afk-prompt","type":"response","command":"prompt"}\n`, "Pi RPC prompt was rejected"},
		{"rejected response", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":false}\n`, "Pi RPC prompt was rejected"},
		{"invalid order", `{"type":"agent_settled"}\n`, "invalidly ordered Pi RPC agent_settled"},
		{"duplicate agent end", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_start"}\n{"type":"turn_end"}\n{"type":"agent_end"}\n{"type":"agent_end"}\n`, "invalidly ordered Pi RPC agent_end"},
		{"unmatched turn end", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_end"}\n`, "invalidly ordered Pi RPC turn_end"},
		{"unsupported dialog", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"extension_ui_request","id":"ui-1","method":"confirm"}\n`, "unsupported interactive Pi RPC request"},
		{"duplicate retry attempt", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_start"}\n{"type":"turn_end"}\n{"type":"agent_end"}\n{"type":"auto_retry_start","attempt":1}\n{"type":"auto_retry_start","attempt":1}\n`, "invalidly ordered Pi RPC auto_retry_start"},
		{"unfinished summarization retry", `{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}\n{"type":"agent_start"}\n{"type":"turn_start"}\n{"type":"turn_end"}\n{"type":"agent_end"}\n{"type":"summarization_retry_scheduled","attempt":1}\n{"type":"agent_settled"}\n`, "invalidly ordered Pi RPC agent_settled"},
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

func TestRPCEntryAppendedDoesNotChangeLifecycleState(t *testing.T) {
	t.Parallel()

	type lifecycleState struct {
		state                     rpcAgentState
		turnOpen                  bool
		completedTurns            int
		messageOpen               bool
		compactionOpen            bool
		retryOpen                 bool
		retryAttempt              int
		summarizationRetryOpen    bool
		summarizationRetryAttempt int
		toolOpen                  bool
		openToolCount             int
	}
	events := newRPCWriter(&strings.Builder{}, nil, "backlog-afk-prompt", 31)
	events.state = rpcAgentRunning
	events.turnOpen = true
	events.completedTurns = 2
	events.messageOpen = true
	events.compactionOpen = true
	events.retryOpen = true
	events.retryAttempt = 3
	events.summarizationRetryOpen = true
	events.summarizationRetryAttempt = 4
	events.openTools["subagent-1"] = struct{}{}
	snapshot := func() lifecycleState {
		_, toolOpen := events.openTools["subagent-1"]
		return lifecycleState{
			state: events.state, turnOpen: events.turnOpen, completedTurns: events.completedTurns,
			messageOpen: events.messageOpen, compactionOpen: events.compactionOpen,
			retryOpen: events.retryOpen, retryAttempt: events.retryAttempt,
			summarizationRetryOpen:    events.summarizationRetryOpen,
			summarizationRetryAttempt: events.summarizationRetryAttempt,
			toolOpen:                  toolOpen, openToolCount: len(events.openTools),
		}
	}

	before := snapshot()
	events.validate([]byte(`{"type":"entry_appended"}`))
	if after := snapshot(); after != before {
		t.Fatalf("entry_appended changed lifecycle state from %#v to %#v", before, after)
	}
	if err := events.Err(); err != nil {
		t.Fatalf("entry_appended was rejected: %v", err)
	}
}

func TestProcessAcceptsEntryAppendedDuringToolExecution(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' \
  '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' \
  '{"type":"agent_start"}' \
  '{"type":"turn_start"}' \
  '{"type":"tool_execution_start","toolCallId":"subagent-1","toolName":"Agent","args":{"subagent_type":"general-purpose","description":"Implement status progress view","prompt":"Implement issue 31"}}' \
  '{"type":"entry_appended","entry":{"type":"custom","customType":"subagents:record","id":"entry-1","parentId":"parent-1","timestamp":"2026-07-25T21:40:01.859Z","data":{"id":"record-1","type":"general-purpose","description":"Implement status progress view","status":"completed","result":"STATUS: success","startedAt":1785014520878,"completedAt":1785015601858}}}' \
  '{"type":"tool_execution_end","toolCallId":"subagent-1","toolName":"Agent","result":{"content":[{"type":"text","text":"STATUS: success"}],"details":{"status":"completed"}},"isError":false}' \
  '{"type":"turn_end"}' \
  '{"type":"agent_end"}' \
  '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(31, "entry-appended-31", root, filepath.Join(root, "sessions", "entry-appended-31")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		_ = process.Abort()
		_ = process.Close()
		t.Fatalf("entry_appended event was rejected: %v", result.Err)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatal(result.Err)
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
  '{"type":"compaction_start"}' \
  '{"type":"summarization_retry_scheduled","attempt":1,"maxAttempts":3,"delayMs":10,"errorMessage":"transient"}' \
  '{"type":"summarization_retry_attempt_start","source":"compaction","reason":"manual"}' \
  '{"type":"summarization_retry_scheduled","attempt":2,"maxAttempts":3,"delayMs":20,"errorMessage":"transient again"}' \
  '{"type":"summarization_retry_attempt_start","source":"compaction","reason":"manual"}' \
  '{"type":"summarization_retry_finished"}' \
  '{"type":"compaction_end"}' \
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

func TestProcessProjectsObservedActivityWithoutChangingRawJSONL(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' \
  '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' \
  '{"type":"agent_start"}' \
  '{"type":"turn_start"}' \
  '{"type":"message_start","message":{"role":"assistant"}}' \
  '{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"private reasoning"}}' \
  '{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"visible"}],"usage":{"totalTokens":41}}}' \
  '{"type":"tool_execution_start","toolCallId":"subagent-malformed","toolName":"Agent","args":{"prompt":"private Subagent prompt"}}' \
  '{"type":"tool_execution_update","toolCallId":"subagent-malformed","toolName":"Agent","partialResult":{"content":[{"type":"text","text":"private Subagent output"}],"details":{"description":42,"status":[],"turnCount":"many","toolUses":-1,"tokens":"unknown","spinnerFrame":4,"durationMs":10}}}' \
  '{"type":"tool_execution_end","toolCallId":"subagent-malformed","toolName":"Agent","result":{"content":[{"type":"text","text":"private Subagent result"}],"details":"unavailable"},"isError":false}' \
  '{"type":"turn_end"}' \
  '{"type":"agent_end"}' \
  '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(28, "activity-28", root, filepath.Join(root, "sessions", "activity-28")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil {
		t.Fatal(result.Err)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatal(result.Err)
	}
	raw, err := os.ReadFile(process.logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "private reasoning") {
		t.Fatalf("raw Worker JSONL was rewritten: %s", raw)
	}
	projection, err := os.ReadFile(activity.PathForLog(process.logPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private reasoning", "private Subagent prompt", "private Subagent output", "private Subagent result"} {
		if strings.Contains(string(projection), private) {
			t.Fatalf("Activity projection exposed %q: %s", private, projection)
		}
	}
	if !strings.Contains(string(projection), "visible") || !strings.Contains(string(projection), `"tokenDelta":41`) || !strings.Contains(string(projection), `"kind":"subagent"`) || !strings.Contains(string(projection), `"description":"Subagent [subagent-malf`) {
		t.Fatalf("Activity projection privacy/usage = %s", projection)
	}
	for number, line := range strings.Split(strings.TrimSpace(string(projection)), "\n") {
		var entry activity.Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.ObservedAt.IsZero() {
			t.Fatalf("projection line %d lacks observation time: %q, err = %v", number+1, line, err)
		}
	}
}

func TestActivityProjectionCreationFailureDoesNotAlterWorkerResult(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logsDir := filepath.Join(root, "logs")
	if err := os.MkdirAll(filepath.Join(logsDir, "activity-failure.activity.jsonl"), 0o700); err != nil {
		t.Fatal(err)
	}
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: logsDir}).Start(
		context.Background(), request(28, "activity-failure", root, filepath.Join(root, "sessions", "activity-failure")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("projection failure changed Worker result: %#v", result)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatal(result.Err)
	}
	if _, err := os.Stat(activity.UnavailablePath(filepath.Join(logsDir, "activity-failure.activity.jsonl"))); err != nil {
		t.Fatalf("projection failure diagnostic was not recorded: %v", err)
	}
}

func TestActivityProjectionAppendFailureDoesNotAlterWorkerResult(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	continuePath := filepath.Join(root, "continue")
	pi := fakePi(t, fmt.Sprintf(`
IFS= read -r command
printf '%%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}'
while [ ! -f %q ]; do sleep 0.01; done
printf '%%s\n' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`, continuePath))
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(28, "activity-append-failure", root, filepath.Join(root, "sessions", "activity-append-failure")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if process.activity == nil {
		t.Fatal("Activity writer was not created")
	}
	if err := process.activity.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(continuePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("append failure changed Worker result: %#v", result)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatal(result.Err)
	}
	raw, err := os.ReadFile(process.logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"type":"turn_end"`) {
		t.Fatalf("append failure changed raw Worker JSONL: %s", raw)
	}
	if _, err := os.Stat(activity.UnavailablePath(activity.PathForLog(process.logPath))); err != nil {
		t.Fatalf("append failure diagnostic was not recorded: %v", err)
	}
}

func TestCloseReportsProtocolFailuresAfterSettlement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event string
	}{
		{name: "unknown event", event: `{"type":"surprise"}`},
		{name: "entry appended", event: `{"type":"entry_appended","entry":{"type":"custom"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
sleep 0.05
printf '%s\n' '`+test.event+`'
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
		})
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
	if result := process.Close(); result.Err != nil || result.ControlErr != nil {
		t.Fatalf("close = %#v", result)
	}
	if _, err := os.Stat(childDone); err != nil {
		t.Fatalf("Close returned before a process-group child exited: %v", err)
	}
}

func TestCloseEscalatesSurvivingProcessGroupAfterLeaderExit(t *testing.T) {
	root := t.TempDir()
	childPIDPath := filepath.Join(root, "child.pid")
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
sh -c 'trap "" TERM; while :; do sleep 1; done' &
printf '%s\n' "$!" > `+shellQuote(childPIDPath)+`
while IFS= read -r ignored; do :; done
exit 9
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 30 * time.Millisecond,
	}).Start(context.Background(), request(13, "run-13", root, filepath.Join(root, "sessions", "run-13")))
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}
	waitForPath(t, childPIDPath)
	started := time.Now()
	result := process.Close()
	if !result.GroupExited || result.Err == nil || !strings.Contains(result.Err.Error(), "exit status 9") {
		t.Fatalf("close surviving process group = %#v", result)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("surviving process-group escalation took %s", elapsed)
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
	if result.Err == nil || !strings.Contains(result.Err.Error(), "did not exit after input closed") ||
		result.ControlErr == nil || !strings.Contains(result.ControlErr.Error(), "did not exit after input closed") {
		t.Fatalf("close result = %#v, want graceful-exit timeout in Err and ControlErr", result)
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
	for deadline := time.Now().Add(5 * time.Second); ; {
		childData, _ = os.ReadFile(childPIDPath)
		if len(strings.TrimSpace(string(childData))) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Worker descendant PID was not written to %s", childPIDPath)
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

func TestCloseWithForceContextBypassesSettledWorkerGrace(t *testing.T) {
	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
trap '' TERM
while :; do sleep 1; done
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 5 * time.Second,
	}).Start(context.Background(), request(19, "run-19", root, filepath.Join(root, "sessions", "run-19")))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	result := process.CloseWithForceContext(ctx, func() error { return nil })
	if !result.GroupExited || !result.ForceStopped || result.Err != nil {
		t.Fatalf("force close settled Worker = %#v", result)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("force context did not bypass settled Worker grace: %s", elapsed)
	}
}

func TestCloseWithForceContextHonorsCancellationAfterGraceExpires(t *testing.T) {
	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
trap '' TERM
while :; do sleep 1; done
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 20 * time.Millisecond,
	}).Start(context.Background(), request(20, "run-20", root, filepath.Join(root, "sessions", "run-20")))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	result := process.CloseWithForceContext(ctx, func() error { return nil })
	if !result.GroupExited || !result.ForceStopped {
		t.Fatalf("force close after grace = %#v", result)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("cancellation after grace did not bound close: %s", elapsed)
	}
}

func TestCloseContextForceStopsWorkerDescendantButNotUnrelatedProcess(t *testing.T) {
	root := t.TempDir()
	childPIDPath := filepath.Join(root, "child.pid")
	pi := fakePi(t, `
IFS= read -r command
sh -c 'trap "" TERM; while :; do sleep 1; done' &
printf '%s\n' "$!" > `+shellQuote(childPIDPath)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
trap '' TERM
while :; do sleep 1; done
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 100 * time.Millisecond,
	}).Start(context.Background(), request(16, "run-16", root, filepath.Join(root, "sessions", "run-16")))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}
	for deadline := time.Now().Add(time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(childPIDPath); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("wait for Worker descendant: %v", err)
		}
	}

	unrelated := exec.Command("sleep", "10")
	unrelated.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := unrelated.Start(); err != nil {
		t.Fatalf("start unrelated process: %v", err)
	}
	defer func() {
		_ = syscall.Kill(-unrelated.Process.Pid, syscall.SIGKILL)
		_ = unrelated.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	result := process.CloseContext(ctx, func() error { return nil })
	if !result.GroupExited || !result.ForceStopped || result.Err != nil {
		t.Fatalf("force close = %#v", result)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("force close took %s", elapsed)
	}
	if err := syscall.Kill(-process.PID(), syscall.Signal(0)); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("Worker process group survived force stop: %v", err)
	}
	if err := unrelated.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unrelated process was signaled: %v", err)
	}
}

func TestCloseContextDoesNotSignalWhenAuthorizationCannotVerifyIdentity(t *testing.T) {
	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
trap '' TERM
while :; do sleep 1; done
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 50 * time.Millisecond,
	}).Start(context.Background(), request(17, "run-17", root, filepath.Join(root, "sessions", "run-17")))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := process.CloseContext(ctx, func() error { return errors.New("process-start identity changed") })
	if result.GroupExited || result.ForceStopped || result.Err == nil || !strings.Contains(result.Err.Error(), "process-start identity changed") {
		t.Fatalf("unauthorized close = %#v", result)
	}
	if err := syscall.Kill(-process.PID(), syscall.Signal(0)); err != nil {
		t.Fatalf("identity-mismatched process was signaled: %v", err)
	}

	if err := syscall.Kill(-process.PID(), syscall.SIGKILL); err != nil {
		t.Fatalf("clean up Worker process group: %v", err)
	}
	if result := process.Close(); !result.GroupExited {
		t.Fatalf("cleanup close = %#v", result)
	}
}

func TestCloseContextPreservesNaturalExitBetweenAuthorizationAndSignal(t *testing.T) {
	root := t.TempDir()
	pi := fakePi(t, `
IFS= read -r command
trap 'exit 7' TERM
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while :; do sleep 1; done
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 50 * time.Millisecond,
	}).Start(context.Background(), request(18, "run-18", root, filepath.Join(root, "sessions", "run-18")))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil || !result.Settled {
		t.Fatalf("wait = %#v", result)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := process.CloseContext(ctx, func() error {
		if err := syscall.Kill(-process.PID(), syscall.SIGTERM); err != nil {
			return fmt.Errorf("stop Worker before force signal: %w", err)
		}
		<-process.exitDone
		return nil
	})
	if !result.GroupExited || result.ForceStopped || result.Err == nil || !strings.Contains(result.Err.Error(), "exit status 7") {
		t.Fatalf("natural exit during force stop = %#v", result)
	}
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
IFS= read -r final_state
printf '%s\n' '{"id":"backlog-suspend-final-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":`+strings.ReplaceAll(strconv.Quote(sessionFile), `'`, `\'`)+`,"sessionId":"backlog-run-50"}}'
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

func TestProcessSuspendRejectsTrailingProtocolFailureBeforeFinalBarrier(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	worktree := filepath.Join(root, "worktree")
	sessionDir := filepath.Join(root, "sessions")
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	started := filepath.Join(root, "started")
	if err := os.MkdirAll(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	header := `{"type":"session","version":3,"id":"backlog-run-53","cwd":` + strconv.Quote(worktree) + `}`
	entry := `{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}`
	stateData := `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(sessionFile) + `,"sessionId":"backlog-run-53"}`
	pi := fakePi(t, `
IFS= read -r prompt
touch `+shellQuote(started)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
IFS= read -r abort
printf '%s\n' `+shellQuote(header)+` `+shellQuote(entry)+` > `+shellQuote(sessionFile)+`
printf '%s\n' '{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
IFS= read -r state
printf '%s\n' `+shellQuote(`{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":`+stateData+`}`)+`
IFS= read -r entries
printf '%s\n' `+shellQuote(`{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":[`+entry+`],"leafId":"leaf"}}`)+`
IFS= read -r final_state
printf '%s\n' 'not-json' `+shellQuote(`{"id":"backlog-suspend-final-state","type":"response","command":"get_state","success":true,"data":`+stateData+`}`)+`
while IFS= read -r ignored; do :; done
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), request(53, "run-53", worktree, sessionDir),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	waitForPath(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = process.Suspend(ctx, ContinuationRequest{SessionID: "backlog-run-53", SessionDir: sessionDir, Worktree: worktree})
	if err == nil || !strings.Contains(err.Error(), "malformed Pi RPC JSON") {
		t.Fatalf("trailing protocol failure = %v", err)
	}
	_ = process.Close()
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
		entriesData func(string) string
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
		{name: "case-variant session identity", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"another-session","SessionID":"backlog-run-52"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "non-canonical key"},
		{name: "lone case-variant session identity", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"SessionID":"backlog-run-52"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "non-canonical key"},
		{name: "case-variant leaf identity", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, entriesData: func(entry string) string {
			return `{"entries":[` + entry + `],"leafId":"wrong","LeafID":"user"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "non-canonical key"},
		{name: "lone case-variant leaf identity", stateData: func(path string) string {
			return `{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":` + strconv.Quote(path) + `,"sessionId":"backlog-run-52"}`
		}, entriesData: func(entry string) string {
			return `{"entries":[` + entry + `],"LeafID":"user"}`
		}, settleEvent: `{"type":"agent_settled"}`, want: "non-canonical key"},
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
			header := `{"type":"session","version":3,"id":"backlog-run-52","cwd":` + strconv.Quote(worktree) + `}`
			entry := `{"type":"message","id":"user","parentId":null,"message":{"role":"user","content":"work"}}`
			stateResponse := `{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":` + test.stateData(sessionFile) + `}`
			entriesData := `{"entries":[` + entry + `],"leafId":"user"}`
			if test.entriesData != nil {
				entriesData = test.entriesData(entry)
			}
			entriesResponse := `{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":` + entriesData + `}`
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
printf '%s\n' `+shellQuote(entriesResponse)+`
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
	header := `{"type":"session","version":3,"id":"session-1","cwd":` + strconv.Quote(worktree) + `}`
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
	caseDistinctContent := `{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":{"type":"data","Type":"value"}}}`
	if err := os.WriteFile(sessionFile, []byte(header+"\n"+caseDistinctContent+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(caseDistinctContent)}, "leaf", syncFile); err != nil {
		t.Fatalf("case-distinct opaque content: %v", err)
	}
	if err := os.WriteFile(sessionFile, []byte(header+"\n"+diskEntry+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(strings.Replace(diskEntry, "9007199254740992", "9007199254740993", 1))}, "leaf", syncFile); err == nil || !strings.Contains(err.Error(), "not synchronized") {
		t.Fatalf("large-number mismatch error = %v", err)
	}
	duplicate := strings.Replace(diskEntry, `"value":9007199254740992`, `"value":9007199254740992,"value":9007199254740992`, 1)
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(duplicate)}, "leaf", syncFile); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("duplicate-key error = %v", err)
	}
	caseVariantHeader := `{"type":"other","Type":"session","id":"wrong","ID":"session-1","cwd":"wrong","CWD":` + strconv.Quote(worktree) + `}`
	if err := os.WriteFile(sessionFile, []byte(caseVariantHeader+"\n"+diskEntry+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(diskEntry)}, "leaf", syncFile); err == nil || !strings.Contains(err.Error(), "non-canonical key") {
		t.Fatalf("case-variant header error = %v", err)
	}
	if err := os.WriteFile(sessionFile, []byte(header+"\n"+diskEntry+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	syncFailure := errors.New("sync failed")
	if _, err := verifyAndSyncSession(sessionFile, expected, []json.RawMessage{json.RawMessage(diskEntry)}, "leaf", func(*os.File) error { return syncFailure }); !errors.Is(err, syncFailure) {
		t.Fatalf("sync failure = %v", err)
	}
}

func TestVerifyContinuationRevalidatesPersistedSessionIdentityLeafAndHash(t *testing.T) {
	t.Parallel()

	sessionDir := t.TempDir()
	worktree := t.TempDir()
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	content := `{"type":"session","version":3,"id":"session-1","cwd":` + strconv.Quote(worktree) + `}` + "\n" +
		`{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}` + "\n"
	if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(content))
	expected := ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree}
	continuation := Continuation{
		SessionID: "session-1", SessionFile: sessionFile, Worktree: worktree,
		LeafID: "leaf", EntryCount: 1, SHA256: hex.EncodeToString(hash[:]),
	}
	if err := VerifyContinuation(expected, continuation); err != nil {
		t.Fatalf("verify continuation: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Continuation)
		write  string
		want   string
	}{
		{name: "changed hash", mutate: func(value *Continuation) { value.SHA256 = strings.Repeat("0", 64) }, want: "hash"},
		{name: "changed leaf", mutate: func(value *Continuation) { value.LeafID = "other" }, want: "leaf"},
		{name: "changed count", mutate: func(value *Continuation) { value.EntryCount = 2 }, want: "entries"},
		{name: "malformed file", write: "not-json\n", want: "malformed JSON"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			value := continuation
			if test.mutate != nil {
				test.mutate(&value)
			}
			if test.write != "" {
				if err := os.WriteFile(sessionFile, []byte(test.write), 0o600); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
						t.Error(err)
					}
				}()
			}
			if err := VerifyContinuation(expected, value); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestVerifyContinuationAcceptsDocumentedMessageRolesAndNonMessageLeaf(t *testing.T) {
	t.Parallel()

	sessionDir := t.TempDir()
	worktree := t.TempDir()
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	entries := []string{
		`{"type":"message","id":"user","parentId":null,"message":{"role":"user","content":"continue"}}`,
		`{"type":"message","id":"custom-message","parentId":"user","message":{"role":"custom"}}`,
		`{"type":"message","id":"bash-message","parentId":"custom-message","message":{"role":"bashExecution"}}`,
		`{"type":"message","id":"branch-message","parentId":"bash-message","message":{"role":"branchSummary"}}`,
		`{"type":"message","id":"compaction-message","parentId":"branch-message","message":{"role":"compactionSummary"}}`,
		`{"type":"session_info","id":"leaf","parentId":"compaction-message","name":"resumable"}`,
	}
	content := `{"type":"session","version":3,"id":"session-1","cwd":` + strconv.Quote(worktree) + `}` + "\n" + strings.Join(entries, "\n") + "\n"
	if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(content))
	err := VerifyContinuation(
		ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree},
		Continuation{SessionID: "session-1", SessionFile: sessionFile, Worktree: worktree, LeafID: "leaf", EntryCount: len(entries), SHA256: hex.EncodeToString(hash[:])},
	)
	if err != nil {
		t.Fatalf("documented Pi session structure: %v", err)
	}

	multiRootEntries := []string{
		`{"type":"message","id":"abandoned","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"abandoned-tool"}]}}`,
		`{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"safe active root"}}`,
	}
	multiRootContent := `{"type":"session","version":3,"id":"session-1","cwd":` + strconv.Quote(worktree) + `}` + "\n" + strings.Join(multiRootEntries, "\n") + "\n"
	if err := os.WriteFile(sessionFile, []byte(multiRootContent), 0o600); err != nil {
		t.Fatal(err)
	}
	multiRootHash := sha256.Sum256([]byte(multiRootContent))
	err = VerifyContinuation(
		ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree},
		Continuation{SessionID: "session-1", SessionFile: sessionFile, Worktree: worktree, LeafID: "leaf", EntryCount: len(multiRootEntries), SHA256: hex.EncodeToString(multiRootHash[:])},
	)
	if err != nil {
		t.Fatalf("documented multi-root Pi session: %v", err)
	}
}

func TestVerifyContinuationValidatesCompactionAwareToolTail(t *testing.T) {
	t.Parallel()

	sessionDir := t.TempDir()
	worktree := t.TempDir()
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	tests := []struct {
		name      string
		firstKept string
		wantError bool
	}{
		{name: "summarized incomplete tool call", firstKept: "kept"},
		{name: "retained incomplete tool call", firstKept: "old-assistant", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entries := []string{
				`{"type":"message","id":"old-assistant","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"old-tool"}]}}`,
				`{"type":"message","id":"kept","parentId":"old-assistant","message":{"role":"user","content":"kept context"}}`,
				`{"type":"compaction","id":"compaction","parentId":"kept","summary":"summary","firstKeptEntryId":"` + test.firstKept + `","tokensBefore":100}`,
				`{"type":"message","id":"leaf","parentId":"compaction","message":{"role":"user","content":"continue"}}`,
			}
			content := `{"type":"session","version":3,"id":"session-1","cwd":` + strconv.Quote(worktree) + `}` + "\n" + strings.Join(entries, "\n") + "\n"
			if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256([]byte(content))
			err := VerifyContinuation(
				ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree},
				Continuation{SessionID: "session-1", SessionFile: sessionFile, Worktree: worktree, LeafID: "leaf", EntryCount: len(entries), SHA256: hex.EncodeToString(hash[:])},
			)
			if test.wantError && (err == nil || !strings.Contains(err.Error(), "without durable results")) {
				t.Fatalf("retained tool tail error = %v", err)
			}
			if !test.wantError && err != nil {
				t.Fatalf("summarized tool tail: %v", err)
			}
		})
	}
}

func TestVerifyContinuationRejectsCaseVariantIdentityAliases(t *testing.T) {
	t.Parallel()

	sessionDir := t.TempDir()
	worktree := t.TempDir()
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	expected := ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree}
	tests := []struct {
		name      string
		header    string
		entries   string
		want      string
		rawHeader bool
	}{
		{
			name:    "session header",
			header:  `{"type":"other","Type":"session","id":"wrong","ID":"session-1","cwd":"wrong","CWD":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:    "non-canonical key",
		},
		{
			name:    "lone session header alias",
			header:  `{"Type":"session","ID":"session-1","CWD":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:    "non-canonical key",
		},
		{
			name:    "durable leaf",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","id":"wrong","ID":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:    "non-canonical key",
		},
		{
			name:    "lone durable leaf alias",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","ID":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:    "non-canonical key",
		},
		{
			name:      "missing header version",
			header:    `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries:   `{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:      "invalid or unsupported header",
			rawHeader: true,
		},
		{
			name:      "aliased header version",
			header:    `{"type":"session","Version":3,"id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries:   `{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:      "non-canonical key",
			rawHeader: true,
		},
		{
			name:      "unsupported header version",
			header:    `{"type":"session","version":2,"id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries:   `{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:      "invalid or unsupported header",
			rawHeader: true,
		},
		{
			name:    "missing entry type",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"id":"leaf","parentId":null}`,
			want:    "identity and type",
		},
		{
			name:    "missing parent identity",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","id":"leaf","message":{"role":"user","content":"continue"}}`,
			want:    "without parent identity",
		},
		{
			name:    "case-variant entry type",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"Message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}`,
			want:    "non-canonical type",
		},
		{
			name:    "case-variant message role",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","id":"leaf","parentId":null,"message":{"role":"Assistant","content":[]}}`,
			want:    "unsupported role",
		},
		{
			name:    "case-variant tool call type",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","id":"leaf","parentId":null,"message":{"role":"assistant","content":[{"type":"ToolCall","id":"tool-1"}]}}`,
			want:    "non-canonical content type",
		},
		{
			name:    "null message metadata",
			header:  `{"type":"session","id":"session-1","cwd":` + strconv.Quote(worktree) + `}`,
			entries: `{"type":"message","id":"leaf","parentId":null,"message":null}`,
			want:    "no message metadata",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := test.header
			if !test.rawHeader && !strings.Contains(header, `"version"`) {
				header = strings.Replace(header, `"type":"session"`, `"type":"session","version":3`, 1)
			}
			content := header + "\n" + test.entries + "\n"
			if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256([]byte(content))
			continuation := Continuation{
				SessionID: "session-1", SessionFile: sessionFile, Worktree: worktree,
				LeafID: "leaf", EntryCount: strings.Count(test.entries, "\n") + 1, SHA256: hex.EncodeToString(hash[:]),
			}
			if err := VerifyContinuation(expected, continuation); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("continuation metadata error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestVerifyContinuationRejectsCoherentlyHashedWrongSessionHeader(t *testing.T) {
	t.Parallel()

	sessionDir := t.TempDir()
	worktree := t.TempDir()
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	expected := ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree}
	tests := []struct {
		name      string
		sessionID string
		worktree  string
	}{
		{name: "wrong session id", sessionID: "session-other", worktree: worktree},
		{name: "wrong worktree", sessionID: "session-1", worktree: t.TempDir()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := `{"type":"session","version":3,"id":` + strconv.Quote(test.sessionID) + `,"cwd":` + strconv.Quote(test.worktree) + `}` + "\n" +
				`{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"continue"}}` + "\n"
			if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256([]byte(content))
			continuation := Continuation{
				SessionID: "session-1", SessionFile: sessionFile, Worktree: worktree,
				LeafID: "leaf", EntryCount: 1, SHA256: hex.EncodeToString(hash[:]),
			}
			if err := VerifyContinuation(expected, continuation); err == nil || !strings.Contains(err.Error(), "identity/path") {
				t.Fatalf("wrong coherent session header error = %v", err)
			}
		})
	}
}

func TestVerifyContinuationRejectsFIFOWithoutBlocking(t *testing.T) {
	t.Parallel()

	sessionDir := t.TempDir()
	worktree := t.TempDir()
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	if err := syscall.Mkfifo(sessionFile, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- VerifyContinuation(
			ContinuationRequest{SessionID: "session-1", SessionDir: sessionDir, Worktree: worktree},
			Continuation{SessionID: "session-1", SessionFile: sessionFile, Worktree: worktree, LeafID: "leaf", EntryCount: 1, SHA256: strings.Repeat("a", 64)},
		)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("FIFO continuation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO continuation verification blocked")
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

func TestReplacementWorkerPromptRequiresFreshRepositoryAndGitHubAssessment(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input")
	argsPath := filepath.Join(root, "args")
	pi := fakePi(t, `
printf '%s\n' "$*" > `+shellQuote(argsPath)+`
IFS= read -r command
printf '%s\n' "$command" > `+shellQuote(inputPath)+`
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	sessionDir := filepath.Join(root, "session")
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	if err := os.WriteFile(sessionFile, []byte("session\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := request(81, "run-81", t.TempDir(), sessionDir)
	request.SessionFile = sessionFile
	request.Resume = true
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Release(); err != nil {
		t.Fatal(err)
	}
	if result := process.Wait(); result.Err != nil {
		t.Fatal(result.Err)
	}
	if result := process.Close(); result.Err != nil {
		t.Fatal(result.Err)
	}
	input, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	message := string(input)
	if !strings.Contains(message, "Reassess the repository and GitHub state") || !strings.Contains(message, "existing AFK workflow") {
		t.Fatalf("replacement prompt = %q", message)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--session "+sessionFile) || strings.Contains(string(args), "--session-id") {
		t.Fatalf("replacement Worker args = %q, want exact verified session file", args)
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
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	path := filepath.Join(directory, "pi")
	if err := os.WriteFile(source, []byte("#!/bin/sh\nset -eu\n"+body), 0o600); err != nil {
		t.Fatal(err)
	}
	// A concurrent fork can briefly inherit a recently closed writable descriptor,
	// causing Linux to reject execution with ETXTBSY. Let a child process create the
	// executable so the parallel test process never opens its inode for writing.
	if output, err := exec.Command("cp", source, path).CombinedOutput(); err != nil {
		t.Fatalf("copy fake Pi executable: %v\n%s", err, output)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
