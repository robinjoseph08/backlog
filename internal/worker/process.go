package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"
)

const promptCommandID = "backlog-afk-prompt"

type Request struct {
	Issue       int
	RunID       string
	Worktree    string
	SessionName string
	SessionID   string
	SessionDir  string
}

type Result struct {
	ExitCode   int
	LogPath    string
	StderrPath string
	Settled    bool
	StreamErr  error
	Err        error
}

type Supervisor struct {
	Executable       string
	LogsDir          string
	Approve          bool
	TerminationGrace time.Duration
}

type Process struct {
	command            *exec.Cmd
	logPath            string
	stderrPath         string
	gatePath           string
	stdin              io.WriteCloser
	stdout             *os.File
	stderr             *os.File
	events             *rpcWriter
	terminate          func() error
	terminationStarted <-chan struct{}
	terminationDone    <-chan struct{}
	processGroupGrace  time.Duration
	releaseOnce        sync.Once
	releaseErr         error
	closeInputOnce     sync.Once
	closeInputErr      error
	exitDone           chan struct{}
	resultMu           sync.Mutex
	result             Result
}

var safeRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
var safeSessionIDPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$`)

func (s Supervisor) Start(ctx context.Context, request Request) (*Process, error) {
	if request.Issue <= 0 || request.RunID == "" || request.Worktree == "" || request.SessionName == "" || request.SessionID == "" || request.SessionDir == "" {
		return nil, fmt.Errorf("worker request is incomplete")
	}
	if !safeRunIDPattern.MatchString(request.RunID) || request.RunID == "." || request.RunID == ".." {
		return nil, fmt.Errorf("run id %q contains unsafe path characters", request.RunID)
	}
	if !safeSessionIDPattern.MatchString(request.SessionID) {
		return nil, fmt.Errorf("session id %q contains unsafe characters", request.SessionID)
	}
	if err := os.MkdirAll(s.LogsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create worker log directory: %w", err)
	}
	if err := os.MkdirAll(request.SessionDir, 0o700); err != nil {
		return nil, fmt.Errorf("create Pi session directory: %w", err)
	}
	logPath := filepath.Join(s.LogsDir, request.RunID+".jsonl")
	stderrPath := filepath.Join(s.LogsDir, request.RunID+".stderr.log")
	gatePath := filepath.Join(s.LogsDir, request.RunID+".start")
	if err := os.Remove(gatePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reset worker start gate: %w", err)
	}
	stdoutLog, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create worker event log: %w", err)
	}
	stderrLog, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		stdoutLog.Close()
		return nil, fmt.Errorf("create worker stderr log: %w", err)
	}

	executable := s.Executable
	if executable == "" {
		executable = "pi"
	}
	piArgs := []string{"--mode", "rpc"}
	if s.Approve {
		piArgs = append(piArgs, "--approve")
	} else {
		piArgs = append(piArgs, "--no-approve")
	}
	piArgs = append(piArgs,
		"--name", request.SessionName,
		"--session-dir", request.SessionDir,
		"--session-id", request.SessionID,
	)

	// The wrapper cannot exec Pi until Release creates the gate. This lets the
	// runner durably record the Worker PID and process-start identity first.
	const wrapper = `gate=$1; shift; attempts=0; while [ ! -f "$gate" ]; do attempts=$((attempts+1)); if [ "$attempts" -ge 36000 ]; then exit 124; fi; sleep 0.1; done; exec "$@"`
	wrapperArgs := []string{"-c", wrapper, "backlog-gate", gatePath, executable}
	wrapperArgs = append(wrapperArgs, piArgs...)
	command := exec.CommandContext(ctx, "/bin/sh", wrapperArgs...)
	stdin, err := command.StdinPipe()
	if err != nil {
		stdoutLog.Close()
		stderrLog.Close()
		return nil, fmt.Errorf("open Pi RPC input: %w", err)
	}
	grace := s.TerminationGrace
	if grace <= 0 {
		grace = 5 * time.Second
	}
	terminationStarted := make(chan struct{})
	terminationDone := make(chan struct{})
	var terminateOnce sync.Once
	var terminateErr error
	terminate := func() error {
		terminateOnce.Do(func() {
			close(terminationStarted)
			terminateErr = beginProcessGroupTermination(command, grace, terminationDone)
		})
		return terminateErr
	}
	events := newRPCWriter(stdoutLog, promptCommandID, request.Issue)
	command.Dir = request.Worktree
	command.Stdout = events
	command.Stderr = stderrLog
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = grace
	command.Cancel = terminate
	if err := command.Start(); err != nil {
		stdin.Close()
		stdoutLog.Close()
		stderrLog.Close()
		return nil, fmt.Errorf("start Pi gate: %w", err)
	}
	process := &Process{
		command: command, logPath: logPath, stderrPath: stderrPath, gatePath: gatePath,
		stdin: stdin, stdout: stdoutLog, stderr: stderrLog, events: events, terminate: terminate,
		terminationStarted: terminationStarted, terminationDone: terminationDone, processGroupGrace: grace, exitDone: make(chan struct{}),
	}
	go process.reap()
	return process, nil
}

func (s Supervisor) Release(runID string) error {
	if !safeRunIDPattern.MatchString(runID) || runID == "." || runID == ".." {
		return fmt.Errorf("run id %q contains unsafe path characters", runID)
	}
	return releaseGate(filepath.Join(s.LogsDir, runID+".start"))
}

func (p *Process) PID() int {
	if p.command.Process == nil {
		return 0
	}
	return p.command.Process.Pid
}

func (p *Process) Release() error {
	p.releaseOnce.Do(func() {
		if err := releaseGate(p.gatePath); err != nil {
			p.releaseErr = err
			return
		}
		command := struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Message string `json:"message"`
		}{ID: promptCommandID, Type: "prompt", Message: fmt.Sprintf("/skill:afk %d", p.events.issue)}
		encoded, err := json.Marshal(command)
		if err == nil {
			encoded = append(encoded, '\n')
			_, err = p.stdin.Write(encoded)
		}
		if err != nil {
			p.releaseErr = fmt.Errorf("submit AFK prompt over Pi RPC: %w", err)
		}
	})
	return p.releaseErr
}

func (p *Process) Abort() error {
	return p.terminate()
}

// Wait returns when the RPC session settles, fails protocol validation, or
// exits unexpectedly. A settled Pi process remains alive until Close is called.
func (p *Process) Wait() Result {
	select {
	case <-p.events.failed:
		return Result{ExitCode: -1, LogPath: p.logPath, StderrPath: p.stderrPath, StreamErr: p.events.Err(), Err: p.events.Err()}
	case <-p.events.settled:
		if err := p.events.Err(); err != nil {
			return Result{ExitCode: -1, LogPath: p.logPath, StderrPath: p.stderrPath, StreamErr: err, Err: err}
		}
		return Result{ExitCode: 0, LogPath: p.logPath, StderrPath: p.stderrPath, Settled: true}
	case <-p.exitDone:
		return p.exitResult()
	}
}

// Close closes RPC input and waits for the Worker process and its entire
// process group to exit. Callers persist the reconciled Run before invoking Close.
func (p *Process) Close() Result {
	p.closeInputOnce.Do(func() { p.closeInputErr = p.stdin.Close() })
	var gracefulErr error
	grace := time.NewTimer(p.processGroupGrace)
	defer grace.Stop()
	select {
	case <-p.exitDone:
	case <-grace.C:
		gracefulErr = errors.New("Pi RPC process did not exit after input closed")
		if err := p.terminate(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			gracefulErr = errors.Join(gracefulErr, fmt.Errorf("terminate Pi RPC process group: %w", err))
		}
		<-p.exitDone
	}
	result := p.exitResult()
	groupErr := waitForProcessGroupExit(p.PID(), p.processGroupGrace)
	result.Err = errors.Join(result.Err, p.closeInputErr, gracefulErr, groupErr)
	return result
}

func (p *Process) reap() {
	waitErr := p.command.Wait()
	select {
	case <-p.terminationStarted:
		<-p.terminationDone
	default:
	}
	streamErr := p.events.Finish()
	closeErr := errors.Join(p.stdout.Close(), p.stderr.Close())
	_ = os.Remove(p.gatePath)
	exitCode := 0
	if p.command.ProcessState != nil {
		exitCode = p.command.ProcessState.ExitCode()
	} else if waitErr != nil {
		exitCode = -1
	}
	p.resultMu.Lock()
	p.result = Result{
		ExitCode: exitCode, LogPath: p.logPath, StderrPath: p.stderrPath,
		Settled:   p.events.Settled() && streamErr == nil,
		StreamErr: streamErr, Err: errors.Join(waitErr, streamErr, closeErr),
	}
	p.resultMu.Unlock()
	close(p.exitDone)
}

func (p *Process) exitResult() Result {
	p.resultMu.Lock()
	defer p.resultMu.Unlock()
	return p.result
}

func releaseGate(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("release worker start gate: %w", err)
	}
	return file.Close()
}

func waitForProcessGroupExit(pid int, grace time.Duration) error {
	if pid <= 0 {
		return nil
	}
	exited, err := waitForProcessGroup(pid, grace)
	if err != nil || exited {
		return err
	}
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("terminate surviving Worker process group: %w", err)
	}
	exited, err = waitForProcessGroup(pid, grace)
	if err != nil || exited {
		return err
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill surviving Worker process group: %w", err)
	}
	exited, err = waitForProcessGroup(pid, grace)
	if err != nil {
		return err
	}
	if !exited {
		return fmt.Errorf("Worker process group %d survived shutdown escalation", pid)
	}
	return nil
}

func waitForProcessGroup(pid int, grace time.Duration) (bool, error) {
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := syscall.Kill(-pid, syscall.Signal(0)); errors.Is(err, syscall.ESRCH) {
			return true, nil
		} else if err != nil && !errors.Is(err, syscall.EPERM) {
			return false, fmt.Errorf("verify Worker process-group exit: %w", err)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return false, nil
		}
	}
}

func beginProcessGroupTermination(command *exec.Cmd, grace time.Duration, done chan<- struct{}) error {
	if command.Process == nil {
		close(done)
		return os.ErrProcessDone
	}
	pid := command.Process.Pid
	err := syscall.Kill(-pid, syscall.SIGTERM)
	if errors.Is(err, syscall.ESRCH) {
		close(done)
		return os.ErrProcessDone
	}
	if err != nil {
		close(done)
		return err
	}
	go func() {
		defer close(done)
		deadline := time.NewTimer(grace)
		defer deadline.Stop()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := syscall.Kill(-pid, syscall.Signal(0)); errors.Is(err, syscall.ESRCH) {
					return
				}
			case <-deadline.C:
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				return
			}
		}
	}()
	return nil
}

type rpcAgentState uint8

const (
	rpcAwaitingResponse rpcAgentState = iota
	rpcAwaitingAgentStart
	rpcAgentRunning
	rpcBetweenAgentRuns
	rpcAgentSettled
)

type rpcWriter struct {
	mu             sync.Mutex
	destination    io.Writer
	buffer         []byte
	lineNumber     int
	issue          int
	commandID      string
	state          rpcAgentState
	turnOpen       bool
	completedTurns int
	messageOpen    bool
	compactionOpen bool
	retryOpen      bool
	retryAttempt   int
	openTools      map[string]struct{}
	parseErrors    []error
	settled        chan struct{}
	failed         chan struct{}
	settledOnce    sync.Once
	failedOnce     sync.Once
}

func newRPCWriter(destination io.Writer, commandID string, issue int) *rpcWriter {
	return &rpcWriter{
		destination: destination, commandID: commandID, issue: issue,
		state: rpcAwaitingResponse, openTools: make(map[string]struct{}),
		settled: make(chan struct{}), failed: make(chan struct{}),
	}
}

func (w *rpcWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.destination.Write(data); err != nil {
		w.addError(fmt.Errorf("write Pi RPC log: %w", err))
		return 0, err
	}
	w.buffer = append(w.buffer, data...)
	for {
		newline := bytes.IndexByte(w.buffer, '\n')
		if newline < 0 {
			break
		}
		line := w.buffer[:newline]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		w.validate(line)
		w.buffer = w.buffer[newline+1:]
	}
	return len(data), nil
}

func (w *rpcWriter) Finish() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buffer) > 0 {
		w.addError(fmt.Errorf("truncated Pi RPC JSON record after line %d", w.lineNumber))
		w.buffer = nil
	}
	if w.state == rpcAwaitingResponse {
		w.addError(errors.New("Pi RPC stream omitted the correlated prompt response"))
	}
	if w.state != rpcAgentSettled {
		w.addError(errors.New("Pi RPC stream ended before agent_settled"))
	}
	return errors.Join(w.parseErrors...)
}

func (w *rpcWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return errors.Join(w.parseErrors...)
}

func (w *rpcWriter) Settled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state == rpcAgentSettled
}

func (w *rpcWriter) validate(line []byte) {
	w.lineNumber++
	if len(line) == 0 {
		w.addError(fmt.Errorf("empty Pi RPC record on line %d", w.lineNumber))
		return
	}
	var message struct {
		Type       string          `json:"type"`
		ID         string          `json:"id"`
		Command    string          `json:"command"`
		Method     string          `json:"method"`
		ToolCallID string          `json:"toolCallId"`
		Attempt    int             `json:"attempt"`
		Success    *bool           `json:"success"`
		Data       json.RawMessage `json:"data"`
	}
	if !json.Valid(line) || json.Unmarshal(line, &message) != nil || message.Type == "" {
		w.addError(fmt.Errorf("malformed Pi RPC JSON on line %d", w.lineNumber))
		return
	}
	if w.state == rpcAgentSettled {
		w.addError(fmt.Errorf("Pi RPC message %q followed agent_settled on line %d", message.Type, w.lineNumber))
		return
	}
	if message.Type == "response" {
		if w.state != rpcAwaitingResponse {
			w.addError(fmt.Errorf("duplicated Pi RPC prompt response or invalid response order on line %d", w.lineNumber))
			return
		}
		if message.ID != w.commandID || message.Command != "prompt" {
			w.addError(fmt.Errorf("mismatched Pi RPC response on line %d", w.lineNumber))
			return
		}
		if message.Success == nil || !*message.Success {
			w.addError(fmt.Errorf("Pi RPC prompt was rejected on line %d", w.lineNumber))
			return
		}
		w.state = rpcAwaitingAgentStart
		return
	}
	if message.Type == "extension_ui_request" {
		w.validateExtensionUIRequest(message.ID, message.Method)
		return
	}
	if message.ID != "" {
		w.addError(fmt.Errorf("unexpected correlated Pi RPC event %q on line %d", message.Type, w.lineNumber))
		return
	}
	switch message.Type {
	case "agent_start":
		if (w.state != rpcAwaitingAgentStart && w.state != rpcBetweenAgentRuns) || w.compactionOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.state = rpcAgentRunning
		w.completedTurns = 0
	case "agent_end":
		if w.state != rpcAgentRunning || w.completedTurns == 0 || w.turnOpen || w.messageOpen || len(w.openTools) != 0 {
			w.invalidOrder(message.Type)
			return
		}
		w.state = rpcBetweenAgentRuns
	case "agent_settled":
		if w.state != rpcBetweenAgentRuns || w.compactionOpen || w.retryOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.state = rpcAgentSettled
		w.settledOnce.Do(func() { close(w.settled) })
	case "turn_start":
		if w.state != rpcAgentRunning || w.turnOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.turnOpen = true
	case "turn_end":
		if w.state != rpcAgentRunning || !w.turnOpen || w.messageOpen || len(w.openTools) != 0 {
			w.invalidOrder(message.Type)
			return
		}
		w.turnOpen = false
		w.completedTurns++
	case "message_start":
		if w.state != rpcAgentRunning || !w.turnOpen || w.messageOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.messageOpen = true
	case "message_update":
		if w.state != rpcAgentRunning || !w.turnOpen || !w.messageOpen {
			w.invalidOrder(message.Type)
		}
	case "message_end":
		if w.state != rpcAgentRunning || !w.turnOpen || !w.messageOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.messageOpen = false
	case "tool_execution_start":
		if w.state != rpcAgentRunning || !w.turnOpen || message.ToolCallID == "" {
			w.invalidOrder(message.Type)
			return
		}
		if _, exists := w.openTools[message.ToolCallID]; exists {
			w.addError(fmt.Errorf("duplicated Pi RPC tool_execution_start on line %d", w.lineNumber))
			return
		}
		w.openTools[message.ToolCallID] = struct{}{}
	case "tool_execution_update":
		if w.state != rpcAgentRunning || !w.turnOpen || message.ToolCallID == "" {
			w.invalidOrder(message.Type)
			return
		}
		if _, exists := w.openTools[message.ToolCallID]; !exists {
			w.invalidOrder(message.Type)
		}
	case "tool_execution_end":
		if w.state != rpcAgentRunning || !w.turnOpen || message.ToolCallID == "" {
			w.invalidOrder(message.Type)
			return
		}
		if _, exists := w.openTools[message.ToolCallID]; !exists {
			w.invalidOrder(message.Type)
			return
		}
		delete(w.openTools, message.ToolCallID)
	case "compaction_start":
		if w.state != rpcBetweenAgentRuns || w.compactionOpen || w.retryOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.compactionOpen = true
	case "compaction_end":
		if w.state != rpcBetweenAgentRuns || !w.compactionOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.compactionOpen = false
	case "auto_retry_start":
		if w.state != rpcBetweenAgentRuns || w.compactionOpen || message.Attempt <= w.retryAttempt {
			w.invalidOrder(message.Type)
			return
		}
		w.retryOpen = true
		w.retryAttempt = message.Attempt
	case "auto_retry_end":
		if (w.state != rpcBetweenAgentRuns && w.state != rpcAgentRunning) || !w.retryOpen || message.Attempt != w.retryAttempt {
			w.invalidOrder(message.Type)
			return
		}
		w.retryOpen = false
		w.retryAttempt = 0
	case "queue_update", "extension_error":
		if w.state == rpcAwaitingResponse {
			w.invalidOrder(message.Type)
		}
	default:
		w.addError(fmt.Errorf("unknown Pi RPC message type %q on line %d", message.Type, w.lineNumber))
	}
}

func (w *rpcWriter) validateExtensionUIRequest(id, method string) {
	if w.state == rpcAwaitingResponse || id == "" {
		w.invalidOrder("extension_ui_request")
		return
	}
	switch method {
	case "notify", "setStatus", "setWidget", "setTitle", "set_editor_text":
		return
	case "select", "confirm", "input", "editor":
		w.addError(fmt.Errorf("unsupported interactive Pi RPC request %q on line %d", method, w.lineNumber))
	default:
		w.addError(fmt.Errorf("unknown Pi RPC extension UI method %q on line %d", method, w.lineNumber))
	}
}

func (w *rpcWriter) invalidOrder(messageType string) {
	w.addError(fmt.Errorf("invalidly ordered Pi RPC %s on line %d", messageType, w.lineNumber))
}

func (w *rpcWriter) addError(err error) {
	w.parseErrors = append(w.parseErrors, err)
	w.failedOnce.Do(func() { close(w.failed) })
}
