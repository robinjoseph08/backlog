package worker

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
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
)

const promptCommandID = "backlog-afk-prompt"

type Request struct {
	Issue             int
	RunID             string
	Worktree          string
	SessionName       string
	SessionID         string
	SessionDir        string
	SessionFile       string
	Resume            bool
	ContinuationStage string
	CheckpointFile    string
	CheckpointSHA256  string
}

type ContinuationRequest struct {
	SessionID  string
	SessionDir string
	Worktree   string
}

type Continuation struct {
	SessionID        string
	SessionFile      string
	Worktree         string
	LeafID           string
	EntryCount       int
	SHA256           string
	Workflow         string
	WorkflowStage    string
	CheckpointFile   string
	CheckpointSHA256 string
	LogPath          string
	StderrPath       string
}

type Result struct {
	ExitCode          int
	GroupExited       bool
	ForceStopped      bool
	LogClosed         bool
	LogPath           string
	StderrPath        string
	Settled           bool
	StreamErr         error
	ControlErr        error
	cleanupErr        error
	Err               error
	ProviderExhausted bool
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
	activity           *activity.Writer
	events             *rpcWriter
	stdinMu            sync.Mutex
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
	resume             bool
	continuationStage  string
	checkpointFile     string
	checkpointSHA256   string
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
	if request.Resume {
		if err := verifySessionPath(request.SessionDir, request.SessionFile); err != nil {
			return nil, fmt.Errorf("verify resumed Pi session path: %w", err)
		}
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
	logFlags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if request.Resume {
		logFlags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	stdoutLog, err := os.OpenFile(logPath, logFlags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create worker event log: %w", err)
	}
	stderrLog, err := os.OpenFile(stderrPath, logFlags, 0o600)
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
	)
	if request.Resume {
		piArgs = append(piArgs, "--session", request.SessionFile)
	} else {
		piArgs = append(piArgs, "--session-id", request.SessionID)
	}

	// The wrapper cannot exec Pi until Release creates the gate. This lets the
	// runner durably record the Worker PID and process-start identity first.
	const wrapper = `gate=$1; shift; attempts=0; while [ ! -f "$gate" ]; do attempts=$((attempts+1)); if [ "$attempts" -ge 36000 ]; then exit 124; fi; sleep 0.1; done; exec "$@"`
	wrapperArgs := []string{"-c", wrapper, "backlog-gate", gatePath, executable}
	wrapperArgs = append(wrapperArgs, piArgs...)
	command := exec.CommandContext(ctx, "/bin/sh", wrapperArgs...)
	// Workers are headless subprocesses, not Herdr panes. Do not let them
	// report against or control the foreground runner's inherited pane.
	command.Env = workerEnvironment(os.Environ(), request.Worktree)
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
	// Activity is a best-effort observational sidecar. Failure to create or
	// append it must never affect the raw transcript or Worker result.
	activityWriter, _ := activity.NewWriter(activity.PathForLog(logPath), request.Resume)
	events := newRPCWriter(stdoutLog, activityWriter, promptCommandID, request.Issue)
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
		if activityWriter != nil {
			_ = activityWriter.Close()
		}
		return nil, fmt.Errorf("start Pi gate: %w", err)
	}
	process := &Process{
		command: command, logPath: logPath, stderrPath: stderrPath, gatePath: gatePath,
		stdin: stdin, stdout: stdoutLog, stderr: stderrLog, activity: activityWriter, events: events, terminate: terminate,
		terminationStarted: terminationStarted, terminationDone: terminationDone, processGroupGrace: grace, exitDone: make(chan struct{}),
		resume: request.Resume, continuationStage: request.ContinuationStage,
		checkpointFile: request.CheckpointFile, checkpointSHA256: request.CheckpointSHA256,
	}
	go process.reap()
	return process, nil
}

func workerEnvironment(environment []string, worktree string) []string {
	filtered := make([]string, 0, len(environment)+1)
	for _, variable := range environment {
		if strings.HasPrefix(variable, "HERDR_") || strings.HasPrefix(variable, "PWD=") {
			continue
		}
		filtered = append(filtered, variable)
	}
	return append(filtered, "PWD="+worktree)
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

// LogPaths returns the durable JSONL and standard-error log identities chosen
// before the Worker process started.
func (p *Process) LogPaths() (string, string) {
	return p.logPath, p.stderrPath
}

func (p *Process) Release() error {
	p.releaseOnce.Do(func() {
		if err := releaseGate(p.gatePath); err != nil {
			p.releaseErr = err
			return
		}
		message := fmt.Sprintf("/skill:afk %d", p.events.issue)
		if p.resume {
			stage := strings.TrimSpace(p.continuationStage)
			if stage == "" {
				stage = "AFK coordinator"
			}
			message = fmt.Sprintf("Recover the existing AFK workflow for issue #%d from the verified durable continuation boundary at %s. Reassess the repository and GitHub state before acting. Inspect the remote branch and expected-branch pull request before any push, pull request, merge, issue, or cleanup mutation, and never repeat a mutation whose prior outcome is uncertain.", p.events.issue, stage)
			if p.checkpointFile != "" {
				message += fmt.Sprintf(" The verified ship-it checkpoint is %s with SHA-256 %s.", p.checkpointFile, p.checkpointSHA256)
			}
		}
		command := struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Message string `json:"message"`
		}{ID: promptCommandID, Type: "prompt", Message: message}
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

// Suspend establishes a continuation boundary while the RPC process is still
// alive. The caller must durably persist the returned marker before CloseContext.
func (p *Process) Suspend(ctx context.Context, expected ContinuationRequest) (Continuation, error) {
	if expected.SessionID == "" || expected.SessionDir == "" || expected.Worktree == "" {
		return Continuation{}, errors.New("continuation request is incomplete")
	}
	abort, err := p.rpcCommand(ctx, "backlog-suspend-abort", "abort")
	if err != nil {
		return Continuation{}, fmt.Errorf("abort Pi agent: %w", err)
	}
	if !abort.Success {
		return Continuation{}, fmt.Errorf("abort Pi agent: %s", abort.Error)
	}
	select {
	case <-p.events.settled:
	case <-p.events.failed:
		return Continuation{}, fmt.Errorf("wait for agent_settled: %w", p.events.Err())
	case <-ctx.Done():
		return Continuation{}, fmt.Errorf("wait for agent_settled: %w", ctx.Err())
	}
	if err := p.events.Idle(); err != nil {
		return Continuation{}, err
	}
	return p.CheckpointSettled(ctx, expected)
}

// CheckpointSettled establishes a continuation boundary for an already settled
// idle RPC session without sending abort or otherwise changing agent state.
func (p *Process) CheckpointSettled(ctx context.Context, expected ContinuationRequest) (Continuation, error) {
	if expected.SessionID == "" || expected.SessionDir == "" || expected.Worktree == "" {
		return Continuation{}, errors.New("continuation request is incomplete")
	}
	if err := p.events.Idle(); err != nil {
		return Continuation{}, err
	}
	stateResponse, err := p.rpcCommand(ctx, "backlog-suspend-state", "get_state")
	if err != nil {
		return Continuation{}, fmt.Errorf("get Pi RPC state: %w", err)
	}
	if !stateResponse.Success {
		return Continuation{}, fmt.Errorf("get Pi RPC state: %s", stateResponse.Error)
	}
	rpcState, err := decodeRPCSessionState(stateResponse.Data, expected.SessionID)
	if err != nil {
		return Continuation{}, err
	}
	if err := verifySessionPath(expected.SessionDir, rpcState.SessionFile); err != nil {
		return Continuation{}, err
	}

	entriesResponse, err := p.rpcCommand(ctx, "backlog-suspend-entries", "get_entries")
	if err != nil {
		return Continuation{}, fmt.Errorf("get Pi session entries: %w", err)
	}
	if !entriesResponse.Success {
		return Continuation{}, fmt.Errorf("get Pi session entries: %s", entriesResponse.Error)
	}
	var rpcEntries struct {
		Entries []json.RawMessage `json:"entries"`
		LeafID  string            `json:"leafId"`
	}
	if _, err := decodeExactJSON(entriesResponse.Data); err != nil {
		return Continuation{}, fmt.Errorf("decode Pi session entries: %w", err)
	}
	if err := rejectNonCanonicalJSONFields(entriesResponse.Data, "entries", "leafId"); err != nil {
		return Continuation{}, fmt.Errorf("decode Pi session entries: %w", err)
	}
	if err := json.Unmarshal(entriesResponse.Data, &rpcEntries); err != nil {
		return Continuation{}, fmt.Errorf("decode Pi session entries: %w", err)
	}
	sha, err := verifyAndSyncSession(rpcState.SessionFile, expected, rpcEntries.Entries, rpcEntries.LeafID, func(file *os.File) error { return file.Sync() })
	if err != nil {
		return Continuation{}, err
	}
	finalStateResponse, err := p.rpcCommand(ctx, "backlog-suspend-final-state", "get_state")
	if err != nil {
		return Continuation{}, fmt.Errorf("confirm final Pi RPC state: %w", err)
	}
	if !finalStateResponse.Success {
		return Continuation{}, fmt.Errorf("confirm final Pi RPC state: %s", finalStateResponse.Error)
	}
	finalState, err := decodeRPCSessionState(finalStateResponse.Data, expected.SessionID)
	if err != nil {
		return Continuation{}, fmt.Errorf("confirm final Pi RPC state: %w", err)
	}
	if finalState.SessionFile != rpcState.SessionFile {
		return Continuation{}, errors.New("final Pi RPC session file changed while establishing continuation boundary")
	}
	if err := p.events.Idle(); err != nil {
		return Continuation{}, fmt.Errorf("confirm final Pi RPC protocol state: %w", err)
	}
	if err := p.events.Err(); err != nil {
		return Continuation{}, fmt.Errorf("confirm final Pi RPC transcript: %w", err)
	}
	workflow, stage, checkpointFile, checkpointSHA, err := InspectWorkflowCheckpoint(expected.Worktree)
	if err != nil {
		return Continuation{}, err
	}
	return Continuation{
		SessionID: expected.SessionID, SessionFile: rpcState.SessionFile, Worktree: expected.Worktree,
		LeafID: rpcEntries.LeafID, EntryCount: len(rpcEntries.Entries), SHA256: sha,
		Workflow: workflow, WorkflowStage: stage, CheckpointFile: checkpointFile, CheckpointSHA256: checkpointSHA,
		LogPath: p.logPath, StderrPath: p.stderrPath,
	}, nil
}

type rpcSessionState struct {
	IsStreaming         *bool  `json:"isStreaming"`
	IsCompacting        *bool  `json:"isCompacting"`
	SessionFile         string `json:"sessionFile"`
	SessionID           string `json:"sessionId"`
	PendingMessageCount *int   `json:"pendingMessageCount"`
}

func decodeRPCSessionState(raw json.RawMessage, expectedSessionID string) (rpcSessionState, error) {
	if _, err := decodeExactJSON(raw); err != nil {
		return rpcSessionState{}, fmt.Errorf("decode Pi RPC state: %w", err)
	}
	if err := rejectNonCanonicalJSONFields(raw, "isStreaming", "isCompacting", "sessionFile", "sessionId", "pendingMessageCount"); err != nil {
		return rpcSessionState{}, fmt.Errorf("decode Pi RPC state: %w", err)
	}
	var state rpcSessionState
	if err := json.Unmarshal(raw, &state); err != nil {
		return rpcSessionState{}, fmt.Errorf("decode Pi RPC state: %w", err)
	}
	if state.IsStreaming == nil || state.IsCompacting == nil || state.PendingMessageCount == nil {
		return rpcSessionState{}, errors.New("Pi RPC state omitted required idle fields")
	}
	if *state.IsStreaming || *state.IsCompacting || *state.PendingMessageCount != 0 {
		return rpcSessionState{}, fmt.Errorf("Pi RPC state is not idle (streaming=%t compacting=%t pendingMessages=%d)", *state.IsStreaming, *state.IsCompacting, *state.PendingMessageCount)
	}
	if state.SessionID != expectedSessionID {
		return rpcSessionState{}, fmt.Errorf("Pi RPC session id %q does not match %q", state.SessionID, expectedSessionID)
	}
	return state, nil
}

type rpcResponse struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Command string          `json:"command"`
	Success bool            `json:"success"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

func (p *Process) rpcCommand(ctx context.Context, id, command string) (rpcResponse, error) {
	response := p.events.ExpectResponse(id, command)
	message := struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}{ID: id, Type: command}
	encoded, err := json.Marshal(message)
	if err == nil {
		encoded = append(encoded, '\n')
		p.stdinMu.Lock()
		_, err = p.stdin.Write(encoded)
		p.stdinMu.Unlock()
	}
	if err != nil {
		p.events.CancelResponse(id)
		return rpcResponse{}, err
	}
	select {
	case value := <-response:
		return value, nil
	case <-p.events.failed:
		p.events.CancelResponse(id)
		return rpcResponse{}, p.events.Err()
	case <-p.exitDone:
		p.events.CancelResponse(id)
		return rpcResponse{}, errors.New("Pi RPC process exited before command response")
	case <-ctx.Done():
		p.events.CancelResponse(id)
		return rpcResponse{}, ctx.Err()
	}
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
		return Result{ExitCode: 0, LogPath: p.logPath, StderrPath: p.stderrPath, Settled: true, ProviderExhausted: p.events.ProviderExhausted()}
	case <-p.exitDone:
		return p.exitResult()
	}
}

// Close closes RPC input and waits for the Worker process and its entire
// process group to exit. Callers persist the reconciled Run before invoking Close.
func (p *Process) Close() Result {
	return p.CloseWithForceContext(context.Background(), nil)
}

// CloseWithForceContext performs ordinary settled-Worker cleanup unless ctx is
// canceled first. Cancellation uses the same authorized SIGKILL path as
// suspension escalation, allowing a third signal to bypass graceful cleanup.
func (p *Process) CloseWithForceContext(ctx context.Context, authorizeKill func() error) Result {
	p.closeInputOnce.Do(func() {
		p.stdinMu.Lock()
		p.closeInputErr = p.stdin.Close()
		p.stdinMu.Unlock()
	})
	var gracefulErr error
	grace := time.NewTimer(p.processGroupGrace)
	defer grace.Stop()
	select {
	case <-p.exitDone:
	case <-ctx.Done():
		return p.forceStop(authorizeKill, ctx.Err())
	case <-grace.C:
		gracefulErr = errors.New("Pi RPC process did not exit after input closed")
		if err := p.terminate(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			gracefulErr = errors.Join(gracefulErr, fmt.Errorf("terminate Pi RPC process group: %w", err))
		}
		select {
		case <-p.exitDone:
		case <-ctx.Done():
			return p.forceStop(authorizeKill, ctx.Err())
		}
	}
	result := p.exitResult()
	groupForceStopped, groupErr := waitForProcessGroupExitBounded(ctx, p.PID(), p.processGroupGrace)
	result.ControlErr = errors.Join(result.ControlErr, p.closeInputErr, gracefulErr, groupErr)
	result.Err = errors.Join(result.Err, result.ControlErr)
	result.GroupExited = groupErr == nil
	result.ForceStopped = result.ForceStopped || groupForceStopped
	return result
}

// CloseContext closes RPC input and proves process-group exit within ctx. If
// ctx expires or is canceled, authorizeKill must revalidate the durable Run,
// liveness, PID, and process-start identity immediately before CloseContext
// force stops the process group. Deadline and signal cancellation therefore
// execute the same force-stop path.
func (p *Process) CloseContext(ctx context.Context, authorizeKill func() error) Result {
	p.closeInputOnce.Do(func() {
		p.stdinMu.Lock()
		p.closeInputErr = p.stdin.Close()
		p.stdinMu.Unlock()
	})

	select {
	case <-p.exitDone:
		if err := waitForProcessGroupExitContext(ctx, p.PID()); err == nil {
			result := p.exitResult()
			result.GroupExited = true
			result.ControlErr = errors.Join(result.ControlErr, p.closeInputErr)
			result.Err = errors.Join(result.Err, result.ControlErr)
			return result
		} else if ctx.Err() == nil {
			result := p.exitResult()
			result.ControlErr = errors.Join(result.ControlErr, p.closeInputErr, err)
			result.Err = errors.Join(result.Err, result.ControlErr)
			return result
		}
	case <-ctx.Done():
	}

	return p.forceStop(authorizeKill, ctx.Err())
}

func (p *Process) forceStop(authorizeKill func() error, triggerErr error) Result {
	unauthorized := func(err error) Result {
		result := p.exitResult()
		result.ControlErr = errors.Join(result.ControlErr, p.closeInputErr, err)
		result.Err = errors.Join(result.Err, triggerErr, result.ControlErr)
		return result
	}
	if authorizeKill == nil {
		return unauthorized(errors.New("Worker process-group exit was not verified; force stop was not authorized"))
	}
	if err := authorizeKill(); err != nil {
		return unauthorized(fmt.Errorf("authorize Worker force stop: %w", err))
	}
	killErr := p.kill()
	if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
		return unauthorized(fmt.Errorf("force stop Worker process group: %w", killErr))
	}
	forceStopped := killErr == nil

	// Reaping waits for an already-started graceful termination helper so it
	// cannot signal a reused process group. Give that helper and reaping their
	// own bounded stages instead of racing both against one grace period.
	select {
	case <-p.exitDone:
	case <-p.terminationStarted:
		<-p.terminationDone
		select {
		case <-p.exitDone:
		case <-time.After(p.processGroupGrace):
			return unauthorized(errors.New("Worker process did not exit after force stop"))
		}
	case <-time.After(p.processGroupGrace):
		return unauthorized(errors.New("Worker process did not exit after force stop"))
	}
	exited, err := waitForProcessGroup(p.PID(), p.processGroupGrace)
	if err != nil {
		return unauthorized(err)
	}
	if !exited {
		return unauthorized(fmt.Errorf("Worker process group %d survived force stop", p.PID()))
	}

	result := p.exitResult()
	result.GroupExited = true
	result.ForceStopped = forceStopped
	result.ControlErr = errors.Join(result.ControlErr, result.cleanupErr, p.closeInputErr)
	if forceStopped {
		// A SIGKILL exit and the context trigger are expected after an authorized
		// force stop. Protocol, log cleanup, and input-close failures remain errors.
		result.Err = errors.Join(result.StreamErr, result.ControlErr)
	} else {
		// The leader exited between authorization and signaling. Preserve its
		// actual exit result instead of claiming that SIGKILL caused the exit.
		result.Err = errors.Join(result.Err, result.ControlErr)
	}
	return result
}

func (p *Process) kill() error {
	if p.PID() <= 0 {
		return os.ErrProcessDone
	}
	if err := syscall.Kill(-p.PID(), syscall.SIGKILL); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	return nil
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
	if p.activity != nil {
		// Projection closure is observational and cannot change the Worker result.
		_ = p.activity.Close()
	}
	_ = os.Remove(p.gatePath)
	exitCode := 0
	if p.command.ProcessState != nil {
		exitCode = p.command.ProcessState.ExitCode()
	} else if waitErr != nil {
		exitCode = -1
	}
	p.resultMu.Lock()
	p.result = Result{
		ExitCode: exitCode, LogClosed: true, LogPath: p.logPath, StderrPath: p.stderrPath,
		Settled: p.events.Settled() && streamErr == nil, ProviderExhausted: p.events.ProviderExhausted(),
		StreamErr: streamErr, ControlErr: closeErr, cleanupErr: closeErr, Err: errors.Join(waitErr, streamErr, closeErr),
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

// VerifyContinuation proves that a persisted continuation marker still matches
// the complete on-disk Pi session before a replacement Worker opens it.
func VerifyContinuation(expected ContinuationRequest, continuation Continuation) error {
	if expected.SessionID == "" || expected.SessionDir == "" || expected.Worktree == "" {
		return errors.New("continuation request is incomplete")
	}
	if continuation.SessionID != expected.SessionID || continuation.Worktree != expected.Worktree || continuation.SessionFile == "" || continuation.LeafID == "" || continuation.EntryCount <= 0 || continuation.SHA256 == "" {
		return errors.New("continuation marker does not match the expected Pi session")
	}
	if err := verifySessionPath(expected.SessionDir, continuation.SessionFile); err != nil {
		return err
	}
	records, sha, err := readSessionRecords(continuation.SessionFile)
	if err != nil {
		return err
	}
	if sha != continuation.SHA256 {
		return fmt.Errorf("Pi session file hash %q does not match continuation marker %q", sha, continuation.SHA256)
	}
	if len(records) != continuation.EntryCount+1 {
		return fmt.Errorf("Pi session file has %d entries, continuation marker recorded %d", len(records)-1, continuation.EntryCount)
	}
	var header struct {
		Type    string `json:"type"`
		Version *int   `json:"version"`
		ID      string `json:"id"`
		CWD     string `json:"cwd"`
	}
	if _, err := decodeExactJSON(records[0]); err != nil {
		return fmt.Errorf("decode Pi session header: %w", err)
	}
	if err := rejectNonCanonicalJSONFields(records[0], "type", "version", "id", "cwd"); err != nil {
		return fmt.Errorf("decode Pi session header: %w", err)
	}
	if err := json.Unmarshal(records[0], &header); err != nil || header.Type != "session" || header.Version == nil || *header.Version != 3 {
		return errors.New("Pi session file has an invalid or unsupported header")
	}
	if header.ID != expected.SessionID || header.CWD != expected.Worktree {
		return fmt.Errorf("Pi session header identity/path %q/%q does not match %q/%q", header.ID, header.CWD, expected.SessionID, expected.Worktree)
	}
	entries := records[1:]
	for index, entry := range entries {
		if _, err := decodeExactJSON(entry); err != nil {
			return fmt.Errorf("decode Pi session file entry %d: %w", index+1, err)
		}
	}
	if err := verifyContinuationLeaf(entries, continuation.LeafID); err != nil {
		return err
	}
	if continuation.Workflow != "" {
		workflow, stage, checkpointFile, checkpointSHA, err := InspectWorkflowCheckpoint(expected.Worktree)
		if err != nil {
			return err
		}
		if workflow != continuation.Workflow || stage != continuation.WorkflowStage || checkpointFile != continuation.CheckpointFile || checkpointSHA != continuation.CheckpointSHA256 {
			return errors.New("workflow checkpoint identity changed after continuation verification")
		}
	}
	return nil
}

// InspectContinuation derives a boundary from one unambiguous offline Pi
// session file. Callers must prove Worker and process-group absence first.
func InspectContinuation(expected ContinuationRequest) (Continuation, error) {
	if expected.SessionID == "" || expected.SessionDir == "" || expected.Worktree == "" {
		return Continuation{}, errors.New("continuation request is incomplete")
	}
	entries, err := os.ReadDir(expected.SessionDir)
	if err != nil {
		return Continuation{}, fmt.Errorf("inspect Pi session directory: %w", err)
	}
	var candidates []string
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return Continuation{}, fmt.Errorf("inspect Pi session entry %q: %w", entry.Name(), err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return Continuation{}, fmt.Errorf("Pi session directory contains unsafe symlink %q", entry.Name())
		}
		if info.Mode().IsRegular() {
			candidates = append(candidates, filepath.Join(expected.SessionDir, entry.Name()))
		}
	}
	if len(candidates) != 1 {
		return Continuation{}, fmt.Errorf("Pi session directory has %d regular session-file candidates; exactly one is required", len(candidates))
	}
	sessionFile := candidates[0]
	if err := verifySessionPath(expected.SessionDir, sessionFile); err != nil {
		return Continuation{}, err
	}
	records, sha, err := readSessionRecords(sessionFile)
	if err != nil {
		return Continuation{}, err
	}
	if len(records) < 2 {
		return Continuation{}, errors.New("Pi session file has no durable continuation entry")
	}
	var leaf struct {
		ID string `json:"id"`
	}
	if _, err := decodeExactJSON(records[len(records)-1]); err != nil {
		return Continuation{}, fmt.Errorf("decode Pi session leaf: %w", err)
	}
	if err := rejectNonCanonicalJSONFields(records[len(records)-1], "id"); err != nil {
		return Continuation{}, fmt.Errorf("decode Pi session leaf: %w", err)
	}
	if err := json.Unmarshal(records[len(records)-1], &leaf); err != nil || leaf.ID == "" {
		return Continuation{}, errors.New("Pi session leaf has no durable identity")
	}
	continuation := Continuation{SessionID: expected.SessionID, SessionFile: sessionFile, Worktree: expected.Worktree, LeafID: leaf.ID, EntryCount: len(records) - 1, SHA256: sha}
	if err := VerifyContinuation(expected, continuation); err != nil {
		return Continuation{}, err
	}
	workflow, stage, checkpointFile, checkpointSHA, err := InspectWorkflowCheckpoint(expected.Worktree)
	if err != nil {
		return Continuation{}, err
	}
	continuation.Workflow, continuation.WorkflowStage = workflow, stage
	continuation.CheckpointFile, continuation.CheckpointSHA256 = checkpointFile, checkpointSHA
	return continuation, nil
}

// SyncContinuation revalidates and durably synchronizes an offline boundary.
// Inspection remains read-only so dry-run Recovery never writes external state.
func SyncContinuation(expected ContinuationRequest, continuation Continuation) error {
	if err := VerifyContinuation(expected, continuation); err != nil {
		return err
	}
	file, err := openSessionFile(continuation.SessionFile)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	directory, dirErr := os.Open(expected.SessionDir)
	if dirErr == nil {
		dirErr = errors.Join(directory.Sync(), directory.Close())
	}
	if err := errors.Join(syncErr, closeErr, dirErr); err != nil {
		return fmt.Errorf("sync offline Pi continuation: %w", err)
	}
	return nil
}

// InspectWorkflowCheckpoint identifies the durable AFK continuation stage. A
// ship-it checkpoint, when present, is hashed and its exact Stage field is used.
func InspectWorkflowCheckpoint(worktree string) (workflow, stage, checkpointFile, checkpointSHA string, resultErr error) {
	gitMarker := filepath.Join(worktree, ".git")
	info, err := os.Lstat(gitMarker)
	if errors.Is(err, os.ErrNotExist) {
		// Pi can be exercised against a synthetic worktree in protocol tests. The
		// absence of ship-it state still has the explicit AFK coordinator boundary.
		return "afk", "afk-coordinator", "", "", nil
	}
	if err != nil {
		return "", "", "", "", fmt.Errorf("inspect worktree Git identity: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", "", "", "", errors.New("worktree Git identity is a symlink")
	}
	gitDir := gitMarker
	if info.Mode().IsRegular() {
		data, err := os.ReadFile(gitMarker)
		if err != nil {
			return "", "", "", "", fmt.Errorf("read worktree Git identity: %w", err)
		}
		value := strings.TrimSpace(string(data))
		if !strings.HasPrefix(value, "gitdir: ") {
			return "", "", "", "", errors.New("worktree Git identity file is malformed")
		}
		gitDir = strings.TrimSpace(strings.TrimPrefix(value, "gitdir: "))
		if !filepath.IsAbs(gitDir) {
			gitDir = filepath.Join(worktree, gitDir)
		}
	} else if !info.IsDir() {
		return "", "", "", "", errors.New("worktree Git identity is not a regular file or directory")
	}
	gitDir, err = filepath.EvalSymlinks(gitDir)
	if err != nil {
		return "", "", "", "", fmt.Errorf("resolve worktree Git directory: %w", err)
	}
	checkpointFile = filepath.Join(gitDir, "ship-it-checkpoint-v1.md")
	// #nosec G703 -- gitDir is the resolved Git directory owned by this verified worktree.
	checkpointInfo, err := os.Lstat(checkpointFile)
	if errors.Is(err, os.ErrNotExist) {
		return "afk", "afk-coordinator", "", "", nil
	}
	if err != nil {
		return "", "", "", "", fmt.Errorf("inspect ship-it checkpoint: %w", err)
	}
	if !checkpointInfo.Mode().IsRegular() || checkpointInfo.Mode()&os.ModeSymlink != 0 {
		return "", "", "", "", errors.New("ship-it checkpoint is not a regular file")
	}
	if checkpointInfo.Size() > 4*1024*1024 {
		return "", "", "", "", errors.New("ship-it checkpoint exceeds the supported size")
	}
	// #nosec G703 -- checkpointFile is a fixed basename under the resolved worktree Git directory.
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return "", "", "", "", fmt.Errorf("read ship-it checkpoint: %w", err)
	}
	if !strings.HasPrefix(string(data), "# Ship-it checkpoint v1\n") {
		return "", "", "", "", errors.New("ship-it checkpoint has an unsupported header")
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "Stage: ") {
			if stage != "" {
				return "", "", "", "", errors.New("ship-it checkpoint contains multiple Stage fields")
			}
			stage = strings.TrimSpace(strings.TrimPrefix(line, "Stage: "))
		}
	}
	if stage == "" {
		return "", "", "", "", errors.New("ship-it checkpoint omits its Stage")
	}
	hash := sha256.Sum256(data)
	return "ship-it", stage, checkpointFile, hex.EncodeToString(hash[:]), nil
}

func readSessionRecords(path string) ([]json.RawMessage, string, error) {
	file, err := openSessionFile(path)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	return scanSessionRecords(file)
}

func openSessionFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open Pi session file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("inspect Pi session file: %w", err)
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("Pi session file %q is not a regular file", path)
	}
	return file, nil
}

func scanSessionRecords(file *os.File) ([]json.RawMessage, string, error) {
	hash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(file, hash))
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var records []json.RawMessage
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if !json.Valid(line) {
			return nil, "", fmt.Errorf("Pi session file contains malformed JSON on line %d", len(records)+1)
		}
		records = append(records, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, "", fmt.Errorf("read Pi session file: %w", err)
	}
	if len(records) == 0 {
		return nil, "", errors.New("Pi session file is empty")
	}
	return records, hex.EncodeToString(hash.Sum(nil)), nil
}

func verifySessionPath(sessionDir, sessionFile string) error {
	if sessionFile == "" || !filepath.IsAbs(sessionFile) {
		return fmt.Errorf("Pi RPC session file path %q is not absolute", sessionFile)
	}
	relative, err := filepath.Rel(sessionDir, sessionFile)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("Pi RPC session file %q is outside expected directory %q", sessionFile, sessionDir)
	}
	resolvedDir, err := filepath.EvalSymlinks(sessionDir)
	if err != nil {
		return fmt.Errorf("resolve Pi session directory: %w", err)
	}
	resolvedFile, err := filepath.EvalSymlinks(sessionFile)
	if err != nil {
		return fmt.Errorf("resolve Pi session file: %w", err)
	}
	resolvedRelative, err := filepath.Rel(resolvedDir, resolvedFile)
	if err != nil || resolvedRelative == "." || resolvedRelative == ".." || filepath.IsAbs(resolvedRelative) || strings.HasPrefix(resolvedRelative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("resolved Pi RPC session file %q is outside expected directory %q", resolvedFile, resolvedDir)
	}
	return nil
}

func verifyAndSyncSession(path string, expected ContinuationRequest, rpcEntries []json.RawMessage, leafID string, syncFile func(*os.File) error) (string, error) {
	if leafID == "" || len(rpcEntries) == 0 {
		return "", errors.New("Pi session has no durable continuation leaf")
	}
	file, err := openSessionFile(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	records, sessionHash, err := scanSessionRecords(file)
	if err != nil {
		return "", err
	}
	if len(records) != len(rpcEntries)+1 {
		return "", fmt.Errorf("Pi session file has %d entries, RPC reported %d", len(records)-1, len(rpcEntries))
	}
	var header struct {
		Type    string `json:"type"`
		Version *int   `json:"version"`
		ID      string `json:"id"`
		CWD     string `json:"cwd"`
	}
	if _, err := decodeExactJSON(records[0]); err != nil {
		return "", fmt.Errorf("decode Pi session header: %w", err)
	}
	if err := rejectNonCanonicalJSONFields(records[0], "type", "version", "id", "cwd"); err != nil {
		return "", fmt.Errorf("decode Pi session header: %w", err)
	}
	if err := json.Unmarshal(records[0], &header); err != nil || header.Type != "session" || header.Version == nil || *header.Version != 3 {
		return "", errors.New("Pi session file has an invalid or unsupported header")
	}
	if header.ID != expected.SessionID || header.CWD != expected.Worktree {
		return "", fmt.Errorf("Pi session header identity/path %q/%q does not match %q/%q", header.ID, header.CWD, expected.SessionID, expected.Worktree)
	}
	for index := range rpcEntries {
		diskValue, err := decodeExactJSON(records[index+1])
		if err != nil {
			return "", fmt.Errorf("decode Pi session file entry %d: %w", index+1, err)
		}
		rpcValue, err := decodeExactJSON(rpcEntries[index])
		if err != nil {
			return "", fmt.Errorf("decode Pi RPC session entry %d: %w", index+1, err)
		}
		if !reflect.DeepEqual(diskValue, rpcValue) {
			return "", fmt.Errorf("Pi session file entry %d is not synchronized with RPC state", index+1)
		}
	}
	if err := verifyContinuationLeaf(rpcEntries, leafID); err != nil {
		return "", err
	}
	if err := syncFile(file); err != nil {
		return "", fmt.Errorf("sync Pi session file: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return "", fmt.Errorf("open Pi session directory: %w", err)
	}
	if err := syncFile(directory); err != nil {
		directory.Close()
		return "", fmt.Errorf("sync Pi session directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return "", fmt.Errorf("close Pi session directory: %w", err)
	}
	return sessionHash, nil
}

func decodeExactJSON(raw []byte) (any, error) {
	if err := rejectDuplicateJSONKeys(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func rejectNonCanonicalJSONFields(raw []byte, canonicalFields ...string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return err
	}
	if object == nil {
		return errors.New("JSON value is not an object")
	}
	for field := range object {
		for _, canonical := range canonicalFields {
			if field != canonical && strings.EqualFold(field, canonical) {
				return fmt.Errorf("JSON object contains non-canonical key %q", field)
			}
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, duplicate := keys[key]; duplicate {
				return fmt.Errorf("JSON object contains duplicate key %q", key)
			}
			keys[key] = struct{}{}
			if err := rejectDuplicateJSONKeys(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := rejectDuplicateJSONKeys(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != map[json.Delim]json.Delim{'{': '}', '[': ']'}[delimiter] {
		return errors.New("JSON composite has mismatched delimiter")
	}
	return nil
}

type continuationEntry struct {
	Type             string          `json:"type"`
	ID               string          `json:"id"`
	ParentID         *string         `json:"parentId"`
	Message          json.RawMessage `json:"message"`
	FirstKeptEntryID string          `json:"firstKeptEntryId"`
}

func verifyContinuationLeaf(entries []json.RawMessage, leafID string) error {
	byID := make(map[string]continuationEntry, len(entries))
	ordered := make([]continuationEntry, 0, len(entries))
	for _, raw := range entries {
		if err := rejectNonCanonicalJSONFields(raw, "type", "id", "parentId", "message", "firstKeptEntryId"); err != nil {
			return fmt.Errorf("decode Pi session entry: %w", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return fmt.Errorf("decode Pi session entry fields: %w", err)
		}
		if _, exists := fields["parentId"]; !exists {
			return errors.New("Pi session contains an entry without parent identity")
		}
		var value continuationEntry
		if err := json.Unmarshal(raw, &value); err != nil || value.ID == "" || value.Type == "" {
			return errors.New("Pi session contains an entry without durable identity and type")
		}
		validType := false
		for _, entryType := range []string{"message", "thinking_level_change", "model_change", "compaction", "branch_summary", "custom", "custom_message", "label", "session_info"} {
			if value.Type == entryType {
				validType = true
				break
			}
			if strings.EqualFold(value.Type, entryType) {
				return fmt.Errorf("Pi session entry %q has non-canonical type %q", value.ID, value.Type)
			}
		}
		if !validType {
			return fmt.Errorf("Pi session entry %q has unsupported type %q", value.ID, value.Type)
		}
		if value.Type == "message" && (len(value.Message) == 0 || string(value.Message) == "null") {
			return fmt.Errorf("Pi session message entry %q has no message metadata", value.ID)
		}
		if _, duplicate := byID[value.ID]; duplicate {
			return fmt.Errorf("Pi session contains duplicate entry %q", value.ID)
		}
		if value.ParentID != nil {
			if *value.ParentID == "" {
				return fmt.Errorf("Pi session entry %q has an empty parent identity", value.ID)
			}
			if _, exists := byID[*value.ParentID]; !exists {
				return fmt.Errorf("Pi session entry %q references missing or out-of-order parent %q", value.ID, *value.ParentID)
			}
		}
		byID[value.ID] = value
		ordered = append(ordered, value)
	}
	if ordered[len(ordered)-1].ID != leafID {
		return fmt.Errorf("Pi session leaf %q is not the durable file leaf %q", leafID, ordered[len(ordered)-1].ID)
	}
	var branch []continuationEntry
	seen := make(map[string]struct{})
	for current := leafID; current != ""; {
		value, exists := byID[current]
		if !exists {
			return fmt.Errorf("Pi session leaf chain references missing entry %q", current)
		}
		if _, cycle := seen[current]; cycle {
			return fmt.Errorf("Pi session leaf chain contains a cycle at %q", current)
		}
		seen[current] = struct{}{}
		branch = append(branch, value)
		if value.ParentID == nil {
			current = ""
		} else {
			current = *value.ParentID
		}
	}
	for left, right := 0, len(branch)-1; left < right; left, right = left+1, right-1 {
		branch[left], branch[right] = branch[right], branch[left]
	}
	contextBranch, err := compactionAwareBranch(branch)
	if err != nil {
		return err
	}
	pending := make(map[string]struct{})
	for _, value := range contextBranch {
		if value.Type != "message" {
			continue
		}
		var message struct {
			Role       string          `json:"role"`
			ToolCallID string          `json:"toolCallId"`
			Content    json.RawMessage `json:"content"`
		}
		if err := rejectNonCanonicalJSONFields(value.Message, "role", "toolCallId", "content"); err != nil {
			return fmt.Errorf("decode Pi session message %q: %w", value.ID, err)
		}
		if err := json.Unmarshal(value.Message, &message); err != nil {
			return fmt.Errorf("decode Pi session message %q: %w", value.ID, err)
		}
		if message.Role == "" {
			return fmt.Errorf("Pi session message %q has no role", value.ID)
		}
		switch message.Role {
		case "user", "custom", "bashExecution", "branchSummary", "compactionSummary":
		case "assistant":
			var rawContents []json.RawMessage
			if err := json.Unmarshal(message.Content, &rawContents); err != nil {
				return fmt.Errorf("decode Pi session assistant content %q: %w", value.ID, err)
			}
			for _, rawContent := range rawContents {
				if err := rejectNonCanonicalJSONFields(rawContent, "type", "id"); err != nil {
					return fmt.Errorf("decode Pi session assistant content %q: %w", value.ID, err)
				}
				var content struct {
					Type string `json:"type"`
					ID   string `json:"id"`
				}
				if err := json.Unmarshal(rawContent, &content); err != nil {
					return fmt.Errorf("decode Pi session assistant content %q: %w", value.ID, err)
				}
				if content.Type != "toolCall" && strings.EqualFold(content.Type, "toolCall") {
					return fmt.Errorf("Pi session assistant entry %q has non-canonical content type %q", value.ID, content.Type)
				}
				if content.Type == "toolCall" {
					if content.ID == "" {
						return fmt.Errorf("Pi session assistant entry %q has a tool call without identity", value.ID)
					}
					if _, duplicate := pending[content.ID]; duplicate {
						return fmt.Errorf("Pi session has duplicate pending tool call %q", content.ID)
					}
					pending[content.ID] = struct{}{}
				}
			}
		case "toolResult":
			if _, exists := pending[message.ToolCallID]; !exists {
				return fmt.Errorf("Pi session tool result %q has no pending tool call", message.ToolCallID)
			}
			delete(pending, message.ToolCallID)
		default:
			return fmt.Errorf("Pi session message %q has unsupported role %q", value.ID, message.Role)
		}
	}
	if len(pending) != 0 {
		return fmt.Errorf("Pi session continuation has %d tool calls without durable results", len(pending))
	}
	return nil
}

func compactionAwareBranch(branch []continuationEntry) ([]continuationEntry, error) {
	latestCompaction := -1
	for index, value := range branch {
		if value.Type == "compaction" {
			latestCompaction = index
		}
	}
	if latestCompaction < 0 {
		return branch, nil
	}
	compaction := branch[latestCompaction]
	if compaction.FirstKeptEntryID == "" {
		return nil, fmt.Errorf("Pi session compaction entry %q has no first kept entry", compaction.ID)
	}
	firstKept := -1
	for index := 0; index < latestCompaction; index++ {
		if branch[index].ID == compaction.FirstKeptEntryID {
			firstKept = index
			break
		}
	}
	if firstKept < 0 {
		return nil, fmt.Errorf("Pi session compaction entry %q references missing first kept entry %q", compaction.ID, compaction.FirstKeptEntryID)
	}
	contextBranch := make([]continuationEntry, 0, 1+latestCompaction-firstKept+len(branch)-latestCompaction-1)
	contextBranch = append(contextBranch, compaction)
	contextBranch = append(contextBranch, branch[firstKept:latestCompaction]...)
	contextBranch = append(contextBranch, branch[latestCompaction+1:]...)
	return contextBranch, nil
}

func waitForProcessGroupExitContext(ctx context.Context, pid int) error {
	if pid <= 0 {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := syscall.Kill(-pid, syscall.Signal(0)); errors.Is(err, syscall.ESRCH) {
			return nil
		} else if err != nil && !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("verify Worker process-group exit: %w", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return errors.Join(ctx.Err(), errors.New("Worker process-group exit was not verified"))
		}
	}
}

func waitForProcessGroupExitBounded(ctx context.Context, pid int, grace time.Duration) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	exited, err := waitForProcessGroupContext(ctx, pid, grace)
	if err != nil || exited {
		return false, err
	}
	if ctx.Err() == nil {
		if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return false, fmt.Errorf("terminate surviving Worker process group: %w", err)
		}
		exited, err = waitForProcessGroupContext(ctx, pid, grace)
		if err != nil || exited {
			return false, err
		}
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false, fmt.Errorf("kill surviving Worker process group: %w", err)
	}
	exited, err = waitForProcessGroup(pid, grace)
	if err != nil {
		return true, err
	}
	if !exited {
		return true, fmt.Errorf("Worker process group %d survived shutdown escalation", pid)
	}
	return true, nil
}

func waitForProcessGroupContext(ctx context.Context, pid int, grace time.Duration) (bool, error) {
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := syscall.Kill(-pid, syscall.Signal(0)); errors.Is(err, syscall.ESRCH) {
			return true, nil
		} else if err != nil && !errors.Is(err, syscall.EPERM) {
			return false, err
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return false, nil
		case <-ctx.Done():
			return false, nil
		}
	}
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

type responseWaiter struct {
	command string
	result  chan rpcResponse
}

type rpcWriter struct {
	mu                        sync.Mutex
	destination               io.Writer
	observer                  *activity.Writer
	buffer                    []byte
	lineNumber                int
	issue                     int
	commandID                 string
	state                     rpcAgentState
	turnOpen                  bool
	completedTurns            int
	messageOpen               bool
	compactionOpen            bool
	retryOpen                 bool
	retryAttempt              int
	summarizationRetryOpen    bool
	summarizationRetryAttempt int
	providerExhausted         bool
	openTools                 map[string]struct{}
	responses                 map[string]responseWaiter
	parseErrors               []error
	settled                   chan struct{}
	failed                    chan struct{}
	settledOnce               sync.Once
	failedOnce                sync.Once
}

func newRPCWriter(destination io.Writer, observer *activity.Writer, commandID string, issue int) *rpcWriter {
	return &rpcWriter{
		destination: destination, observer: observer, commandID: commandID, issue: issue,
		state: rpcAwaitingResponse, openTools: make(map[string]struct{}), responses: make(map[string]responseWaiter),
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
		if w.observer != nil {
			_ = w.observer.Observe(line)
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

func (w *rpcWriter) ProviderExhausted() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.providerExhausted
}

func (w *rpcWriter) ExpectResponse(id, command string) <-chan rpcResponse {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make(chan rpcResponse, 1)
	w.responses[id] = responseWaiter{command: command, result: result}
	return result
}

func (w *rpcWriter) CancelResponse(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.responses, id)
}

func (w *rpcWriter) Idle() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != rpcAgentSettled || w.turnOpen || w.messageOpen || w.compactionOpen || w.retryOpen || w.summarizationRetryOpen || len(w.openTools) != 0 {
		return errors.New("Pi RPC events do not prove streaming, compaction, retries, and tools are idle")
	}
	return nil
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
	if _, err := decodeExactJSON(line); err != nil {
		w.addError(fmt.Errorf("malformed Pi RPC JSON on line %d: %w", w.lineNumber, err))
		return
	}
	if err := rejectNonCanonicalJSONFields(line, "type", "id", "command", "method", "toolCallId", "attempt", "maxAttempts", "delayMs", "errorMessage", "source", "reason", "success", "data", "error"); err != nil {
		w.addError(fmt.Errorf("malformed Pi RPC JSON on line %d: %w", w.lineNumber, err))
		return
	}
	if json.Unmarshal(line, &message) != nil || message.Type == "" {
		w.addError(fmt.Errorf("malformed Pi RPC JSON on line %d", w.lineNumber))
		return
	}
	if message.Type == "response" {
		if message.ID != w.commandID {
			waiter, exists := w.responses[message.ID]
			if !exists || message.Command != waiter.command {
				w.addError(fmt.Errorf("unexpected or mismatched Pi RPC response on line %d", w.lineNumber))
				return
			}
			var response rpcResponse
			if err := json.Unmarshal(line, &response); err != nil {
				w.addError(fmt.Errorf("decode Pi RPC response on line %d: %w", w.lineNumber, err))
				return
			}
			delete(w.responses, message.ID)
			waiter.result <- response
			return
		}
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
	if w.state == rpcAgentSettled {
		w.addError(fmt.Errorf("Pi RPC message %q followed agent_settled on line %d", message.Type, w.lineNumber))
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
		if w.state != rpcBetweenAgentRuns || w.compactionOpen || w.retryOpen || w.summarizationRetryOpen {
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
		if w.state != rpcBetweenAgentRuns || !w.compactionOpen || w.summarizationRetryOpen {
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
		if message.Success != nil && !*message.Success {
			w.providerExhausted = true
		}
	case "summarization_retry_scheduled":
		if w.state != rpcBetweenAgentRuns || w.retryOpen || message.Attempt <= w.summarizationRetryAttempt {
			w.invalidOrder(message.Type)
			return
		}
		w.summarizationRetryOpen = true
		w.summarizationRetryAttempt = message.Attempt
	case "summarization_retry_attempt_start":
		if w.state != rpcBetweenAgentRuns || !w.summarizationRetryOpen {
			w.invalidOrder(message.Type)
			return
		}
	case "summarization_retry_finished":
		if w.state != rpcBetweenAgentRuns || !w.summarizationRetryOpen {
			w.invalidOrder(message.Type)
			return
		}
		w.summarizationRetryOpen = false
		w.summarizationRetryAttempt = 0
	case "entry_appended":
		// Pi extensions persist custom session metadata through appendEntry.
		// The resulting event does not change the agent lifecycle.
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
