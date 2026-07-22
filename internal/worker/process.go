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
	"strings"
	"sync"
	"syscall"
	"time"
)

type Request struct {
	Issue       int
	RunID       string
	Worktree    string
	SessionName string
}

type Result struct {
	ExitCode   int
	LogPath    string
	StderrPath string
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
	stdout             *os.File
	stderr             *os.File
	events             *eventWriter
	terminate          func() error
	terminationStarted <-chan struct{}
	terminationDone    <-chan struct{}
	waitOnce           sync.Once
	result             Result
}

var safeRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func (s Supervisor) Start(ctx context.Context, request Request) (*Process, error) {
	if request.Issue <= 0 || request.RunID == "" || request.Worktree == "" || request.SessionName == "" {
		return nil, fmt.Errorf("worker request is incomplete")
	}
	if !safeRunIDPattern.MatchString(request.RunID) || request.RunID == "." || request.RunID == ".." {
		return nil, fmt.Errorf("run id %q contains unsafe path characters", request.RunID)
	}
	if err := os.MkdirAll(s.LogsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create worker log directory: %w", err)
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
	piArgs := []string{"--mode", "json", "-p"}
	if s.Approve {
		piArgs = append(piArgs, "--approve")
	} else {
		piArgs = append(piArgs, "--no-approve")
	}
	piArgs = append(piArgs, "--name", request.SessionName, fmt.Sprintf("/skill:afk %d", request.Issue))

	// The wrapper cannot exec Pi until Release creates the gate. This lets the
	// runner persist the PID before the worker can claim or modify an issue.
	const wrapper = `gate=$1; shift; attempts=0; while [ ! -f "$gate" ]; do attempts=$((attempts+1)); if [ "$attempts" -ge 36000 ]; then exit 124; fi; sleep 0.1; done; exec "$@"`
	wrapperArgs := []string{"-c", wrapper, "backlog-gate", gatePath, executable}
	wrapperArgs = append(wrapperArgs, piArgs...)
	command := exec.CommandContext(ctx, "/bin/sh", wrapperArgs...)
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
	events := &eventWriter{destination: stdoutLog}
	command.Dir = request.Worktree
	command.Stdout = events
	command.Stderr = stderrLog
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = grace
	command.Cancel = terminate
	if err := command.Start(); err != nil {
		stdoutLog.Close()
		stderrLog.Close()
		return nil, fmt.Errorf("start Pi gate: %w", err)
	}
	return &Process{
		command: command, logPath: logPath, stderrPath: stderrPath, gatePath: gatePath,
		stdout: stdoutLog, stderr: stderrLog, events: events, terminate: terminate,
		terminationStarted: terminationStarted, terminationDone: terminationDone,
	}, nil
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
	return releaseGate(p.gatePath)
}

func (p *Process) Abort() error {
	return p.terminate()
}

func (p *Process) Wait() Result {
	p.waitOnce.Do(func() {
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
		p.result = Result{
			ExitCode:   exitCode,
			LogPath:    p.logPath,
			StderrPath: p.stderrPath,
			StreamErr:  streamErr,
			Err:        errors.Join(waitErr, streamErr, closeErr),
		}
	})
	return p.result
}

func releaseGate(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("release worker start gate: %w", err)
	}
	return file.Close()
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

type eventWriter struct {
	destination io.Writer
	buffer      []byte
	lineNumber  int
	eventCount  int
	sawSession  bool
	sawSettled  bool
	parseErrors []error
}

func (w *eventWriter) Write(data []byte) (int, error) {
	if _, err := w.destination.Write(data); err != nil {
		return 0, fmt.Errorf("write Pi event log: %w", err)
	}
	w.buffer = append(w.buffer, data...)
	for {
		newline := bytes.IndexByte(w.buffer, '\n')
		if newline < 0 {
			break
		}
		w.validate(w.buffer[:newline])
		w.buffer = w.buffer[newline+1:]
	}
	return len(data), nil
}

func (w *eventWriter) Finish() error {
	if len(w.buffer) > 0 {
		w.validate(w.buffer)
		w.buffer = nil
	}
	if w.eventCount == 0 {
		w.parseErrors = append(w.parseErrors, errors.New("Pi JSON stream contained no events"))
	} else {
		if !w.sawSession {
			w.parseErrors = append(w.parseErrors, errors.New("Pi JSON stream omitted its session event"))
		}
		if !w.sawSettled {
			w.parseErrors = append(w.parseErrors, errors.New("Pi JSON stream ended before agent_settled"))
		}
	}
	return errors.Join(w.parseErrors...)
}

func (w *eventWriter) validate(line []byte) {
	w.lineNumber++
	trimmed := strings.TrimSpace(string(line))
	if trimmed == "" {
		return
	}
	var event struct {
		Type string `json:"type"`
	}
	if !json.Valid([]byte(trimmed)) || json.Unmarshal([]byte(trimmed), &event) != nil || event.Type == "" {
		w.parseErrors = append(w.parseErrors, fmt.Errorf("malformed Pi JSON event on line %d", w.lineNumber))
		return
	}
	w.eventCount++
	if event.Type == "session" {
		w.sawSession = true
	}
	if event.Type == "agent_settled" {
		w.sawSettled = true
	}
}
