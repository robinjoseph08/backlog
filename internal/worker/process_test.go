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

func TestProcessStartsNamedPersistentAFKSessionInWorktree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	argsPath := filepath.Join(root, "args")
	cwdPath := filepath.Join(root, "cwd")
	pi := fakePi(t, `
printf '%s\n' "$*" > `+shellQuote(argsPath)+`
pwd > `+shellQuote(cwdPath)+`
printf '%s\n' '{"type":"session"}' '{"type":"agent_start"}' '{"type":"agent_settled"}'
echo 'diagnostic' >&2
`)
	worktree := filepath.Join(root, "worktree")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs"), Approve: true}).Start(
		context.Background(), Request{Issue: 42, RunID: "run-42", Worktree: worktree, SessionName: "afk #42"},
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if process.PID() <= 0 {
		t.Fatalf("pid = %d, want positive", process.PID())
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	result := process.Wait()
	if result.Err != nil || result.ExitCode != 0 {
		t.Fatalf("wait = %#v", result)
	}

	args, _ := os.ReadFile(argsPath)
	wantArgs := `--mode json -p --approve --name afk #42 /skill:afk 42`
	if strings.TrimSpace(string(args)) != wantArgs {
		t.Fatalf("args = %q, want %q", strings.TrimSpace(string(args)), wantArgs)
	}
	cwd, _ := os.ReadFile(cwdPath)
	if strings.TrimSpace(string(cwd)) != worktree {
		t.Fatalf("cwd = %q, want %q", strings.TrimSpace(string(cwd)), worktree)
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

func TestProcessCannotRunPiUntilReleased(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	marker := filepath.Join(root, "started")
	pi := fakePi(t, `
printf started > `+shellQuote(marker)+`
printf '%s\n' '{"type":"session"}' '{"type":"agent_settled"}'
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs")}).Start(
		context.Background(), Request{Issue: 5, RunID: "run-5", Worktree: root, SessionName: "afk #5"},
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Pi ran before durable release, stat error = %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if result := process.Wait(); result.Err != nil {
		t.Fatalf("wait: %v", result.Err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("Pi did not run after release: %v", err)
	}
}

func TestAbortEscalatesForWorkerIgnoringTermination(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	childPIDPath := filepath.Join(root, "child.pid")
	pi := fakePi(t, `
sh -c 'trap "" TERM; while :; do sleep 1; done' &
child=$!
printf '%s\n' "$child" > `+shellQuote(childPIDPath)+`
trap 'exit 0' TERM
wait "$child"
`)
	process, err := (Supervisor{
		Executable: pi, LogsDir: filepath.Join(root, "logs"), TerminationGrace: 30 * time.Millisecond,
	}).Start(context.Background(), Request{Issue: 6, RunID: "run-6", Worktree: root, SessionName: "afk #6"})
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
	result := process.Wait()
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
printf '%s\n' '{"type":"session"}' '{"type":"agent_settled"}'
`)
	process, err := (Supervisor{Executable: pi, LogsDir: filepath.Join(root, "logs"), Approve: false}).Start(
		context.Background(), Request{Issue: 8, RunID: "run-8", Worktree: root, SessionName: "afk #8"},
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
	args, _ := os.ReadFile(argsPath)
	if !strings.Contains(string(args), "--no-approve") || strings.Contains(string(args), " --approve") {
		t.Fatalf("args = %q, want --no-approve only", strings.TrimSpace(string(args)))
	}
}

func TestProcessFailsSafelyOnMalformedJSONOutput(t *testing.T) {
	t.Parallel()

	pi := fakePi(t, `
printf '%s\n' '{"type":"agent_start"}' 'not-json'
`)
	worktree := t.TempDir()
	process, err := (Supervisor{Executable: pi, LogsDir: t.TempDir()}).Start(
		context.Background(), Request{Issue: 7, RunID: "run-7", Worktree: worktree, SessionName: "afk #7"},
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := process.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	result := process.Wait()
	if result.Err == nil || !strings.Contains(result.Err.Error(), "malformed Pi JSON") {
		t.Fatalf("wait error = %v, want malformed JSON failure", result.Err)
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
