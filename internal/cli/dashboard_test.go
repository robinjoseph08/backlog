package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

type dashboardTestSource struct {
	current state.State
}

func (s *dashboardTestSource) Preview() (state.State, bool, error) {
	return s.current, false, nil
}

func (s *dashboardTestSource) RunnerSupervised() (bool, error) { return true, nil }

func lastDashboardFrame(output string) string {
	const redraw = "\x1b[H\x1b[2J"
	index := strings.LastIndex(output, redraw)
	if index < 0 {
		return output
	}
	return output[index+len(redraw):]
}

func TestRunOutputSelectionUsesTerminalCapabilityAndPlainOverride(t *testing.T) {
	for _, test := range []struct {
		name       string
		terminal   bool
		plain      bool
		wantANSI   bool
		wantOutput string
	}{
		{name: "terminal automatically selects dashboard", terminal: true, wantANSI: true, wantOutput: "Backlog Run Dashboard"},
		{name: "plain overrides terminal", terminal: true, plain: true, wantOutput: "Final aggregate summary"},
		{name: "redirected output stays append-only", wantOutput: "Final aggregate summary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := initializeFollowRepository(t)
			gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
			args := []string{"run", "--repo-dir", repository, "--state-dir", t.TempDir(), "--poll", "5ms", "--gh", gh}
			if test.plain {
				args = append(args, "--plain")
			}
			var stdout, stderr bytes.Buffer
			exit := MainWithSignalsAndTerminal(context.Background(), args, &stdout, &stderr, nil, func(io.Writer) bool { return test.terminal })
			if exit != 0 {
				t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
			}
			if strings.Contains(stdout.String(), "\x1b[") != test.wantANSI {
				t.Fatalf("ANSI presence = %t, want %t: %q", strings.Contains(stdout.String(), "\x1b["), test.wantANSI, stdout.String())
			}
			if !strings.Contains(stdout.String(), test.wantOutput) {
				t.Fatalf("stdout missing %q: %q", test.wantOutput, stdout.String())
			}
			if test.wantANSI {
				if strings.Count(stdout.String(), "\x1b[?25l") != 1 || strings.Count(stdout.String(), "\x1b[?25h") != 1 || !strings.HasSuffix(stdout.String(), "\x1b[?25h\n") {
					t.Fatalf("dashboard did not hide and restore the cursor exactly once: %q", stdout.String())
				}
			}
		})
	}
}

func TestTerminalDashboardPreservesDrainAndSuspensionMessages(t *testing.T) {
	for _, test := range []struct {
		name       string
		signal     os.Signal
		wantExit   int
		wantOutput []string
	}{
		{
			name: "Drain", signal: os.Interrupt, wantExit: 0,
			wantOutput: []string{"Drain: admission stopped; 0 Workers remaining", "Drain complete: 0 Workers remaining; exiting successfully", "Drain complete: no Owned Workers remain"},
		},
		{
			name: "suspension", signal: syscall.SIGTERM, wantExit: 143,
			wantOutput: []string{"Suspension: SIGTERM accepted; 0 Workers share one 1m0s deadline", "Suspension complete: 0 Workers remaining", "Suspension finished: no further interrupt has an effect before exit"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := initializeFollowRepository(t)
			discovered := filepath.Join(t.TempDir(), "discovered")
			gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url") touch `+quote(discovered)+`; printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
			signals := make(chan os.Signal, 2)
			done := make(chan int, 1)
			var stdout, stderr bytes.Buffer
			go func() {
				done <- MainWithSignalsAndTerminal(context.Background(), []string{
					"run", "--watch", "--repo-dir", repository, "--state-dir", t.TempDir(), "--poll", "5ms", "--gh", gh,
				}, &stdout, &stderr, signals, func(io.Writer) bool { return true })
			}()
			waitForFile(t, discovered)
			signals <- test.signal
			select {
			case exit := <-done:
				if exit != test.wantExit {
					t.Fatalf("exit = %d, want %d; stderr = %q", exit, test.wantExit, stderr.String())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("terminal runner did not finish shutdown")
			}
			for _, want := range test.wantOutput {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("dashboard shutdown output missing %q: %q", want, stdout.String())
				}
			}
		})
	}
}

func TestTerminalRunFinalSummaryRetainsEarlierAttentionRequired(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	run := scheduler.Run{
		Issue: 33, IssueTitle: "Operator decision", IssueURL: "https://github.com/acme/widgets/issues/33",
		RunID: "run-33", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModeRPC, Error: "review retained Worker",
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url") printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	var stdout, stderr bytes.Buffer
	exit := MainWithSignalsAndTerminal(context.Background(), []string{
		"run", "--repo-dir", repository, "--state-dir", stateDir, "--max-workers", "1", "--poll", "5ms", "--gh", gh,
	}, &stdout, &stderr, nil, func(io.Writer) bool { return true })
	if exit != 1 {
		t.Fatalf("exit = %d, want unresolved-intervention exit 1; stderr = %q", exit, stderr.String())
	}
	finalFrame := lastDashboardFrame(stdout.String())
	for _, want := range []string{"Final aggregate summary", "Attention Required (1)", "#33  Operator decision", "review retained Worker"} {
		if !strings.Contains(finalFrame, want) {
			t.Fatalf("terminal final frame missing %q: %q", want, finalFrame)
		}
	}
}

func TestTerminalDashboardRedrawsForLiveWorkerActivity(t *testing.T) {
	repository := initializeFollowRepository(t)
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	activityEmitted := filepath.Join(root, "activity-emitted")
	releaseWorker := filepath.Join(root, "release-worker")
	closedMarker := filepath.Join(root, "issue-closed")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if test -f `+quote(closedMarker)+`; then printf '%s\n' '[]'; else printf '%s\n' '[{"number":45,"title":"Observable Worker","createdAt":"2026-07-26T17:00:00Z","url":"https://github.com/acme/widgets/issues/45"}]'; fi ;;
  "issue view 45 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":45,"title":"Observable Worker","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/45","createdAt":"2026-07-26T17:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/45/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/45/dependencies/blocked_by?per_page=100 --paginate --slurp") printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-45-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    head=$8; printf '[{"number":145,"url":"https://github.com/acme/widgets/pull/145","state":"MERGED","mergedAt":"2026-07-26T17:01:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"%s","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$head" ;;
  "issue view 45 --repo acme/widgets --json number,state,title,url") touch `+quote(closedMarker)+`; printf '%s\n' '{"number":45,"state":"CLOSED","title":"Observable Worker","url":"https://github.com/acme/widgets/issues/45"}' ;;
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
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}' '{"type":"message_start","message":{"role":"assistant"}}' '{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"working"}],"usage":{"totalTokens":1200}}}' '{"type":"tool_execution_start","toolCallId":"tool-1","toolName":"bash"}'
touch `+quote(activityEmitted)+`
while ! test -f `+quote(releaseWorker)+`; do sleep 0.01; done
printf '%s\n' '{"type":"tool_execution_end","toolCallId":"tool-1","toolName":"bash","isError":false}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`)
	var stdout synchronizedBuffer
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- MainWithSignalsAndTerminal(context.Background(), []string{
			"run", "--repo-dir", repository, "--state-dir", stateDir, "--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi,
		}, &stdout, &stderr, nil, func(io.Writer) bool { return true })
	}()
	waitForFile(t, activityEmitted)
	for deadline := time.Now().Add(3 * time.Second); ; {
		observed := stdout.String()
		if strings.Contains(observed, "Deepest operation: bash") && strings.Contains(observed, "Observed tokens: 1200") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dashboard did not redraw for live Activity: %q", observed)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(releaseWorker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case exit := <-done:
		if exit != 0 {
			t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not exit after live Activity test Worker settled")
	}
	if strings.Count(stdout.String(), "\x1b[H\x1b[2J") < 2 {
		t.Fatalf("dashboard did not redraw: %q", stdout.String())
	}
}

func TestTerminalDashboardKeepsRunFinishedDuringInvocation(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	started := time.Now().Add(-time.Minute)
	run := scheduler.Run{
		Issue: 44, IssueTitle: "Merge while watching", IssueURL: "https://github.com/acme/widgets/issues/44",
		RunID: "run-44", Status: scheduler.StatusWaitingForMerge, WorkerMode: scheduler.WorkerModeRPC,
		Branch: "agent/issue-44-run-44", SessionID: "backlog-run-44", SessionDir: filepath.Join(stateDir, "sessions", "run-44"),
		StartedAt: started, UpdatedAt: started,
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-44-run-44 --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":144,"url":"https://github.com/acme/widgets/pull/144","state":"MERGED","mergedAt":"2026-07-26T17:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-44-run-44","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "issue view 44 --repo acme/widgets --json number,state,title,url") printf '%s\n' '{"number":44,"state":"CLOSED","title":"Merge while watching","url":"https://github.com/acme/widgets/issues/44"}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url") printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	var stdout, stderr bytes.Buffer
	exit := MainWithSignalsAndTerminal(context.Background(), []string{
		"run", "--repo-dir", repository, "--state-dir", stateDir, "--max-workers", "1", "--poll", "5ms", "--gh", gh,
	}, &stdout, &stderr, nil, func(io.Writer) bool { return true })
	if exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	finalFrame := lastDashboardFrame(stdout.String())
	for _, want := range []string{"Final aggregate summary", "Recently Finished (1)", "#44  Merge while watching", "State: merged"} {
		if !strings.Contains(finalFrame, want) {
			t.Fatalf("terminal final frame missing %q: %q", want, finalFrame)
		}
	}
}

func TestDashboardRedrawsForActivityWithoutCreatingActivity(t *testing.T) {
	now := time.Date(2026, 7, 26, 17, 0, 0, 0, time.UTC)
	logPath := filepath.Join(t.TempDir(), "worker.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	projectionPath := activity.PathForLog(logPath)
	projection, err := os.Create(projectionPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-2 * time.Second), Kind: "tool", Description: "Tool edit started",
		Operation: "edit", OperationChanged: true, TurnDelta: 1, ResponseCompleted: true, TokenDelta: 1200, TokensKnown: true,
	}
	if err := json.NewEncoder(projection).Encode(entry); err != nil {
		t.Fatal(err)
	}
	turns, tools, tokens := 2, 3, int64(500)
	if err := json.NewEncoder(projection).Encode(activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-time.Second), Kind: "subagent", Description: "Subagent testing",
		Subagent: &activity.SubagentSnapshot{ID: "review", Description: "Review dashboard", Status: "running", Activity: "testing", Turns: &turns, ToolUses: &tools, ApproxTokens: &tokens, Active: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(projectionPath)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		Issue: 41, IssueTitle: "Live observation", IssueURL: "https://github.com/acme/widgets/issues/41",
		RunID: "run-41", Status: scheduler.StatusRunning, PID: os.Getpid(), ProcessIdentity: identity,
		LogPath: logPath, StartedAt: now.Add(-time.Minute), UpdatedAt: now,
	}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	source := &dashboardTestSource{current: current}
	var output bytes.Buffer
	dashboard := newLiveDashboard(&output, source, current, func() time.Time { return now })
	dashboard.redraw()
	for _, want := range []string{
		"Repository: acme/widgets", "Worker capacity: 1 used | 1 available | 2 total", "#41  Live observation",
		"Issue: https://github.com/acme/widgets/issues/41", "State: running | Elapsed: 1m0s", "alive (PID",
		"Activity age: 1s | Deepest operation: Subagent \"Review dashboard\": testing",
		"Turns: Worker 1 | Subagent ~2 | Observed tokens: ~1700",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("dashboard missing %q:\n%s", want, output.String())
		}
	}
	cachedSource := dashboard.observations[run.RunID].source
	dashboard.redraw()
	if cachedSource == nil || dashboard.observations[run.RunID].source != cachedSource {
		t.Fatal("dashboard reopened Activity from the beginning during an elapsed-only redraw")
	}
	after, err := os.Stat(projectionPath)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("dashboard redraw changed Activity sidecar: before=%v after=%v", before, after)
	}
	if strings.Count(output.String(), "\x1b[H\x1b[2J") != 2 {
		t.Fatalf("redraw control sequence count = %d, want 2", strings.Count(output.String(), "\x1b[H\x1b[2J"))
	}
	projection, err = os.OpenFile(projectionPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(projection).Encode(activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-time.Second), Kind: "tool", Description: "Tool bash started",
		Operation: "bash", OperationChanged: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}
	if !dashboard.activityChanged() {
		t.Fatal("meaningful Activity append did not request a dashboard refresh")
	}
	dashboard.redraw()
	if !strings.Contains(output.String(), "Activity age: 1s | Deepest operation: Subagent \"Review dashboard\": testing") {
		t.Fatalf("Activity redraw did not retain the deepest active operation:\n%s", output.String())
	}
	suspendingAt := now
	current.Runs[0].SuspendingAt = &suspendingAt
	source.current = current
	dashboard.update(current)
	dashboard.redraw()
	if !strings.Contains(lastDashboardFrame(output.String()), "State: suspending") {
		t.Fatalf("dashboard did not use the shared Suspending state:\n%s", output.String())
	}
	if dashboardElapsedInterval > time.Second {
		t.Fatalf("elapsed refresh interval = %s, want at most 1s", dashboardElapsedInterval)
	}
}

func TestDashboardPresentsQuietAgeAndUnavailableTurnsFromSharedProgress(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	logPath := filepath.Join(t.TempDir(), "quiet.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-10 * time.Minute), Kind: "tool", Description: "Tool test started",
	})
	run := scheduler.Run{
		Issue: 58, RunID: "quiet-dashboard", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		LogPath: logPath, StartedAt: now.Add(-time.Hour),
	}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	source := &dashboardTestSource{current: current}
	var output bytes.Buffer
	dashboard := newLiveDashboard(&output, source, current, func() time.Time { return now })
	dashboard.redraw()
	got := lastDashboardFrame(output.String())
	for _, want := range []string{
		"State: running | Elapsed: 1h0m0s", "Activity age: 10m0s (quiet)",
		"Turns: Worker n/a | Subagent n/a | Observed tokens: n/a",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("quiet dashboard output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(strings.ToLower(got), "stalled") {
		t.Fatalf("quiet dashboard presentation implied a stalled state:\n%s", got)
	}
}

func TestDashboardElapsedTimerRedrawsWithoutStateActivity(t *testing.T) {
	started := time.Now().Add(-time.Minute).Truncate(time.Second)
	var clock atomic.Int64
	clock.Store(started.Add(time.Minute).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	run := scheduler.Run{Issue: 51, RunID: "run-51", Status: scheduler.StatusRunning, StartedAt: started}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	source := &dashboardTestSource{current: current}
	var output synchronizedBuffer
	dashboard := newLiveDashboard(&output, source, current, now)
	dashboard.start()
	defer dashboard.close()
	clock.Store(started.Add(62 * time.Second).UnixNano())
	deadline := time.Now().Add(2 * dashboardElapsedInterval)
	for !strings.Contains(lastDashboardFrame(output.String()), "Elapsed: 1m2s") {
		if time.Now().After(deadline) {
			t.Fatalf("elapsed timer did not update the dashboard without state or Activity:\n%s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDashboardCapacityMatchesSchedulerSemantics(t *testing.T) {
	for _, test := range []struct {
		name string
		run  scheduler.Run
		want int
	}{
		{name: "running", run: scheduler.Run{RunID: "run", Status: scheduler.StatusRunning}, want: 1},
		{name: "suspended", run: scheduler.Run{RunID: "run", Status: scheduler.StatusSuspended}, want: 1},
		{name: "waiting for merge", run: scheduler.Run{RunID: "run", Status: scheduler.StatusWaitingForMerge}},
		{name: "needs human without Worker", run: scheduler.Run{RunID: "run", Status: scheduler.StatusNeedsHuman}},
		{name: "needs human with retained Worker", run: scheduler.Run{RunID: "run", Status: scheduler.StatusNeedsHuman, PID: 42}, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := state.State{Runs: []scheduler.Run{test.run}, Leases: []scheduler.Lease{{LeaseID: test.run.RunID, Issue: 1, RunID: test.run.RunID}}}
			if got := dashboardUsedCapacity(current); got != test.want {
				t.Fatalf("used capacity = %d, want %d", got, test.want)
			}
		})
	}
}

func TestPlainRunOutputRemovesSplitTerminalControls(t *testing.T) {
	var output bytes.Buffer
	writer := &terminalControlWriter{output: &output}
	for _, content := range []string{"discovery ", "\x1b[3", "1mfailed\x1b[0m\n", "next\a line\n"} {
		if _, err := writer.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := output.String(), "discovery failed\nnext line\n"; got != want {
		t.Fatalf("sanitized plain output = %q, want %q", got, want)
	}
}

func TestDashboardFinalSummaryReturnsOutputFailure(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	source := &dashboardTestSource{current: current}
	dashboard := newLiveDashboard(failingStatusWriter{}, source, current, time.Now)
	dashboard.start()
	if err := dashboard.finalSummary(current); err == nil || !strings.Contains(err.Error(), "status output failed") {
		t.Fatalf("final dashboard output error = %v", err)
	}
	dashboard.close()
}

func TestDashboardCloseShowsThatRunnerStopped(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	source := &dashboardTestSource{current: current}
	var output bytes.Buffer
	dashboard := newLiveDashboard(&output, source, current, time.Now)
	dashboard.start()
	dashboard.close()
	if !strings.Contains(output.String(), "Stopped: the runner is exiting; interrupts have no further effect.") {
		t.Fatalf("closed dashboard retained an active footer:\n%s", output.String())
	}
}

func TestDashboardKeepsTerminalRunsAndShutdownMessagesVisible(t *testing.T) {
	now := time.Date(2026, 7, 26, 17, 0, 0, 0, time.UTC)
	logPath := filepath.Join(t.TempDir(), "finished.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-10 * time.Minute), Kind: "turn",
		Description: "Worker turn completed", TurnDelta: 1,
	})
	running := scheduler.Run{
		Issue: 42, IssueTitle: "Finished here", RunID: "run-42", Status: scheduler.StatusRunning,
		LogPath: logPath, StartedAt: now.Add(-time.Hour),
	}
	initial := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{running}, Leases: []scheduler.Lease{{LeaseID: running.RunID, Issue: running.Issue, RunID: running.RunID}},
	}
	source := &dashboardTestSource{current: initial}
	var output bytes.Buffer
	dashboard := newLiveDashboard(&output, source, initial, func() time.Time { return now })
	dashboard.redraw()
	finishedAt := now.Add(-time.Second)
	terminal := running
	terminal.Status, terminal.UpdatedAt, terminal.CompletedAt = scheduler.StatusMerged, finishedAt, &finishedAt
	current := initial
	current.Runs, current.Leases = []scheduler.Run{terminal}, nil
	source.current = current
	dashboard.update(current)
	if _, err := dashboard.Write([]byte("Drain: admission stopped; 1 Worker remaining; next SIGINT will be recorded as a suspension request\n")); err != nil {
		t.Fatal(err)
	}
	dashboard.redraw()
	for _, want := range []string{
		"Recently Finished (1)", "#42  Finished here", "Activity age: 10m0s (quiet)",
		"Turns: Worker 1 | Subagent n/a", "Drain: admission stopped; 1 Worker remaining",
		"Draining: admission is stopped; next Ctrl-C suspends unfinished Runs",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("dashboard missing %q:\n%s", want, output.String())
		}
	}
	if _, err := dashboard.Write([]byte("Drain: additional interrupt recorded as a suspension request; 1 Worker remaining\n")); err != nil {
		t.Fatal(err)
	}
	dashboard.redraw()
	if !strings.Contains(output.String(), "Suspending: continuation boundaries are being established; next Ctrl-C force stops") {
		t.Fatalf("suspension footer did not describe next interrupt:\n%s", output.String())
	}
	if _, err := dashboard.Write([]byte("Force stop: additional signal accepted; requesting force stop for 1 Worker\nSuspension: 1 Worker remaining\n")); err != nil {
		t.Fatal(err)
	}
	dashboard.redraw()
	if !strings.Contains(output.String(), "Force stopping: Worker identities are revalidated before signaling; next Ctrl-C repeats") {
		t.Fatalf("suspension progress regressed the force-stop footer:\n%s", output.String())
	}
}
