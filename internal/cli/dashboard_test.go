package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/runner"
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
	if index := strings.LastIndex(output, redraw); index >= 0 {
		return output[index+len(redraw):]
	}
	const title = "Backlog Run Dashboard"
	if index := strings.LastIndex(output, title); index >= 0 {
		return output[index:]
	}
	return output
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
				output := stdout.String()
				restore := strings.Index(output, "\x1b[?1049l")
				summary := strings.Index(output, "Final aggregate summary")
				if strings.Count(output, "\x1b[?1049h") != 1 || strings.Count(output, "\x1b[?1049l") != 1 || strings.Count(output, "\x1b[?25l") != 1 || strings.Count(output, "\x1b[?25h") != 1 || restore < 0 || summary < restore {
					t.Fatalf("Bubble Tea did not restore the screen and cursor before the summary: %q", output)
				}
			}
		})
	}
}

func TestPlainRunEmitsCompleteCandidateDiscoveryEvidenceForEveryRetry(t *testing.T) {
	repository := initializeFollowRepository(t)
	attemptsPath := filepath.Join(t.TempDir(), "attempts")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    attempts=0
    if [ -f `+quote(attemptsPath)+` ]; then attempts=$(cat `+quote(attemptsPath)+`); fi
    attempts=$((attempts + 1))
    printf '%s\n' "$attempts" > `+quote(attemptsPath)+`
    if [ "$attempts" -le 2 ]; then
      printf '%s\n' "TLS handshake timeout" "complete stderr evidence for retry $attempts" >&2
      exit 1
    fi
    printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	var stdout, stderr bytes.Buffer
	exit := MainWithSignalsAndTerminal(context.Background(), []string{
		"run", "--plain", "--repo-dir", repository, "--state-dir", t.TempDir(), "--poll", "5ms", "--gh", gh,
	}, &stdout, &stderr, nil, func(io.Writer) bool { return true })
	if exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	output := stdout.String()
	command := "gh issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url"
	if count := strings.Count(output, "candidate discovery failed; admission paused"); count != 2 {
		t.Fatalf("plain failure rows = %d, want 2:\n%s", count, output)
	}
	if count := strings.Count(output, command); count != 2 {
		t.Fatalf("full command occurrences = %d, want one for each retry:\n%s", count, output)
	}
	for _, evidence := range []string{"TLS handshake timeout", "complete stderr evidence for retry 1", "complete stderr evidence for retry 2", "candidate discovery recovered; admission resumed after 2 failures"} {
		if !strings.Contains(output, evidence) {
			t.Fatalf("plain retry output omitted %q:\n%s", evidence, output)
		}
	}
	if stderr.Len() != 0 || strings.Contains(output, "Backlog Run Dashboard") || strings.Contains(output, "\x1b[") {
		t.Fatalf("plain output compatibility changed: stdout=%q stderr=%q", output, stderr.String())
	}
}

func TestAutomaticDashboardPresentsCandidateDiscoveryFailureThroughAdmission(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	historical := scheduler.Run{
		Issue: 55, IssueTitle: "Existing outcome remains reachable", RunID: "historical-55",
		Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "retained outcome evidence",
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{historical},
	}); err != nil {
		t.Fatal(err)
	}
	attemptsPath := filepath.Join(t.TempDir(), "attempts")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    attempts=0
    if [ -f `+quote(attemptsPath)+` ]; then attempts=$(cat `+quote(attemptsPath)+`); fi
    attempts=$((attempts + 1))
    printf '%s\n' "$attempts" > `+quote(attemptsPath)+`
    printf '%s\n' "TLS handshake timeout" "retained stderr evidence for attempt $attempts" >&2
    exit 1 ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	input, writeInput := io.Pipe()
	defer input.Close()
	defer writeInput.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout synchronizedBuffer
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- MainWithTerminal(ctx, []string{
			"run", "--watch", "--repo-dir", repository, "--state-dir", stateDir, "--max-workers", "1", "--poll", "150ms", "--gh", gh,
		}, TerminalDependencies{
			Input: input, Output: &stdout, ErrorOutput: &stderr,
			IsTerminal:   func() bool { return true },
			Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 100, Height: 12}, nil },
			ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
		})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		attempts, err := os.ReadFile(attemptsPath)
		if err == nil && strings.TrimSpace(string(attempts)) == "2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Candidate discovery attempts did not reach 2: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := writeInput.Write([]byte("g")); err != nil {
		t.Fatalf("return to Admission after retries: %v", err)
	}
	waitForDashboardScreen(t, &stdout, "2 consecutive failures")
	closedOutput := stdout.String()
	if !strings.Contains(closedOutput, "Admission health") || !strings.Contains(closedOutput, "DEGRADED") || !strings.Contains(closedOutput, "Diagnostics: closed") {
		t.Fatalf("automatic dashboard did not render closed Admission health: %q", closedOutput)
	}
	if strings.Contains(closedOutput, "retained stderr evidence") || strings.Contains(closedOutput, "Operational messages") || strings.Contains(closedOutput, "candidate discovery failed; admission paused") {
		t.Fatalf("closed automatic dashboard exposed full evidence or duplicated Admission as operational rows: %q", closedOutput)
	}

	diagnosticsOffset := len(stdout.String())
	if _, err := writeInput.Write([]byte("d" + strings.Repeat("j", 12))); err != nil {
		t.Fatalf("open and scroll Diagnostics: %v", err)
	}
	waitForDashboardOutputAfter(t, &stdout, diagnosticsOffset, "retained stderr evidence")

	if _, err := writeInput.Write([]byte("G")); err != nil {
		t.Fatalf("jump to bottom of existing dashboard sections: %v", err)
	}
	waitForDashboardScreen(t, &stdout, "Diagnostic: retained outcome evidence")
	if _, err := writeInput.Write([]byte("b")); err != nil {
		t.Fatalf("page to existing outcome section: %v", err)
	}
	waitForDashboardScreen(t, &stdout, "Outcomes to Acknowledge")

	if _, err := writeInput.Write([]byte("g")); err != nil {
		t.Fatalf("return to Admission: %v", err)
	}
	waitForDashboardScreen(t, &stdout, "Admission: DEGRADED")
	if _, err := writeInput.Write([]byte{0x03}); err != nil {
		t.Fatalf("start Drain: %v", err)
	}
	select {
	case exit := <-done:
		if exit != 0 {
			t.Fatalf("Drain exit = %d, stderr = %q", exit, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("automatic dashboard did not finish Drain")
	}
	output := stdout.String()
	if !strings.Contains(output, "\x1b[?1049h") || !strings.Contains(output, "\x1b[?1049l") || stderr.Len() != 0 {
		t.Fatalf("automatic dashboard terminal lifecycle changed: stdout=%q stderr=%q", output, stderr.String())
	}
	visible := terminalScreenText(output, 100, 12)
	for _, want := range []string{"Admission: DEGRADED", "Retry: stopped"} {
		if !strings.Contains(visible, want) {
			t.Fatalf("final degraded Admission screen missing %q:\n%s\nraw output: %q", want, visible, output)
		}
	}
	if strings.Contains(visible, "Next retry:") {
		t.Fatalf("final degraded Admission screen retained an actionable retry:\n%s\nraw output: %q", visible, output)
	}
}

func waitForDashboardOutputAfter(t *testing.T, output *synchronizedBuffer, offset int, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		current := output.String()
		if offset > len(current) {
			offset = len(current)
		}
		if strings.Contains(current[offset:], want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("dashboard output after byte %d never contained %q: %q", offset, want, output.String())
}

func waitForDashboardScreen(t *testing.T, output *synchronizedBuffer, want string) {
	t.Helper()
	const width, height = 100, 12
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if visible := terminalScreenText(output.String(), width, height); strings.Contains(visible, want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	visible := terminalScreenText(output.String(), width, height)
	t.Fatalf("dashboard screen never contained %q:\n%s\nraw output: %q", want, visible, output.String())
}

func TestAutomaticBubbleDashboardRawCtrlCCompletesDrainThroughTerminalInput(t *testing.T) {
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
	input, writeInput := io.Pipe()
	defer input.Close()
	defer writeInput.Close()
	var stdout synchronizedBuffer
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- MainWithTerminal(context.Background(), []string{
			"run", "--watch", "--repo-dir", repository, "--state-dir", t.TempDir(), "--poll", "5ms", "--gh", gh,
		}, TerminalDependencies{
			Input: input, Output: &stdout, ErrorOutput: &stderr,
			IsTerminal:   func() bool { return true },
			Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 80, Height: 24}, nil },
			ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
		})
	}()
	waitForFile(t, discovered)
	waitForBuffer(t, &stdout, "Backlog Run Dashboard")
	if _, err := writeInput.Write([]byte{0x03}); err != nil {
		t.Fatalf("write raw Ctrl-C: %v", err)
	}
	select {
	case exit := <-done:
		if exit != 0 {
			t.Fatalf("raw Ctrl-C exit = %d, stderr = %q", exit, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("automatic Bubble Tea input did not complete Drain")
	}
	output := stdout.String()
	for _, want := range []string{"\x1b[?1049h", "\x1b[?1049l"} {
		if !strings.Contains(output, want) {
			t.Fatalf("raw Ctrl-C dashboard output missing %q: %q", want, output)
		}
	}
	visible := terminalScreenText(output, 80, 24)
	for _, want := range []string{"Runner stage: Drain complete", "Next Ctrl-C: no effect"} {
		if !strings.Contains(visible, want) {
			t.Fatalf("raw Ctrl-C dashboard screen missing %q:\n%s\nraw output: %q", want, visible, output)
		}
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
			wantOutput: []string{"Drain complete", "no effect"},
		},
		{
			name: "suspension", signal: syscall.SIGTERM, wantExit: 143,
			wantOutput: []string{"Suspension finished", "no effect"},
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
			output := stdout.String()
			visible := terminalScreenText(output, 80, 24)
			for _, want := range test.wantOutput {
				if !strings.Contains(visible, want) {
					t.Fatalf("dashboard shutdown screen missing %q:\n%s\nraw output: %q", want, visible, output)
				}
			}
		})
	}
}

func TestDashboardReceivesShutdownStageWithoutParsingFormattedMessages(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	source := &dashboardTestSource{current: current}
	var output bytes.Buffer
	dashboard := newLiveDashboard(&output, source, current, time.Now)
	if _, err := dashboard.Write([]byte("Force stop: this is diagnostic text, not a lifecycle event\n")); err != nil {
		t.Fatal(err)
	}
	dashboard.redraw()
	if !strings.Contains(lastDashboardFrame(output.String()), "Running: Ctrl-C starts Drain") {
		t.Fatalf("formatted message changed shutdown stage:\n%s", output.String())
	}
	shutdownMessage := "Suspension: establishing continuation boundaries for 2 Workers"
	if _, err := fmt.Fprintln(dashboard, shutdownMessage); err != nil {
		t.Fatal(err)
	}
	// Runner event delivery is asynchronous, so enough newer plain output may
	// evict the compatible rendering before its typed classification arrives.
	for index := range 12 {
		if _, err := fmt.Fprintf(dashboard, "ordinary lifecycle message %d\n", index); err != nil {
			t.Fatal(err)
		}
	}
	dashboard.operationalEvent(runner.ShutdownEvent{
		Stage: runner.ShutdownStageSuspending, Action: "establishing continuation boundaries", RemainingWorkers: 2,
		NextInterrupt: runner.NextInterruptForceStops, Message: shutdownMessage,
	})
	dashboard.redraw()
	frame := lastDashboardFrame(output.String())
	if !strings.Contains(frame, "Suspending: continuation boundaries are being established") {
		t.Fatalf("structured event did not change shutdown stage:\n%s", output.String())
	}
	if !strings.Contains(frame, shutdownMessage) {
		t.Fatalf("typed shutdown message was evicted before ordinary history:\n%s", output.String())
	}
	if !strings.Contains(frame, "Operational messages") || strings.Contains(frame, "Lifecycle messages") {
		t.Fatalf("dashboard used an inaccurate operational message heading:\n%s", output.String())
	}
	if strings.Contains(frame, "Force stop: this is diagnostic text") {
		t.Fatalf("formatted prefix received shutdown retention without a typed event:\n%s", output.String())
	}
}

func TestDashboardAggregatesAdmissionFailuresAndBoundsDiagnostics(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 30, 0, time.UTC)
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	dashboard := newLiveDashboard(io.Discard, &dashboardTestSource{current: current}, current, func() time.Time { return now })
	issue := 70
	for failure := 1; failure <= 23; failure++ {
		operation := runner.CandidateDiscoveryList
		cause := "TLS handshake timeout"
		if failure == 2 {
			operation = runner.CandidateDiscoveryInspect
			cause = "i/o timeout"
		}
		dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
			Operation: operation, Issue: &issue, Err: fmt.Errorf("full gh command %d: %s", failure, cause), Cause: cause,
			OccurredAt: now.Add(time.Duration(failure-23) * time.Second), RetryAt: now.Add(30 * time.Second),
			ConsecutiveFailures: failure,
		})
	}

	_, body, _ := dashboard.renderParts(now)
	for _, want := range []string{
		"Admission: DEGRADED", "23 consecutive failures", "First failure: 2026-07-28T12:00:08Z",
		"Latest failure: 2026-07-28T12:00:30Z", "Operation: list candidates",
		"Issue: #70", "Cause: TLS handshake timeout", "Equivalent failures: 22", "Diagnostics: closed (d to open; 20 recent)",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("Admission banner missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "full gh command") {
		t.Fatalf("closed Diagnostics exposed full errors:\n%s", body)
	}
	for _, check := range []struct {
		at   time.Time
		want string
	}{
		{at: now, want: "Next retry: 30s"},
		{at: now.Add(11 * time.Second), want: "Next retry: 19s"},
		{at: now.Add(30 * time.Second), want: "Next retry: 0s"},
		{at: now.Add(31 * time.Second), want: "Next retry: 0s"},
	} {
		_, countdownBody, _ := dashboard.renderParts(check.at)
		if !strings.Contains(countdownBody, check.want) {
			t.Fatalf("Admission countdown at %s missing %q:\n%s", check.at, check.want, countdownBody)
		}
	}
	if len(dashboard.admission.failures) != 20 {
		t.Fatalf("retained failure references = %d, want 20", len(dashboard.admission.failures))
	}

	dashboard.toggleDiagnostics()
	_, body, _ = dashboard.renderParts(now)
	if !strings.Contains(body, "Diagnostics (20 recent Candidate discovery failure records; d to close)") ||
		!strings.Contains(body, "full gh command 4") || !strings.Contains(body, "full gh command 23") ||
		strings.Contains(body, "full gh command 3") {
		t.Fatalf("Diagnostics did not contain exactly the latest 20 full failures:\n%s", body)
	}
}

func TestDashboardDiagnosticsPreserveReadableWhitespaceAndRemoveControls(t *testing.T) {
	var output strings.Builder
	renderAdmissionDiagnostics(&output, []runner.CandidateDiscoveryFailed{{
		Operation:  runner.CandidateDiscoveryList,
		Err:        errors.New("gh issue list:\nTLS\thandshake\r\n\x1b[31mtemporary\x1b[0m\a failure"),
		OccurredAt: time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
	}})
	got := output.String()
	if !strings.Contains(got, "gh issue list: TLS handshake temporary failure") {
		t.Fatalf("full Diagnostics lost readable separation: %q", got)
	}
	if strings.ContainsAny(got, "\x1b\r\t\a") || strings.Contains(got, "[31m") {
		t.Fatalf("full Diagnostics retained terminal controls: %q", got)
	}
}

func TestDashboardDiagnosticsLabelsExpiredReferencesHonestly(t *testing.T) {
	var output strings.Builder
	renderAdmissionDiagnostics(&output, []runner.CandidateDiscoveryFailed{{
		Operation:  runner.CandidateDiscoveryList,
		Err:        runner.ErrCandidateDiscoveryDiagnosticExpired,
		OccurredAt: time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
	}})
	got := output.String()
	if !strings.Contains(got, "Diagnostic unavailable: full Candidate discovery diagnostic is no longer retained") {
		t.Fatalf("expired diagnostic did not expose an honest unavailable state: %q", got)
	}
	if strings.Contains(got, "Full error/command:") {
		t.Fatalf("expired diagnostic was falsely labeled as full evidence: %q", got)
	}
}

func TestDashboardStopsIncompleteAdmissionAfterRunnerLeavesRunningStage(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name  string
		stage dashboardStage
	}{
		{name: "Drain", stage: dashboardDraining},
		{name: "suspension", stage: dashboardSuspending},
		{name: "Drain complete", stage: dashboardDrainComplete},
		{name: "suspension complete", stage: dashboardSuspensionComplete},
		{name: "stopped", stage: dashboardStopped},
		{name: "natural completion", stage: dashboardFinished},
	} {
		t.Run(test.name, func(t *testing.T) {
			dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, func() time.Time { return now })
			dashboard.stage = test.stage

			_, body, _ := dashboard.renderParts(now)
			if !strings.Contains(body, "Admission: stopped | Candidate snapshot not completed") || strings.Contains(body, "Admission: checking") {
				t.Fatalf("non-running stage retained active Admission check:\n%s", body)
			}

			admission := dashboardSectionAnchor("Admission health")
			_, compact, _ := dashboard.renderResponsiveParts(now, responsiveDashboardOptions{
				density: dashboardDensityConstrained, width: 80, selected: admission,
				expansionOverrides: map[string]bool{admission: false},
			})
			if !strings.Contains(compact.text, "Admission: stopped | Candidate snapshot not completed") || strings.Contains(compact.text, "Admission: checking") {
				t.Fatalf("compact non-running stage retained active Admission check:\n%s", compact.text)
			}
		})
	}
}

func TestDashboardStopsAdmissionRetryCountdownDuringShutdown(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, func() time.Time { return now })
	dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Cause: "connection refused",
		OccurredAt: now, RetryAt: now.Add(30 * time.Second), ConsecutiveFailures: 1,
	})

	_, body, _ := dashboard.renderParts(now)
	if !strings.Contains(body, "Next retry: 30s") {
		t.Fatalf("running Admission omitted retry countdown:\n%s", body)
	}

	dashboard.operationalEvent(runner.ShutdownEvent{Stage: runner.ShutdownStageDraining})
	_, body, _ = dashboard.renderParts(now.Add(5 * time.Second))
	if !strings.Contains(body, "Admission: DEGRADED") || !strings.Contains(body, "Retry: stopped") || strings.Contains(body, "Next retry:") {
		t.Fatalf("shutdown Admission rendered an actionable retry:\n%s", body)
	}
}

func TestPresentationQueuePreservesAdmissionAggregateThroughDashboard(t *testing.T) {
	firstFailure := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	queue := newPresentationEventQueue()
	for failure := 1; failure <= 25; failure++ {
		failureFirst := time.Time{}
		if failure == 1 {
			failureFirst = firstFailure
		}
		queue.publish(runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList,
			Err:       fmt.Errorf("full gh command %d", failure), Cause: "TLS handshake timeout",
			FirstFailureAt: failureFirst, OccurredAt: firstFailure.Add(time.Duration(failure-1) * time.Second),
			RetryAt: firstFailure.Add(time.Minute), ConsecutiveFailures: failure, Occurrences: 1,
		})
	}

	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		event, err := queue.next(ctx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			t.Fatalf("read presentation queue: %v", err)
		}
		dashboard.operationalEvent(event)
		queue.complete()
	}

	now := firstFailure.Add(24 * time.Second)
	_, body, _ := dashboard.renderParts(now)
	for _, want := range []string{
		"25 consecutive failures", "First failure: 2026-07-28T12:00:00Z", "Latest failure: 2026-07-28T12:00:24Z",
		"Equivalent failures: 25", "Diagnostics: closed (d to open; 20 recent)",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("queue-to-dashboard aggregate missing %q:\n%s", want, body)
		}
	}
	dashboard.toggleDiagnostics()
	_, body, _ = dashboard.renderParts(now)
	if !strings.Contains(body, "Diagnostics (20 recent Candidate discovery failure records; d to close)") ||
		!strings.Contains(body, "full gh command 6") || !strings.Contains(body, "full gh command 25") || strings.Contains(body, "full gh command 5") {
		t.Fatalf("queue-to-dashboard Diagnostics did not truthfully label the latest twenty records representing 25 failures:\n%s", body)
	}
}

func TestPresentationQueuePreservesRecurringAdmissionAggregateAfterEviction(t *testing.T) {
	firstFailure := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	queue := newPresentationEventQueue()
	publish := func(failure int, cause string) {
		queue.publish(runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList,
			Err:       fmt.Errorf("full gh command %d", failure), Cause: cause,
			FirstFailureAt: firstFailure, OccurredAt: firstFailure.Add(time.Duration(failure-1) * time.Second),
			RetryAt: firstFailure.Add(time.Minute), ConsecutiveFailures: failure, Occurrences: 1,
		})
	}
	publish(1, "recurring cause")
	for failure := 2; failure <= 22; failure++ {
		publish(failure, fmt.Sprintf("distinct cause %d", failure))
	}
	publish(23, "recurring cause")

	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		event, err := queue.next(ctx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			t.Fatalf("read presentation queue: %v", err)
		}
		dashboard.operationalEvent(event)
		queue.complete()
	}

	_, body, _ := dashboard.renderParts(firstFailure.Add(22 * time.Second))
	for _, want := range []string{
		"23 consecutive failures", "Cause: recurring cause | Equivalent failures: 2",
		"Diagnostics: closed (d to open; 20 recent)",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("queue-to-dashboard recurring aggregate missing %q:\n%s", want, body)
		}
	}
	dashboard.toggleDiagnostics()
	_, body, _ = dashboard.renderParts(firstFailure.Add(22 * time.Second))
	if !strings.Contains(body, "full gh command 4") || !strings.Contains(body, "full gh command 23") || strings.Contains(body, "full gh command 3") {
		t.Fatalf("queue-to-dashboard Diagnostics did not remain bounded to the latest twenty failures:\n%s", body)
	}
}

func TestDashboardAggregatesRecurringAdmissionFailureAfterManyDistinctCauses(t *testing.T) {
	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
	firstCause := "recurring cause"
	dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Cause: firstCause,
		OccurredAt: time.Unix(1, 0), ConsecutiveFailures: 1, Occurrences: 1,
	})
	for failure := 2; failure <= 201; failure++ {
		dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList, Cause: fmt.Sprintf("varying cause %d", failure),
			OccurredAt: time.Unix(int64(failure), 0), ConsecutiveFailures: failure, Occurrences: 1,
		})
	}
	dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Cause: firstCause,
		OccurredAt: time.Unix(202, 0), ConsecutiveFailures: 202, Occurrences: 1,
	})

	header, body, footer := dashboard.renderParts(time.Unix(202, 0))
	if !strings.Contains(header, "Backlog Run Dashboard") || !strings.Contains(footer, "Runner stage: Running") {
		t.Fatalf("dashboard chrome = header %q, footer %q", header, footer)
	}
	if !strings.Contains(body, "Cause: recurring cause | Equivalent failures: 2") {
		t.Fatalf("recurring failure lost its episode-wide aggregate:\n%s", body)
	}
	if count := len(dashboard.admission.equivalentFailures); count != 201 {
		t.Fatalf("lightweight aggregation counts = %d, want one for every episode identity", count)
	}

	for failure := 203; failure <= admissionAggregationIdentityLimit+500; failure++ {
		dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList, Cause: fmt.Sprintf("later varying cause %d", failure),
			OccurredAt: time.Unix(int64(failure), 0), ConsecutiveFailures: failure, Occurrences: 1,
		})
	}
	for occurrence := 1; occurrence <= 2; occurrence++ {
		dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList, Cause: "current recurring cause",
			OccurredAt:          time.Unix(int64(admissionAggregationIdentityLimit+500+occurrence), 0),
			ConsecutiveFailures: admissionAggregationIdentityLimit + 500 + occurrence, Occurrences: 1,
		})
	}
	if count := len(dashboard.admission.equivalentFailures); count > admissionAggregationIdentityLimit {
		t.Fatalf("bounded lightweight aggregation identities = %d, want at most %d", count, admissionAggregationIdentityLimit)
	}
	_, body, _ = dashboard.renderParts(time.Unix(int64(admissionAggregationIdentityLimit+502), 0))
	if !strings.Contains(body, "Cause: current recurring cause | Equivalent failures: 2") {
		t.Fatalf("current recurring cause lost its bounded aggregate:\n%s", body)
	}

	dashboard.operationalEvent(runner.CandidateDiscoveryRecovered{OccurredAt: time.Unix(203, 0), Failures: 202})
	if dashboard.admission.equivalentFailures != nil {
		t.Fatalf("recovery retained episode aggregation counts: %#v", dashboard.admission.equivalentFailures)
	}
}

func TestDashboardAdmissionRecoveryNoticeExpiresAfterTenSeconds(t *testing.T) {
	recoveredAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	dashboard := newLiveDashboard(io.Discard, &dashboardTestSource{current: current}, current, time.Now)
	dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Err: errors.New("gh command: unavailable"), Cause: "unavailable",
		OccurredAt: recoveredAt.Add(-time.Second), RetryAt: recoveredAt.Add(time.Minute), ConsecutiveFailures: 1,
	})
	dashboard.operationalEvent(runner.CandidateDiscoveryRecovered{OccurredAt: recoveredAt, Failures: 1})

	_, body, _ := dashboard.renderParts(recoveredAt.Add(9 * time.Second))
	if !strings.Contains(body, "Admission: healthy | Recovered 9s ago after 1 failure") || strings.Contains(body, "DEGRADED") {
		t.Fatalf("active recovery notice = %q", body)
	}
	if remaining := dashboard.recoveryNoticeRemaining(recoveredAt.Add(9 * time.Second)); remaining != time.Second {
		t.Fatalf("recovery notice remaining = %s, want 1s", remaining)
	}
	admission := dashboardSectionAnchor("Admission health")
	options := responsiveDashboardOptions{
		density: dashboardDensityConstrained, width: 100, selected: admission,
		expansionOverrides: map[string]bool{admission: false},
	}
	_, collapsed, _ := dashboard.renderResponsiveParts(recoveredAt.Add(9*time.Second), options)
	if !strings.Contains(collapsed.text, "> Admission health [collapsed]") || !strings.Contains(collapsed.text, "Admission: healthy | Recovered 9s ago after 1 failure") {
		t.Fatalf("selected collapsed Admission omitted active recovery notice:\n%s", collapsed.text)
	}
	_, collapsed, _ = dashboard.renderResponsiveParts(recoveredAt.Add(10*time.Second), options)
	if !strings.Contains(collapsed.text, "Admission: healthy") || strings.Contains(collapsed.text, "Recovered") {
		t.Fatalf("selected collapsed Admission retained expired recovery notice:\n%s", collapsed.text)
	}
	_, body, _ = dashboard.renderParts(recoveredAt.Add(10 * time.Second))
	if !strings.Contains(body, "Admission: healthy") || strings.Contains(body, "Recovered") {
		t.Fatalf("expired recovery notice = %q", body)
	}
}

func TestDashboardMessageLimitPrioritizesOnlyShutdownEvents(t *testing.T) {
	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
	shutdownMessage := "Drain: preserving shutdown history"
	dashboard.operationalEvent(runner.ShutdownEvent{Stage: runner.ShutdownStageDraining, Message: shutdownMessage})

	var oldestOperational string
	for index := range dashboardMessageLimit - 1 {
		event := runner.RunLifecycleEvent{
			Stage:   runner.RunLifecycleClaimed,
			Message: fmt.Sprintf("Run %d claimed", index),
		}
		if index == 0 {
			oldestOperational = normalizedDashboardMessage(runner.FormatOperationalEvent(event))
		}
		dashboard.operationalEvent(event)
	}
	ordinary := "latest ordinary runtime message"
	dashboard.recordMessage(ordinary)

	visible := make(map[string]dashboardMessage, len(dashboard.messages))
	for _, message := range dashboard.messages {
		visible[message.text] = message
	}
	if len(dashboard.messages) != dashboardMessageLimit {
		t.Fatalf("visible message history = %d entries, want %d", len(dashboard.messages), dashboardMessageLimit)
	}
	if _, ok := visible[ordinary]; !ok {
		t.Fatalf("latest ordinary message evicted itself: %#v", dashboard.messages)
	}
	if shutdown, ok := visible[shutdownMessage]; !ok || !shutdown.shutdownPriority {
		t.Fatalf("shutdown message lost retention priority: %#v", shutdown)
	}
	if _, ok := visible[oldestOperational]; ok {
		t.Fatalf("ordinary typed operational event retained shutdown priority: %#v", visible[oldestOperational])
	}
}

func TestDashboardReconcilesOperationalSemanticsInBothDeliveryOrders(t *testing.T) {
	event := runner.RunLifecycleEvent{
		Stage:   runner.RunLifecycleClaimed,
		Message: "Run #70 claimed",
	}
	message := normalizedDashboardMessage(runner.FormatOperationalEvent(event))

	for _, eventFirst := range []bool{false, true} {
		name := "output first"
		if eventFirst {
			name = "event first"
		}
		t.Run(name, func(t *testing.T) {
			dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
			if eventFirst {
				dashboard.operationalEvent(event)
				dashboard.recordMessage(message)
			} else {
				dashboard.recordMessage(message)
				dashboard.operationalEvent(event)
			}

			if len(dashboard.messages) != 1 {
				t.Fatalf("reconciled message history = %#v, want one typed occurrence", dashboard.messages)
			}
			stored := dashboard.messages[0]
			if stored.text != message || stored.semantic != dashboardSemanticActive || !stored.shutdown || stored.shutdownPriority || !stored.plainMatched {
				t.Fatalf("reconciled operational message = %#v", stored)
			}
			if _, exists := dashboard.messageOccurrences[message]; exists {
				t.Fatal("reconciled plain occurrence remained tracked")
			}
			if _, exists := dashboard.shutdownOccurrences[message]; exists {
				t.Fatal("reconciled typed occurrence remained tracked")
			}
		})
	}
}

func TestDashboardBoundsOccurrenceTrackingAndSuppressesTypedFirstOutput(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	source := &dashboardTestSource{current: current}
	var output bytes.Buffer
	dashboard := newLiveDashboard(&output, source, current, time.Now)

	typedFirst := "Drain: typed event arrived before compatible output"
	dashboard.operationalEvent(runner.ShutdownEvent{Stage: runner.ShutdownStageDraining, Message: typedFirst})
	for index := range dashboardOccurrenceLimit * 4 {
		if _, err := fmt.Fprintf(dashboard, "operational message %d\n", index); err != nil {
			t.Fatal(err)
		}
	}
	if len(dashboard.messageOccurrences) > dashboardOccurrenceLimit || len(dashboard.shutdownOccurrences) > dashboardOccurrenceLimit || len(dashboard.occurrenceOrder) > dashboardOccurrenceLimit {
		t.Fatalf("occurrence tracking grew beyond %d entries before delayed output: messages=%d shutdown=%d order=%d", dashboardOccurrenceLimit, len(dashboard.messageOccurrences), len(dashboard.shutdownOccurrences), len(dashboard.occurrenceOrder))
	}
	if len(dashboard.messages) != dashboardMessageLimit {
		t.Fatalf("visible message history = %d entries, want %d", len(dashboard.messages), dashboardMessageLimit)
	}
	if dashboard.shutdownOccurrences[typedFirst] != 1 {
		t.Fatalf("typed-first occurrence was pruned while its shutdown history remained visible: %#v", dashboard.shutdownOccurrences)
	}

	if _, err := fmt.Fprintln(dashboard, typedFirst); err != nil {
		t.Fatal(err)
	}
	matches := 0
	for _, message := range dashboard.messages {
		if message.text == typedFirst {
			matches++
			if !message.shutdown {
				t.Fatalf("typed-first message was not retained as shutdown history: %#v", message)
			}
		}
	}
	if matches != 1 {
		t.Fatalf("typed-first message occurrences after delayed plain output = %d, want 1", matches)
	}
	if len(dashboard.messageOccurrences) > dashboardOccurrenceLimit || len(dashboard.shutdownOccurrences) > dashboardOccurrenceLimit || len(dashboard.occurrenceOrder) > dashboardOccurrenceLimit {
		t.Fatalf("occurrence tracking grew beyond %d entries after delayed output: messages=%d shutdown=%d order=%d", dashboardOccurrenceLimit, len(dashboard.messageOccurrences), len(dashboard.shutdownOccurrences), len(dashboard.occurrenceOrder))
	}
	if _, exists := dashboard.messageOccurrences[typedFirst]; exists {
		t.Fatal("resolved plain occurrence remained tracked")
	}
	if _, exists := dashboard.shutdownOccurrences[typedFirst]; exists {
		t.Fatal("resolved shutdown occurrence remained tracked")
	}

	for index := range dashboardOccurrenceLimit * 2 {
		dashboard.operationalEvent(runner.ShutdownEvent{
			Stage: runner.ShutdownStageDraining, Message: fmt.Sprintf("typed shutdown message %d", index),
		})
	}
	if len(dashboard.messages) > dashboardMessageLimit || len(dashboard.messageOccurrences) > dashboardOccurrenceLimit || len(dashboard.shutdownOccurrences) > dashboardOccurrenceLimit || len(dashboard.occurrenceOrder) > dashboardOccurrenceLimit {
		t.Fatalf("typed shutdown tracking exceeded its bounds: visible=%d messages=%d shutdown=%d order=%d", len(dashboard.messages), len(dashboard.messageOccurrences), len(dashboard.shutdownOccurrences), len(dashboard.occurrenceOrder))
	}
}

func TestDashboardClosePreservesIncompleteShutdownStages(t *testing.T) {
	for _, test := range []struct {
		name  string
		stage runner.ShutdownStage
		want  string
	}{
		{name: "Drain incomplete", stage: runner.ShutdownStageDrainIncomplete, want: "Drain incomplete: Worker liveness remains unverified"},
		{name: "Suspension incomplete", stage: runner.ShutdownStageSuspensionIncomplete, want: "Suspension incomplete: one or more shutdown steps failed; review operational messages and Run diagnostics"},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
			source := &dashboardTestSource{current: current}
			var output bytes.Buffer
			dashboard := newLiveDashboard(&output, source, current, time.Now)
			dashboard.start()
			dashboard.operationalEvent(runner.ShutdownEvent{Stage: test.stage, NextInterrupt: runner.NextInterruptNone})
			dashboard.close()

			frame := lastDashboardFrame(output.String())
			if !strings.Contains(frame, test.want) || strings.Contains(frame, "Stopped: the runner is exiting") {
				t.Fatalf("close replaced %s stage:\n%s", test.name, output.String())
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

func TestTerminalBubbleDashboardHandlesWorkerActivityChanges(t *testing.T) {
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
	waitForBuffer(t, &stdout, "Deepest operation: bash")
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
	if !strings.Contains(stdout.String(), "\x1b[?1049h") || !strings.Contains(stdout.String(), "\x1b[?1049l") {
		t.Fatalf("Bubble Tea did not enter and leave the alternate screen: %q", stdout.String())
	}
}

func TestTerminalDashboardKeepsRunFinishedDuringInvocation(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	started := time.Now().Add(-time.Minute)
	logPath := filepath.Join(stateDir, "run-44.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: time.Now().Add(-10 * time.Minute), Kind: "turn",
		Description: "Worker turn completed", TurnDelta: 1,
	})
	run := scheduler.Run{
		Issue: 44, IssueTitle: "Merge while watching", IssueURL: "https://github.com/acme/widgets/issues/44",
		RunID: "run-44", Status: scheduler.StatusWaitingForMerge, WorkerMode: scheduler.WorkerModeRPC,
		Branch: "agent/issue-44-run-44", SessionID: "backlog-run-44", SessionDir: filepath.Join(stateDir, "sessions", "run-44"),
		LogPath: logPath, StartedAt: started, UpdatedAt: started,
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
	for _, want := range []string{
		"Final aggregate summary", "Repository: acme/widgets", "Runs: 1", "Active Leases: 0",
		"Active (0)", "Attention Required (0)",
	} {
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
	if strings.Count(output.String(), "Backlog Run Dashboard") != 2 {
		t.Fatalf("aggregate render count = %d, want 2", strings.Count(output.String(), "Backlog Run Dashboard"))
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

	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-10 * time.Minute), Kind: "turn",
		Description: "Worker turn completed", TurnDelta: 1,
	})
	run.Status = scheduler.StatusNeedsHuman
	run.Error = "review Worker outcome"
	current.Runs[0] = run
	source.current = current
	dashboard.update(current)
	dashboard.redraw()
	got = lastDashboardFrame(output.String())
	for _, want := range []string{
		"Attention Required (1)", "Activity age: 10m0s (quiet)",
		"Turns: Worker 1 | Subagent n/a", "Diagnostic: review Worker outcome",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("attention dashboard output lost %q:\n%s", want, got)
		}
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

func TestDashboardStagePresentationCoversEveryStage(t *testing.T) {
	seen := make(map[string]dashboardStage)
	for stage := dashboardRunning; stage < dashboardStageCount; stage++ {
		presentation := dashboardStagePresentationFor(stage)
		if presentation.summary == "" || presentation.stage == "" || presentation.nextInterrupt == "" {
			t.Fatalf("stage %d has incomplete presentation: %#v", stage, presentation)
		}
		if semantic := dashboardStageSemantic(stage); semantic == dashboardSemanticNone {
			t.Fatalf("stage %d has no semantic styling", stage)
		}
		if previous, exists := seen[presentation.stage]; exists {
			t.Fatalf("stages %d and %d share presentation %q; the stage switch may be incomplete", previous, stage, presentation.stage)
		}
		seen[presentation.stage] = stage
		if got := dashboardFooter(stage); got != presentation.summary {
			t.Fatalf("stage %d summary = %q, want %q", stage, got, presentation.summary)
		}
		wantParts := fmt.Sprintf("Runner stage: %s\nNext Ctrl-C: %s", presentation.stage, presentation.nextInterrupt)
		if got := dashboardFooterParts(stage); got != wantParts {
			t.Fatalf("stage %d footer parts = %q, want %q", stage, got, wantParts)
		}
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

func TestDashboardRenderersShareProjectionMetadata(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	active := scheduler.Run{Issue: 69, RunID: "active", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Minute)}
	attention := scheduler.Run{Issue: 70, RunID: "attention", Status: scheduler.StatusNeedsHuman}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{active, attention},
		Leases: []scheduler.Lease{
			{LeaseID: active.RunID, Issue: active.Issue, RunID: active.RunID},
			{LeaseID: attention.RunID, Issue: attention.Issue, RunID: attention.RunID},
		},
	}
	dashboard := newLiveDashboard(io.Discard, &dashboardTestSource{current: current}, current, func() time.Time { return now })
	dashboard.recordMessage("observation changed")
	responsive, responsiveLayout, _ := dashboard.renderResponsiveParts(now, responsiveDashboardOptions{
		density: dashboardDensityConstrained, width: 120,
	})
	plainHeader, plainLayout, plainFooter := dashboard.renderPartsForWithLayout(current, []dashboardMessage{{text: "observation changed"}}, dashboardRunning, now, dashboardStyler{})

	if responsive.header != plainHeader || responsive.footer != plainFooter || !maps.Equal(responsiveLayout.attention, plainLayout.attention) {
		t.Fatalf("responsive and plain projection metadata drifted:\nresponsive header: %q\nplain header: %q\nresponsive footer: %q\nplain footer: %q\nresponsive Attention: %#v\nplain Attention: %#v",
			responsive.header, plainHeader, responsive.footer, plainFooter, responsiveLayout.attention, plainLayout.attention)
	}
	responsive.header = "presentation labels changed independently"
	minimal := minimalDashboardHeader(responsive.metadata, len(responsiveLayout.attention), 0)
	for _, want := range []string{"Backlog: acme/widgets", "R:acme/widgets", "W:1u/1a/2t", "Health:0 healthy, 1 anomalous", "Attention:1"} {
		if !strings.Contains(minimal, want) {
			t.Fatalf("minimal projection omitted structured metadata %q after display header changed:\n%s", want, minimal)
		}
	}
}

func TestPlainRunOutputRemovesSplitTerminalControls(t *testing.T) {
	var output bytes.Buffer
	writer := &terminalControlWriter{output: &output}
	for _, content := range []string{
		"discovery ", "\x1b[3", "1mfailed\x1b[0m\n", "next\a line\n",
		"\x1b]8;;https://example.test\aissue link\x1b]8;;\a\n",
		"\x1b[?1049h\x1b[?25l\x1b[?1000hcontrols removed\x1b[?1000l\x1b[?25h\x1b[?1049l\n",
	} {
		if _, err := writer.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := output.String(), "discovery failed\nnext line\nissue link\ncontrols removed\n"; got != want {
		t.Fatalf("sanitized plain output = %q, want %q", got, want)
	}
}

type dashboardWriterContractProbe struct {
	bytes.Buffer
	writeCalls       int
	writeStringCalls int
}

func (b *dashboardWriterContractProbe) Write(content []byte) (int, error) {
	b.writeCalls++
	return b.Buffer.Write(content)
}

func (b *dashboardWriterContractProbe) WriteString(content string) (int, error) {
	b.writeStringCalls++
	return b.Buffer.WriteString(content)
}

func TestDashboardRenderingUsesSynchronizedWriterContract(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	source := &dashboardTestSource{current: current}
	var output dashboardWriterContractProbe
	dashboard := newLiveDashboard(&output, source, current, time.Now)

	dashboard.redraw()
	if output.writeCalls != 1 || output.writeStringCalls != 0 {
		t.Fatalf("dashboard writes = %d Write and %d WriteString calls, want 1 synchronized Write call", output.writeCalls, output.writeStringCalls)
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

func TestDashboardWorkerExpectationFollowsRunLifecycle(t *testing.T) {
	t.Parallel()

	for _, status := range []scheduler.Status{
		scheduler.StatusClaimed,
		scheduler.StatusWorktreeReady,
		scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge,
		scheduler.StatusSuspended,
		scheduler.StatusResetting,
		scheduler.StatusReset,
		scheduler.StatusMerged,
		scheduler.StatusFailed,
		scheduler.StatusNeedsHuman,
	} {
		status := status
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			run := scheduler.Run{Status: status, PID: 42, ProcessIdentity: "42:retained"}
			observed := statusRun{run: run, observation: runObservation{process: followObservation{
				supervision:         "UNSUPERVISED",
				workerLiveness:      "dead",
				workerLivenessState: workerLivenessDead,
			}}}
			wantExpected := status == scheduler.StatusRunning
			if got := dashboardRunExpectsWorker(run); got != wantExpected {
				t.Fatalf("Worker expectation = %t, want %t", got, wantExpected)
			}
			promotions := dashboardLivenessPromotions(observed)
			healthy, anomalous := dashboardWorkerHealth([]statusRun{observed})
			if wantExpected {
				if len(promotions) == 0 || healthy != 0 || anomalous != 1 {
					t.Fatalf("running Worker promotions = %q, health = %d healthy and %d anomalous", promotions, healthy, anomalous)
				}
				return
			}
			if len(promotions) != 0 || healthy != 0 || anomalous != 0 {
				t.Fatalf("Run without an expected Worker promotions = %q, health = %d healthy and %d anomalous", promotions, healthy, anomalous)
			}
		})
	}
}

func TestDashboardActiveLivenessAnomalyUsesVerifiedState(t *testing.T) {
	t.Parallel()

	run := scheduler.Run{Status: scheduler.StatusRunning}
	verified := statusRun{run: run, observation: runObservation{process: followObservation{
		workerLiveness:      "live Worker presentation changed",
		workerLivenessState: workerLivenessAlive,
	}}}
	if dashboardActiveLivenessAnomaly(verified) {
		t.Fatal("verified-live Worker was classified as a liveness anomaly after presentation changed")
	}

	unverified := statusRun{run: run, observation: runObservation{process: followObservation{
		workerLiveness:      "alive (presentation alone is not verification)",
		workerLivenessState: workerLivenessUnknown,
	}}}
	if !dashboardActiveLivenessAnomaly(unverified) {
		t.Fatal("presentation text caused an unverified Worker to avoid anomaly priority")
	}
}

func TestDashboardProjectsCurrentAndHistoricalRunsAcrossInvocations(t *testing.T) {
	now := time.Date(2026, 7, 27, 18, 0, 0, 0, time.UTC)
	started := now.Add(-4 * time.Hour)
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	active := []scheduler.Run{
		{Issue: 3, IssueTitle: "Verified Worker", RunID: "active-c", Status: scheduler.StatusRunning, PID: os.Getpid(), ProcessIdentity: identity, StartedAt: started.Add(2 * time.Hour)},
		{Issue: 2, IssueTitle: "Preparing second", RunID: "active-b", Status: scheduler.StatusClaimed, StartedAt: started.Add(time.Hour)},
		{Issue: 1, IssueTitle: "Missing Worker", RunID: "active-a", Status: scheduler.StatusRunning, StartedAt: started.Add(3 * time.Hour)},
		{Issue: 4, IssueTitle: "Preparing first", RunID: "active-a-tie", Status: scheduler.StatusWorktreeReady, StartedAt: started.Add(time.Hour)},
		{Issue: 5, IssueTitle: "Waiting for merge", RunID: "active-d", Status: scheduler.StatusWaitingForMerge, StartedAt: started.Add(90 * time.Minute)},
		{Issue: 6, IssueTitle: "Suspended continuation", RunID: "active-e", Status: scheduler.StatusSuspended, StartedAt: started.Add(150 * time.Minute)},
	}
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 6, Runs: append([]scheduler.Run(nil), active...)}
	for _, run := range active {
		current.Leases = append(current.Leases, scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID})
	}
	for index := range 12 {
		status := scheduler.StatusNeedsHuman
		switch index {
		case 0:
			status = scheduler.StatusFailed
		case 1:
			status = scheduler.StatusResetting
		}
		run := scheduler.Run{
			Issue: 100 + index, IssueTitle: fmt.Sprintf("Attention %02d", index), RunID: fmt.Sprintf("attention-%02d", index),
			Status: status, StartedAt: started, UpdatedAt: now.Add(time.Duration(index) * time.Minute),
		}
		current.Runs = append(current.Runs, run)
		current.Leases = append(current.Leases, scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID})
	}
	for index := range 12 {
		updatedAt := now.Add(time.Duration(index) * time.Minute)
		if index == 10 {
			updatedAt = now.Add(11 * time.Minute)
		}
		status := scheduler.StatusFailed
		if index == 0 {
			status = scheduler.StatusNeedsHuman
		}
		issue := 200 + index
		if index == 11 {
			issue = 210
		}
		current.Runs = append(current.Runs, scheduler.Run{
			Issue: issue, IssueTitle: fmt.Sprintf("Outcome %02d", index), RunID: fmt.Sprintf("outcome-%02d", index),
			Status: status, StartedAt: started, UpdatedAt: updatedAt,
		})
	}
	acknowledgedAt := now
	current.Runs = append(current.Runs, scheduler.Run{
		Issue: 299, IssueTitle: "Acknowledged outcome", RunID: "outcome-acknowledged", Status: scheduler.StatusNeedsHuman,
		StartedAt: started, UpdatedAt: now.Add(time.Hour), AcknowledgedAt: &acknowledgedAt,
	})
	for index := range 12 {
		completedAt := now.Add(time.Duration(index-12) * time.Minute)
		if index == 10 {
			completedAt = now.Add(-time.Minute)
		}
		issue := 300 + index
		if index == 11 {
			issue = 310
		}
		current.Runs = append(current.Runs, scheduler.Run{
			Issue: issue, IssueTitle: fmt.Sprintf("Completion %02d", index), RunID: fmt.Sprintf("completion-%02d", index),
			Status: scheduler.StatusMerged, PID: 999999, StartedAt: started, UpdatedAt: completedAt, CompletedAt: &completedAt,
			PullRequest: fmt.Sprintf("https://github.com/acme/widgets/pull/%d", 300+index), LogPath: "/missing/worker.jsonl",
		})
	}

	var output bytes.Buffer
	dashboard := newLiveDashboard(&output, &dashboardTestSource{current: current}, current, func() time.Time { return now })
	dashboard.redraw()
	frame := lastDashboardFrame(output.String())
	activeOutput := dashboardSectionOutput(t, frame, "Active Runs", "Attention Required")
	for _, runID := range []string{"Missing Worker", "Preparing first", "Preparing second", "Waiting for merge", "Verified Worker", "Suspended continuation"} {
		if !strings.Contains(activeOutput, runID) {
			t.Fatalf("Active Runs omitted %q:\n%s", runID, activeOutput)
		}
	}
	assertDashboardOrder(t, activeOutput, "Missing Worker", "Preparing first", "Preparing second", "Waiting for merge", "Verified Worker", "Suspended continuation")

	attentionOutput := dashboardSectionOutput(t, frame, "Attention Required", "Outcomes to Acknowledge")
	if !strings.Contains(attentionOutput, "Attention Required (12)") || strings.Count(attentionOutput, "Attention ") != 13 {
		t.Fatalf("Attention Required did not retain every leased intervention-required Run:\n%s", attentionOutput)
	}
	outcomesOutput := dashboardSectionOutput(t, frame, "Outcomes to Acknowledge", "Recent Completions")
	if !strings.Contains(outcomesOutput, "Outcomes to Acknowledge (12)") || strings.Contains(outcomesOutput, "Acknowledged outcome") {
		t.Fatalf("historical outcome projection was incomplete:\n%s", outcomesOutput)
	}
	assertDashboardOrder(t, outcomesOutput, "Outcome 11", "Outcome 10", "Outcome 00")

	completionsOutput := dashboardSectionOutput(t, frame, "Recent Completions", "")
	for _, want := range []string{"Recent Completions (10)", "Completion 11", "Completion 10", "Completion 09", "7 more completions"} {
		if !strings.Contains(completionsOutput, want) {
			t.Fatalf("Recent Completions missing %q:\n%s", want, completionsOutput)
		}
	}
	for _, hidden := range []string{"Completion 08", "Completion 01", "Completion 00"} {
		if strings.Contains(completionsOutput, hidden) {
			t.Fatalf("collapsed or old Completion %q was visible by default:\n%s", hidden, completionsOutput)
		}
	}
	for _, verbose := range []string{"Worker liveness", "Activity age", "Turns:", "Observed tokens"} {
		if strings.Contains(completionsOutput, verbose) {
			t.Fatalf("compact Completion rows included %q:\n%s", verbose, completionsOutput)
		}
	}
}

func TestRenderDashboardCompletionsUsesOneLineRowsAndVerifiedCompletionTime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 27, 18, 0, 0, 0, time.UTC)
	completedAt := now.Add(-30 * time.Minute)
	runs := []statusRun{
		{run: scheduler.Run{
			Issue: 42, IssueTitle: "Populated metadata", RunID: "completion-current", Status: scheduler.StatusMerged,
			PullRequest: "https://github.com/acme/widgets/pull/42", StartedAt: now.Add(-time.Hour), CompletedAt: &completedAt,
		}},
		{run: scheduler.Run{
			Issue: 42, IssueTitle: "Legacy completion", RunID: "completion-legacy", Status: scheduler.StatusMerged,
			PullRequest: "https://github.com/acme/widgets/pull/41", StartedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-time.Hour),
		}},
	}
	var output strings.Builder
	renderDashboardCompletions(&output, runs, now, dashboardStyler{})
	want := "\nRecent Completions (2)\n" +
		"  #42  Populated metadata | PR: https://github.com/acme/widgets/pull/42 | Elapsed: 30m0s | Completed: 30m0s ago\n" +
		"  #42  Legacy completion | PR: https://github.com/acme/widgets/pull/41 | Elapsed: 1h0m0s | Completed: n/a\n"
	if got := output.String(); got != want {
		t.Fatalf("compact Completion output = %q, want %q", got, want)
	}
}

func dashboardSectionOutput(t *testing.T, output, name, next string) string {
	t.Helper()
	start := strings.Index(output, name+" (")
	if start < 0 {
		t.Fatalf("dashboard section %q missing:\n%s", name, output)
	}
	if next == "" {
		return output[start:]
	}
	end := strings.Index(output[start:], "\n"+next+" (")
	if end < 0 {
		t.Fatalf("dashboard section %q terminator %q missing:\n%s", name, next, output)
	}
	return output[start : start+end]
}

func assertDashboardOrder(t *testing.T, output string, values ...string) {
	t.Helper()
	previous := -1
	for _, value := range values {
		index := strings.Index(output, value)
		if index < 0 || index <= previous {
			t.Fatalf("dashboard values are not ordered as %q:\n%s", values, output)
		}
		previous = index
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
	dashboard.operationalEvent(runner.ShutdownEvent{
		Stage: runner.ShutdownStageDraining, Action: "admission stopped", RemainingWorkers: 1,
		NextInterrupt: runner.NextInterruptSuspends,
	})
	if _, err := dashboard.Write([]byte("Drain: admission stopped; 1 Worker remaining; next SIGINT will be recorded as a suspension request\n")); err != nil {
		t.Fatal(err)
	}
	dashboard.redraw()
	for _, want := range []string{
		"Recent Completions (1)", "#42  Finished here | Elapsed: 59m59s | Completed: 1s ago",
		"Drain: admission stopped; 1 Worker remaining",
		"Draining: admission is stopped; next Ctrl-C suspends unfinished Runs",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("dashboard missing %q:\n%s", want, output.String())
		}
	}
	dashboard.operationalEvent(runner.ShutdownEvent{
		Stage: runner.ShutdownStageSuspending, Action: "suspension requested", RemainingWorkers: 1,
		NextInterrupt: runner.NextInterruptForceStops,
	})
	if _, err := dashboard.Write([]byte("Drain: additional interrupt recorded as a suspension request; 1 Worker remaining\n")); err != nil {
		t.Fatal(err)
	}
	dashboard.redraw()
	if !strings.Contains(output.String(), "Suspending: continuation boundaries are being established; next Ctrl-C force stops") {
		t.Fatalf("suspension footer did not describe next interrupt:\n%s", output.String())
	}
	dashboard.operationalEvent(runner.ShutdownEvent{
		Stage: runner.ShutdownStageForceStopping, Action: "requesting force stop", RemainingWorkers: 1,
		NextInterrupt: runner.NextInterruptRepeatsForceStop,
	})
	if _, err := dashboard.Write([]byte("Force stop: additional signal accepted; requesting force stop for 1 Worker\nSuspension: 1 Worker remaining\n")); err != nil {
		t.Fatal(err)
	}
	dashboard.redraw()
	if !strings.Contains(output.String(), "Force stopping: Worker identities are revalidated before signaling; next Ctrl-C repeats") {
		t.Fatalf("suspension progress regressed the force-stop footer:\n%s", output.String())
	}
}
