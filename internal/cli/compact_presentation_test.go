package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/x/ansi"
	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestInteractiveStatusUsesCompactColoredRunRows(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	now := time.Date(2026, 7, 31, 8, 9, 10, 0, time.UTC)
	completedAt := now.Add(-time.Minute)
	current := state.State{
		Version: state.CurrentVersion,
		Repo:    "acme/widgets",
		Runs: []scheduler.Run{
			{Issue: 11, IssueTitle: "Compact active work", RunID: "active", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint, StartedAt: now.Add(-2 * time.Minute)},
			{Issue: 12, IssueTitle: "Compact completion", RunID: "complete", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, StartedAt: now.Add(-5 * time.Minute), CompletedAt: &completedAt},
		},
		Leases: []scheduler.Lease{{LeaseID: "active", Issue: 11, RunID: "active"}},
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(current); err != nil {
		t.Fatal(err)
	}

	var output, diagnostics bytes.Buffer
	exit := MainWithTerminal(context.Background(), []string{
		"status", "--repo-dir", repository, "--state-dir", stateDir,
	}, TerminalDependencies{
		Output: &output, ErrorOutput: &diagnostics,
		IsTerminal: func() bool { return true },
		Dimensions: func() (TerminalDimensions, error) {
			return TerminalDimensions{Width: 58, Height: 24}, nil
		},
		ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
		Now:          func() time.Time { return now },
	})
	if exit != 0 {
		t.Fatalf("status exit = %d, diagnostics = %q", exit, diagnostics.String())
	}
	got := output.String()
	plain := ansi.Strip(got)
	for _, want := range []string{
		"Backlog Status",
		"acme/widgets | 2 runs, 2 shown | 1 leases",
		"Active (1)",
		"#11  Compact active",
		"State: claimed | Elapsed: 2m0s",
		"Recent Completions (1)",
		"#12  Compact completion | Elapsed: 4m0s",
	} {
		if !strings.Contains(plain, want) {
			t.Fatalf("compact status missing %q:\n%s", want, plain)
		}
	}
	if !strings.Contains(got, "\x1b[") {
		t.Fatalf("interactive status did not use color: %q", got)
	}
	for _, verbose := range []string{"    Issue:", "    Progress:", "Acknowledged outcomes hidden by default:"} {
		if strings.Contains(plain, verbose) {
			t.Fatalf("compact status retained verbose field %q:\n%s", verbose, plain)
		}
	}
	assertCompactLineWidths(t, got, 58)
}

func TestInteractiveStatusNoColorRemainsCompactAndPlain(t *testing.T) {
	presentation := compactPresentation{enabled: true, width: 40, styler: newDashboardStyler(TerminalColorNone, true)}
	now := time.Date(2026, 7, 31, 8, 9, 10, 0, time.UTC)
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", Runs: []scheduler.Run{{
		Issue: 1, IssueTitle: "A title that needs truncation", RunID: "one", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint, StartedAt: now.Add(-time.Minute),
	}}, Leases: []scheduler.Lease{{RunID: "one"}}}
	var output bytes.Buffer
	if err := printCompactStatusProjection(&output, current, &sequenceFollowSource{}, now, false, presentation); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); strings.Contains(got, "\x1b[") || !strings.Contains(got, "Backlog Status") || !strings.Contains(got, "Run: one") {
		t.Fatalf("NO_COLOR compact status = %q", got)
	}
	assertCompactLineWidths(t, output.String(), 40)
}

func TestInteractiveFollowUsesCompactColoredSummaryAndActivity(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	now := time.Date(2026, 7, 31, 8, 9, 10, 0, time.Local)
	startedAt := now.Add(-2 * time.Minute)
	completedAt := now.Add(-time.Minute)
	logPath := filepath.Join(stateDir, "follow.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-5 * time.Second), Kind: "tool",
		Description: "Tool test \x1b[31mstarted", Operation: "test", OperationChanged: true,
	})
	run := scheduler.Run{
		Issue: 21, IssueTitle: "Compact \x1bfollow", RunID: "follow-compact", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint,
		StartedAt: startedAt, CompletedAt: &completedAt, LogPath: logPath,
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", Runs: []scheduler.Run{run},
	}); err != nil {
		t.Fatal(err)
	}

	var output, diagnostics bytes.Buffer
	exit := MainWithTerminal(context.Background(), []string{
		"follow", run.RunID, "--repo-dir", repository, "--state-dir", stateDir,
	}, TerminalDependencies{
		Output: &output, ErrorOutput: &diagnostics,
		IsTerminal: func() bool { return true },
		Dimensions: func() (TerminalDimensions, error) {
			return TerminalDimensions{Width: 62, Height: 24}, nil
		},
		ColorProfile: func() TerminalColorProfile { return TerminalColorANSI256 },
		Now:          func() time.Time { return now },
	})
	if exit != 0 {
		t.Fatalf("follow exit = %d, diagnostics = %q", exit, diagnostics.String())
	}
	got := output.String()
	plain := ansi.Strip(got)
	for _, want := range []string{
		"Backlog Follow",
		"#21  Compact follow | State: merged | Elapsed: 1m0s",
		"Run: follow-compact | Runner: n/a (terminal Run)",
		"Activity: 5s | Worker operation: test",
		"Subagents: 0 (0 active) | Deepest: test",
		"Turns: Worker n/a, Subagent n/a",
		"Tokens: Worker n/a, Subagent n/a, Total n/a",
		"Activity (latest 20)",
		"Tool test started",
	} {
		if !strings.Contains(plain, want) {
			t.Fatalf("compact Follow missing %q:\n%s", want, plain)
		}
	}
	for _, verbose := range []string{"Completed Worker turns:", "Completed Worker tokens:", "Deepest current operation:", "Approximate Subagent turns:"} {
		if strings.Contains(plain, verbose) {
			t.Fatalf("compact Follow retained verbose field %q:\n%s", verbose, plain)
		}
	}
	if !strings.Contains(got, "\x1b[") || strings.Contains(plain, "\x1b") || strings.Contains(plain, "[31m") {
		t.Fatalf("compact Follow styling or sanitization failed: %q", got)
	}
	assertCompactLineWidths(t, got, 62)
}

func TestInteractiveFollowRawStdoutRemainsVerbatim(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "raw.jsonl")
	want := "{\"event\":1}\n{\"event\":2}\n"
	if err := os.WriteFile(logPath, []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{Issue: 22, RunID: "raw-terminal", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, LogPath: logPath}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	exit := MainWithTerminal(context.Background(), []string{
		"follow", run.RunID, "--raw", "--repo-dir", repository, "--state-dir", stateDir,
	}, TerminalDependencies{
		Output: &output, ErrorOutput: &diagnostics,
		IsTerminal:   func() bool { return true },
		Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 40, Height: 10}, nil },
		ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
	})
	if exit != 0 {
		t.Fatalf("raw Follow exit = %d, diagnostics = %q", exit, diagnostics.String())
	}
	if got := output.String(); got != want {
		t.Fatalf("interactive raw Follow stdout = %q, want %q", got, want)
	}
}

func TestCompactStatusRetainsUnavailableElapsedAndRunningTelemetry(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 9, 10, 0, time.UTC)
	turns, tools := 2, 3
	subagentTokens := int64(50)
	metrics := followMetrics{}
	metrics.apply(activity.Entry{ObservedAt: now.Add(-3 * time.Second), Kind: "tool", Operation: "test", OperationChanged: true})
	metrics.apply(activity.Entry{ObservedAt: now.Add(-2 * time.Second), Kind: "turn", TurnDelta: 1})
	metrics.apply(activity.Entry{ObservedAt: now.Add(-time.Second), ResponseCompleted: true, TokensKnown: true, TokenDelta: 100})
	metrics.apply(activity.Entry{ObservedAt: now, Kind: "subagent", Subagent: &activity.SubagentSnapshot{
		ID: "review", Description: "Review", Activity: "checking", Active: true,
		Turns: &turns, ToolUses: &tools, ApproxTokens: &subagentTokens,
	}})
	observed := statusRun{
		run: scheduler.Run{Issue: 30, RunID: "running", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint},
		observation: runObservation{metrics: metrics, observed: now, process: followObservation{
			workerLiveness: "alive (verified)", workerLivenessState: workerLivenessAlive, supervision: "SUPERVISED",
		}},
	}
	var output bytes.Buffer
	printer := compactStatusPrinter{
		output: &output, now: now,
		presentation: compactPresentation{enabled: true, width: 58, styler: newDashboardStyler(TerminalColorNone, true)},
	}
	printer.section(statusActive, "Active", []statusRun{observed})
	if printer.err != nil {
		t.Fatal(printer.err)
	}
	got := output.String()
	for _, want := range []string{
		"State: running | Elapsed: n/a",
		"Worker: alive (verified)",
		"Activity: 0s",
		`Deepest: Subagent "Review": checking`,
		"Turns: Worker 1, Subagent ~2",
		"Tokens: ~150",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("compact status telemetry missing %q:\n%s", want, got)
		}
	}
	assertCompactLineWidths(t, got, 58)
}

func TestInteractiveFollowStreamsCompactFinalSummaryWithOutcomeColor(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "live-compact.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	projectionPath := activity.PathForLog(logPath)
	observedAt := time.Date(2026, 7, 31, 8, 9, 10, 0, time.Local)
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: observedAt, Kind: "lifecycle", Description: "Worker started",
		Operation: "starting", OperationChanged: true,
	})
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	store := state.FileStore{Path: filepath.Join(directory, "state.json")}
	run := scheduler.Run{
		Issue: 31, RunID: "live-compact", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: identity, LogPath: logPath, WorkerLogOpen: true, StartedAt: observedAt.Add(-time.Minute),
	}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}}}); err != nil {
		t.Fatal(err)
	}
	presentation := compactPresentation{enabled: true, width: 72, styler: newDashboardStyler(TerminalColorTrueColor, true)}
	var output synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- followNormalizedPresented(context.Background(), store, run.RunID, &output, io.Discard, 5*time.Millisecond, func() time.Time {
			return observedAt.Add(10 * time.Second)
		}, presentation)
	}()
	waitForBuffer(t, &output, "Worker started")
	writeActivityEntries(t, projectionPath, activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: observedAt.Add(9 * time.Second), Kind: "model",
		Description: "Assistant response completed", ResponseCompleted: true, TokensKnown: true, TokenDelta: 77,
	})
	waitForBuffer(t, &output, "Assistant response completed")
	run.Status = scheduler.StatusFailed
	run.WorkerLogOpen = false
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("compact follower did not exit after terminal transition")
	}
	got := output.String()
	plain := ansi.Strip(got)
	for _, want := range []string{
		"Run state changed to failed", "Final", "State: failed", "Tokens: Worker 77, Subagent n/a, Total 77",
	} {
		if !strings.Contains(plain, want) {
			t.Fatalf("streaming compact Follow missing %q:\n%s", want, plain)
		}
	}
	if strings.Count(plain, "Backlog Follow") != 1 || strings.Count(plain, "Final") != 1 {
		t.Fatalf("compact final summary duplicated headings:\n%s", plain)
	}
	if !strings.Contains(got, presentation.styler.attention.Render("Run state changed to failed")) ||
		!strings.Contains(got, presentation.styler.attention.Render("Final")) {
		t.Fatalf("failed lifecycle/final output lost attention color: %q", got)
	}
	assertCompactLineWidths(t, got, 72)
}

func TestInteractiveStatusJSONBypassesCompactPresentation(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets"}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(current); err != nil {
		t.Fatal(err)
	}
	current, _, err := store.Preview()
	if err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	exit := MainWithTerminal(context.Background(), []string{
		"status", "--json", "--repo-dir", repository, "--state-dir", stateDir,
	}, TerminalDependencies{
		Output: &output, ErrorOutput: &diagnostics,
		IsTerminal:   func() bool { return true },
		Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 40, Height: 10}, nil },
		ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
	})
	if exit != 0 {
		t.Fatalf("terminal status JSON exit = %d, diagnostics = %q", exit, diagnostics.String())
	}
	if strings.Contains(output.String(), "\x1b") {
		t.Fatalf("terminal status JSON contained ANSI: %q", output.String())
	}
	var got state.State
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("terminal status JSON: %v\n%s", err, output.String())
	}
	if !reflect.DeepEqual(got, current) {
		t.Fatalf("terminal status JSON = %#v, want %#v", got, current)
	}
}

func TestInteractiveStatusAndFollowHonorNoColorEnvironment(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLORTERM", "truecolor")
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	run := scheduler.Run{Issue: 32, RunID: "no-color", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}); err != nil {
		t.Fatal(err)
	}
	for _, command := range [][]string{
		{"status", "--repo-dir", repository, "--state-dir", stateDir},
		{"follow", run.RunID, "--repo-dir", repository, "--state-dir", stateDir},
	} {
		var output, diagnostics bytes.Buffer
		exit := MainWithTerminal(context.Background(), command, TerminalDependencies{
			Output: &output, ErrorOutput: &diagnostics,
			IsTerminal: func() bool { return true },
			Dimensions: func() (TerminalDimensions, error) {
				return TerminalDimensions{Width: 60, Height: 20}, nil
			},
		})
		if exit != 0 {
			t.Fatalf("%v exit = %d, diagnostics = %q", command, exit, diagnostics.String())
		}
		if got := output.String(); strings.Contains(got, "\x1b") || !strings.Contains(got, "Backlog ") {
			t.Fatalf("%v NO_COLOR output = %q", command, got)
		}
	}
}

func TestCompactPresentationHandlesNarrowWideUnicode(t *testing.T) {
	presentation := compactPresentation{enabled: true, width: 18, styler: newDashboardStyler(TerminalColorTrueColor, true)}
	var output bytes.Buffer
	renderer := newFollowRenderer(&output, presentation)
	if err := renderer.activityEntry(activity.Entry{
		ObservedAt: time.Date(2026, 7, 31, 8, 9, 10, 0, time.Local),
		Kind:       "tool", Description: "测试 🧪 e\u0301 activity",
	}); err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(output.String()) || !strings.Contains(output.String(), "\x1b[") || !strings.Contains(ansi.Strip(output.String()), "测试") {
		t.Fatalf("narrow Unicode Activity was invalid, empty, or unstyled: %q", output.String())
	}
	assertCompactLineWidths(t, output.String(), 18)
	lines := presentation.fieldLines("  ", "测试字段", "emoji 🧪 value", "e\u0301 combining")
	if len(lines) == 0 || !strings.Contains(lines[0], "测试") {
		t.Fatalf("packed Unicode fields lost visible content: %q", lines)
	}
	for _, line := range lines {
		if !utf8.ValidString(line) || ansi.StringWidth(line) > presentation.width {
			t.Fatalf("packed Unicode field line = %q, width %d", line, ansi.StringWidth(line))
		}
	}
}

func TestInteractiveStatusLoadsRunningTelemetryThroughCommandBoundary(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	now := time.Date(2026, 7, 31, 8, 9, 10, 0, time.UTC)
	logPath := filepath.Join(stateDir, "status-live.jsonl")
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath),
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: now.Add(-2 * time.Second), Kind: "tool", Description: "Tool test started", Operation: "test", OperationChanged: true},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: now.Add(-time.Second), Kind: "turn", Description: "Turn completed", TurnDelta: 1},
		activity.Entry{Version: activity.CurrentVersion, ObservedAt: now, Kind: "model", Description: "Response completed", ResponseCompleted: true, TokensKnown: true, TokenDelta: 42},
	)
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	run := scheduler.Run{
		Issue: 33, RunID: "status-live", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint,
		PID: os.Getpid(), ProcessIdentity: identity, LogPath: logPath, StartedAt: now.Add(-time.Minute),
	}
	if err := (state.FileStore{Path: filepath.Join(stateDir, "state.json")}).Save(state.State{
		Version: state.CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	exit := MainWithTerminal(context.Background(), []string{
		"status", "--repo-dir", repository, "--state-dir", stateDir,
	}, TerminalDependencies{
		Output: &output, ErrorOutput: &diagnostics,
		IsTerminal:   func() bool { return true },
		Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 72, Height: 20}, nil },
		ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
		Now:          func() time.Time { return now },
	})
	if exit != 0 {
		t.Fatalf("live terminal status exit = %d, diagnostics = %q", exit, diagnostics.String())
	}
	for _, want := range []string{"Run: status-live", "Worker: alive", "Worker operation: test", "Turns: Worker 1", "Tokens: 42"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("live terminal status missing %q:\n%s", want, output.String())
		}
	}
}

func TestCompactStatusPreservesDiagnosticsAndHistoricalOutcomeIdentity(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 9, 10, 0, time.UTC)
	resolvedAt := now.Add(-time.Hour)
	failed := statusRun{run: scheduler.Run{
		Issue: 40, RunID: "failed-old", Status: scheduler.StatusFailed, Error: "validation failed",
		FailureClass: scheduler.FailureValidation, WorkflowStage: "validation", BlockerKind: "evidence", BlockerCause: "missing", BlockerFingerprint: "check-1",
	}}
	merged := statusRun{run: scheduler.Run{Issue: 41, RunID: "merged-old", Status: scheduler.StatusMerged, CleanupPending: true}}
	resolved := statusRun{run: scheduler.Run{
		Issue: 42, RunID: "resolved-old", Status: scheduler.StatusResolvedExternally, ResolvedExternallyAt: &resolvedAt,
		ClosureReason: "not-planned", DiagnosticWarning: "cleanup uncertain",
	}}
	var output bytes.Buffer
	printer := compactStatusPrinter{output: &output, now: now, presentation: compactPresentation{enabled: true, width: 80, styler: newDashboardStyler(TerminalColorNone, true)}}
	printer.section(statusHistory, "History", []statusRun{failed, merged, resolved})
	if printer.err != nil {
		t.Fatal(printer.err)
	}
	for _, want := range []string{
		"Run: failed-old", "State: failed", "Diagnostic: validation failed", "Failure class: validation-failure", "Workflow stage: validation",
		"Blocker kind: evidence", "Blocker cause: missing", "Blocker fingerprint: check-1",
		"Run: merged-old", "State: merged", "Completion cleanup: pending",
		"Run: resolved-old", "State: resolved-externally", "GitHub closure reason: not-planned", "Diagnostic warning: cleanup uncertain",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("compact History missing %q:\n%s", want, output.String())
		}
	}
}

func TestCompactFollowPreservesExternalResolutionMetadata(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 9, 10, 0, time.UTC)
	resolvedAt := now.Add(-time.Hour)
	run := scheduler.Run{
		Issue: 43, RunID: "resolved", Status: scheduler.StatusResolvedExternally, ResolvedExternallyAt: &resolvedAt,
		ClosureReason: "completed", Error: "retained diagnostic", DiagnosticWarning: "cleanup warning",
	}
	var output bytes.Buffer
	renderer := newFollowRenderer(&output, compactPresentation{enabled: true, width: 80, styler: newDashboardStyler(TerminalColorNone, true)})
	if err := renderer.summary(run, followMetrics{}, followObservation{supervision: "n/a (terminal Run)", workerLiveness: "absent", workerLivenessState: workerLivenessAbsent}, now); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"State: resolved-externally", "Resolved: " + resolvedAt.Format(time.RFC3339), "GitHub: completed", "Retained diagnostic: retained diagnostic", "Diagnostic warning: cleanup warning",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("compact resolved Follow missing %q:\n%s", want, output.String())
		}
	}
}

func TestFollowSemanticClassifiersCoverWarningsAndCompletion(t *testing.T) {
	completed := activity.SubagentSnapshot{Completed: true}
	active := activity.SubagentSnapshot{Active: true}
	for name, gotWant := range map[string][2]dashboardSemantic{
		"completed Subagent": {followSubagentSemantic(completed), dashboardSemanticCompletion},
		"active Subagent":    {followSubagentSemantic(active), dashboardSemanticActive},
		"retry":              {followActivitySemantic(activity.Entry{Kind: "retry"}), dashboardSemanticWarning},
		"compaction":         {followActivitySemantic(activity.Entry{Kind: "compaction"}), dashboardSemanticWarning},
		"unsupervised":       {followObservationSemantic(followObservation{supervision: "UNSUPERVISED", workerLivenessState: workerLivenessAlive}), dashboardSemanticWarning},
		"dead Worker":        {followLivenessSemantic(followObservation{workerLivenessState: workerLivenessDead}), dashboardSemanticWarning},
		"live Worker":        {followLivenessSemantic(followObservation{workerLivenessState: workerLivenessAlive}), dashboardSemanticActive},
	} {
		if gotWant[0] != gotWant[1] {
			t.Fatalf("%s semantic = %d, want %d", name, gotWant[0], gotWant[1])
		}
	}
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	var output bytes.Buffer
	renderer := newFollowRenderer(&output, compactPresentation{enabled: true, width: 60, styler: styler})
	entry := activity.Entry{Kind: "retry", Description: "Worker retry started"}
	if err := renderer.activityEntry(entry); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), styler.warning.Render(entry.Description)) {
		t.Fatalf("retry semantic did not reach rendered output: %q", output.String())
	}
}

func TestCompactPresentationsReturnOutputFailures(t *testing.T) {
	presentation := compactPresentation{enabled: true, width: 40, styler: newDashboardStyler(TerminalColorTrueColor, true)}
	if err := printCompactStatusProjection(failingStatusWriter{}, state.State{Version: state.CurrentVersion}, &sequenceFollowSource{}, time.Now(), false, presentation); err == nil || !strings.Contains(err.Error(), "status output failed") {
		t.Fatalf("compact status output error = %v", err)
	}
	renderer := newFollowRenderer(failingStatusWriter{}, presentation)
	if err := renderer.summary(scheduler.Run{}, followMetrics{}, followObservation{}, time.Now()); err == nil || !strings.Contains(err.Error(), "status output failed") {
		t.Fatalf("compact Follow output error = %v", err)
	}
}

func TestRunLifecycleSemanticCoversEveryStatus(t *testing.T) {
	for _, test := range []struct {
		status   scheduler.Status
		liveness workerLivenessState
		want     dashboardSemantic
	}{
		{scheduler.StatusClaimed, workerLivenessAbsent, dashboardSemanticActive},
		{scheduler.StatusWorktreeReady, workerLivenessAbsent, dashboardSemanticActive},
		{scheduler.StatusRunning, workerLivenessAlive, dashboardSemanticActive},
		{scheduler.StatusRunning, workerLivenessDead, dashboardSemanticWarning},
		{scheduler.StatusWaitingForMerge, workerLivenessAbsent, dashboardSemanticWarning},
		{scheduler.StatusSuspended, workerLivenessAbsent, dashboardSemanticWarning},
		{scheduler.StatusResetting, workerLivenessAbsent, dashboardSemanticWarning},
		{scheduler.StatusReset, workerLivenessAbsent, dashboardSemanticCompletion},
		{scheduler.StatusResolvingExternally, workerLivenessAbsent, dashboardSemanticAttention},
		{scheduler.StatusResolvedExternally, workerLivenessAbsent, dashboardSemanticCompletion},
		{scheduler.StatusMerged, workerLivenessAbsent, dashboardSemanticCompletion},
		{scheduler.StatusFailed, workerLivenessAbsent, dashboardSemanticAttention},
		{scheduler.StatusNeedsHuman, workerLivenessAbsent, dashboardSemanticAttention},
	} {
		observed := statusRun{run: scheduler.Run{Status: test.status}, observation: runObservation{process: followObservation{workerLivenessState: test.liveness}}}
		if got := dashboardRunLifecycleSemantic(observed); got != test.want {
			t.Fatalf("status %s liveness %d semantic = %d, want %d", test.status, test.liveness, got, test.want)
		}
	}
}

func assertCompactLineWidths(t *testing.T, output string, width int) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
		if got := ansi.StringWidth(line); got > width {
			t.Fatalf("line width = %d, want <= %d: %q", got, width, ansi.Strip(line))
		}
	}
}
