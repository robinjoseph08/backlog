package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		"#11 | State: claimed | Elapsed: 2m0s",
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
	for _, verbose := range []string{"    Run:", "    Issue:", "    Progress:", "Acknowledged outcomes hidden by default:"} {
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
	if got := output.String(); strings.Contains(got, "\x1b[") || !strings.Contains(got, "Backlog Status") || strings.Contains(got, "    Run:") {
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
		"#21 | State: merged | Elapsed: 1m0s",
		"Run: follow-compact | Runner: n/a (terminal Run)",
		"Tokens: n/a | Subagents: 0 (0 active)",
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

func assertCompactLineWidths(t *testing.T, output string, width int) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
		if got := ansi.StringWidth(line); got > width {
			t.Fatalf("line width = %d, want <= %d: %q", got, width, ansi.Strip(line))
		}
	}
}
