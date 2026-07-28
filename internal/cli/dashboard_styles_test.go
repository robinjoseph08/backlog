package cli

import (
	"context"
	"image/color"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
)

func TestDashboardStylingUsesTypedRunSemanticsWithoutChangingLabels(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	now := time.Date(2026, 7, 27, 20, 0, 0, 0, time.UTC)
	active := []statusRun{
		{run: scheduler.Run{Issue: 1, IssueTitle: "Active work", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Minute)}, observation: runObservation{process: followObservation{workerLiveness: "healthy presentation", workerLivenessState: workerLivenessAlive}}},
		{run: scheduler.Run{Issue: 6, IssueTitle: "Degraded work", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Minute)}, observation: runObservation{process: followObservation{workerLiveness: "wording that does not identify a warning", workerLivenessState: workerLivenessUnknown}}},
		{run: scheduler.Run{Issue: 2, IssueTitle: "Waiting work", Status: scheduler.StatusWaitingForMerge, StartedAt: now.Add(-2 * time.Minute)}, observation: runObservation{process: followObservation{workerLiveness: "absent", workerLivenessState: workerLivenessAbsent}}},
		{run: scheduler.Run{Issue: 3, IssueTitle: "Suspended work", Status: scheduler.StatusSuspended, StartedAt: now.Add(-3 * time.Minute)}, observation: runObservation{process: followObservation{workerLiveness: "absent", workerLivenessState: workerLivenessAbsent}}},
	}
	attention := []statusRun{{run: scheduler.Run{Issue: 4, IssueTitle: "Fatal work", Status: scheduler.StatusNeedsHuman, Error: "Worker exited without a verified outcome", StartedAt: now.Add(-4 * time.Minute)}, observation: runObservation{process: followObservation{workerLiveness: "different dead wording", workerLivenessState: workerLivenessDead}}}}
	completedAt := now.Add(-time.Minute)
	completions := []statusRun{{run: scheduler.Run{Issue: 5, IssueTitle: "Completed work", Status: scheduler.StatusMerged, PullRequest: "https://example.test/5", StartedAt: now.Add(-5 * time.Minute), CompletedAt: &completedAt}}}

	var styled, plain strings.Builder
	renderDashboardSection(&styled, statusActive, "Active Runs", active, now, styler)
	renderDashboardSection(&styled, statusAttention, "Attention Required", attention, now, styler)
	renderDashboardCompletions(&styled, completions, now, styler)
	renderDashboardSection(&plain, statusActive, "Active Runs", active, now, dashboardStyler{})
	renderDashboardSection(&plain, statusAttention, "Attention Required", attention, now, dashboardStyler{})
	renderDashboardCompletions(&plain, completions, now, dashboardStyler{})
	got := styled.String()
	if stripped := ansi.Strip(got); stripped != plain.String() {
		t.Fatalf("styling changed semantic labels:\n%s", stripped)
	}
	for _, want := range []string{
		styler.active.Render("  #1  Active work"),
		styler.active.Render("    State: running"),
		styler.warning.Render("  #6  Degraded work"),
		styler.warning.Render("Worker liveness: wording that does not identify a warning"),
		styler.warning.Render("  #2  Waiting work"),
		styler.warning.Render("    State: waiting-for-merge"),
		styler.warning.Render("  #3  Suspended work"),
		styler.attention.Render("Attention Required (1)"),
		styler.attention.Render("    Diagnostic: Worker exited without a verified outcome"),
		styler.completion.Render("  #5  Completed work"),
		styler.metadata.Render(" | Elapsed: 1m0s | "),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("styled dashboard omitted semantic span %q:\n%q", ansi.Strip(want), got)
		}
	}
}

func TestDashboardPaletteAdaptsToBackgroundAndReducedProfiles(t *testing.T) {
	for _, profile := range []TerminalColorProfile{TerminalColorANSI, TerminalColorANSI256, TerminalColorTrueColor} {
		dark := newDashboardStyler(profile, true)
		light := newDashboardStyler(profile, false)
		if sameColor(dark.active.GetForeground(), light.active.GetForeground()) ||
			sameColor(dark.completion.GetForeground(), light.completion.GetForeground()) ||
			sameColor(dark.warning.GetForeground(), light.warning.GetForeground()) ||
			sameColor(dark.attention.GetForeground(), light.attention.GetForeground()) {
			t.Fatalf("profile %d did not adapt every semantic color to terminal background", profile)
		}
		for _, semantic := range []color.Color{dark.active.GetForeground(), dark.completion.GetForeground(), dark.warning.GetForeground(), dark.attention.GetForeground()} {
			if sameColor(dark.metadata.GetForeground(), semantic) {
				t.Fatalf("profile %d metadata foreground was not visually muted from semantic state", profile)
			}
		}
		got := dark.render(dashboardSemanticActive, "Active Runs (0)") + "\n" + dark.render(dashboardSemanticMetadata, "  none")
		if ansi.Strip(got) != "Active Runs (0)\n  none" || !strings.Contains(got, "\x1b[") {
			t.Fatalf("profile %d did not preserve readable text while styling: %q", profile, got)
		}
		if profile == TerminalColorANSI && (strings.Contains(got, "38;2;") || strings.Contains(got, "38;5;")) {
			t.Fatalf("ANSI profile used a higher color profile: %q", got)
		}
		if profile == TerminalColorANSI256 && strings.Contains(got, "38;2;") {
			t.Fatalf("ANSI256 profile used true color: %q", got)
		}
	}
}

func TestDashboardChromeUsesTypedStageStylesWithoutRemovingNavigation(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	for _, test := range []struct {
		stage dashboardStage
		line  string
		style lipgloss.Style
	}{
		{stage: dashboardRunning, line: "changed running presentation | changed navigation", style: styler.active},
		{stage: dashboardDraining, line: "changed draining presentation | changed navigation", style: styler.warning},
		{stage: dashboardDrainComplete, line: "changed completion presentation | changed navigation", style: styler.completion},
		{stage: dashboardForceStopping, line: "changed fatal presentation | changed navigation", style: styler.attention},
		{stage: dashboardDrainFailed, line: "changed failed drain presentation | changed navigation", style: styler.attention},
		{stage: dashboardSuspensionIncomplete, line: "changed incomplete suspension presentation | changed navigation", style: styler.attention},
	} {
		got := styler.render(dashboardStageSemantic(test.stage), test.line)
		if ansi.Strip(got) != test.line || got != test.style.Render(test.line) {
			t.Fatalf("styled chrome = %q, want labeled %q", got, test.line)
		}
	}
}

func TestDashboardOperationalMessagesUseTypedEventSemantics(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	for _, test := range []struct {
		event    runner.OperationalEvent
		semantic dashboardSemantic
		style    lipgloss.Style
	}{
		{event: runner.CandidateDiscoveryFailed{}, semantic: dashboardSemanticWarning, style: styler.warning},
		{event: runner.CandidateDiscoveryRecovered{}, semantic: dashboardSemanticActive, style: styler.active},
		{event: runner.ShutdownEvent{Stage: runner.ShutdownStageForceStopping, Message: "presentation wording changed"}, semantic: dashboardSemanticAttention, style: styler.attention},
		{event: runner.ShutdownEvent{Stage: runner.ShutdownStageDrainComplete, Result: runner.ShutdownResultSuccess, Message: "presentation wording changed"}, semantic: dashboardSemanticCompletion, style: styler.completion},
		{event: runner.ShutdownEvent{Stage: runner.ShutdownStageDrainComplete, Result: runner.ShutdownResultFailure, Message: "presentation wording changed"}, semantic: dashboardSemanticAttention, style: styler.attention},
		{event: runner.ShutdownEvent{Stage: runner.ShutdownStageSuspensionIncomplete, Message: "presentation wording changed"}, semantic: dashboardSemanticAttention, style: styler.attention},
	} {
		if got := dashboardOperationalEventSemantic(test.event); got != test.semantic {
			t.Fatalf("typed event semantic = %d, want %d", got, test.semantic)
		}
		line := "presentation wording changed"
		if got := styler.render(dashboardOperationalEventSemantic(test.event), line); got != test.style.Render(line) {
			t.Fatalf("typed event style = %q, want %q", got, test.style.Render(line))
		}
	}

	for _, test := range []struct {
		event runner.ShutdownEvent
		stage dashboardStage
	}{
		{event: runner.ShutdownEvent{Stage: runner.ShutdownStageDrainComplete, Result: runner.ShutdownResultFailure}, stage: dashboardDrainFailed},
		{event: runner.ShutdownEvent{Stage: runner.ShutdownStageSuspensionIncomplete}, stage: dashboardSuspensionIncomplete},
	} {
		if got, ok := dashboardStageForOperationalEvent(test.event); !ok || got != test.stage {
			t.Fatalf("fatal shutdown stage = %d, %t, want %d, true", got, ok, test.stage)
		}
	}
}

func TestDashboardTitleUsesStructuralEmphasisWithoutActiveWorkColor(t *testing.T) {
	title := "Backlog Run Dashboard"
	got := renderDashboardTitle(title)
	if ansi.Strip(got) != title {
		t.Fatalf("styled title = %q, want unchanged text", got)
	}
	if strings.Contains(got, "38;2;") {
		t.Fatalf("static title received semantic foreground styling: %q", got)
	}
	if want := lipgloss.NewStyle().Bold(true).Render(title); got != want {
		t.Fatalf("styled title = %q, want structural emphasis %q", got, want)
	}
}

func TestDashboardStylingDisablesColorsForNoColorProfile(t *testing.T) {
	styler := newDashboardStyler(TerminalColorNone, true)
	plainBody := "Active Runs (1)\n  #1  Active work\n    State: running | Elapsed: 1s | Worker liveness: alive"
	plainChrome := "Runner stage: Running | Next Ctrl-C: start Drain and stop Admission"
	if got := styler.render(dashboardSemanticActive, plainBody); got != plainBody || strings.Contains(got, "\x1b[") {
		t.Fatalf("colorless body changed: %q", got)
	}
	if got := styler.render(dashboardStageSemantic(dashboardRunning), plainChrome); got != plainChrome || strings.Contains(got, "\x1b[") {
		t.Fatalf("colorless navigation changed: %q", got)
	}
}

func TestBubbleDashboardUsesReadableFallbackUntilBackgroundResponse(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: time.Now, ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 12})
	if model.styler.enabled || strings.Contains(model.styler.render(dashboardSemanticActive, "Active"), "\x1b[") {
		t.Fatal("dashboard applied a background-specific palette before the terminal background response")
	}

	updated, _ := model.Update(tea.BackgroundColorMsg{Color: color.White})
	model = updated.(bubbleDashboardModel)
	light := newDashboardStyler(TerminalColorTrueColor, false)
	if !model.styler.enabled || !sameColor(light.active.GetForeground(), model.styler.active.GetForeground()) {
		t.Fatal("light terminal background response did not apply the light palette")
	}
	updated, _ = model.Update(tea.BackgroundColorMsg{Color: color.Black})
	model = updated.(bubbleDashboardModel)
	dark := newDashboardStyler(TerminalColorTrueColor, true)
	if !sameColor(dark.active.GetForeground(), model.styler.active.GetForeground()) {
		t.Fatal("dark terminal background response did not apply the dark palette")
	}
}

func TestEnvironmentColorProfileHonorsNoColorAndReducedTerminals(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLORTERM", "truecolor")
	if got := environmentColorProfile(); got != TerminalColorNone {
		t.Fatalf("NO_COLOR profile = %d, want none", got)
	}

	t.Setenv("NO_COLOR", "")
	t.Setenv("COLORTERM", "")
	if got := environmentColorProfile(); got != TerminalColorANSI256 {
		t.Fatalf("reduced TERM profile = %d, want ANSI256", got)
	}
}

func sameColor(left, right color.Color) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftR, leftG, leftB, leftA := left.RGBA()
	rightR, rightG, rightB, rightA := right.RGBA()
	return leftR == rightR && leftG == rightG && leftB == rightB && leftA == rightA
}
