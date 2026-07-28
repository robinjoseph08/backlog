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
	"github.com/robinjoseph08/backlog/internal/state"
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
	outcomes := []statusRun{{run: scheduler.Run{Issue: 7, IssueTitle: "Unacknowledged failure", Status: scheduler.StatusFailed, StartedAt: now.Add(-6 * time.Minute)}, observation: runObservation{process: followObservation{workerLiveness: "absent", workerLivenessState: workerLivenessAbsent}}}}
	completedAt := now.Add(-time.Minute)
	completions := []statusRun{{run: scheduler.Run{Issue: 5, IssueTitle: "Completed work", Status: scheduler.StatusMerged, PullRequest: "https://example.test/5", StartedAt: now.Add(-5 * time.Minute), CompletedAt: &completedAt}}}

	var styled, plain strings.Builder
	renderDashboardSection(&styled, statusActive, "Active Runs", active, now, styler)
	renderDashboardSection(&styled, statusAttention, "Attention Required", attention, now, styler)
	renderDashboardSection(&styled, statusOutcomes, "Outcomes to Acknowledge", outcomes, now, styler)
	renderDashboardCompletions(&styled, completions, now, styler)
	renderDashboardSection(&plain, statusActive, "Active Runs", active, now, dashboardStyler{})
	renderDashboardSection(&plain, statusAttention, "Attention Required", attention, now, dashboardStyler{})
	renderDashboardSection(&plain, statusOutcomes, "Outcomes to Acknowledge", outcomes, now, dashboardStyler{})
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
		styler.attention.Render("  #4  Fatal work"),
		styler.attention.Render("    State: needs-human"),
		styler.attention.Render("    Diagnostic: Worker exited without a verified outcome"),
		styler.attention.Render("Outcomes to Acknowledge (1)"),
		styler.attention.Render("  #7  Unacknowledged failure"),
		styler.attention.Render("    State: failed"),
		styler.completion.Render("  #5  Completed work"),
		styler.metadata.Render(" | Elapsed: 1m0s | "),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("styled dashboard omitted semantic span %q:\n%q", ansi.Strip(want), got)
		}
	}
}

func TestDashboardAdmissionStylingUsesHealthSemanticsWithoutChangingLabels(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	degraded := dashboardAdmission{
		degraded: true, consecutiveFailures: 2,
		firstFailure: now.Add(-time.Minute), latestFailure: now, retryAt: now.Add(30 * time.Second),
		operation: runner.CandidateDiscoveryList, cause: "connection refused",
		equivalentFailures: map[string]int{string(runner.CandidateDiscoveryList) + "\x00connection refused": 2},
	}

	var styled, plain strings.Builder
	renderAdmissionHealth(&styled, degraded, false, dashboardRunning, now, styler)
	renderAdmissionHealth(&plain, degraded, false, dashboardRunning, now, dashboardStyler{})
	if stripped := ansi.Strip(styled.String()); stripped != plain.String() {
		t.Fatalf("Admission styling changed semantic labels:\n%s", stripped)
	}
	for _, want := range []string{
		styler.warning.Render("Admission health"),
		styler.warning.Render("  Admission: DEGRADED | 2 consecutive failures"),
		styler.warning.Render("    Cause: connection refused | Equivalent failures: 2"),
		styler.metadata.Render("    First failure: 2026-07-28T00:59:00Z | Latest failure: 2026-07-28T01:00:00Z | Next retry: 30s"),
		styler.metadata.Render("  Diagnostics: closed (d to open; 0 recent)"),
	} {
		if !strings.Contains(styled.String(), want) {
			t.Fatalf("styled degraded Admission omitted semantic span %q: %q", ansi.Strip(want), styled.String())
		}
	}

	var checking strings.Builder
	renderAdmissionHealth(&checking, dashboardAdmission{}, false, dashboardRunning, now, styler)
	for _, want := range []string{styler.metadata.Render("Admission health"), styler.metadata.Render("  Admission: checking | Candidate snapshot not yet complete")} {
		if !strings.Contains(checking.String(), want) {
			t.Fatalf("styled checking Admission omitted metadata span %q: %q", ansi.Strip(want), checking.String())
		}
	}
	if strings.Contains(ansi.Strip(checking.String()), "Admission: healthy") {
		t.Fatalf("styled checking Admission claimed unverified health: %q", checking.String())
	}

	var healthy strings.Builder
	renderAdmissionHealth(&healthy, dashboardAdmission{snapshotComplete: true}, false, dashboardRunning, now, styler)
	for _, want := range []string{styler.active.Render("Admission health"), styler.active.Render("  Admission: healthy")} {
		if !strings.Contains(healthy.String(), want) {
			t.Fatalf("styled healthy Admission omitted active span %q: %q", ansi.Strip(want), healthy.String())
		}
	}
}

func TestDashboardPaletteAdaptsToBackgroundAndReducedProfiles(t *testing.T) {
	tests := []struct {
		name       string
		profile    TerminalColorProfile
		dark       bool
		active     color.Color
		completion color.Color
		warning    color.Color
		attention  color.Color
		metadata   color.Color
	}{
		{name: "ANSI dark", profile: TerminalColorANSI, dark: true, active: lipgloss.BrightCyan, completion: lipgloss.BrightGreen, warning: lipgloss.BrightYellow, attention: lipgloss.BrightRed, metadata: lipgloss.BrightBlack},
		{name: "ANSI light", profile: TerminalColorANSI, active: lipgloss.Blue, completion: lipgloss.Green, warning: lipgloss.Yellow, attention: lipgloss.Red, metadata: lipgloss.BrightBlack},
		{name: "ANSI256 dark", profile: TerminalColorANSI256, dark: true, active: lipgloss.ANSIColor(81), completion: lipgloss.ANSIColor(78), warning: lipgloss.ANSIColor(221), attention: lipgloss.ANSIColor(203), metadata: lipgloss.ANSIColor(245)},
		{name: "ANSI256 light", profile: TerminalColorANSI256, active: lipgloss.ANSIColor(25), completion: lipgloss.ANSIColor(28), warning: lipgloss.ANSIColor(94), attention: lipgloss.ANSIColor(124), metadata: lipgloss.ANSIColor(242)},
		{name: "true color dark", profile: TerminalColorTrueColor, dark: true, active: lipgloss.Color("#5FD7FF"), completion: lipgloss.Color("#5FD787"), warning: lipgloss.Color("#FFD75F"), attention: lipgloss.Color("#FF6B6B"), metadata: lipgloss.Color("#A0A0A0")},
		{name: "true color light", profile: TerminalColorTrueColor, active: lipgloss.Color("#005FAF"), completion: lipgloss.Color("#1A7F37"), warning: lipgloss.Color("#8A4B00"), attention: lipgloss.Color("#B42318"), metadata: lipgloss.Color("#5F6368")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			styler := newDashboardStyler(test.profile, test.dark)
			for semantic, colors := range map[string][2]color.Color{
				"active cyan or blue": {styler.active.GetForeground(), test.active},
				"completion green":    {styler.completion.GetForeground(), test.completion},
				"warning amber":       {styler.warning.GetForeground(), test.warning},
				"attention red":       {styler.attention.GetForeground(), test.attention},
				"metadata gray":       {styler.metadata.GetForeground(), test.metadata},
			} {
				if !sameColor(colors[0], colors[1]) {
					t.Fatalf("%s foreground = %v, want required hue family color %v", semantic, colors[0], colors[1])
				}
			}
			got := styler.render(dashboardSemanticActive, "Active Runs (0)") + "\n" + styler.render(dashboardSemanticMetadata, "  none")
			if ansi.Strip(got) != "Active Runs (0)\n  none" || !strings.Contains(got, "\x1b[") {
				t.Fatalf("palette did not preserve readable text while styling: %q", got)
			}
			if test.profile == TerminalColorANSI && (strings.Contains(got, "38;2;") || strings.Contains(got, "38;5;")) {
				t.Fatalf("ANSI profile used a higher color profile: %q", got)
			}
			if test.profile == TerminalColorANSI256 && strings.Contains(got, "38;2;") {
				t.Fatalf("ANSI256 profile used true color: %q", got)
			}
		})
	}

	for _, profile := range []TerminalColorProfile{TerminalColorANSI, TerminalColorANSI256, TerminalColorTrueColor} {
		dark := newDashboardStyler(profile, true)
		light := newDashboardStyler(profile, false)
		if sameColor(dark.active.GetForeground(), light.active.GetForeground()) ||
			sameColor(dark.completion.GetForeground(), light.completion.GetForeground()) ||
			sameColor(dark.warning.GetForeground(), light.warning.GetForeground()) ||
			sameColor(dark.attention.GetForeground(), light.attention.GetForeground()) {
			t.Fatalf("profile %d did not adapt every semantic color to terminal background", profile)
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

func TestDashboardConstrainedChromeKeepsMetadataMuted(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	header := []string{
		"Repository: acme/widgets",
		"Worker capacity: 1 used | 2 available | 3 total",
	}
	footer := []string{
		"Runner stage: Draining",
		"Next Ctrl-C: request suspension",
	}
	for _, test := range []struct {
		name          string
		width         int
		metadata      string
		capacity      string
		lifecycle     string
		nextInterrupt string
	}{
		{name: "combined", width: 200, metadata: header[0], capacity: header[1], lifecycle: footer[0], nextInterrupt: footer[1]},
		{name: "compact", width: 80, metadata: "R:acme/widgets", capacity: "W:1u/2a/3t", lifecycle: "S:Draining", nextInterrupt: "^C:request suspension"},
	} {
		t.Run(test.name, func(t *testing.T) {
			chrome := dashboardChromeLines(header, footer, 0, dashboardDraining, styler, test.width, 1)
			got := strings.Join(append(append([]string(nil), chrome.top...), chrome.bottom...), "\n")
			for _, want := range []string{
				styler.metadata.Render(test.metadata),
				styler.metadata.Render(test.capacity),
				styler.warning.Render(test.lifecycle),
				styler.warning.Render(test.nextInterrupt),
			} {
				if !strings.Contains(got, want) {
					t.Fatalf("constrained chrome omitted semantic span %q: %q", ansi.Strip(want), got)
				}
			}
		})
	}
}

func TestBubbleDashboardStyledConstrainedChromeKeepsLifecycleAndNavigation(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 3}
	for _, dimensions := range []TerminalDimensions{
		{Width: 18, Height: 12},
		{Width: 120, Height: 2},
		{Width: 200, Height: 1},
	} {
		model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
			Now: time.Now, ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
		}}, newBubbleDashboardSession(time.Now), dimensions)
		model.styler = styler
		model.dashboard.update(current)
		updated, _ := model.Update(dashboardOperationalMsg{event: runner.ShutdownEvent{Stage: runner.ShutdownStageDraining}})
		model = updated.(bubbleDashboardModel)

		frame := model.dashboardFrame()
		rendered := strings.Join(append(append([]string(nil), frame.chrome.top...), frame.chrome.bottom...), "\n")
		plain := ansi.Strip(rendered)
		for name, variants := range map[string][]string{
			"Runner stage": {"Runner stage:", "S:"},
			"next Ctrl-C":  {"Next Ctrl-C:", "^C:"},
			"navigation":   {"Nav:", "N:"},
		} {
			if !containsAny(plain, variants...) {
				t.Fatalf("%dx%d styled chrome omitted %s:\n%s", dimensions.Width, dimensions.Height, name, plain)
			}
		}
		warningPrefix := dashboardStylePrefix(t, styler.warning)
		if !containsAny(rendered, warningPrefix+"Runner stage:", warningPrefix+"S:", warningPrefix+"Draining") {
			t.Fatalf("%dx%d lifecycle chrome lost warning styling: %q", dimensions.Width, dimensions.Height, rendered)
		}
		metadataPrefix := dashboardStylePrefix(t, styler.metadata)
		if !containsAny(rendered, metadataPrefix+"Nav:", metadataPrefix+"N:") {
			t.Fatalf("%dx%d navigation chrome lost metadata styling: %q", dimensions.Width, dimensions.Height, rendered)
		}
		lines := strings.Split(plain, "\n")
		if len(lines) > dimensions.Height {
			t.Fatalf("%dx%d styled chrome used %d lines:\n%s", dimensions.Width, dimensions.Height, len(lines), plain)
		}
		for _, line := range lines {
			if width := lipgloss.Width(line); width > dimensions.Width {
				t.Fatalf("%dx%d styled chrome line width = %d: %q", dimensions.Width, dimensions.Height, width, line)
			}
		}
	}
}

func TestDashboardOffscreenAttentionIndicatorUsesAttentionStyle(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	for _, test := range []struct {
		name    string
		header  string
		compact bool
		notice  string
	}{
		{name: "full", header: "Repository: acme/widgets", notice: "NEW ATTENTION (2): press a"},
		{name: "compact", header: "R:acme/widgets", compact: true, notice: "! (2): press a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := styledDashboardHeaderItems([]string{test.header}, 2, test.compact, styler)[0]
			want := styler.metadata.Render(test.header) + styler.metadata.Render(" | ") + styler.attention.Render(test.notice)
			if got != want {
				t.Fatalf("offscreen Attention header = %q, want metadata and Attention spans %q", got, want)
			}
		})
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
		{event: runner.CandidateSnapshotCompleted{}, semantic: dashboardSemanticActive, style: styler.active},
		{event: runner.CandidateDiscoveryRecovered{}, semantic: dashboardSemanticActive, style: styler.active},
		{event: runner.RunLifecycleEvent{Stage: runner.RunLifecycleClaimed}, semantic: dashboardSemanticActive, style: styler.active},
		{event: runner.RunLifecycleEvent{Stage: runner.RunLifecycleStarted}, semantic: dashboardSemanticActive, style: styler.active},
		{event: runner.RunLifecycleEvent{Stage: runner.RunLifecycleResumed}, semantic: dashboardSemanticActive, style: styler.active},
		{event: runner.RunLifecycleEvent{Stage: runner.RunLifecycleCleanupCompleted}, semantic: dashboardSemanticCompletion, style: styler.completion},
		{event: runner.RunLifecycleEvent{Stage: runner.RunLifecycleMerged}, semantic: dashboardSemanticCompletion, style: styler.completion},
		{event: runner.RunLifecycleEvent{Stage: runner.RunLifecycleFailed}, semantic: dashboardSemanticAttention, style: styler.attention},
		{event: runner.RunLifecycleEvent{Stage: runner.RunLifecycleNeedsHuman}, semantic: dashboardSemanticAttention, style: styler.attention},
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
	got := renderDashboardTitle(title, newDashboardFallbackStyler(TerminalColorTrueColor))
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

func TestBubbleDashboardUsesSemanticFallbackUntilBackgroundResponse(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: time.Now, ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 12})
	fallback := newDashboardFallbackStyler(TerminalColorTrueColor)
	for semantic, want := range map[dashboardSemantic]string{
		dashboardSemanticActive:     fallback.active.Render("status"),
		dashboardSemanticCompletion: fallback.completion.Render("status"),
		dashboardSemanticWarning:    fallback.warning.Render("status"),
		dashboardSemanticAttention:  fallback.attention.Render("status"),
		dashboardSemanticMetadata:   fallback.metadata.Render("status"),
	} {
		got := model.styler.render(semantic, "status")
		if !model.styler.enabled || got != want || !strings.Contains(got, "\x1b[") || strings.Contains(got, "38;") {
			t.Fatalf("semantic %d fallback = %q, want attribute-only styling %q", semantic, got, want)
		}
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

func TestBubbleDashboardBackgroundAdaptationPreservesNavigationPosition(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 7)
	source := &dashboardTestSource{current: current}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now }, ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 64, Height: 14})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: source})
	model = updated.(bubbleDashboardModel)

	selected := dashboardRunAnchor("run-4")
	line, exists := model.anchorVisualLine(selected)
	if !exists {
		t.Fatalf("selected Run anchor %q missing", selected)
	}
	model.viewport.SetYOffset(line + 1)
	model.selectViewportAnchor()
	if model.selectedAnchor != selected {
		t.Fatalf("selected anchor = %q, want %q", model.selectedAnchor, selected)
	}
	beforeSelection := model.currentSelection()
	beforeOffset := model.viewport.YOffset()
	beforeScreenLine := dashboardVisibleLine(t, model.View().Content, "State: claimed")

	updated, _ = model.Update(tea.BackgroundColorMsg{Color: color.White})
	model = updated.(bubbleDashboardModel)
	if selection := model.currentSelection(); selection != beforeSelection {
		t.Fatalf("background adaptation changed selection from %#v to %#v", beforeSelection, selection)
	}
	if offset := model.viewport.YOffset(); offset != beforeOffset {
		t.Fatalf("background adaptation changed viewport offset from %d to %d", beforeOffset, offset)
	}
	if screenLine := dashboardVisibleLine(t, model.View().Content, "State: claimed"); screenLine != beforeScreenLine {
		t.Fatalf("background adaptation moved selected Run from screen line %d to %d", beforeScreenLine, screenLine)
	}
	if rendered := model.View().Content; !strings.Contains(rendered, "\x1b[") {
		t.Fatalf("background response did not apply ANSI styling: %q", rendered)
	}

	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'j', Text: "j"}))
	model = updated.(bubbleDashboardModel)
	if offset := model.viewport.YOffset(); offset != beforeOffset+1 {
		t.Fatalf("styled navigation offset = %d, want %d", offset, beforeOffset+1)
	}
}

func TestBubbleDashboardNoColorProfilePreservesLabelsAndNavigationWithoutANSI(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLORTERM", "truecolor")
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 8)
	source := &dashboardTestSource{current: current}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now }, ColorProfile: environmentColorProfile,
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 64, Height: 14})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: source})
	model = updated.(bubbleDashboardModel)

	if model.colorProfile != TerminalColorNone || model.styler.enabled {
		t.Fatalf("NO_COLOR model profile = %d with styler enabled = %t", model.colorProfile, model.styler.enabled)
	}
	assertPlainModel := func(name string) {
		t.Helper()
		rendered := model.View().Content
		if strings.Contains(rendered, "\x1b[") {
			t.Fatalf("%s NO_COLOR model emitted ANSI: %q", name, rendered)
		}
		for _, want := range []string{"Backlog Run Dashboard", "#1  Navigation Run 0", "State: claimed", "Runner stage: Running", "Next Ctrl-C:", "Nav:"} {
			if !strings.Contains(rendered, want) {
				t.Fatalf("%s NO_COLOR model omitted %q:\n%s", name, want, rendered)
			}
		}
	}
	assertPlainModel("initial")
	beforeOffset := model.viewport.YOffset()
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'j', Text: "j"}))
	model = updated.(bubbleDashboardModel)
	if offset := model.viewport.YOffset(); offset != beforeOffset+1 {
		t.Fatalf("NO_COLOR keyboard navigation offset = %d, want %d", offset, beforeOffset+1)
	}
	assertPlainModel("after navigation")
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

func dashboardStylePrefix(t *testing.T, style lipgloss.Style) string {
	t.Helper()
	const marker = "dashboard-style-marker"
	rendered := style.Render(marker)
	index := strings.Index(rendered, marker)
	if index <= 0 {
		t.Fatalf("style did not emit an ANSI prefix: %q", rendered)
	}
	return rendered[:index]
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}

func sameColor(left, right color.Color) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftR, leftG, leftB, leftA := left.RGBA()
	rightR, rightG, rightB, rightA := right.RGBA()
	return leftR == rightR && leftG == rightG && leftB == rightB && leftA == rightA
}
