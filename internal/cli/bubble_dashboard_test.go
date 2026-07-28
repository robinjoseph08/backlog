package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestBubbleDashboardModelResizesViewportAroundFixedLifecycleChrome(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	run := scheduler.Run{Issue: 66, IssueTitle: "Bubble Tea viewport", RunID: "run-66", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Minute)}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	source := &dashboardTestSource{current: current}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(func() time.Time { return now }), TerminalDimensions{Width: 48, Height: 10})
	model.dashboard.source = source
	model.dashboard.update(current)
	for index := range 20 {
		model.dashboard.recordMessage("operational event " + strings.Repeat("x", index))
	}

	assertBubbleDashboardFits(t, model, 48, 10, 4)
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 36, Height: 8})
	model = updated.(bubbleDashboardModel)
	assertBubbleDashboardFits(t, model, 36, 8, 1)
}

func assertBubbleDashboardFits(t *testing.T, model bubbleDashboardModel, width, height, viewportHeight int) {
	t.Helper()
	model.refreshViewport(model.currentSelection())
	frame := model.dashboardFrame()
	view := model.View()
	if !view.AltScreen {
		t.Fatal("dashboard view did not request the alternate screen")
	}
	plain := ansi.Strip(view.Content)
	lines := strings.Split(plain, "\n")
	if len(lines) > height {
		t.Fatalf("rendered height = %d, want at most %d:\n%s", len(lines), height, plain)
	}
	for _, line := range lines {
		if got := lipgloss.Width(line); got > width {
			t.Fatalf("rendered line width = %d, want at most %d: %q", got, width, line)
		}
	}
	for _, want := range []string{"Repository: acme/widgets", "Worker capacity:", "Runner stage: Running", "Next Ctrl-C: start Drain"} {
		if !strings.Contains(plain, want) {
			t.Fatalf("fixed dashboard chrome omitted %q after resize:\n%s", want, plain)
		}
	}
	if frame.bodyHeight != viewportHeight || model.viewport.Height() != frame.bodyHeight || model.viewport.Width() != width {
		t.Fatalf("frame body and viewport size = %d and %dx%d, want %d and %dx%d", frame.bodyHeight, model.viewport.Width(), model.viewport.Height(), viewportHeight, width, viewportHeight)
	}
	if frame.bodyHeight > 0 {
		body := strings.Split(ansi.Strip(model.viewport.View()), "\n")
		if frame.bodyStart >= len(lines) || strings.TrimSpace(lines[frame.bodyStart]) != strings.TrimSpace(body[0]) {
			t.Fatalf("rendered body does not start at derived line %d:\n%s", frame.bodyStart, plain)
		}
	}
}

func TestBubbleDashboardInitialFrameShowsCapacityPendingUntilConfigured(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 12})
	initial := ansi.Strip(model.View().Content)
	if !strings.Contains(initial, "Worker capacity: pending configuration") || strings.Contains(initial, "Worker capacity: 0 used | 0 available | 0 total") {
		t.Fatalf("initial capacity was not pending configuration:\n%s", initial)
	}

	configured := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 3}
	updated, _ := model.Update(dashboardConfiguredMsg{initial: configured, source: &dashboardTestSource{current: configured}})
	configuredView := ansi.Strip(updated.(bubbleDashboardModel).View().Content)
	if !strings.Contains(configuredView, "Worker capacity: 0 used | 3 available | 3 total") || strings.Contains(configuredView, "pending configuration") {
		t.Fatalf("configured capacity was not rendered:\n%s", configuredView)
	}
}

func TestBubbleDashboardElapsedTickAdvancesAndReschedulesWithoutExternalUpdates(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	clock := now
	run := scheduler.Run{Issue: 66, RunID: "run-66", Status: scheduler.StatusRunning, StartedAt: now.Add(-5 * time.Second)}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return clock }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 16})
	model.dashboard.source = &dashboardTestSource{current: current}
	model.dashboard.update(current)
	if initial := ansi.Strip(model.View().Content); !strings.Contains(initial, "Elapsed: 5s") {
		t.Fatalf("initial Bubble Tea elapsed value missing:\n%s", initial)
	}

	clock = now.Add(time.Second)
	updated, command := model.Update(dashboardElapsedMsg(clock))
	model = updated.(bubbleDashboardModel)
	if advanced := ansi.Strip(model.View().Content); !strings.Contains(advanced, "Elapsed: 6s") {
		t.Fatalf("elapsed tick did not advance the Bubble Tea value:\n%s", advanced)
	}
	if command == nil {
		t.Fatal("elapsed tick did not schedule its successor")
	}
	if _, ok := command().(dashboardElapsedMsg); !ok {
		t.Fatal("elapsed tick successor did not emit dashboardElapsedMsg")
	}
}

func TestBubbleDashboardTogglesAdmissionDiagnosticsWithDWithoutScrolling(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 100, Height: 14})
	for failure := 1; failure <= 8; failure++ {
		model.dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList, Err: fmt.Errorf("gh issue list --repo acme/widgets: connection refused %d", failure), Cause: "connection refused",
			OccurredAt: now, RetryAt: now.Add(30 * time.Second), ConsecutiveFailures: failure,
		})
	}
	if view := ansi.Strip(model.View().Content); strings.Contains(view, "gh issue list") {
		t.Fatalf("closed Diagnostics exposed full command:\n%s", view)
	}
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
	model = updated.(bubbleDashboardModel)
	if view := ansi.Strip(model.View().Content); !strings.Contains(view, "Diagnostics (8 recent Candidate discovery failures; d to close)") || !strings.Contains(model.viewport.GetContent(), "gh issue list") {
		t.Fatalf("d did not open Diagnostics:\n%s", view)
	}
	if !model.viewport.AtTop() {
		t.Fatalf("d also scrolled the viewport to offset %d", model.viewport.YOffset())
	}
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
	model = updated.(bubbleDashboardModel)
	if view := ansi.Strip(model.View().Content); strings.Contains(view, "gh issue list") {
		t.Fatalf("second d did not close Diagnostics:\n%s", view)
	}
}

func TestBubbleDashboardViewportSupportsDocumentedKeyboardNavigation(t *testing.T) {
	newModel := func() bubbleDashboardModel {
		model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 60, Height: 12})
		for index := range 30 {
			model.dashboard.recordMessage(fmt.Sprintf("event line %02d", index))
		}
		updated, _ := model.Update(dashboardElapsedMsg(time.Now()))
		return updated.(bubbleDashboardModel)
	}
	press := func(model bubbleDashboardModel, key tea.Key) bubbleDashboardModel {
		updated, _ := model.Update(tea.KeyPressMsg(key))
		return updated.(bubbleDashboardModel)
	}

	for _, test := range []struct {
		name      string
		key       tea.Key
		startPage bool
		wantPage  bool
		wantTop   bool
		wantDelta int
	}{
		{name: "down arrow", key: tea.Key{Code: tea.KeyDown}, wantDelta: 1},
		{name: "j", key: tea.Key{Code: 'j', Text: "j"}, wantDelta: 1},
		{name: "page down", key: tea.Key{Code: tea.KeyPgDown}, wantPage: true},
		{name: "f", key: tea.Key{Code: 'f', Text: "f"}, wantPage: true},
		{name: "up arrow", key: tea.Key{Code: tea.KeyUp}, startPage: true, wantDelta: -1},
		{name: "k", key: tea.Key{Code: 'k', Text: "k"}, startPage: true, wantDelta: -1},
		{name: "page up", key: tea.Key{Code: tea.KeyPgUp}, startPage: true, wantTop: true},
		{name: "b", key: tea.Key{Code: 'b', Text: "b"}, startPage: true, wantTop: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := newModel()
			page := model.viewport.Height()
			start := 0
			if test.startPage {
				start = page
				model.viewport.SetYOffset(start)
			}
			want := start + test.wantDelta
			if test.wantPage {
				want += page
			}
			if test.wantTop {
				want = 0
			}
			model = press(model, test.key)
			if got := model.viewport.YOffset(); got != want {
				t.Fatalf("%s offset = %d, want %d (page height %d)", test.name, got, want, page)
			}
		})
	}
	for _, test := range []struct {
		name   string
		bottom tea.Key
		top    tea.Key
	}{
		{name: "Home and End", bottom: tea.Key{Code: tea.KeyEnd}, top: tea.Key{Code: tea.KeyHome}},
		{name: "g and G", bottom: tea.Key{Code: 'G', Text: "G"}, top: tea.Key{Code: 'g', Text: "g"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := press(newModel(), test.bottom)
			if !model.viewport.AtBottom() {
				t.Fatalf("%s bottom key stopped at offset %d", test.name, model.viewport.YOffset())
			}
			model = press(model, test.top)
			if !model.viewport.AtTop() {
				t.Fatalf("%s top key stopped at offset %d", test.name, model.viewport.YOffset())
			}
		})
	}
}

func TestBubbleDashboardMouseWheelScrollsWithoutClickHandling(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 60, Height: 12})
	for index := range 30 {
		model.dashboard.recordMessage(fmt.Sprintf("event line %02d", index))
	}
	updated, _ := model.Update(dashboardElapsedMsg(time.Now()))
	model = updated.(bubbleDashboardModel)
	updated, _ = model.Update(tea.MouseWheelMsg{Button: tea.MouseWheelDown})
	model = updated.(bubbleDashboardModel)
	if got := model.viewport.YOffset(); got != model.viewport.MouseWheelDelta {
		t.Fatalf("mouse wheel down offset = %d, want %d", got, model.viewport.MouseWheelDelta)
	}
	updated, _ = model.Update(tea.MouseWheelMsg{Button: tea.MouseWheelUp})
	model = updated.(bubbleDashboardModel)
	if !model.viewport.AtTop() {
		t.Fatalf("mouse wheel up did not return viewport to top; offset = %d", model.viewport.YOffset())
	}
	updated, _ = model.Update(tea.MouseWheelMsg{Button: tea.MouseWheelDown})
	model = updated.(bubbleDashboardModel)
	offset := model.viewport.YOffset()
	updated, command := model.Update(tea.MouseClickMsg{Button: tea.MouseLeft, X: 1, Y: 1})
	model = updated.(bubbleDashboardModel)
	if command != nil || model.viewport.YOffset() != offset {
		t.Fatalf("application handled mouse click: command nil = %t, offset = %d, want %d", command == nil, model.viewport.YOffset(), offset)
	}
	if model.View().MouseMode != tea.MouseModeCellMotion {
		t.Fatal("dashboard did not request terminal mouse wheel events")
	}
}

func TestBubbleDashboardPreservesSelectedRunAcrossLiveProjectionChanges(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 7)
	logPath := t.TempDir() + "/selected.jsonl"
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	current.Runs[4].LogPath = logPath
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-time.Second), Kind: "turn",
		Description: "Worker turn completed", TurnDelta: 1,
	})
	source := &dashboardTestSource{current: current}
	model := configuredNavigationTestModel(t, now, current, source)
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
	wantRelative := model.currentSelection().relative
	if wantRelative == model.dashboardBodyStart() {
		t.Fatal("selected Run test did not start within the Run body")
	}
	wantScreenLine := dashboardVisibleLine(t, model.View().Content, "State: claimed")

	assertStable := func(name string, msg tea.Msg) {
		t.Helper()
		updated, _ := model.Update(msg)
		model = updated.(bubbleDashboardModel)
		selection := model.currentSelection()
		if !selection.valid || selection.identity != selected || selection.relative != wantRelative {
			t.Fatalf("%s selection = %#v, want identity %q at relative line %d", name, selection, selected, wantRelative)
		}
		if got := dashboardVisibleLine(t, model.View().Content, "State: claimed"); got != wantScreenLine {
			t.Fatalf("%s moved selected Run body from screen line %d to %d", name, wantScreenLine, got)
		}
	}
	assertStable("elapsed tick", dashboardElapsedMsg(now.Add(time.Second)))
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now, Kind: "tool", Description: "Tool edit started",
		Operation: "edit", OperationChanged: true,
	})
	assertStable("Activity refresh", dashboardActivityMsg(now.Add(time.Second)))
	if body := model.viewport.GetContent(); !strings.Contains(body, "Deepest operation: edit") {
		t.Fatalf("Activity refresh did not change selected Run content:\n%s", body)
	}
	assertStable("unchanged state save", dashboardStateMsg(current))

	older := scheduler.Run{Issue: 99, IssueTitle: "Earlier new Run", RunID: "run-new", Status: scheduler.StatusClaimed, StartedAt: now.Add(-2 * time.Hour)}
	current.Runs = append(current.Runs, older)
	current.Leases = append(current.Leases, scheduler.Lease{LeaseID: older.RunID, Issue: older.Issue, RunID: older.RunID})
	source.current = current
	assertStable("new Run", dashboardStateMsg(current))

	completedAt := now
	current.Runs[0].Status = scheduler.StatusMerged
	current.Runs[0].CompletedAt = &completedAt
	current.Runs[0].UpdatedAt = completedAt
	current.Leases = current.Leases[1:]
	source.current = current
	assertStable("Completion", dashboardStateMsg(current))
}

func TestBubbleDashboardPreservesSelectedRunAcrossAdmissionChanges(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 7)
	model := configuredNavigationTestModel(t, now, current, &dashboardTestSource{current: current})
	selected := dashboardRunAnchor("run-4")
	line, exists := model.anchorVisualLine(selected)
	if !exists {
		t.Fatalf("selected Run anchor %q missing", selected)
	}
	model.viewport.SetYOffset(line + 1)
	model.selectViewportAnchor()
	wantSelection := model.currentSelection()
	if !wantSelection.valid || wantSelection.identity != selected {
		t.Fatalf("selected anchor = %#v, want %q", wantSelection, selected)
	}

	assertStable := func(name string, msg tea.Msg, bodyText string) {
		t.Helper()
		updated, _ := model.Update(msg)
		model = updated.(bubbleDashboardModel)
		if selection := model.currentSelection(); selection != wantSelection {
			t.Fatalf("%s selection = %#v, want %#v", name, selection, wantSelection)
		}
		if body := model.viewport.GetContent(); !strings.Contains(body, bodyText) {
			t.Fatalf("%s did not update Admission content with %q:\n%s", name, bodyText, body)
		}
	}

	failure := runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList,
		Err:       errors.New("gh issue list: connection refused"), Cause: "connection refused",
		FirstFailureAt: now, OccurredAt: now, RetryAt: now.Add(30 * time.Second), ConsecutiveFailures: 1,
	}
	assertStable("failure", dashboardOperationalMsg{event: failure}, "Admission: DEGRADED")
	assertStable("Diagnostics toggle", tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}), "Full error/command: candidate discovery failed")
	assertStable("recovery", dashboardOperationalMsg{event: runner.CandidateDiscoveryRecovered{OccurredAt: now.Add(time.Second), Failures: 1}}, "Admission: healthy | Recovered")
}

func TestBubbleDashboardPreservesDownstreamSelectionAcrossStyledDiagnosticsChanges(t *testing.T) {
	profiles := []struct {
		name    string
		profile TerminalColorProfile
	}{
		{name: "no color", profile: TerminalColorNone},
		{name: "ANSI", profile: TerminalColorANSI},
		{name: "ANSI 256", profile: TerminalColorANSI256},
		{name: "true color", profile: TerminalColorTrueColor},
	}
	for _, test := range profiles {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
			current := navigationTestState(now, 10)
			model := configuredNavigationTestModel(t, now, current, &dashboardTestSource{current: current})
			model.styler = newDashboardStyler(test.profile, true)
			for failure := 1; failure <= dashboardDiagnosticLimit; failure++ {
				updated, _ := model.Update(dashboardOperationalMsg{event: runner.CandidateDiscoveryFailed{
					Operation: runner.CandidateDiscoveryList,
					Err:       fmt.Errorf("gh issue list: long diagnostic %02d %s", failure, strings.Repeat("wrapped evidence ", 8)),
					Cause:     "connection refused", FirstFailureAt: now,
					OccurredAt: now.Add(time.Duration(failure) * time.Second), RetryAt: now.Add(time.Minute),
					ConsecutiveFailures: failure, Occurrences: 1,
				}})
				model = updated.(bubbleDashboardModel)
			}
			updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
			model = updated.(bubbleDashboardModel)

			selected := dashboardRunAnchor("run-5")
			line, exists := model.anchorVisualLine(selected)
			if !exists {
				t.Fatalf("selected downstream Run anchor %q missing", selected)
			}
			model.viewport.SetYOffset(line)
			model.selectViewportAnchor()
			wantSelection := model.currentSelection()
			if !wantSelection.valid || wantSelection.identity != selected {
				t.Fatalf("selected anchor = %#v, want %q", wantSelection, selected)
			}

			assertStable := func(name string, msg tea.Msg) {
				t.Helper()
				beforeLine := dashboardVisibleLine(t, model.View().Content, "Navigation Run 5")
				updated, _ := model.Update(msg)
				model = updated.(bubbleDashboardModel)
				selection := model.currentSelection()
				if !selection.valid || selection.identity != selected {
					t.Fatalf("%s selection = %#v, want identity %q", name, selection, selected)
				}
				wantLine := max(beforeLine, model.dashboardBodyStart())
				if got := dashboardVisibleLine(t, model.View().Content, "Navigation Run 5"); got != wantLine {
					t.Fatalf("%s moved selected downstream Run from screen line %d to %d, want %d", name, beforeLine, got, wantLine)
				}
			}

			assertStable("narrow resize", tea.WindowSizeMsg{Width: 44, Height: 24})
			assertStable("wide resize", tea.WindowSizeMsg{Width: 120, Height: 24})
			assertStable("new failure", dashboardOperationalMsg{event: runner.CandidateDiscoveryFailed{
				Operation: runner.CandidateDiscoveryList,
				Err:       errors.New("gh issue list: latest diagnostic after navigation"), Cause: "connection refused",
				FirstFailureAt: now, OccurredAt: now.Add(21 * time.Second), RetryAt: now.Add(time.Minute),
				ConsecutiveFailures: 21, Occurrences: 1,
			}})
			if body := ansi.Strip(model.viewport.GetContent()); !strings.Contains(body, "latest diagnostic after navigation") || strings.Contains(body, "long diagnostic 01") {
				t.Fatalf("bounded Diagnostics did not retain the latest twenty entries:\n%s", body)
			}
			assertStable("drawer close", tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
			if body := ansi.Strip(model.viewport.GetContent()); strings.Contains(body, "latest diagnostic after navigation") || !strings.Contains(body, "Diagnostics: closed") {
				t.Fatalf("closed Diagnostics retained full evidence:\n%s", body)
			}
		})
	}
}

func TestBubbleDashboardPreservesSelectedSectionAcrossLiveProjectionChanges(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 5)
	source := &dashboardTestSource{current: current}
	model := configuredNavigationTestModel(t, now, current, source)
	selected := dashboardSectionAnchor("Attention Required")
	line, exists := model.anchorVisualLine(selected)
	if !exists {
		t.Fatalf("selected section anchor %q missing", selected)
	}
	model.viewport.SetYOffset(line)
	model.selectViewportAnchor()
	if model.selectedAnchor != selected {
		t.Fatalf("selected anchor = %q, want %q", model.selectedAnchor, selected)
	}
	wantRelative := model.currentSelection().relative
	wantScreenLine := dashboardVisibleLine(t, model.View().Content, "Attention Required")

	assertStable := func(name string) {
		t.Helper()
		updated, _ := model.Update(dashboardStateMsg(current))
		model = updated.(bubbleDashboardModel)
		selection := model.currentSelection()
		if !selection.valid || selection.identity != selected || selection.relative != wantRelative {
			t.Fatalf("%s selection = %#v, want identity %q at relative line %d", name, selection, selected, wantRelative)
		}
		if got := dashboardVisibleLine(t, model.View().Content, "Attention Required"); got != wantScreenLine {
			t.Fatalf("%s moved selected section from screen line %d to %d", name, wantScreenLine, got)
		}
	}

	older := scheduler.Run{Issue: 99, IssueTitle: "Earlier new Run", RunID: "run-new", Status: scheduler.StatusClaimed, StartedAt: now.Add(-2 * time.Hour)}
	current.Runs = append(current.Runs, older)
	current.Leases = append(current.Leases, scheduler.Lease{LeaseID: older.RunID, Issue: older.Issue, RunID: older.RunID})
	source.current = current
	assertStable("new Run")

	completedAt := now
	current.Runs[0].Status = scheduler.StatusMerged
	current.Runs[0].CompletedAt = &completedAt
	current.Runs[0].UpdatedAt = completedAt
	current.Leases = current.Leases[1:]
	source.current = current
	assertStable("Completion")
}

func TestBubbleDashboardMarksAndJumpsToNewOffscreenAttention(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 8)
	source := &dashboardTestSource{current: current}
	model := configuredNavigationTestModel(t, now, current, source)
	model.styler = newDashboardStyler(TerminalColorTrueColor, true)
	selected := model.currentSelection()

	attention := scheduler.Run{Issue: 100, IssueTitle: "Operator decision", RunID: "attention-new", Status: scheduler.StatusNeedsHuman, StartedAt: now, UpdatedAt: now, Error: "inspect outcome"}
	current.Runs = append(current.Runs, attention)
	current.Leases = append(current.Leases, scheduler.Lease{LeaseID: attention.RunID, Issue: attention.Issue, RunID: attention.RunID})
	source.current = current
	updated, _ := model.Update(dashboardStateMsg(current))
	model = updated.(bubbleDashboardModel)
	if got := model.currentSelection(); got.identity != selected.identity || got.relative != selected.relative {
		t.Fatalf("new Attention moved selection from %#v to %#v", selected, got)
	}
	rendered := model.View().Content
	view := ansi.Strip(rendered)
	if !strings.Contains(view, "NEW ATTENTION (1): press a") {
		t.Fatalf("fixed header did not mark offscreen Attention:\n%s", view)
	}
	if want := model.styler.attention.Render("NEW ATTENTION (1): press a"); !strings.Contains(rendered, want) {
		t.Fatalf("offscreen Attention marker did not use Attention styling: %q", rendered)
	}
	for _, want := range []string{"Next Ctrl-C:", "Nav:", "a:Attention"} {
		if !strings.Contains(view, want) {
			t.Fatalf("fixed footer omitted %q:\n%s", want, view)
		}
	}

	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'a', Text: "a"}))
	model = updated.(bubbleDashboardModel)
	if model.selectedAnchor != dashboardRunAnchor(attention.RunID) {
		t.Fatalf("Attention jump selected %q, want Run %q", model.selectedAnchor, attention.RunID)
	}
	line, _ := model.anchorVisualLine(model.selectedAnchor)
	if !model.visualLineVisible(line) || strings.Contains(ansi.Strip(model.View().Content), "NEW ATTENTION") {
		t.Fatal("Attention jump did not reveal and clear the new-Attention marker")
	}
}

func TestBubbleDashboardClearsPendingAttentionWhenRefreshRevealsRun(t *testing.T) {
	for _, refresh := range []string{"resize", "state update"} {
		t.Run(refresh, func(t *testing.T) {
			now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
			current := navigationTestState(now, 8)
			source := &dashboardTestSource{current: current}
			model := configuredNavigationTestModel(t, now, current, source)
			if refresh == "state update" {
				updated, _ := model.Update(tea.WindowSizeMsg{Width: 64, Height: 17})
				model = updated.(bubbleDashboardModel)
			}
			attention := scheduler.Run{Issue: 100, IssueTitle: "Operator decision", RunID: "attention-new", Status: scheduler.StatusNeedsHuman, StartedAt: now, UpdatedAt: now, Error: "inspect outcome"}
			current.Runs = append(current.Runs, attention)
			current.Leases = append(current.Leases, scheduler.Lease{LeaseID: attention.RunID, Issue: attention.Issue, RunID: attention.RunID})
			source.current = current
			updated, _ := model.Update(dashboardStateMsg(current))
			model = updated.(bubbleDashboardModel)
			if _, pending := model.attentionPending[attention.RunID]; !pending {
				t.Fatal("new offscreen Attention was not marked pending")
			}

			switch refresh {
			case "resize":
				updated, _ = model.Update(tea.WindowSizeMsg{Width: 64, Height: 80})
			case "state update":
				current.Runs = []scheduler.Run{attention}
				current.Leases = []scheduler.Lease{{LeaseID: attention.RunID, Issue: attention.Issue, RunID: attention.RunID}}
				source.current = current
				updated, _ = model.Update(dashboardStateMsg(current))
			}
			model = updated.(bubbleDashboardModel)
			line, exists := model.anchorVisualLine(dashboardRunAnchor(attention.RunID))
			if !exists || !model.visualLineVisible(line) {
				t.Fatal("refresh did not reveal the pending Attention Run")
			}
			if _, pending := model.attentionPending[attention.RunID]; pending || strings.Contains(ansi.Strip(model.View().Content), "NEW ATTENTION") {
				t.Fatal("visible Attention remained marked pending after refresh")
			}
		})
	}
}

func TestBubbleDashboardQIsUnassigned(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 4)
	model := configuredNavigationTestModel(t, now, current, &dashboardTestSource{current: current})
	before := model.currentSelection()
	offset := model.viewport.YOffset()
	stage := model.dashboard.stage
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'q', Text: "q"}))
	model = updated.(bubbleDashboardModel)
	if command != nil || model.viewport.YOffset() != offset || model.currentSelection() != before || model.dashboard.stage != stage || model.interruptsWaiting != 0 {
		t.Fatalf("q changed dashboard behavior: command nil=%t offset=%d selection=%#v stage=%d interrupts=%d", command == nil, model.viewport.YOffset(), model.currentSelection(), model.dashboard.stage, model.interruptsWaiting)
	}
}

func navigationTestState(now time.Time, count int) state.State {
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: count + 2}
	for index := range count {
		run := scheduler.Run{
			Issue: index + 1, IssueTitle: fmt.Sprintf("Navigation Run %d", index), RunID: fmt.Sprintf("run-%d", index),
			Status: scheduler.StatusClaimed, StartedAt: now.Add(time.Duration(index-count) * time.Minute),
		}
		current.Runs = append(current.Runs, run)
		current.Leases = append(current.Leases, scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID})
	}
	return current
}

func configuredNavigationTestModel(t *testing.T, now time.Time, current state.State, source *dashboardTestSource) bubbleDashboardModel {
	t.Helper()
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 64, Height: 14})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: source})
	return updated.(bubbleDashboardModel)
}

func dashboardVisibleLine(t *testing.T, view, content string) int {
	t.Helper()
	for line, rendered := range strings.Split(ansi.Strip(view), "\n") {
		if strings.Contains(rendered, content) {
			return line
		}
	}
	t.Fatalf("dashboard view omitted %q:\n%s", content, ansi.Strip(view))
	return -1
}

func TestBubbleDashboardRawCtrlCUsesOrderedRunnerIngress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingress := newOrderedSignalIngress(ctx, nil)
	control := PresentationControl{Terminal: PresentationTerminal{Now: time.Now}, ingress: ingress}
	model := newBubbleDashboardModel(ctx, control, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 60, Height: 10})

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}))
	model = updated.(bubbleDashboardModel)
	if model.interruptsWaiting != 1 || command == nil {
		t.Fatalf("raw Ctrl-C pending = %d, command nil = %t", model.interruptsWaiting, command == nil)
	}
	for range 2 {
		var queued tea.Cmd
		updated, queued = model.Update(tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}))
		model = updated.(bubbleDashboardModel)
		if queued != nil {
			t.Fatal("concurrent raw Ctrl-C bypassed serialized Runner ingress")
		}
	}
	if model.interruptsWaiting != 3 {
		t.Fatalf("queued raw Ctrl-C count = %d, want 3", model.interruptsWaiting)
	}

	for accepted := 1; accepted <= 3; accepted++ {
		returned := make(chan tea.Msg, 1)
		go func(current tea.Cmd) { returned <- current() }(command)
		event := <-ingress.events
		if event.signal != os.Interrupt {
			t.Fatalf("raw Ctrl-C ingress signal = %v, want os.Interrupt", event.signal)
		}
		select {
		case <-returned:
			t.Fatal("raw Ctrl-C command returned before Runner lifecycle acceptance")
		default:
		}
		event.accept()
		result := (<-returned).(dashboardInterruptResultMsg)
		updated, command = model.Update(result)
		model = updated.(bubbleDashboardModel)
		if accepted < 3 && command == nil {
			t.Fatalf("queued raw Ctrl-C %d was not submitted after prior acceptance", accepted+1)
		}
	}
	if model.interruptsWaiting != 0 || command != nil {
		t.Fatalf("accepted raw Ctrl-C state = pending %d, command nil %t", model.interruptsWaiting, command == nil)
	}
}

func TestBubbleDashboardSessionSafelyDeliversStateOutputAndOperationalEvents(t *testing.T) {
	session := newBubbleDashboardSession(time.Now)
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	session.configure(current, &dashboardTestSource{current: current})
	session.stateSaved(current)
	if _, err := io.WriteString(session, "persist runner state failed\n"); err != nil {
		t.Fatal(err)
	}
	events := newPresentationEventQueue()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	events.publish(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Err: errors.New("gh issue list: unavailable"), Cause: "unavailable",
		OccurredAt: now, RetryAt: now.Add(30 * time.Second), ConsecutiveFailures: 1,
	})
	events.publish(runner.ShutdownEvent{Stage: runner.ShutdownStageDraining, Message: "Drain accepted"})
	control := PresentationControl{Terminal: PresentationTerminal{Now: time.Now}, operationalEvents: events}
	model := newBubbleDashboardModel(context.Background(), control, session, TerminalDimensions{Width: 80, Height: 20})
	for range 3 {
		msg, err := session.next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		updated, _ := model.Update(msg)
		model = updated.(bubbleDashboardModel)
	}
	admission := model.waitForOperationalEvent()()
	updated, _ := model.Update(dashboardFlushMsg{acknowledged: make(chan struct{})})
	model = updated.(bubbleDashboardModel)
	if len(model.pendingFlushes) != 1 {
		t.Fatal("final render flush did not wait for the in-flight operational event")
	}
	updated, _ = model.Update(admission)
	model = updated.(bubbleDashboardModel)
	if len(model.pendingFlushes) != 1 {
		t.Fatal("final render flush resumed before the queued shutdown event")
	}
	shutdown := model.waitForOperationalEvent()()
	updated, _ = model.Update(shutdown)
	model = updated.(bubbleDashboardModel)
	if len(model.pendingFlushes) != 0 || !events.idle() {
		t.Fatal("final render flush did not resume after all operational events were applied")
	}
	view := ansi.Strip(model.View().Content)
	for _, want := range []string{"Repository: acme/widgets", "Runner stage: Draining", "Next Ctrl-C: suspend"} {
		if !strings.Contains(view, want) {
			t.Fatalf("Bubble Tea Update did not receive %q:\n%s", want, view)
		}
	}
	if body := model.viewport.GetContent(); !strings.Contains(body, "Admission: DEGRADED") || !strings.Contains(body, "persist runner state failed") || !strings.Contains(body, "Drain accepted") || strings.Contains(body, "gh issue list") {
		t.Fatalf("primary body did not separate health, operational output, and closed Diagnostics: %q", body)
	}
}

func TestBubbleDashboardRendersCompleteSuspensionFooterAfterSuspending(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 24})
	for _, test := range []struct {
		stage runner.ShutdownStage
		want  string
	}{
		{stage: runner.ShutdownStageSuspending, want: "Runner stage: Suspending"},
		{stage: runner.ShutdownStageSuspensionComplete, want: "Runner stage: Suspension finished"},
	} {
		updated, _ := model.Update(dashboardOperationalMsg{event: runner.ShutdownEvent{Stage: test.stage}})
		model = updated.(bubbleDashboardModel)
		if view := ansi.Strip(model.View().Content); !strings.Contains(view, test.want) {
			t.Fatalf("shutdown stage %q missing from Bubble Tea view:\n%s", test.want, view)
		}
	}
}

func TestBubbleDashboardQueueBoundsOnlyOptionalOutput(t *testing.T) {
	session := newBubbleDashboardSession(time.Now)
	initial := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	latest := initial
	latest.MaxConcurrentIssues = 3
	acknowledged := make(chan struct{})
	session.configure(initial, &dashboardTestSource{current: initial})
	session.stateSaved(initial)
	session.publish(dashboardFlushMsg{acknowledged: acknowledged})
	for index := range dashboardOutputUpdateLimit + 6 {
		session.publish(dashboardOutputMsg("output " + fmt.Sprint(index)))
	}
	session.stateSaved(latest)

	var configured dashboardConfiguredMsg
	var saved dashboardStateMsg
	var flush dashboardFlushMsg
	var outputs []string
	session.mu.Lock()
	updates := append([]tea.Msg(nil), session.updates...)
	session.mu.Unlock()
	for _, update := range updates {
		switch update := update.(type) {
		case dashboardConfiguredMsg:
			configured = update
		case dashboardStateMsg:
			saved = update
		case dashboardFlushMsg:
			flush = update
		case dashboardOutputMsg:
			outputs = append(outputs, string(update))
		}
	}
	if configured.initial.Repo != initial.Repo || state.State(saved).MaxConcurrentIssues != latest.MaxConcurrentIssues || flush.acknowledged != acknowledged {
		t.Fatalf("required dashboard updates were lost: configured=%#v saved=%#v flush=%#v", configured, saved, flush)
	}
	if len(outputs) != dashboardOutputUpdateLimit || outputs[0] != "output 6" {
		t.Fatalf("bounded output = %d messages starting at %q", len(outputs), outputs[0])
	}
}

func TestBubbleDashboardConstrainedChromeKeepsRequiredLifecycleInformation(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 3}
	for _, dimensions := range []TerminalDimensions{
		{Width: 18, Height: 12},
		{Width: 120, Height: 2},
		{Width: 200, Height: 1},
	} {
		model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), dimensions)
		model.dashboard.update(current)
		plain := ansi.Strip(model.View().Content)
		for _, expected := range []struct {
			name     string
			variants []string
		}{
			{name: "repository", variants: []string{"acme/widgets"}},
			{name: "Worker capacity", variants: []string{"Worker capacity:", "W:"}},
			{name: "Runner stage", variants: []string{"Runner stage:", "S:"}},
			{name: "next Ctrl-C", variants: []string{"Next Ctrl-C:", "^C:"}},
			{name: "navigation", variants: []string{"Nav:", "N:"}},
			{name: "f/b shortcuts", variants: []string{"f/b", "fb"}},
		} {
			found := false
			for _, variant := range expected.variants {
				found = found || strings.Contains(plain, variant)
			}
			if !found {
				t.Fatalf("%dx%d dashboard omitted %s:\n%s", dimensions.Width, dimensions.Height, expected.name, plain)
			}
		}
		lines := strings.Split(plain, "\n")
		if len(lines) > dimensions.Height {
			t.Fatalf("%dx%d dashboard used %d lines:\n%s", dimensions.Width, dimensions.Height, len(lines), plain)
		}
		for _, line := range lines {
			if lipgloss.Width(line) > dimensions.Width {
				t.Fatalf("%dx%d dashboard overflowed with %q", dimensions.Width, dimensions.Height, line)
			}
		}
	}
}

func TestBubbleDashboardConstrainedFallbackKeepsGuidanceInFixedFooter(t *testing.T) {
	footer := []string{
		"Runner stage: Running",
		"Next Ctrl-C: start Drain and stop Admission",
		dashboardNavigationHelp,
	}
	chrome := dashboardChromeLines(
		[]string{"Repository: acme/widgets", "Worker capacity: 1 used | 2 available | 3 total"},
		footer,
		0,
		dashboardRunning,
		dashboardStyler{},
		120,
		2,
	)
	if len(chrome.bottom) == 0 {
		t.Fatal("constrained dashboard moved fixed-footer guidance above the body")
	}
	fixedFooter := strings.Join(chrome.bottom, "\n")
	for _, want := range []string{"Next Ctrl-C: start Drain and stop Admission", "Nav: ↑↓/jk PgUp/Dn/f/b Home/End g/G a:Attention"} {
		if !strings.Contains(fixedFooter, want) {
			t.Fatalf("constrained fixed footer omitted %q: %#v", want, chrome)
		}
	}
}

func TestBubbleDashboardPresentationRejectsDimensionFailureBeforeRunnerStartup(t *testing.T) {
	for _, test := range []struct {
		name       string
		dimensions func() (TerminalDimensions, error)
		want       string
	}{
		{name: "query failure", dimensions: func() (TerminalDimensions, error) { return TerminalDimensions{}, errors.New("terminal unavailable") }, want: "terminal unavailable"},
		{name: "invalid size", dimensions: func() (TerminalDimensions, error) { return TerminalDimensions{Width: 0, Height: 24}, nil }, want: "invalid size 0x24"},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := newBubbleDashboardSession(time.Now)
			err := session.presentation(context.Background(), PresentationControl{Terminal: PresentationTerminal{Dimensions: test.dimensions}})
			if err == nil || !strings.Contains(err.Error(), "initial terminal dimensions") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("presentation dimension error = %v", err)
			}
			if startupErr := session.waitForStartup(context.Background()); startupErr == nil || startupErr.Error() != err.Error() {
				t.Fatalf("Runner startup error = %v, want %v", startupErr, err)
			}
		})
	}
}

func TestBubbleDashboardFinalFlushKeepsRecoveryNoticeVisible(t *testing.T) {
	recoveredAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
	dashboard.operationalEvent(runner.CandidateDiscoveryRecovered{OccurredAt: recoveredAt, Failures: 3})

	if delay := dashboardFlushDelay(dashboard, recoveredAt.Add(2*time.Second)); delay != 8*time.Second {
		t.Fatalf("final flush delay = %s, want 8s", delay)
	}
	if delay := dashboardFlushDelay(dashboard, recoveredAt.Add(10*time.Second)); delay != time.Second/30 {
		t.Fatalf("expired-notice flush delay = %s, want %s", delay, time.Second/30)
	}
}

func TestBubbleDashboardFlushFailsWhenPresentationAlreadyStopped(t *testing.T) {
	session := newBubbleDashboardSession(time.Now)
	close(session.done)
	if err := session.flush(context.Background()); err == nil || !strings.Contains(err.Error(), "stopped before pending updates") {
		t.Fatalf("flush error = %v", err)
	}
}

func TestBubbleDashboardStoreDoesNotPublishFailedSave(t *testing.T) {
	root := t.TempDir()
	blockedParent := root + "/not-a-directory"
	if err := os.WriteFile(blockedParent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	session := newBubbleDashboardSession(time.Now)
	store := bubbleDashboardStore{FileStore: state.FileStore{Path: blockedParent + "/state.json"}, session: session}
	if err := store.Save(state.State{Version: state.CurrentVersion}); err == nil {
		t.Fatal("dashboard store save unexpectedly succeeded")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if len(session.updates) != 0 {
		t.Fatalf("failed state save published %d dashboard updates", len(session.updates))
	}
}

func TestBubbleDashboardStaticSummaryReturnsOutputFailure(t *testing.T) {
	session := newBubbleDashboardSession(time.Now)
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	session.configure(current, &dashboardTestSource{current: current})
	if err := session.captureFinalSummary(current); err != nil {
		t.Fatal(err)
	}
	if err := session.printFinalSummary(failingStatusWriter{}); err == nil || !strings.Contains(err.Error(), "status output failed") {
		t.Fatalf("static summary output error = %v", err)
	}
}

func TestBubbleDashboardWriterReassemblesSplitOperationalLines(t *testing.T) {
	session := newBubbleDashboardSession(time.Now)
	for _, part := range []string{"Drain: admission", " stopped\nnext", " event\n"} {
		if _, err := io.Copy(session, bytes.NewBufferString(part)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"Drain: admission stopped", "next event"} {
		msg, err := session.next(context.Background())
		if err != nil || string(msg.(dashboardOutputMsg)) != want {
			t.Fatalf("reassembled output = %#v, %v; want %q", msg, err, want)
		}
	}
}
