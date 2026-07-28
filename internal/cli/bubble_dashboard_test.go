package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

	assertBubbleDashboardFits(t, model, 48, 10)
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 36, Height: 8})
	model = updated.(bubbleDashboardModel)
	assertBubbleDashboardFits(t, model, 36, 8)
}

func TestBubbleDashboardLinksSafeIssueIdentitiesAndPullRequests(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	completedAt := now.Add(-time.Minute)
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{
			{
				Issue: 12, IssueTitle: "Current issue", IssueURL: "https://github.com/acme/widgets/issues/12",
				RunID: "current", Status: scheduler.StatusWaitingForMerge, PullRequest: "https://github.com/acme/widgets/pull/112", StartedAt: now.Add(-time.Hour),
			},
			{
				Issue: 13, IssueTitle: "Historical issue", RunID: "historical", Status: scheduler.StatusFailed,
				PullRequest: "https://github.com/acme/widgets/pull/not-a-number\x1b]8;;https://attacker.test", StartedAt: now.Add(-2 * time.Hour), UpdatedAt: now,
			},
			{
				Issue: 14, IssueTitle: "Malformed issue URL", IssueURL: "javascript:alert(1)",
				RunID: "malformed-issue", Status: scheduler.StatusMerged, PullRequest: "https://github.com/acme/widgets/pull/114",
				StartedAt: now.Add(-3 * time.Hour), CompletedAt: &completedAt,
			},
		},
		Leases: []scheduler.Lease{{LeaseID: "current", Issue: 12, RunID: "current"}},
	}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now },
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 160, Height: 24})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
	model = updated.(bubbleDashboardModel)
	body := model.viewport.GetContent()
	for _, want := range []string{
		"\x1b]8;;https://github.com/acme/widgets/issues/12\x1b\\#12  Current issue\x1b]8;;\x1b\\ | \x1b]8;;https://github.com/acme/widgets/pull/112\x1b\\PR #112\x1b]8;;\x1b\\",
		"\x1b]8;;https://github.com/acme/widgets/issues/13\x1b\\#13\x1b]8;;\x1b\\",
		"PR: https://github.com/acme/widgets/pull/not-a-number",
		"#14  Malformed issue URL | \x1b]8;;https://github.com/acme/widgets/pull/114\x1b\\PR #114\x1b]8;;\x1b\\",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("linked dashboard body missing %q:\n%q", want, body)
		}
	}
	if strings.Contains(body, "\x1b]8;;javascript:") || strings.Contains(body, "PR #not-a-number") {
		t.Fatalf("unsafe resource metadata became a hyperlink or guessed PR number:\n%q", body)
	}
}

func TestBubbleDashboardKeyboardOpensSelectedIssueAndPullRequest(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 12, IssueTitle: "Open resources", IssueURL: "https://github.com/acme/widgets/issues/12",
			RunID: "current", Status: scheduler.StatusWaitingForMerge, PullRequest: "https://github.com/acme/widgets/pull/112", StartedAt: now.Add(-time.Hour),
		}},
		Leases: []scheduler.Lease{{LeaseID: "current", Issue: 12, RunID: "current"}},
	}
	opened := make(chan string, 2)
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now },
		OpenURL: func(_ context.Context, target string) error {
			opened <- target
			return nil
		},
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 16})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
	model = updated.(bubbleDashboardModel)
	line, exists := model.anchorVisualLine(dashboardRunAnchor("current"))
	if !exists {
		t.Fatal("selected Run anchor missing")
	}
	model.viewport.SetYOffset(line)
	model.selectViewportAnchor()
	if command := model.openSelectedURL(dashboardRunAnchor("current"), dashboardResourceKind(255)); command != nil {
		t.Fatal("unknown resource kind selected the issue URL")
	}

	for _, test := range []struct {
		key  rune
		want string
	}{
		{key: 'o', want: "https://github.com/acme/widgets/issues/12"},
		{key: 'p', want: "https://github.com/acme/widgets/pull/112"},
	} {
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: test.key, Text: string(test.key)}))
		model = updated.(bubbleDashboardModel)
		if command == nil {
			t.Fatalf("%c did not return an asynchronous opener command", test.key)
		}
		result := command()
		if message, ok := result.(dashboardOpenURLResultMsg); !ok || message.err != nil {
			t.Fatalf("%c opener result = %#v", test.key, result)
		}
		if got := <-opened; got != test.want {
			t.Fatalf("%c opened %q, want %q", test.key, got, test.want)
		}
	}
}

func TestBubbleDashboardURLFailureIsTemporaryDiagnosticOnly(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs:   []scheduler.Run{{Issue: 12, RunID: "current", Status: scheduler.StatusClaimed, StartedAt: now}},
		Leases: []scheduler.Lease{{LeaseID: "current", Issue: 12, RunID: "current"}},
	}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now },
		OpenURL: func(context.Context, string) error {
			return errors.New("opener unavailable")
		},
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 16})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
	model = updated.(bubbleDashboardModel)
	line, _ := model.anchorVisualLine(dashboardRunAnchor("current"))
	model.viewport.SetYOffset(line)
	model.selectViewportAnchor()
	before := cloneDashboardState(model.dashboard.current)

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'o', Text: "o"}))
	model = updated.(bubbleDashboardModel)
	result := command().(dashboardOpenURLResultMsg)
	updated, expiry := model.Update(result)
	model = updated.(bubbleDashboardModel)
	if expiry == nil || !strings.Contains(ansi.Strip(model.View().Content), "Open issue failed: opener unavailable") {
		t.Fatalf("opener failure did not become a temporary diagnostic:\n%s", ansi.Strip(model.View().Content))
	}
	if !reflect.DeepEqual(model.dashboard.current, before) {
		t.Fatal("URL opener failure altered Runner state projection")
	}
	updated, _ = model.Update(dashboardURLDiagnosticExpiredMsg{id: model.urlDiagnosticID})
	model = updated.(bubbleDashboardModel)
	if strings.Contains(ansi.Strip(model.View().Content), "opener unavailable") {
		t.Fatal("URL opener diagnostic did not expire")
	}

	updated, command = model.Update(tea.KeyPressMsg(tea.Key{Code: 'p', Text: "p"}))
	model = updated.(bubbleDashboardModel)
	if command != nil {
		t.Fatal("p attempted to open an unavailable pull request")
	}
	model.control.Terminal.OpenURL = nil
	_, command = model.Update(tea.KeyPressMsg(tea.Key{Code: 'o', Text: "o"}))
	if result := command().(dashboardOpenURLResultMsg); result.err == nil || result.err.Error() != "URL opener unavailable" {
		t.Fatalf("missing URL opener result = %#v", result)
	}
}

func assertBubbleDashboardFits(t *testing.T, model bubbleDashboardModel, width, height int) {
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
	for _, expected := range []struct {
		name     string
		variants []string
	}{
		{name: "repository", variants: []string{"Repository: acme/widgets", "Backlog: acme/widgets", "R:acme/widgets"}},
		{name: "Worker capacity", variants: []string{"Worker capacity:", "W:"}},
		{name: "Runner stage", variants: []string{"Runner stage: Running", "S:Running"}},
		{name: "next Ctrl-C", variants: []string{"Next Ctrl-C: start Drain", "^C:start Drain"}},
		{name: "Diagnostics help", variants: []string{"d:Diagnostics", "a d Ent"}},
	} {
		found := false
		for _, variant := range expected.variants {
			found = found || strings.Contains(plain, variant)
		}
		if !found {
			t.Fatalf("fixed dashboard chrome omitted %s after resize:\n%s", expected.name, plain)
		}
	}
	if frame.bodyHeight <= 0 || model.viewport.Height() != frame.bodyHeight || model.viewport.Width() != width {
		t.Fatalf("frame body and viewport size = %d and %dx%d, want a positive body and width %d", frame.bodyHeight, model.viewport.Width(), model.viewport.Height(), width)
	}
	if frame.bodyHeight > 0 {
		body := strings.Split(ansi.Strip(model.viewport.View()), "\n")
		if frame.bodyStart >= len(lines) || strings.TrimSpace(lines[frame.bodyStart]) != strings.TrimSpace(body[0]) {
			t.Fatalf("rendered body does not start at derived line %d:\n%s", frame.bodyStart, plain)
		}
	}
}

func TestBubbleDashboardShowsAdmissionCheckingThroughSetupUntilSnapshotCompletes(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 12})
	initial := ansi.Strip(model.View().Content)
	if !strings.Contains(initial, "Worker capacity: pending configuration") || strings.Contains(initial, "Worker capacity: 0 used | 0 available | 0 total") {
		t.Fatalf("initial capacity was not pending configuration:\n%s", initial)
	}
	if !strings.Contains(initial, "Admission: checking | Candidate snapshot not yet complete") || strings.Contains(initial, "Admission: healthy") {
		t.Fatalf("startup Admission claimed health before a Candidate snapshot completed:\n%s", initial)
	}

	configured := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 3}
	updated, _ := model.Update(dashboardConfiguredMsg{initial: configured, source: &dashboardTestSource{current: configured}})
	model = updated.(bubbleDashboardModel)
	configuredView := ansi.Strip(model.View().Content)
	if !strings.Contains(configuredView, "Worker capacity: 0 used | 3 available | 3 total") || strings.Contains(configuredView, "pending configuration") {
		t.Fatalf("configured capacity was not rendered:\n%s", configuredView)
	}
	if !strings.Contains(configuredView, "Admission: checking | Candidate snapshot not yet complete") || strings.Contains(configuredView, "Admission: healthy") {
		t.Fatalf("setup configuration falsely established Admission health:\n%s", configuredView)
	}

	updated, _ = model.Update(dashboardOperationalMsg{event: runner.CandidateSnapshotCompleted{OccurredAt: time.Now()}})
	completedView := ansi.Strip(updated.(bubbleDashboardModel).View().Content)
	if !strings.Contains(completedView, "Admission: healthy") || strings.Contains(completedView, "Admission: checking") || strings.Contains(completedView, "Recovered") {
		t.Fatalf("first complete Candidate snapshot did not establish immediate health:\n%s", completedView)
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
	model.refreshViewport(model.currentSelection())
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
	if view := ansi.Strip(model.View().Content); !strings.Contains(view, "Diagnostics (8 recent Candidate discovery failure records; d to close)") || !strings.Contains(model.viewport.GetContent(), "gh issue list") {
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

func TestBubbleDashboardKeyboardNavigationSelectsCollapsedSectionWhenContentFits(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	completedAt := now.Add(-time.Minute)
	active := scheduler.Run{Issue: 69, IssueTitle: "Active Run", RunID: "active", Status: scheduler.StatusClaimed, StartedAt: now.Add(-time.Hour)}
	completion := scheduler.Run{Issue: 68, IssueTitle: "Reachable Completion", RunID: "completion", Status: scheduler.StatusMerged, StartedAt: now.Add(-2 * time.Hour), CompletedAt: &completedAt}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs:   []scheduler.Run{active, completion},
		Leases: []scheduler.Lease{{LeaseID: active.RunID, Issue: active.Issue, RunID: active.RunID}},
	}
	newModel := func() bubbleDashboardModel {
		model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 23})
		updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
		model = updated.(bubbleDashboardModel)
		if model.viewport.YOffset() != 0 || !model.viewport.AtBottom() {
			t.Fatalf("test dashboard content did not fit at offset zero: offset=%d bottom=%t", model.viewport.YOffset(), model.viewport.AtBottom())
		}
		return model
	}

	for _, test := range []struct {
		name string
		key  tea.Key
		want string
	}{
		{name: "Down", key: tea.Key{Code: tea.KeyDown}, want: dashboardSectionAnchor("Attention Required")},
		{name: "j", key: tea.Key{Code: 'j', Text: "j"}, want: dashboardSectionAnchor("Attention Required")},
		{name: "Up", key: tea.Key{Code: tea.KeyUp}, want: dashboardSectionAnchor("Active Runs")},
		{name: "k", key: tea.Key{Code: 'k', Text: "k"}, want: dashboardSectionAnchor("Active Runs")},
		{name: "Page Down", key: tea.Key{Code: tea.KeyPgDown}, want: dashboardSectionAnchor("Recent Completions")},
		{name: "f", key: tea.Key{Code: 'f', Text: "f"}, want: dashboardSectionAnchor("Recent Completions")},
		{name: "Page Up", key: tea.Key{Code: tea.KeyPgUp}, want: dashboardSectionAnchor("Admission health")},
		{name: "b", key: tea.Key{Code: 'b', Text: "b"}, want: dashboardSectionAnchor("Admission health")},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := newModel()
			updated, _ := model.Update(tea.KeyPressMsg(test.key))
			model = updated.(bubbleDashboardModel)
			if model.selectedAnchor != test.want {
				t.Fatalf("selected %q, want %q", model.selectedAnchor, test.want)
			}
			if model.viewport.YOffset() != 0 {
				t.Fatalf("fitting content scrolled to offset %d", model.viewport.YOffset())
			}
		})
	}

	model := newModel()
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnd}))
	model = updated.(bubbleDashboardModel)
	section := dashboardSectionAnchor("Recent Completions")
	if model.selectedAnchor != section {
		t.Fatalf("End selected %q, want collapsed section %q", model.selectedAnchor, section)
	}
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	if !model.expansionOverrides[section] || !strings.Contains(model.viewport.GetContent(), completion.IssueTitle) {
		t.Fatalf("Enter did not expand keyboard-selected Recent Completions:\n%s", model.viewport.GetContent())
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
	model.viewport.SetYOffset(line)
	model.selectViewportAnchor()
	if model.selectedAnchor != selected {
		t.Fatalf("selected anchor = %q, want %q", model.selectedAnchor, selected)
	}
	wantRelative := model.currentSelection().relative
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
	if body := model.viewport.GetContent(); !strings.Contains(body, "Deepest operation: edi") {
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
	model.selectedAnchor = selected
	model.refreshViewport(dashboardSelection{identity: selected, relative: model.dashboardBodyStart() + 1, valid: true})
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
	assertStable("Diagnostics toggle", tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}), "Full error/command (page 1/1): candidate discovery failed")
	assertStable("recovery", dashboardOperationalMsg{event: runner.CandidateDiscoveryRecovered{OccurredAt: now.Add(time.Second), Failures: 1}}, "Admission: healthy | Recovered")
}

func TestBubbleDashboardPagesFullDiagnosticsWithBoundedTickAndResizeWork(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	model := newBubbleDashboardModel(
		context.Background(),
		PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}},
		newBubbleDashboardSession(time.Now),
		TerminalDimensions{Width: 80, Height: 24},
	)
	for failure := 1; failure <= dashboardDiagnosticLimit; failure++ {
		tail := fmt.Sprintf("complete multibyte diagnostic tail 界%02d", failure)
		evidence := strings.Repeat(fmt.Sprintf("oversized evidence %02d ", failure), (64<<10)/22) + tail
		updated, _ := model.Update(dashboardOperationalMsg{event: runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList, Err: errors.New(evidence), Cause: "oversized evidence",
			OccurredAt: now.Add(time.Duration(failure) * time.Second), RetryAt: now.Add(time.Minute),
			ConsecutiveFailures: failure, Occurrences: 1,
		}})
		model = updated.(bubbleDashboardModel)
	}
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
	model = updated.(bubbleDashboardModel)

	content := ansi.Strip(model.viewport.GetContent())
	if len(content) > dashboardDiagnosticPageByteLimit+(12<<10) {
		t.Fatalf("open Diagnostics viewport bytes = %d, want one bounded evidence page plus dashboard chrome", len(content))
	}
	if strings.Contains(content, "complete multibyte diagnostic tail 界20") {
		t.Fatal("first bounded evidence page unexpectedly contained the oversized record tail")
	}
	diagnostic := model.dashboard.admission.failures[dashboardDiagnosticLimit-1]
	pages := len(diagnostic.pageStarts)
	var complete strings.Builder
	for page := range pages {
		chunk := diagnostic.page(page)
		if len(chunk) > dashboardDiagnosticPageByteLimit+3 || !utf8.ValidString(chunk) {
			t.Fatalf("evidence page %d is invalid or %d bytes, want bounded valid UTF-8", page+1, len(chunk))
		}
		complete.WriteString(chunk)
	}
	if complete.String() != diagnostic.evidence {
		t.Fatal("paged retrieval lost or duplicated full diagnostic evidence")
	}
	for page := 1; page < pages; page++ {
		updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: '.', Text: "."}))
		model = updated.(bubbleDashboardModel)
	}
	if content = ansi.Strip(model.viewport.GetContent()); !utf8.ValidString(content) || !strings.Contains(content, "complete multibyte diagnostic tail 界20") || !strings.Contains(content, fmt.Sprintf("Evidence page %d/%d", pages, pages)) {
		t.Fatalf("paged Diagnostics did not retrieve the complete oversized evidence tail:\n%s", content)
	}

	activityAllocs := testing.AllocsPerRun(20, func() {
		updated, _ := model.Update(dashboardActivityMsg(now))
		model = updated.(bubbleDashboardModel)
		_ = model.View()
	})
	if activityAllocs > 1500 {
		t.Fatalf("unchanged activity Update+View allocations = %.0f, want at most 1500", activityAllocs)
	}
	width := 79
	resizeAllocs := testing.AllocsPerRun(10, func() {
		updated, _ := model.Update(tea.WindowSizeMsg{Width: width, Height: 24})
		model = updated.(bubbleDashboardModel)
		_ = model.View()
		width = 159 - width
	})
	if resizeAllocs > 2500 {
		t.Fatalf("Diagnostics resize Update+View allocations = %.0f, want at most 2500", resizeAllocs)
	}
	shutdownAllocs := testing.AllocsPerRun(5, func() {
		updated, _ := model.Update(dashboardOperationalMsg{event: runner.ShutdownEvent{Stage: runner.ShutdownStageDraining}})
		model = updated.(bubbleDashboardModel)
		_ = model.View()
	})
	if shutdownAllocs > 2500 {
		t.Fatalf("Diagnostics shutdown Update+View allocations = %.0f, want at most 2500", shutdownAllocs)
	}
	if content = ansi.Strip(model.viewport.GetContent()); len(content) > dashboardDiagnosticPageByteLimit+(12<<10) || !strings.Contains(content, "Retry: stopped") {
		t.Fatalf("shutdown lost bounded Diagnostics or stopped retry state: bytes=%d\n%s", len(content), content)
	}
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
			model.refreshViewport(wantSelection)

			assertStable := func(name string, msg tea.Msg) {
				t.Helper()
				beforeLine := dashboardVisibleLine(t, model.View().Content, "> #6")
				updated, _ := model.Update(msg)
				model = updated.(bubbleDashboardModel)
				selection := model.currentSelection()
				if !selection.valid || selection.identity != selected {
					t.Fatalf("%s selection = %#v, want identity %q", name, selection, selected)
				}
				wantLine := max(beforeLine, model.dashboardBodyStart())
				if got := dashboardVisibleLine(t, model.View().Content, "> #6"); got != wantLine {
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

func TestBubbleDashboardKeepsSelectedRunWhenItBecomesACompletion(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	active := scheduler.Run{Issue: 69, IssueTitle: "Selected completion", RunID: "selected", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Hour)}
	other := scheduler.Run{Issue: 70, IssueTitle: "Unrelated active Run", RunID: "other", Status: scheduler.StatusClaimed, StartedAt: now.Add(-30 * time.Minute)}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{active, other},
		Leases: []scheduler.Lease{
			{LeaseID: active.RunID, Issue: active.Issue, RunID: active.RunID},
			{LeaseID: other.RunID, Issue: other.Issue, RunID: other.RunID},
		},
	}
	source := &dashboardTestSource{current: current}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 23})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: source})
	model = updated.(bubbleDashboardModel)
	selected := dashboardRunAnchor(active.RunID)
	model.selectedAnchor = selected
	model.refreshViewport(dashboardSelection{identity: selected, relative: model.dashboardBodyStart() + 1, valid: true})

	completedAt := now
	current.Runs[0].Status = scheduler.StatusMerged
	current.Runs[0].CompletedAt = &completedAt
	current.Runs[0].UpdatedAt = completedAt
	current.Leases = current.Leases[1:]
	source.current = current
	updated, _ = model.Update(dashboardStateMsg(current))
	model = updated.(bubbleDashboardModel)

	if model.selectedAnchor != selected {
		t.Fatalf("completed selected Run changed selection to %q, want %q", model.selectedAnchor, selected)
	}
	completionSection := dashboardSectionAnchor("Recent Completions")
	if !model.expansionOverrides[completionSection] {
		t.Fatal("default-collapsed Recent Completions was not expanded to reveal the selected Run")
	}
	line, exists := model.anchorVisualLine(selected)
	if !exists || !model.visualLineVisible(line) {
		t.Fatalf("selected Completion was not visible after its section changed:\n%s", ansi.Strip(model.View().Content))
	}
}

func TestBubbleDashboardPreservesExplicitRunExpansionAcrossLiveUpdatesAndDensityChanges(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name          string
		initialHeight int
		resizedHeight int
		expanded      bool
	}{
		{name: "expanded", initialHeight: 23, resizedHeight: 24, expanded: true},
		{name: "collapsed", initialHeight: 24, resizedHeight: 23, expanded: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := navigationTestState(now, 12)
			logPath := t.TempDir() + "/selected.jsonl"
			if err := os.WriteFile(logPath, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			current.Runs[6].LogPath = logPath
			writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
				Version: activity.CurrentVersion, ObservedAt: now.Add(-time.Second), Kind: "turn",
				Description: "Worker turn completed", TurnDelta: 1,
			})
			source := &dashboardTestSource{current: current}
			model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: test.initialHeight})
			updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: source})
			model = updated.(bubbleDashboardModel)
			selected := dashboardRunAnchor(current.Runs[6].RunID)
			model.selectedAnchor = selected
			model.refreshViewport(dashboardSelection{identity: selected, relative: model.dashboardBodyStart() + 2, valid: true})
			updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
			model = updated.(bubbleDashboardModel)
			wantSelection := model.currentSelection()
			wantScreenLine := dashboardVisibleLine(t, model.View().Content, "> #7")

			assertStable := func(name string, msg tea.Msg) {
				t.Helper()
				updated, _ := model.Update(msg)
				model = updated.(bubbleDashboardModel)
				selection := model.currentSelection()
				if !selection.valid || selection.identity != selected || selection.relative != wantSelection.relative {
					t.Fatalf("%s selection = %#v, want identity %q at relative line %d", name, selection, selected, wantSelection.relative)
				}
				if got := dashboardVisibleLine(t, model.View().Content, "> #7"); got != wantScreenLine {
					t.Fatalf("%s moved selected Run from screen line %d to %d", name, wantScreenLine, got)
				}
				if got := strings.Contains(model.viewport.GetContent(), "Run: "+current.Runs[6].RunID); got != test.expanded {
					t.Fatalf("%s expanded details visible = %t, want %t", name, got, test.expanded)
				}
				if got, exists := model.expansionOverrides[selected]; !exists || got != test.expanded {
					t.Fatalf("%s expansion override = %t (present %t), want %t", name, got, exists, test.expanded)
				}
			}

			writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
				Version: activity.CurrentVersion, ObservedAt: now, Kind: "tool", Description: "Tool edit started",
				Operation: "edit", OperationChanged: true,
			})
			assertStable("Activity update", dashboardActivityMsg(now))
			current.Runs[6].UpdatedAt = now
			source.current = current
			assertStable("state update", dashboardStateMsg(current))
			assertStable("density transition", tea.WindowSizeMsg{Width: 80, Height: test.resizedHeight})
			assertStable("density return", tea.WindowSizeMsg{Width: 80, Height: test.initialHeight})
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
	for _, want := range []string{"Next Ctrl-C:", "Nav:", "d:Diagnostics", "a:Attention"} {
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

func TestBubbleDashboardMarksAndJumpsToNewAttentionInCollapsedSection(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 4)
	source := &dashboardTestSource{current: current}
	model := configuredNavigationTestModel(t, now, current, source)
	section := dashboardSectionAnchor("Attention Required")
	model.expansionOverrides[section] = false
	model.refreshViewport(model.currentSelection())

	attention := scheduler.Run{Issue: 100, IssueTitle: "Collapsed intervention", RunID: "attention-collapsed", Status: scheduler.StatusNeedsHuman, StartedAt: now, UpdatedAt: now, Error: "inspect outcome"}
	current.Runs = append(current.Runs, attention)
	current.Leases = append(current.Leases, scheduler.Lease{LeaseID: attention.RunID, Issue: attention.Issue, RunID: attention.RunID})
	source.current = current
	updated, _ := model.Update(dashboardStateMsg(current))
	model = updated.(bubbleDashboardModel)
	if _, pending := model.attentionPending[attention.RunID]; !pending {
		t.Fatal("new Attention in collapsed section was not marked pending")
	}
	if _, exists := model.anchorVisualLine(dashboardRunAnchor(attention.RunID)); exists {
		t.Fatal("collapsed Attention section unexpectedly exposed the new Run anchor")
	}

	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'a', Text: "a"}))
	model = updated.(bubbleDashboardModel)
	if model.selectedAnchor != dashboardRunAnchor(attention.RunID) {
		t.Fatalf("Attention jump selected %q, want Run %q", model.selectedAnchor, attention.RunID)
	}
	if expanded := model.expansionOverrides[section]; !expanded {
		t.Fatal("Attention jump did not expand the collapsed section")
	}
	line, exists := model.anchorVisualLine(model.selectedAnchor)
	if !exists || !model.visualLineVisible(line) {
		t.Fatal("Attention jump did not reveal the new Run")
	}
	if _, pending := model.attentionPending[attention.RunID]; pending {
		t.Fatal("revealed Attention Run remained pending")
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

func TestBubbleDashboardAdmissionExpansionRemainsUsefulAndResponsive(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 30, 0, time.UTC)
	model := newBubbleDashboardModel(
		context.Background(),
		PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}},
		newBubbleDashboardSession(time.Now),
		TerminalDimensions{Width: 180, Height: 18},
	)
	issue := 70
	model.dashboard.operationalEvent(runner.CandidateDiscoveryFailed{
		Operation:  runner.CandidateDiscoveryInspect,
		Issue:      &issue,
		Err:        errors.New("gh issue view 70: retained diagnostic evidence"),
		Cause:      "upstream API temporarily unavailable",
		OccurredAt: now.Add(-30 * time.Second), RetryAt: now.Add(30 * time.Second),
		ConsecutiveFailures: 3,
	})
	admission := dashboardSectionAnchor("Admission health")
	model.selectedAnchor = admission
	model.refreshViewport(dashboardSelection{identity: admission, relative: model.dashboardBodyStart(), valid: true})

	expanded := model.viewport.GetContent()
	for _, want := range []string{"> Admission health", "First failure:", "Operation: inspect candidate", "Cause: upstream API temporarily unavailable"} {
		if !strings.Contains(expanded, want) {
			t.Fatalf("expanded selected Admission omitted %q:\n%s", want, expanded)
		}
	}

	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	collapsed := model.viewport.GetContent()
	for _, want := range []string{"> Admission health [collapsed]", "Admission: DEGRADED | 3 consecutive failures", "inspect candidate #70", "Cause: upstream API temporarily unavailable", "Diagnostics: closed"} {
		if !strings.Contains(collapsed, want) {
			t.Fatalf("collapsed selected Admission omitted %q:\n%s", want, collapsed)
		}
	}
	for _, hidden := range []string{"First failure:", "Latest failure:", "    Operation:"} {
		if strings.Contains(collapsed, hidden) {
			t.Fatalf("collapsed Admission retained expanded field %q:\n%s", hidden, collapsed)
		}
	}

	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
	model = updated.(bubbleDashboardModel)
	if body := model.viewport.GetContent(); !strings.Contains(body, "> Admission health [collapsed]") || !strings.Contains(body, "retained diagnostic evidence") {
		t.Fatalf("collapsed Admission made Diagnostics inaccessible:\n%s", body)
	}
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
	model = updated.(bubbleDashboardModel)

	updated, _ = model.Update(tea.WindowSizeMsg{Width: 32, Height: 11})
	model = updated.(bubbleDashboardModel)
	narrow := model.viewport.GetContent()
	for _, line := range []string{
		dashboardContentLine(t, narrow, "Admission health"),
		dashboardContentLine(t, narrow, "Admission: DEGRADED"),
		dashboardContentLine(t, narrow, "Diagnostics:"),
	} {
		if width := ansi.StringWidth(line); width > 32 {
			t.Fatalf("collapsed Admission line width = %d, want at most 32: %q", width, line)
		}
	}
	if !strings.Contains(narrow, "> Admission health [collapsed]") || !strings.Contains(narrow, "Admission: DEGRADED") {
		t.Fatalf("narrow collapsed Admission lost its selected degraded summary:\n%s", narrow)
	}

	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	if body := model.viewport.GetContent(); !strings.Contains(body, "> Admission health") || strings.Contains(body, "Admission health [collapsed]") || !strings.Contains(body, "upstream API temporarily unavailable") {
		t.Fatalf("narrow Admission did not restore complete expanded details for wrapping:\n%s", body)
	}
}

func TestBubbleDashboardResponsiveDensityAndSelectedDetails(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	logPath := t.TempDir() + "/responsive.jsonl"
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeActivityEntries(t, activity.PathForLog(logPath), activity.Entry{
		Version: activity.CurrentVersion, ObservedAt: now.Add(-5 * time.Second), Kind: "turn", Description: "Worker turn completed",
		Operation: "edit", OperationChanged: true, TurnDelta: 1,
	})
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	active := scheduler.Run{
		Issue: 69, IssueTitle: "Responsive layouts", IssueURL: "https://github.com/acme/widgets/issues/69",
		RunID: "run-responsive", Status: scheduler.StatusRunning, PullRequest: "https://github.com/acme/widgets/pull/169",
		PID: os.Getpid(), ProcessIdentity: identity, LogPath: logPath, StartedAt: now.Add(-time.Minute),
	}
	completedAt := now.Add(-time.Minute)
	completion := scheduler.Run{
		Issue: 68, IssueTitle: "Observation model", RunID: "run-complete", Status: scheduler.StatusMerged,
		PullRequest: "https://github.com/acme/widgets/pull/168", StartedAt: now.Add(-time.Hour), CompletedAt: &completedAt,
	}
	attention := scheduler.Run{Issue: 70, IssueTitle: "Operator decision", RunID: "run-attention", Status: scheduler.StatusNeedsHuman, Error: "choose recovery", StartedAt: now.Add(-time.Minute)}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{active, completion, attention},
		Leases: []scheduler.Lease{
			{LeaseID: active.RunID, Issue: active.Issue, RunID: active.RunID},
			{LeaseID: attention.RunID, Issue: attention.Issue, RunID: attention.RunID},
		},
	}
	newModel := func(height int) bubbleDashboardModel {
		model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 160, Height: height})
		updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
		return updated.(bubbleDashboardModel)
	}

	roomy := newModel(24)
	if roomy.selectedAnchor != dashboardRunAnchor(active.RunID) {
		t.Fatalf("roomy initial selection = %q, want active Run %q", roomy.selectedAnchor, active.RunID)
	}
	roomyBody := roomy.viewport.GetContent()
	activeRow := dashboardContentLine(t, roomyBody, "#69  Responsive layouts")
	for _, want := range []string{"PR #169", "State: running", "Elapsed: 1m0s", "Deepest operation: edit", "Activity: 5s", "Turns: 1"} {
		if !strings.Contains(activeRow, want) {
			t.Fatalf("roomy compact Active row omitted %q: %q", want, activeRow)
		}
	}
	for _, omitted := range []string{active.IssueURL, active.RunID, "tokens", "Worker liveness", "PID"} {
		if strings.Contains(ansi.Strip(activeRow), omitted) {
			t.Fatalf("roomy compact Active row included %q: %q", omitted, activeRow)
		}
	}
	for _, want := range []string{"Issue URL: " + active.IssueURL, "Run: " + active.RunID, "Worker liveness: alive"} {
		if !strings.Contains(roomyBody, want) {
			t.Fatalf("roomy selected details omitted %q:\n%s", want, roomyBody)
		}
	}

	constrained := newModel(23)
	constrainedBody := constrained.viewport.GetContent()
	if strings.Contains(constrainedBody, "Issue URL:") {
		t.Fatalf("12-23 row layout expanded Run details by default:\n%s", constrainedBody)
	}
	if !strings.Contains(constrainedBody, "Recent Completions (1) [collapsed]") || strings.Contains(constrainedBody, completion.IssueTitle) {
		t.Fatalf("12-23 row layout did not collapse Recent Completions:\n%s", constrainedBody)
	}
	constrainedView := ansi.Strip(constrained.View().Content)
	if !strings.Contains(constrainedView, "Enter:Toggle") || strings.Contains(constrainedView, "Enter:Details") {
		t.Fatalf("dashboard footer did not describe Enter for Runs and sections:\n%s", constrainedView)
	}

	minimal := newModel(11)
	minimalView := ansi.Strip(minimal.View().Content)
	for _, want := range []string{"acme/widgets", "Health:1 healthy, 0 anomalous", "Attention:1", "#69", "N:jk/fb", "a d Ent"} {
		if !strings.Contains(minimalView, want) {
			t.Fatalf("sub-12-row layout omitted %q:\n%s", want, minimalView)
		}
	}
	activeAnchor := dashboardRunAnchor(active.RunID)
	if minimal.selectedAnchor != activeAnchor {
		t.Fatalf("minimal initial selection = %q, want active Run %q", minimal.selectedAnchor, active.RunID)
	}
	updated, _ := minimal.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	minimal = updated.(bubbleDashboardModel)
	if !minimal.expansionOverrides[activeAnchor] {
		t.Fatal("minimal initial Enter did not expand the selected Run")
	}
}

func TestBubbleDashboardSelectsRunDetailsWhenResizedFromMediumToRoomy(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 2)
	model := configuredNavigationTestModel(t, now, current, &dashboardTestSource{current: current})
	section := dashboardSectionAnchor("Attention Required")
	model.selectedAnchor = section
	model.refreshViewport(dashboardSelection{identity: section, valid: true})

	updated, _ := model.Update(tea.WindowSizeMsg{Width: 64, Height: 24})
	model = updated.(bubbleDashboardModel)
	if model.selectedAnchor != dashboardRunAnchor("run-0") {
		t.Fatalf("roomy resize selection = %q, want first Run", model.selectedAnchor)
	}
	if !strings.Contains(model.viewport.GetContent(), "Run: run-0") {
		t.Fatalf("roomy resize did not show selected Run details:\n%s", model.viewport.GetContent())
	}
}

func TestBubbleDashboardPreservesSelectedRunWhenResizedToMinimal(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 4)
	model := configuredNavigationTestModel(t, now, current, &dashboardTestSource{current: current})
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 64, Height: 24})
	model = updated.(bubbleDashboardModel)

	selected := dashboardRunAnchor("run-2")
	model.selectedAnchor = selected
	model.refreshViewport(dashboardSelection{identity: selected, relative: model.dashboardBodyStart(), valid: true})
	updated, _ = model.Update(tea.WindowSizeMsg{Width: 64, Height: 11})
	model = updated.(bubbleDashboardModel)

	if model.selectedAnchor != selected {
		t.Fatalf("minimal resize selection = %q, want %q", model.selectedAnchor, selected)
	}
	view := ansi.Strip(model.View().Content)
	if !strings.Contains(view, "> #3") {
		t.Fatalf("minimal resize did not keep the non-first selected Run visible:\n%s", view)
	}
}

func TestBubbleDashboardEnterExpandsRunsAndSections(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	completedAt := now.Add(-time.Minute)
	completion := scheduler.Run{Issue: 68, IssueTitle: "Only completion fields", RunID: "completion-secret", Status: scheduler.StatusMerged, PullRequest: "https://github.com/acme/widgets/pull/168", StartedAt: now.Add(-time.Hour), CompletedAt: &completedAt}
	secondCompletedAt := now.Add(-2 * time.Minute)
	thirdCompletedAt := now.Add(-3 * time.Minute)
	fourthCompletedAt := now.Add(-4 * time.Minute)
	secondCompletion := scheduler.Run{Issue: 67, IssueTitle: "Second completion", RunID: "completion-second", Status: scheduler.StatusMerged, StartedAt: now.Add(-time.Hour), CompletedAt: &secondCompletedAt}
	thirdCompletion := scheduler.Run{Issue: 66, IssueTitle: "Third completion", RunID: "completion-third", Status: scheduler.StatusMerged, StartedAt: now.Add(-time.Hour), CompletedAt: &thirdCompletedAt}
	fourthCompletion := scheduler.Run{Issue: 65, IssueTitle: "Fourth completion", RunID: "completion-fourth", Status: scheduler.StatusMerged, StartedAt: now.Add(-time.Hour), CompletedAt: &fourthCompletedAt}
	active := scheduler.Run{Issue: 69, IssueTitle: "Expandable", IssueURL: "https://github.com/acme/widgets/issues/69", RunID: "run-expandable", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Minute)}
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1, Runs: []scheduler.Run{active, completion, secondCompletion, thirdCompletion, fourthCompletion}, Leases: []scheduler.Lease{{LeaseID: active.RunID, Issue: active.Issue, RunID: active.RunID}}}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 72, Height: 18})
	model.dashboard.source = &dashboardTestSource{current: current}
	model.dashboard.update(current)
	model.selectedAnchor = dashboardRunAnchor(active.RunID)
	model.refreshViewport(dashboardSelection{identity: model.selectedAnchor, valid: true})
	if strings.Contains(model.viewport.GetContent(), "Issue URL:") {
		t.Fatal("constrained Run started expanded")
	}
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	if !strings.Contains(model.viewport.GetContent(), "Issue URL: "+active.IssueURL) {
		t.Fatalf("Enter did not expand selected Run:\n%s", model.viewport.GetContent())
	}
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	if strings.Contains(model.viewport.GetContent(), "Issue URL:") {
		t.Fatal("second Enter did not collapse selected Run")
	}

	model.selectedAnchor = dashboardSectionAnchor("Recent Completions")
	model.refreshViewport(dashboardSelection{identity: model.selectedAnchor, valid: true})
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	if !strings.Contains(model.viewport.GetContent(), completion.IssueTitle) {
		t.Fatalf("Enter did not expand collapsed completion section:\n%s", model.viewport.GetContent())
	}
	fourthAnchor := dashboardRunAnchor(fourthCompletion.RunID)
	if !strings.Contains(model.viewport.GetContent(), fourthCompletion.IssueTitle) {
		t.Fatalf("expanded completion section remained capped before %q:\n%s", fourthCompletion.IssueTitle, model.viewport.GetContent())
	}
	if _, exists := model.anchorVisualLine(fourthAnchor); !exists {
		t.Fatalf("expanded completion section omitted navigation anchor %q", fourthAnchor)
	}
	model.selectedAnchor = fourthAnchor
	model.refreshViewport(dashboardSelection{identity: fourthAnchor, valid: true})
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	if details := dashboardSectionOutput(t, model.viewport.GetContent(), "Recent Completions", ""); !strings.Contains(details, "Issue: #65  Fourth completion") {
		t.Fatalf("additional Completion could not be expanded:\n%s", details)
	}

	model.selectedAnchor = dashboardRunAnchor(completion.RunID)
	model.refreshViewport(dashboardSelection{identity: model.selectedAnchor, valid: true})
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	completionDetails := dashboardSectionOutput(t, model.viewport.GetContent(), "Recent Completions", "")
	for _, want := range []string{"Issue: #68  Only completion fields", "Pull request: PR #168", "Elapsed: 59m0s", "Completed: 1m0s ago"} {
		if !strings.Contains(completionDetails, want) {
			t.Fatalf("expanded Completion omitted %q:\n%s", want, completionDetails)
		}
	}
	for _, omitted := range []string{"Worker liveness", "Activity age", "Turns:", completion.RunID, "State:"} {
		if strings.Contains(completionDetails, omitted) {
			t.Fatalf("expanded Completion included %q:\n%s", omitted, completionDetails)
		}
	}
}

func TestCompactDashboardActiveRowPrioritizesElapsedBeforeVariableFields(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	observed := statusRun{
		run: scheduler.Run{
			Issue: 69, IssueTitle: "Responsive layouts", RunID: "run-responsive", Status: scheduler.StatusRunning,
			PullRequest: "https://github.com/acme/widgets/pull/169", StartedAt: now.Add(-time.Minute),
		},
		observation: runObservation{
			metrics: followMetrics{operation: strings.Repeat("deep-operation-", 8)},
			process: followObservation{
				workerLivenessState: workerLivenessAbsent,
				supervision:         "UNSUPERVISED",
			},
		},
	}
	full := compactDashboardRun(observed, now, false, 68)
	for _, variable := range []string{"Liveness: missing", "Deepest operation:"} {
		if elapsed, field := strings.Index(full, "Elapsed: 1m0s"), strings.Index(full, variable); elapsed < 0 || field < 0 || elapsed > field {
			t.Fatalf("elapsed did not precede %q in compact Active row: %q", variable, full)
		}
	}
	row := truncateDashboardContent("  "+full, 70)
	if !strings.Contains(row, "Elapsed: 1m0s") {
		t.Fatalf("compact Active row dropped elapsed behind variable fields: %q", row)
	}
}

func TestBubbleDashboardCompactRowsTruncateAndExpandedDetailsWrap(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	urlPrefix := "https://github.com/acme/widgets/issues/69/start/"
	urlSuffix := "reachable-detail-suffix"
	issueURL := urlPrefix + strings.Repeat("segment/", 60) + "x" + urlSuffix
	run := scheduler.Run{Issue: 69, IssueTitle: strings.Repeat("wide界", 20), IssueURL: issueURL, RunID: "run-wide", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Minute)}
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}}}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 32, Height: 18})
	model.dashboard.source = &dashboardTestSource{current: current}
	model.dashboard.update(current)
	model.selectedAnchor = dashboardRunAnchor(run.RunID)
	model.refreshViewport(dashboardSelection{identity: model.selectedAnchor, valid: true})
	row := dashboardContentLine(t, model.viewport.GetContent(), "#69")
	if width := ansi.StringWidth(row); width > 32 {
		t.Fatalf("compact row width = %d, want at most 32: %q", width, row)
	}
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(bubbleDashboardModel)
	if !strings.Contains(model.viewport.GetContent(), issueURL) {
		t.Fatalf("expanded details were truncated instead of retained for wrapping:\n%s", model.viewport.GetContent())
	}
	initial := ansi.Strip(model.View().Content)
	if !strings.Contains(initial, "Issue URL:") || strings.Contains(initial, urlSuffix) {
		t.Fatalf("expanded detail did not begin as an offscreen wrapped suffix:\n%s", initial)
	}

	reached := false
	for range 128 {
		updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
		model = updated.(bubbleDashboardModel)
		rendered := ansi.Strip(model.View().Content)
		if !strings.Contains(rendered, urlSuffix) {
			continue
		}
		reached = true
		for _, line := range strings.Split(rendered, "\n") {
			if strings.Contains(line, urlSuffix) && strings.Contains(line, "Issue URL:") {
				t.Fatalf("expanded detail suffix did not render on a later wrapped visual line: %q", line)
			}
		}
		break
	}
	if !reached {
		t.Fatalf("expanded detail suffix was not reachable by scrolling:\n%s", ansi.Strip(model.View().Content))
	}
	for _, line := range strings.Split(ansi.Strip(model.View().Content), "\n") {
		if width := lipgloss.Width(line); width > 32 {
			t.Fatalf("wrapped viewport line width = %d, want at most 32: %q", width, line)
		}
	}
}

func TestBubbleDashboardExpandedOperationalMessagesWrapAndRemainScrollable(t *testing.T) {
	const width = 28
	messagePrefix := "candidate discovery failed: "
	messageSuffix := "retry-with-manual-token"
	message := messagePrefix + strings.Repeat("upstream-unavailable/", 40) + messageSuffix
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: width, Height: 12})
	model.dashboard.update(current)
	model.dashboard.recordMessage(message)
	operational := dashboardSectionAnchor("Operational messages")
	model.selectedAnchor = operational
	model.refreshViewport(dashboardSelection{identity: operational, relative: model.dashboardBodyStart(), valid: true})

	if !strings.Contains(model.viewport.GetContent(), messageSuffix) {
		t.Fatalf("expanded Operational message lost its actionable suffix:\n%s", model.viewport.GetContent())
	}
	initial := ansi.Strip(model.View().Content)
	if !strings.Contains(initial, "Operational messages (1)") || strings.Contains(initial, messageSuffix) {
		t.Fatalf("Operational message did not begin with its suffix offscreen:\n%s", initial)
	}

	sawPrefix := false
	reached := false
	for range 128 {
		updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
		model = updated.(bubbleDashboardModel)
		rendered := ansi.Strip(model.View().Content)
		for _, line := range strings.Split(rendered, "\n") {
			if lipgloss.Width(line) > width {
				t.Fatalf("wrapped Operational message overflowed width %d: %q", width, line)
			}
			if strings.Contains(line, "candidate") {
				sawPrefix = true
			}
			if strings.Contains(line, messageSuffix) {
				if !sawPrefix || strings.Contains(line, "candidate") {
					t.Fatalf("Operational suffix did not render on a later wrapped visual line: %q", line)
				}
				reached = true
			}
		}
		if reached {
			break
		}
	}
	if !reached {
		t.Fatalf("Operational message suffix was not reachable by scrolling:\n%s", ansi.Strip(model.View().Content))
	}
}

type dashboardPreviewOnlySource struct {
	current state.State
}

func (s *dashboardPreviewOnlySource) Preview() (state.State, bool, error) {
	return s.current, false, nil
}

func TestBubbleDashboardPromotesOnlyAnomalousLivenessInCompactRows(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	identity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	runs := []scheduler.Run{
		{Issue: 1, IssueTitle: "Healthy", RunID: "healthy", Status: scheduler.StatusRunning, PID: os.Getpid(), ProcessIdentity: identity, StartedAt: now},
		{Issue: 2, IssueTitle: "Missing", RunID: "missing", Status: scheduler.StatusRunning, StartedAt: now},
	}
	current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 2, Runs: runs, Leases: []scheduler.Lease{{LeaseID: "healthy", Issue: 1, RunID: "healthy"}, {LeaseID: "missing", Issue: 2, RunID: "missing"}}}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 120, Height: 11})
	model.dashboard.source = &dashboardPreviewOnlySource{current: current}
	model.dashboard.update(current)
	model.refreshViewport(dashboardSelection{})
	healthy := dashboardContentLine(t, model.viewport.GetContent(), "#1  Healthy")
	missing := dashboardContentLine(t, model.viewport.GetContent(), "#2  Missing")
	if strings.Contains(healthy, "Liveness:") || strings.Contains(healthy, "PID") {
		t.Fatalf("healthy liveness was promoted in compact row: %q", healthy)
	}
	if !strings.Contains(healthy, "Supervision: unsupervised") {
		t.Fatalf("unsupervised healthy Worker anomaly was not promoted: %q", healthy)
	}
	for _, want := range []string{"Liveness: missing", "Supervision: unsupervised"} {
		if !strings.Contains(missing, want) {
			t.Fatalf("anomalous compact row omitted %q: %q", want, missing)
		}
	}
	if view := ansi.Strip(model.View().Content); !strings.Contains(view, "Health:0 healthy, 2 anomalous") {
		t.Fatalf("minimal dashboard omitted anomalous Worker health:\n%s", view)
	}
}

func dashboardContentLine(t *testing.T, content, contains string) string {
	t.Helper()
	for _, line := range strings.Split(content, "\n") {
		if strings.Contains(line, contains) {
			return line
		}
	}
	t.Fatalf("dashboard content omitted line containing %q:\n%s", contains, content)
	return ""
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
		model.refreshViewport(model.currentSelection())
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
			{name: "Diagnostics", variants: []string{"d:Diagnostics", "a d Ent"}},
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

func TestBubbleDashboardFrameKeepsCapacityAndHealthAsSeparateMetadata(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	run := scheduler.Run{Issue: 69, RunID: "active", Status: scheduler.StatusRunning, StartedAt: now.Add(-time.Minute)}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 18, Height: 12})
	model.dashboard.update(current)
	model.refreshViewport(dashboardSelection{})
	frame := model.dashboardFrame()
	chrome := ansi.Strip(strings.Join(append(append([]string(nil), frame.chrome.top...), frame.chrome.bottom...), "\n"))
	normalized := strings.Join(strings.Fields(chrome), " ")
	for _, want := range []string{"W:1u/1a/2t", "Worker health: 0 healthy, 1 anomalous"} {
		if !strings.Contains(normalized, want) {
			t.Fatalf("production dashboard frame malformed or omitted %q:\n%s", want, chrome)
		}
	}
}

func TestBubbleDashboardShortNarrowChromePrioritizesHealthAndAttention(t *testing.T) {
	metadata := dashboardProjectionMetadata{
		repository: "acme/widgets",
		capacity:   dashboardCapacity{configured: true, used: 1, available: 2, total: 3},
		healthy:    1,
		anomalous:  2,
	}
	header := strings.Split(minimalDashboardHeader(metadata, 3, 0), "\n")[1:]
	footer := strings.Split(minimalDashboardFooter(strings.Join([]string{
		"Runner stage: Running",
		"Next Ctrl-C: start Drain and stop Admission",
		dashboardNavigationHelp,
	}, "\n")), "\n")
	chrome := dashboardChromeLines(header, footer, 0, dashboardRunning, dashboardStyler{}, 24, 7)
	plain := strings.Join(append(append([]string(nil), chrome.top...), chrome.bottom...), "\n")
	normalized := strings.Join(strings.Fields(plain), " ")
	for _, want := range []string{"Health:1 healthy, 2 anomalous", "Attention:3"} {
		if !strings.Contains(normalized, want) {
			t.Fatalf("short narrow dashboard chrome omitted %q:\n%s", want, plain)
		}
	}
}

func TestBubbleDashboardAdaptiveNavigationIncludesEnterGuidance(t *testing.T) {
	footer := []string{
		"Runner stage: Running",
		"Next Ctrl-C: start Drain and stop Admission",
		dashboardNavigationHelp,
	}
	chrome := dashboardChromeLines(
		[]string{
			"Repository: acme/widgets",
			"Worker capacity: 1 used | 2 available | 3 total",
			"Worker health: 1 healthy, 0 anomalous",
		},
		footer,
		0,
		dashboardRunning,
		dashboardStyler{},
		50,
		7,
	)
	fixedFooter := strings.Join(chrome.bottom, "\n")
	if !strings.Contains(fixedFooter, dashboardCompactNavigationHelp) {
		t.Fatalf("adaptive navigation omitted compact Diagnostics and Enter guidance: %#v", chrome)
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
	for _, want := range []string{"^C:start Drain and stop Admission", dashboardCompactNavigationHelp} {
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

func TestBubbleDashboardFinalFlushWaitsForRecoveryNoticeBeforeAcknowledgingNaturalExit(t *testing.T) {
	recoveredAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	now := recoveredAt.Add(2 * time.Second)
	control := PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}
	session := newBubbleDashboardSession(time.Now)
	if err := session.captureFinalSummary(state.State{Version: state.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	flushDone := make(chan error, 1)
	go func() { flushDone <- session.flush(ctx) }()
	msg, err := session.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	flush, ok := msg.(dashboardFlushMsg)
	if !ok || !flush.naturalExit {
		t.Fatalf("post-Runner flush = %#v, want natural-exit marker", msg)
	}

	requestedDelay := make(chan time.Duration, 1)
	clockFired := make(chan time.Time, 1)
	model := newBubbleDashboardModel(context.Background(), control, session, TerminalDimensions{Width: 140, Height: 20})
	model.flushAfter = func(delay time.Duration) <-chan time.Time {
		requestedDelay <- delay
		return clockFired
	}
	model.dashboard.operationalEvent(runner.CandidateDiscoveryRecovered{OccurredAt: recoveredAt, Failures: 3})
	updated, command := model.Update(flush)
	model = updated.(bubbleDashboardModel)
	batch, ok := command().(tea.BatchMsg)
	if !ok || len(batch) < 1 {
		t.Fatalf("natural-exit Update command = %T, want a delayed render batch", command())
	}
	rendered := make(chan tea.Msg, 1)
	go func() { rendered <- batch[0]() }()
	if delay := <-requestedDelay; delay != 8*time.Second {
		t.Fatalf("controllable clock delay = %s, want remaining 8s", delay)
	}

	view := ansi.Strip(model.View().Content)
	for _, want := range []string{
		"Admission: stopped | Last Candidate snapshot completed successfully | Recovered 2s ago after 3 failures",
		"Runner stage: Complete; the Runner has exited",
		"Next Ctrl-C: no effect",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("natural-exit recovery hold omitted %q:\n%s", want, view)
		}
	}
	select {
	case <-flush.acknowledged:
		t.Fatal("natural-exit flush acknowledged before the remaining recovery notice duration")
	default:
	}

	clockFired <- recoveredAt.Add(admissionRecoveryNotice)
	renderedMsg := <-rendered
	select {
	case <-flush.acknowledged:
		t.Fatal("natural-exit flush acknowledged before the delayed render message was applied")
	default:
	}
	updated, _ = model.Update(renderedMsg)
	model = updated.(bubbleDashboardModel)
	if err := <-flushDone; err != nil {
		t.Fatalf("delayed natural-exit acknowledgment: %v", err)
	}
	view = ansi.Strip(model.View().Content)
	for _, want := range []string{"Admission: stopped | Last Candidate snapshot completed successfully | Recovered", "Runner stage: Complete; the Runner has exited", "Next Ctrl-C: no effect"} {
		if !strings.Contains(view, want) {
			t.Fatalf("acknowledged natural-exit frame omitted %q:\n%s", want, view)
		}
	}
	for _, stale := range []string{"Runner stage: Running", "Next Ctrl-C: start Drain and stop Admission"} {
		if strings.Contains(view, stale) {
			t.Fatalf("natural-exit recovery hold retained stale lifecycle guidance %q:\n%s", stale, view)
		}
	}

	shutdownDashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, func() time.Time { return now })
	shutdownDashboard.operationalEvent(runner.ShutdownEvent{Stage: runner.ShutdownStageDraining})
	shutdownDashboard.markNaturalExit()
	_, _, footer := shutdownDashboard.renderParts(now)
	if !strings.Contains(footer, "Runner stage: Draining") || strings.Contains(footer, "Complete; the Runner has exited") {
		t.Fatalf("natural-exit marker replaced an established shutdown stage: %q", footer)
	}
}

func TestBubbleDashboardPromptShutdownFlushUsesOneRenderFrameDuringRecoveryNotice(t *testing.T) {
	recoveredAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	now := recoveredAt.Add(2 * time.Second)
	session := newBubbleDashboardSession(time.Now)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	flushDone := make(chan error, 1)
	go func() { flushDone <- session.flush(ctx) }()
	msg, err := session.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	flush, ok := msg.(dashboardFlushMsg)
	if !ok || flush.naturalExit {
		t.Fatalf("prompt shutdown flush = %#v, want non-natural flush", msg)
	}

	requestedDelay := make(chan time.Duration, 1)
	clockFired := make(chan time.Time, 1)
	model := newBubbleDashboardModel(
		context.Background(),
		PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}},
		session,
		TerminalDimensions{Width: 100, Height: 20},
	)
	model.flushAfter = func(delay time.Duration) <-chan time.Time {
		requestedDelay <- delay
		return clockFired
	}
	model.dashboard.operationalEvent(runner.CandidateDiscoveryRecovered{OccurredAt: recoveredAt, Failures: 3})
	updated, command := model.Update(flush)
	model = updated.(bubbleDashboardModel)
	batch, ok := command().(tea.BatchMsg)
	if !ok || len(batch) < 1 {
		t.Fatalf("prompt shutdown Update command = %T, want a render batch", command)
	}
	rendered := make(chan tea.Msg, 1)
	go func() { rendered <- batch[0]() }()
	if delay := <-requestedDelay; delay != time.Second/30 {
		t.Fatalf("prompt shutdown flush delay = %s, want one render frame %s", delay, time.Second/30)
	}
	clockFired <- now.Add(time.Second / 30)
	updated, _ = model.Update(<-rendered)
	_ = updated.(bubbleDashboardModel)
	if err := <-flushDone; err != nil {
		t.Fatalf("prompt shutdown terminal restoration: %v", err)
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
