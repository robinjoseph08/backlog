package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/creack/pty"
	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"golang.org/x/term"
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
	if strings.Contains(body, "\x1b]8;;javascript:") || strings.Contains(body, "PR #not-a-number") || strings.Contains(body, "https://attacker.test") {
		t.Fatalf("unsafe resource metadata became a hyperlink, leaked an attacker target, or guessed a PR number:\n%q", body)
	}

	for _, width := range []int{160, 24} {
		if width != 160 {
			updated, _ = model.Update(tea.WindowSizeMsg{Width: width, Height: 24})
			model = updated.(bubbleDashboardModel)
		}
		rendered := model.View().Content
		for _, target := range []string{
			"\x1b]8;;https://github.com/acme/widgets/issues/12\x1b\\",
			"\x1b]8;;https://github.com/acme/widgets/pull/112\x1b\\",
		} {
			if !strings.Contains(rendered, target) {
				t.Fatalf("%d-column rendered view stripped linked target %q:\n%q", width, target, rendered)
			}
		}
		for _, line := range strings.Split(rendered, "\n") {
			opens := strings.Count(line, "\x1b]8;;https://")
			closes := strings.Count(line, "\x1b]8;;\x1b\\")
			if opens != closes {
				t.Fatalf("%d-column rendered row has %d OSC 8 opens and %d closes: %q", width, opens, closes, line)
			}
		}
	}
}

func TestBubbleDashboardKeyboardOpensSelectedIssueAndPullRequestAsynchronously(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{{
			Issue: 12, IssueTitle: "Open resources", IssueURL: "https://github.com/acme/widgets/issues/12",
			RunID: "current", Status: scheduler.StatusWaitingForMerge, PullRequest: "https://github.com/acme/widgets/pull/112", StartedAt: now.Add(-time.Hour),
		}},
		Leases: []scheduler.Lease{{LeaseID: "current", Issue: 12, RunID: "current"}},
	}
	type openInvocation struct {
		target   string
		deadline time.Time
	}
	opened := make(chan openInvocation, 1)
	release := make(chan struct{})
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now },
		OpenURL: func(ctx context.Context, target string) error {
			deadline, hasDeadline := ctx.Deadline()
			if !hasDeadline {
				return errors.New("opener context has no deadline")
			}
			opened <- openInvocation{target: target, deadline: deadline}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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
		key := tea.KeyPressMsg(tea.Key{Code: test.key, Text: string(test.key)})
		updated, command := model.Update(key)
		model = updated.(bubbleDashboardModel)
		if command == nil {
			t.Fatalf("%c did not return an asynchronous opener command", test.key)
		}
		select {
		case invocation := <-opened:
			t.Fatalf("%c invoked opener inside Update: %#v", test.key, invocation)
		default:
		}

		result := make(chan tea.Msg, 1)
		go func() { result <- command() }()
		var invocation openInvocation
		select {
		case invocation = <-opened:
		case <-time.After(time.Second):
			t.Fatalf("%c asynchronous opener did not start", test.key)
		}
		if invocation.target != test.want {
			t.Fatalf("%c opened %q, want %q", test.key, invocation.target, test.want)
		}
		remaining := time.Until(invocation.deadline)
		if remaining <= 0 || remaining > dashboardURLOpenTimeout {
			t.Fatalf("%c opener deadline remaining = %s, want within (0, %s]", test.key, remaining, dashboardURLOpenTimeout)
		}
		updated, repeated := model.Update(key)
		model = updated.(bubbleDashboardModel)
		if repeated != nil {
			t.Fatalf("%c key repeat started another opener while one was in flight", test.key)
		}

		release <- struct{}{}
		var message tea.Msg
		select {
		case message = <-result:
		case <-time.After(time.Second):
			t.Fatalf("%c asynchronous opener did not finish", test.key)
		}
		openResult, ok := message.(dashboardOpenURLResultMsg)
		if !ok || openResult.err != nil {
			t.Fatalf("%c opener result = %#v", test.key, message)
		}
		updated, _ = model.Update(openResult)
		model = updated.(bubbleDashboardModel)
	}

	ctx, cancel := context.WithCancel(context.Background())
	model.ctx = ctx
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'o', Text: "o"}))
	model = updated.(bubbleDashboardModel)
	cancel()
	result := command().(dashboardOpenURLResultMsg)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("canceled opener result = %#v, want context cancellation", result)
	}
}

func TestBubbleDashboardKeyboardOpensSynthesizedIssueOnlyAndRejectsMalformedResources(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 3,
		Runs: []scheduler.Run{
			{Issue: 13, IssueTitle: "Historical issue", RunID: "historical", Status: scheduler.StatusFailed, StartedAt: now.Add(-time.Hour)},
			{Issue: 14, IssueTitle: "Malformed issue", IssueURL: "javascript:alert(1)", RunID: "malformed-issue", Status: scheduler.StatusRunning, StartedAt: now},
			{
				Issue: 15, IssueTitle: "Malformed pull request", IssueURL: "https://github.com/acme/widgets/issues/15",
				RunID: "malformed-pr", Status: scheduler.StatusWaitingForMerge,
				PullRequest: "https://github.com/acme/widgets/pull/not-a-number\x1b]8;;https://attacker.test", StartedAt: now,
			},
		},
		Leases: []scheduler.Lease{
			{LeaseID: "malformed-issue", Issue: 14, RunID: "malformed-issue"},
			{LeaseID: "malformed-pr", Issue: 15, RunID: "malformed-pr"},
		},
	}
	var opened []string
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now },
		OpenURL: func(_ context.Context, target string) error {
			opened = append(opened, target)
			return nil
		},
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 20})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
	model = updated.(bubbleDashboardModel)

	selectRun := func(runID string) {
		t.Helper()
		anchor := dashboardRunAnchor(runID)
		if _, exists := model.anchorVisualLine(anchor); !exists {
			t.Fatalf("Run %q anchor missing", runID)
		}
		model.selectedAnchor = anchor
	}
	selectRun("historical")
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'o', Text: "o"}))
	model = updated.(bubbleDashboardModel)
	if command == nil {
		t.Fatal("o did not open the synthesized historical issue URL")
	}
	result := command().(dashboardOpenURLResultMsg)
	if result.err != nil {
		t.Fatalf("synthesized historical issue opener result = %#v", result)
	}
	updated, _ = model.Update(result)
	model = updated.(bubbleDashboardModel)
	if !reflect.DeepEqual(opened, []string{"https://github.com/acme/widgets/issues/13"}) {
		t.Fatalf("opened URLs = %q, want only synthesized historical issue URL", opened)
	}

	for _, test := range []struct {
		runID string
		key   rune
	}{
		{runID: "malformed-issue", key: 'o'},
		{runID: "malformed-pr", key: 'p'},
	} {
		selectRun(test.runID)
		updated, command = model.Update(tea.KeyPressMsg(tea.Key{Code: test.key, Text: string(test.key)}))
		model = updated.(bubbleDashboardModel)
		if command != nil {
			t.Fatalf("%c returned an opener command for malformed metadata on %q", test.key, test.runID)
		}
	}
	if len(opened) != 1 {
		t.Fatalf("malformed metadata reached the opener: %q", opened)
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
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 2})
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
	firstDiagnosticID := model.urlDiagnosticID
	updated, newerExpiry := model.Update(dashboardOpenURLResultMsg{resource: dashboardIssueResource, err: errors.New("newer opener failure")})
	model = updated.(bubbleDashboardModel)
	if newerExpiry == nil || !strings.Contains(ansi.Strip(model.View().Content), "Open issue failed: newer opener failure") {
		t.Fatalf("newer opener failure did not replace the diagnostic:\n%s", ansi.Strip(model.View().Content))
	}
	updated, _ = model.Update(dashboardURLDiagnosticExpiredMsg{id: firstDiagnosticID})
	model = updated.(bubbleDashboardModel)
	if !strings.Contains(ansi.Strip(model.View().Content), "newer opener failure") {
		t.Fatal("older diagnostic timer cleared the newer diagnostic")
	}
	updated, _ = model.Update(dashboardURLDiagnosticExpiredMsg{id: model.urlDiagnosticID})
	model = updated.(bubbleDashboardModel)
	if strings.Contains(ansi.Strip(model.View().Content), "newer opener failure") {
		t.Fatal("newer URL opener diagnostic did not expire")
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
		selectFirstRunForTest(t, &model)
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
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	run := scheduler.Run{
		Issue: 71, IssueTitle: "Linked resource row", IssueURL: "https://github.com/acme/widgets/issues/71",
		RunID: "linked", Status: scheduler.StatusWaitingForMerge, PullRequest: "https://github.com/acme/widgets/pull/171", StartedAt: now,
	}
	current := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
	}
	opened := 0
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: func() time.Time { return now },
		OpenURL: func(context.Context, string) error {
			opened++
			return nil
		},
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 60, Height: 12})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
	model = updated.(bubbleDashboardModel)
	anchor := dashboardRunAnchor(run.RunID)
	model.selectedAnchor = anchor
	model.refreshViewport(dashboardSelection{identity: anchor, relative: model.dashboardBodyStart(), valid: true})
	line, exists := model.anchorVisualLine(anchor)
	if !exists || !model.visualLineVisible(line) || !strings.Contains(model.View().Content, "\x1b]8;;"+run.IssueURL) {
		t.Fatalf("linked Run row is not visible for click coverage:\n%q", model.View().Content)
	}
	selection, offset := model.selectedAnchor, model.viewport.YOffset()
	rowY := model.dashboardBodyStart() + line - offset
	updated, command := model.Update(tea.MouseClickMsg{Button: tea.MouseLeft, X: 4, Y: rowY})
	model = updated.(bubbleDashboardModel)
	if command != nil || opened != 0 || model.selectedAnchor != selection || model.viewport.YOffset() != offset {
		t.Fatalf("application handled linked-row mouse click: command nil = %t, opens = %d, selection = %q, offset = %d", command == nil, opened, model.selectedAnchor, model.viewport.YOffset())
	}

	for index := range 30 {
		model.dashboard.recordMessage(fmt.Sprintf("event line %02d", index))
	}
	updated, _ = model.Update(dashboardElapsedMsg(now))
	model = updated.(bubbleDashboardModel)
	model.viewport.GotoTop()
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
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, source)
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
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, &dashboardTestSource{current: current})
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
			model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, &dashboardTestSource{current: current})
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
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, source)
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
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, source)
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
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, source)
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
			model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, source)
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
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, &dashboardTestSource{current: current})
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

func configuredNavigationTestModelWithFirstRunSelected(t *testing.T, now time.Time, current state.State, source *dashboardTestSource) bubbleDashboardModel {
	t.Helper()
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 64, Height: 14})
	updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: source})
	model = updated.(bubbleDashboardModel)
	selectFirstRunForTest(t, &model)
	return model
}

func selectFirstRunForTest(t *testing.T, model *bubbleDashboardModel) {
	t.Helper()
	for _, anchor := range model.layout.anchors {
		if !strings.HasPrefix(anchor.identity, "run:") {
			continue
		}
		model.selectedAnchor = anchor.identity
		model.refreshViewport(dashboardSelection{identity: anchor.identity, relative: model.dashboardBodyStart(), valid: true})
		return
	}
	t.Fatal("dashboard has no Run anchor")
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

func TestBubbleDashboardStartsWithAdmissionHealthSelected(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 1)
	admission := dashboardSectionAnchor("Admission health")

	for _, height := range []int{24, 23, 11} {
		t.Run(fmt.Sprintf("height-%d", height), func(t *testing.T) {
			model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: func() time.Time { return now }}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 160, Height: height})
			updated, _ := model.Update(dashboardConfiguredMsg{initial: current, source: &dashboardTestSource{current: current}})
			model = updated.(bubbleDashboardModel)

			if model.selectedAnchor != admission {
				t.Fatalf("initial selection = %q, want %q", model.selectedAnchor, admission)
			}
			if !strings.Contains(ansi.Strip(model.View().Content), "> Admission health") {
				t.Fatalf("initial view did not select Admission health:\n%s", ansi.Strip(model.View().Content))
			}
		})
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
		model = updated.(bubbleDashboardModel)
		selected := dashboardRunAnchor(active.RunID)
		model.selectedAnchor = selected
		model.refreshViewport(dashboardSelection{identity: selected, relative: model.dashboardBodyStart(), valid: true})
		return model
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

func TestBubbleDashboardPreservesSelectedSectionWhenResizedFromMediumToRoomy(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 2)
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, &dashboardTestSource{current: current})
	section := dashboardSectionAnchor("Attention Required")
	model.selectedAnchor = section
	model.refreshViewport(dashboardSelection{identity: section, valid: true})

	updated, _ := model.Update(tea.WindowSizeMsg{Width: 64, Height: 24})
	model = updated.(bubbleDashboardModel)
	if model.selectedAnchor != section {
		t.Fatalf("roomy resize selection = %q, want %q", model.selectedAnchor, section)
	}
	if !strings.Contains(ansi.Strip(model.View().Content), "> Attention Required") {
		t.Fatalf("roomy resize did not preserve the selected section:\n%s", ansi.Strip(model.View().Content))
	}
}

func TestBubbleDashboardPreservesSelectedRunWhenResizedToMinimal(t *testing.T) {
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	current := navigationTestState(now, 4)
	model := configuredNavigationTestModelWithFirstRunSelected(t, now, current, &dashboardTestSource{current: current})
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

type oneShotPresentationFailureWriter struct {
	bytes.Buffer
	panic  bool
	failed bool
}

func (w *oneShotPresentationFailureWriter) Write(content []byte) (int, error) {
	written, _ := w.Buffer.Write(content)
	if w.failed || !bytes.Contains(content, []byte("Backlog Run Dashboard")) {
		return written, nil
	}
	w.failed = true
	if w.panic {
		panic("terminal writer panic")
	}
	return written, errors.New("terminal output lost")
}

func TestBubbleDashboardFailureRestoresTerminalBeforeStaticErrorResult(t *testing.T) {
	for _, test := range []struct {
		name      string
		panic     bool
		wantError string
	}{
		{name: "output loss", wantError: "terminal output lost"},
		{name: "recovered TUI panic", panic: true, wantError: "terminal output panic: terminal writer panic"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := &oneShotPresentationFailureWriter{panic: test.panic}
			session := newBubbleDashboardSession(time.Now)
			host := runnerHost{terminal: TerminalDependencies{
				Input: strings.NewReader(""), Output: output,
				Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 80, Height: 24}, nil },
				ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
			}}
			err := host.run(context.Background(), func(signals <-chan lifecycleSignal, _ func(runner.OperationalEvent)) error {
				event := <-signals
				if event.signal != syscall.SIGTERM {
					t.Errorf("presentation failure signal = %v, want SIGTERM", event.signal)
				}
				event.accept()
				return &runner.SignalExit{Code: 143}
			}, session.presentation)
			var failure *PresentationFailure
			if !errors.As(err, &failure) || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("host failure = %v, want recovered presentation failure containing %q", err, test.wantError)
			}
			session.setResult(context.Background(), err)
			if summaryErr := session.printFinalSummary(output); summaryErr != nil {
				t.Fatalf("static error result: %v", summaryErr)
			}

			raw := output.String()
			for _, control := range []string{"\x1b[?1049h", "\x1b[?1049l", "\x1b[?25l", "\x1b[?25h"} {
				if !strings.Contains(raw, control) {
					t.Fatalf("presentation failure output missing restoration control %q: %q", control, raw)
				}
			}
			restore := strings.LastIndex(raw, "\x1b[?1049l")
			summary := strings.Index(raw, "Final aggregate summary")
			if restore < 0 || summary < restore || !strings.Contains(raw[summary:], "Final outcome: Error: presentation failed:") {
				t.Fatalf("static error result was not printed after restoration: %q", raw)
			}
		})
	}
}

type ptyPresentationInput struct {
	*os.File
	readFinished chan struct{}
	finishOnce   sync.Once
}

func newPTYPresentationInput(file *os.File) *ptyPresentationInput {
	return &ptyPresentationInput{File: file, readFinished: make(chan struct{})}
}

func (r *ptyPresentationInput) Read(content []byte) (int, error) {
	defer r.finishOnce.Do(func() { close(r.readFinished) })
	if len(content) == 0 {
		return 0, nil
	}
	if _, err := r.File.Read(content[:1]); err != nil {
		return 0, err
	}
	return 0, io.EOF
}

func (r *ptyPresentationInput) Close() error { return nil }

// Bubble Tea v2.0.8's kill path closes its cancelreader without waiting for
// the input loop. Finish all PTY descriptor access before tests induce it.
func finishPTYPresentationInput(ctx context.Context, primary *os.File, input *ptyPresentationInput) error {
	if _, err := primary.Write([]byte{0}); err != nil {
		return fmt.Errorf("make PTY input readable: %w", err)
	}
	select {
	case <-input.readFinished:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("finish PTY presentation input: %w", ctx.Err())
	}
}

type ptyPresentationOutput struct {
	*os.File
	output                 synchronizedBuffer
	enteredAlternateScreen chan struct{}
	entered                atomic.Bool
	fail                   atomic.Bool
	failed                 atomic.Bool
	failedWriteSize        atomic.Int64
	failedWriteAttemptSize atomic.Int64
}

func newPTYPresentationOutput(file *os.File) *ptyPresentationOutput {
	return &ptyPresentationOutput{File: file, enteredAlternateScreen: make(chan struct{})}
}

func (w *ptyPresentationOutput) Write(content []byte) (int, error) {
	failThisWrite := len(content) > 1 && w.fail.Load() && w.failed.CompareAndSwap(false, true)
	writeContent := content
	if failThisWrite {
		writeContent = content[:len(content)/2]
	}
	written, err := w.File.Write(writeContent)
	_, _ = w.output.Write(writeContent[:written])
	if bytes.Contains(writeContent[:written], []byte("\x1b[?1049h")) && w.entered.CompareAndSwap(false, true) {
		close(w.enteredAlternateScreen)
	}
	if err == nil && failThisWrite {
		w.failedWriteSize.Store(int64(written))
		w.failedWriteAttemptSize.Store(int64(len(content)))
		return written, errors.New("terminal output lost")
	}
	return written, err
}

func (w *ptyPresentationOutput) String() string { return w.output.String() }

func TestBubbleDashboardPostStartFailuresRestorePTYStateBeforeStaticErrorResult(t *testing.T) {
	for _, test := range []struct {
		name       string
		modelPanic bool
	}{
		{name: "output loss"},
		{name: "model panic", modelPanic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary, terminal, err := pty.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			defer terminal.Close()
			if err := pty.Setsize(terminal, &pty.Winsize{Rows: 24, Cols: 80}); err != nil {
				t.Fatal(err)
			}

			initialState, err := term.GetState(int(terminal.Fd()))
			if err != nil {
				t.Fatal(err)
			}
			output := newPTYPresentationOutput(terminal)
			input := newPTYPresentationInput(terminal)
			go func() { _, _ = io.Copy(io.Discard, primary) }()

			now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
			var panicEnabled atomic.Bool
			var panicked atomic.Bool
			clock := func() time.Time {
				if panicEnabled.Load() {
					panicked.Store(true)
					panic("dashboard model panic")
				}
				return now
			}
			session := newBubbleDashboardSession(clock)
			host := runnerHost{terminal: TerminalDependencies{
				Input: input, Output: output,
				Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 80, Height: 24}, nil },
				ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
				Now:          clock,
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = host.run(ctx, func(signals <-chan lifecycleSignal, _ func(runner.OperationalEvent)) error {
				current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
				session.configure(current, &dashboardTestSource{current: current})
				if startupErr := session.waitForStartup(ctx); startupErr != nil {
					return startupErr
				}
				select {
				case <-output.enteredAlternateScreen:
				case <-ctx.Done():
					return ctx.Err()
				}
				if inputErr := finishPTYPresentationInput(ctx, primary, input); inputErr != nil {
					return inputErr
				}
				if test.modelPanic {
					panicEnabled.Store(true)
					session.publish(dashboardElapsedMsg(now))
				} else {
					output.fail.Store(true)
					updated := current
					updated.Repo = "acme/updated-widgets"
					session.publish(dashboardStateMsg(updated))
				}
				select {
				case event := <-signals:
					if event.signal != syscall.SIGTERM {
						t.Errorf("presentation failure signal = %v, want SIGTERM", event.signal)
					}
					event.accept()
					return &runner.SignalExit{Code: 143}
				case <-ctx.Done():
					return ctx.Err()
				}
			}, session.presentation)
			panicEnabled.Store(false)

			var failure *PresentationFailure
			if !errors.As(err, &failure) {
				t.Fatalf("post-start presentation result = %v, want presentation failure", err)
			}
			if test.modelPanic {
				if !panicked.Load() || !errors.Is(failure.Err, tea.ErrProgramPanic) {
					t.Fatalf("model panic result = %v, panicked = %t", err, panicked.Load())
				}
			} else if written, attempted := output.failedWriteSize.Load(), output.failedWriteAttemptSize.Load(); !output.failed.Load() || written <= 0 || written >= attempted || !strings.Contains(failure.Err.Error(), "terminal output lost") {
				t.Fatalf("output-loss result = %v, failed = %t, interrupted write = %d/%d bytes", err, output.failed.Load(), written, attempted)
			}

			restoredState, stateErr := term.GetState(int(terminal.Fd()))
			if stateErr != nil {
				t.Fatal(stateErr)
			}
			if !reflect.DeepEqual(restoredState, initialState) {
				t.Fatalf("terminal state after %s = %#v, want %#v", test.name, restoredState, initialState)
			}
			session.setResult(context.Background(), err)
			if summaryErr := session.printFinalSummary(output); summaryErr != nil {
				t.Fatalf("static %s result: %v", test.name, summaryErr)
			}
			content := output.String()
			enter := strings.Index(content, "\x1b[?1049h")
			hideCursor := strings.Index(content, "\x1b[?25l")
			restore := strings.LastIndex(content, "\x1b[?1049l")
			showCursor := strings.LastIndex(content, "\x1b[?25h")
			summary := strings.Index(content, "Final aggregate summary")
			if enter < 0 || hideCursor < 0 || restore < enter || showCursor < hideCursor || summary < restore || summary < showCursor || !strings.Contains(content[summary:], "Final outcome: Error: presentation failed:") {
				t.Fatalf("%s did not restore the normal screen and cursor before static output: %q", test.name, content)
			}
		})
	}
}

func TestDefaultDashboardThreeExternalSIGINTsDuringSetupPrintForceStopAfterPTYRestoration(t *testing.T) {
	root := t.TempDir()
	setupStarted := filepath.Join(root, "git-started")
	git := writeExecutable(t, `#!/bin/sh
set -eu
touch `+quote(setupStarted)+`
exec sleep 30
`)
	primary, terminal, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	defer terminal.Close()
	if err := pty.Setsize(terminal, &pty.Winsize{Rows: 24, Cols: 80}); err != nil {
		t.Fatal(err)
	}
	initialState, err := term.GetState(int(terminal.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	output := newPTYPresentationOutput(terminal)
	input := newPTYPresentationInput(terminal)
	go func() { _, _ = io.Copy(io.Discard, primary) }()
	externalSignals := make(chan os.Signal)
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- MainWithTerminal(context.Background(), []string{"run", "--git", git}, TerminalDependencies{
			Input: input, Output: output, ErrorOutput: &stderr,
			IsTerminal: func() bool { return true },
			Dimensions: func() (TerminalDimensions, error) {
				return TerminalDimensions{Width: 80, Height: 24}, nil
			},
			ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
			Signals:      externalSignals,
		})
	}()
	waitForFile(t, setupStarted)
	select {
	case <-output.enteredAlternateScreen:
	case <-time.After(10 * time.Second):
		t.Fatal("default dashboard did not enter the alternate screen during blocked setup")
	}
	inputCtx, cancelInput := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelInput()
	if err := finishPTYPresentationInput(inputCtx, primary, input); err != nil {
		t.Fatal(err)
	}
	for stage := 1; stage <= 3; stage++ {
		select {
		case externalSignals <- os.Interrupt:
		case exit := <-done:
			t.Fatalf("default dashboard exited with %d before staged SIGINT %d", exit, stage)
		case <-time.After(10 * time.Second):
			t.Fatalf("staged SIGINT %d was not accepted by the signal ingress", stage)
		}
	}

	select {
	case exit := <-done:
		if exit != 130 {
			t.Fatalf("setup force-stop exit = %d, want 130; output = %q; stderr = %q", exit, output.String(), stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("default dashboard did not finish setup force stop")
	}
	restoredState, stateErr := term.GetState(int(terminal.Fd()))
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if !reflect.DeepEqual(restoredState, initialState) {
		t.Fatalf("terminal state after setup force stop = %#v, want %#v", restoredState, initialState)
	}

	raw := output.String()
	restore := strings.LastIndex(raw, "\x1b[?1049l")
	showCursor := strings.LastIndex(raw, "\x1b[?25h")
	summary := strings.Index(raw, "Final aggregate summary")
	if restore < 0 || showCursor < 0 || summary < restore || summary < showCursor {
		t.Fatalf("setup force-stop summary preceded normal-screen or cursor restoration: %q", raw)
	}
	if !strings.Contains(raw[summary:], "Final outcome: Force stop complete\n") || strings.Contains(raw[summary:], "Final outcome: Suspension complete") {
		t.Fatalf("setup force-stop final outcome was not exact: %q", raw[summary:])
	}
	if stderr.Len() != 0 {
		t.Fatalf("setup force stop wrote stderr: %q", stderr.String())
	}
}

func TestBubbleDashboardModelPanicAfterStartupRestoresTerminalBeforeStaticErrorResult(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	var panicEnabled atomic.Bool
	var panicked atomic.Bool
	clock := func() time.Time {
		if panicEnabled.Load() {
			panicked.Store(true)
			panic("dashboard model panic")
		}
		return now
	}
	var output synchronizedBuffer
	session := newBubbleDashboardSession(clock)
	host := runnerHost{terminal: TerminalDependencies{
		Input: strings.NewReader(""), Output: &output,
		Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 80, Height: 24}, nil },
		ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
		Now:          clock,
	}}
	err := host.run(context.Background(), func(signals <-chan lifecycleSignal, _ func(runner.OperationalEvent)) error {
		current := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
		session.configure(current, &dashboardTestSource{current: current})
		if startupErr := session.waitForStartup(context.Background()); startupErr != nil {
			return startupErr
		}
		deadline := time.Now().Add(2 * time.Second)
		for !strings.Contains(output.String(), "\x1b[?1049h") {
			if time.Now().After(deadline) {
				return errors.New("dashboard did not enter the alternate screen before model panic")
			}
			time.Sleep(time.Millisecond)
		}
		panicEnabled.Store(true)
		session.publish(dashboardElapsedMsg(now))
		event := <-signals
		if event.signal != syscall.SIGTERM {
			t.Errorf("model panic signal = %v, want SIGTERM", event.signal)
		}
		event.accept()
		return &runner.SignalExit{Code: 143}
	}, session.presentation)

	var failure *PresentationFailure
	if !panicked.Load() || !errors.As(err, &failure) || !errors.Is(failure.Err, tea.ErrProgramPanic) {
		t.Fatalf("post-start model panic result = %v, panicked = %t", err, panicked.Load())
	}
	panicEnabled.Store(false)
	session.setResult(context.Background(), err)
	if summaryErr := session.printFinalSummary(&output); summaryErr != nil {
		t.Fatalf("static model-panic result: %v", summaryErr)
	}

	raw := output.String()
	restore := strings.LastIndex(raw, "\x1b[?1049l")
	summary := strings.Index(raw, "Final aggregate summary")
	if restore < 0 || summary < restore || !strings.Contains(raw[summary:], "Final outcome: Error: presentation failed:") {
		t.Fatalf("model panic static result was not printed after restoration: %q", raw)
	}
}

type shortPresentationWriter struct{}

func (shortPresentationWriter) Write(content []byte) (int, error) {
	return len(content) - 1, nil
}

func TestPresentationOutputMonitorTreatsShortWriteAsOutputLoss(t *testing.T) {
	monitor := newPresentationOutputMonitor(shortPresentationWriter{})
	if _, err := monitor.Write([]byte("dashboard")); !errors.Is(err, io.ErrShortWrite) || !errors.Is(monitor.failure(), io.ErrShortWrite) {
		t.Fatalf("short terminal write = %v, retained = %v", err, monitor.failure())
	}
	select {
	case <-monitor.failed:
	default:
		t.Fatal("short terminal write did not wake the presentation failure monitor")
	}
}

func TestRestoreDashboardTerminalReturnsOutputFailure(t *testing.T) {
	if err := restoreDashboardTerminal(failingStatusWriter{}); err == nil || !strings.Contains(err.Error(), "status output failed") {
		t.Fatalf("terminal restoration error = %v", err)
	}
}

type boundedRecoveryWriter struct {
	bytes.Buffer
	limit  int
	writes int
}

func (w *boundedRecoveryWriter) Write(content []byte) (int, error) {
	w.writes++
	if len(content) > w.limit {
		content = content[:w.limit]
	}
	return w.Buffer.Write(content)
}

func TestRestoreDashboardTerminalCompletesNilErrorShortWrites(t *testing.T) {
	output := &boundedRecoveryWriter{limit: 3}
	if err := restoreDashboardTerminal(output); err != nil {
		t.Fatal(err)
	}
	if output.String() != dashboardTerminalRestoration || !strings.HasSuffix(output.String(), "\x1b[?1049l\x1b[?25h") || output.writes <= 1 {
		t.Fatalf("short-write restoration = %q in %d writes, want complete alternate-screen and cursor restoration in multiple writes", output.String(), output.writes)
	}
}

func TestRestoreDashboardTerminalReturnsShortWriteAfterNoProgress(t *testing.T) {
	output := &boundedRecoveryWriter{}
	if err := restoreDashboardTerminal(output); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero-progress restoration error = %v, want %v", err, io.ErrShortWrite)
	}
	if output.writes != 1 || output.Len() != 0 {
		t.Fatalf("zero-progress restoration made %d writes and produced %q", output.writes, output.String())
	}
}

type recoveryInterpretingWriter struct {
	parser          *ansi.Parser
	raw             bytes.Buffer
	visible         strings.Builder
	hyperlink       bool
	styled          bool
	alternateScreen bool
	cursorVisible   bool
	report          bool
	reportLinked    bool
	reportStyled    bool
}

func newRecoveryInterpretingWriter() *recoveryInterpretingWriter {
	output := &recoveryInterpretingWriter{alternateScreen: true}
	parser := ansi.NewParser()
	parser.SetHandler(ansi.Handler{
		Print: func(value rune) {
			output.visible.WriteRune(value)
			if output.report {
				output.reportLinked = output.reportLinked || output.hyperlink
				output.reportStyled = output.reportStyled || output.styled
			}
		},
		HandleOsc: func(command int, data []byte) {
			if command != 8 {
				return
			}
			parts := strings.SplitN(string(data), ";", 3)
			output.hyperlink = len(parts) == 3 && parts[2] != ""
		},
		HandleCsi: func(command ansi.Cmd, params ansi.Params) {
			switch command.Final() {
			case 'm':
				if command.Prefix() != 0 {
					return
				}
				if len(params) == 0 {
					output.styled = false
					return
				}
				params.ForEach(0, func(_ int, parameter int, _ bool) {
					if parameter == 0 {
						output.styled = false
					} else {
						output.styled = true
					}
				})
			case 'h', 'l':
				if command.Prefix() != '?' {
					return
				}
				enabled := command.Final() == 'h'
				params.ForEach(0, func(_ int, parameter int, _ bool) {
					switch parameter {
					case 25:
						output.cursorVisible = enabled
					case 1049:
						output.alternateScreen = enabled
					}
				})
			}
		},
	})
	output.parser = parser
	return output
}

func (w *recoveryInterpretingWriter) Write(content []byte) (int, error) {
	_, _ = w.raw.Write(content)
	w.parser.Parse(content)
	return len(content), nil
}

func TestOutputLossRecoveryCancelsPartialHyperlinkAndStyleSequencesBeforeReport(t *testing.T) {
	for _, test := range []struct {
		name        string
		interrupted string
		wantLinked  bool
		wantStyled  bool
	}{
		{
			name:        "OSC 8 close",
			interrupted: "\x1b]8;;https://github.com/acme/widgets/issues/73\x1b\\#73\x1b]8;",
			wantLinked:  true,
		},
		{
			name:        "SGR style",
			interrupted: "\x1b[31mstyled dashboard\x1b[38;5",
			wantStyled:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := newRecoveryInterpretingWriter()
			_, _ = io.WriteString(output, test.interrupted)
			if output.hyperlink != test.wantLinked || output.styled != test.wantStyled {
				t.Fatalf("interrupted dashboard state: hyperlink=%t styled=%t, want hyperlink=%t styled=%t", output.hyperlink, output.styled, test.wantLinked, test.wantStyled)
			}

			if err := restoreDashboardTerminal(output); err != nil {
				t.Fatal(err)
			}
			const report = "Final aggregate summary\nFinal outcome: Error: terminal output lost\n"
			output.report = true
			_, _ = io.WriteString(output, report)

			recovery := output.raw.String()[len(test.interrupted):]
			if !strings.HasPrefix(recovery, "\x18\x1b]8;;\x1b\\\x1b[0m") {
				t.Fatalf("recovery did not cancel the partial sequence, close OSC 8, and reset SGR first: %q", recovery)
			}
			visible := output.visible.String()
			if output.hyperlink || output.styled || output.alternateScreen || !output.cursorVisible || output.reportLinked || output.reportStyled || !strings.Contains(visible, "Final aggregate summary") || !strings.Contains(visible, "Final outcome: Error: terminal output lost") {
				t.Fatalf("static report remained captured or formatted after recovery: hyperlink=%t styled=%t alternate=%t cursor=%t report_linked=%t report_styled=%t visible=%q", output.hyperlink, output.styled, output.alternateScreen, output.cursorVisible, output.reportLinked, output.reportStyled, output.visible.String())
			}
		})
	}
}

type statefulTerminalWriter struct {
	visible      bytes.Buffer
	pending      bytes.Buffer
	synchronized bool
	unicodeCore  bool
}

func (w *statefulTerminalWriter) Write(content []byte) (int, error) {
	rest := string(content)
	for rest != "" {
		if w.synchronized {
			reset := strings.Index(rest, ansi.ResetModeSynchronizedOutput)
			if reset < 0 {
				_, _ = w.pending.WriteString(rest)
				break
			}
			_, _ = w.pending.WriteString(rest[:reset])
			_, _ = w.visible.Write(w.pending.Bytes())
			w.pending.Reset()
			_, _ = w.visible.WriteString(ansi.ResetModeSynchronizedOutput)
			w.synchronized = false
			rest = rest[reset+len(ansi.ResetModeSynchronizedOutput):]
			continue
		}

		control, index := nextTerminalModeControl(rest)
		if index < 0 {
			_, _ = w.visible.WriteString(rest)
			break
		}
		_, _ = w.visible.WriteString(rest[:index+len(control)])
		switch control {
		case ansi.SetModeSynchronizedOutput:
			w.synchronized = true
		case ansi.SetModeUnicodeCore:
			w.unicodeCore = true
		case ansi.ResetModeUnicodeCore:
			w.unicodeCore = false
		}
		rest = rest[index+len(control):]
	}
	return len(content), nil
}

func nextTerminalModeControl(content string) (string, int) {
	control, first := "", -1
	for _, candidate := range []string{ansi.SetModeSynchronizedOutput, ansi.SetModeUnicodeCore, ansi.ResetModeUnicodeCore} {
		if index := strings.Index(content, candidate); index >= 0 && (first < 0 || index < first) {
			control, first = candidate, index
		}
	}
	return control, first
}

func TestRestoreDashboardTerminalReleasesInterruptedModesBeforeStaticOutput(t *testing.T) {
	output := &statefulTerminalWriter{}
	_, _ = io.WriteString(output, ansi.SetModeUnicodeCore+ansi.SetModeSynchronizedOutput+"interrupted dashboard frame")
	if !output.unicodeCore || !output.synchronized || output.pending.Len() == 0 {
		t.Fatal("test terminal did not retain the interrupted Unicode Core synchronized frame")
	}
	if err := restoreDashboardTerminal(output); err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(output, "Final aggregate summary")

	visible := output.visible.String()
	synchronizedReset := strings.Index(visible, ansi.ResetModeSynchronizedOutput)
	unicodeReset := strings.Index(visible, ansi.ResetModeUnicodeCore)
	restore := strings.Index(visible, "\x1b[?1049l")
	summary := strings.Index(visible, "Final aggregate summary")
	if output.synchronized || output.unicodeCore || output.pending.Len() != 0 || synchronizedReset < 0 || unicodeReset != synchronizedReset+len(ansi.ResetModeSynchronizedOutput) || restore < unicodeReset || summary < restore {
		t.Fatalf("interrupted terminal modes retained restoration or static output: synchronized=%t unicode_core=%t visible=%q pending=%q", output.synchronized, output.unicodeCore, visible, output.pending.String())
	}
}

type initializationFailureTTY struct {
	*os.File
	fdCalls atomic.Int32
}

func (f *initializationFailureTTY) Fd() uintptr {
	if f.fdCalls.Add(1) == 1 {
		return f.File.Fd()
	}
	return ^uintptr(0)
}

func TestBubbleDashboardRestoresRawModeAfterBubbleTeaInitializationFailure(t *testing.T) {
	primary, terminal, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	defer terminal.Close()

	initialState, err := term.GetState(int(terminal.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	output := &initializationFailureTTY{File: terminal}
	session := newBubbleDashboardSession(time.Now)
	err = session.presentation(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Input: terminal, Output: output,
		Dimensions:   func() (TerminalDimensions, error) { return TerminalDimensions{Width: 80, Height: 24}, nil },
		ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
	}})
	if err == nil || !strings.Contains(err.Error(), "getting terminal size") {
		t.Fatalf("Bubble Tea initialization error = %v", err)
	}
	if calls := output.fdCalls.Load(); calls < 2 {
		t.Fatalf("output file descriptor calls = %d, want initialization to fail after TTY detection", calls)
	}
	restoredState, stateErr := term.GetState(int(terminal.Fd()))
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if !reflect.DeepEqual(restoredState, initialState) {
		t.Fatalf("terminal remained in raw mode after Bubble Tea initialization failure: got %#v, want %#v", restoredState, initialState)
	}
	if startupErr := session.waitForStartup(context.Background()); startupErr == nil || startupErr.Error() != err.Error() {
		t.Fatalf("Runner startup error = %v, want %v", startupErr, err)
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

func TestDashboardResultErrorSeparatesParentCancellationFromCommandStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := dashboardResultError(ctx, nil, false); !errors.Is(got, context.Canceled) {
		t.Fatalf("dashboard cancellation outcome = %v, want context cancellation", got)
	}

	cleanupErr := errors.New("persist interrupted Runs")
	if got := dashboardResultError(ctx, cleanupErr, false); got != cleanupErr {
		t.Fatalf("dashboard cleanup outcome = %v, want %v", got, cleanupErr)
	}
	if got := dashboardResultError(ctx, nil, true); got != nil {
		t.Fatalf("late cancellation replaced natural exhaustion with %v", got)
	}
	intervention := &runner.InterventionRequired{Count: 1}
	if got := dashboardResultError(ctx, intervention, true); got != intervention {
		t.Fatalf("late cancellation replaced natural exhaustion with attention: %v", got)
	}
	if got := dashboardResultError(context.Background(), nil, false); got != nil {
		t.Fatalf("dashboard result for a clean live context = %v, want nil", got)
	}
}

func TestDashboardFinalOutcomeDistinguishesEveryExitPath(t *testing.T) {
	for _, test := range []struct {
		name          string
		natural       bool
		forceStopping bool
		result        runner.ShutdownResult
		err           error
		want          string
	}{
		{name: "natural", natural: true, want: "Natural exhaustion"},
		{name: "natural attention", natural: true, err: &runner.InterventionRequired{Count: 1}, want: "Natural exhaustion with Attention Required"},
		{name: "Drain", want: "Drain complete"},
		{name: "bounded suspension", err: &runner.SignalExit{Code: 143}, want: "Suspension complete"},
		{name: "bounded suspension failure", result: runner.ShutdownResultFailure, err: &runner.SignalExit{Code: 130, Cause: errors.New("boundary failed")}, want: "Suspension finished with errors"},
		{name: "force stop", forceStopping: true, err: &runner.SignalExit{Code: 130}, want: "Force stop complete"},
		{name: "force stop failure", forceStopping: true, result: runner.ShutdownResultFailure, err: &runner.SignalExit{Code: 130, Cause: errors.New("kill failed")}, want: "Force stop finished with errors"},
		{name: "Runner failure", err: errors.New("state unavailable"), want: "Error: state unavailable"},
		{name: "parent cancellation", err: context.Canceled, want: "Error: context canceled"},
		{name: "presentation failure", err: &PresentationFailure{Err: errors.New("renderer panic"), RunnerErr: &runner.SignalExit{Code: 143}}, want: "Error: presentation failed: renderer panic; Runner completion: signal shutdown (143)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := dashboardFinalOutcome(test.natural, test.forceStopping, test.result, test.err); got != test.want {
				t.Fatalf("final outcome = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBubbleDashboardReconfigurationPreservesLockedCompletionBaseline(t *testing.T) {
	session := newBubbleDashboardSession(time.Now)
	initial := state.State{Version: state.CurrentVersion, Repo: "acme/widgets", MaxConcurrentIssues: 1}
	source := &dashboardTestSource{current: initial}
	session.configure(initial, source)

	completed := initial
	completed.Runs = []scheduler.Run{{Issue: 73, IssueTitle: "Invocation completion", RunID: "run-73", Status: scheduler.StatusMerged}}
	source.current = completed
	session.configure(completed, source)
	if err := session.captureFinalSummary(completed); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := session.printFinalSummary(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Completions produced (1)") || !strings.Contains(output.String(), "#73  Invocation completion") {
		t.Fatalf("reconfiguration replaced the invocation completion baseline:\n%s", output.String())
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
