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

	assertBubbleDashboardFits(t, model, 48, 10, 5)
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 36, Height: 8})
	model = updated.(bubbleDashboardModel)
	assertBubbleDashboardFits(t, model, 36, 8, 1)
}

func assertBubbleDashboardFits(t *testing.T, model bubbleDashboardModel, width, height, viewportHeight int) {
	t.Helper()
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
	if model.viewport.Height() != viewportHeight || model.viewport.Width() != width {
		t.Fatalf("viewport size = %dx%d, want %dx%d", model.viewport.Width(), model.viewport.Height(), width, viewportHeight)
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

func TestBubbleDashboardViewportSupportsKeyboardScrolling(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{Now: time.Now}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 60, Height: 9})
	for range 30 {
		model.dashboard.recordMessage("event line")
	}
	model.View()
	for range 5 {
		updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'j', Text: "j"}))
		model = updated.(bubbleDashboardModel)
	}
	if model.viewport.YOffset() == 0 {
		t.Fatal("j key did not scroll the viewport body")
	}
	for range 2 {
		updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyPgUp}))
		model = updated.(bubbleDashboardModel)
	}
	if !model.viewport.AtTop() {
		t.Fatalf("page-up did not return viewport to top; offset = %d", model.viewport.YOffset())
	}
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
	if _, err := io.WriteString(session, "candidate discovery failed\n"); err != nil {
		t.Fatal(err)
	}
	events := newPresentationEventQueue()
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
	operational := model.waitForOperationalEvent()()
	updated, _ := model.Update(dashboardFlushMsg{acknowledged: make(chan struct{})})
	model = updated.(bubbleDashboardModel)
	if len(model.pendingFlushes) != 1 {
		t.Fatal("final render flush did not wait for the in-flight operational event")
	}
	updated, _ = model.Update(operational)
	model = updated.(bubbleDashboardModel)
	if len(model.pendingFlushes) != 0 || !events.idle() {
		t.Fatal("final render flush did not resume after the operational event was applied")
	}
	view := ansi.Strip(model.View().Content)
	for _, want := range []string{"Repository: acme/widgets", "Runner stage: Draining", "Next Ctrl-C: suspend"} {
		if !strings.Contains(view, want) {
			t.Fatalf("Bubble Tea Update did not receive %q:\n%s", want, view)
		}
	}
	if body := model.viewport.GetContent(); !strings.Contains(body, "candidate discovery failed") || !strings.Contains(body, "Drain accepted") {
		t.Fatalf("scrollable body did not receive output and typed event: %q", body)
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
		for _, want := range []string{"acme/widgets", "Worker capacity:", "Runner stage:", "Next Ctrl-C:"} {
			if !strings.Contains(plain, want) {
				t.Fatalf("%dx%d dashboard omitted %q:\n%s", dimensions.Width, dimensions.Height, want, plain)
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
