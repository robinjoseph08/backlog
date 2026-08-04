package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/ansi"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

const (
	dashboardOutputUpdateLimit     = 64
	dashboardURLOpenTimeout        = 15 * time.Second
	dashboardURLDiagnosticTimeout  = 5 * time.Second
	dashboardNavigationHelp        = "Nav: d:Diagnostics ↑↓/jk PgUp/Dn/f/b Home/End g/G a:Attention o:Issue p:PR Enter:Toggle"
	dashboardCompactNavigationHelp = "N:jk/fb op a d Ent"
)

type dashboardConfiguredMsg struct {
	initial state.State
	source  followStateSource
}

type dashboardStateMsg state.State
type dashboardOutputMsg string
type dashboardOperationalMsg struct{ event runner.OperationalEvent }
type dashboardInterruptResultMsg struct{ err error }
type dashboardOpenURLResultMsg struct {
	resource dashboardResourceKind
	err      error
}
type dashboardURLDiagnosticExpiredMsg struct{ id uint64 }
type dashboardElapsedMsg time.Time
type dashboardActivityMsg time.Time
type dashboardQueueStoppedMsg struct{ err error }
type dashboardFlushMsg struct {
	acknowledged chan struct{}
	naturalExit  bool
}
type dashboardFlushRenderedMsg struct{ acknowledged chan struct{} }

// bubbleDashboardSession is the asynchronous boundary between Runner writes
// and Bubble Tea's single Update loop. State updates are coalesced while plain
// operational lines remain bounded independently of required configuration,
// state, and flush updates.
type bubbleDashboardSession struct {
	mu      sync.Mutex
	updates []tea.Msg
	wake    chan struct{}

	outputMu sync.Mutex
	pending  bytes.Buffer

	startupOnce sync.Once
	startup     chan error
	doneOnce    sync.Once
	done        chan struct{}

	finalMu               sync.Mutex
	finalState            *state.State
	initialCompletions    map[string]struct{}
	completionBaselineSet bool
	source                followStateSource
	naturalExit           bool
	shutdownResult        runner.ShutdownResult
	forceStopping         bool
	resultErr             error
	now                   func() time.Time
}

func newBubbleDashboardSession(now func() time.Time) *bubbleDashboardSession {
	if now == nil {
		now = time.Now
	}
	return &bubbleDashboardSession{
		wake: make(chan struct{}, 1), startup: make(chan error, 1), done: make(chan struct{}), now: now,
	}
}

func (s *bubbleDashboardSession) presentation(ctx context.Context, control PresentationControl) (resultErr error) {
	started := false
	defer func() {
		if !started {
			s.signalStartup(resultErr)
		}
		s.doneOnce.Do(func() { close(s.done) })
	}()
	dimensions, err := control.Terminal.Dimensions()
	if err != nil {
		return fmt.Errorf("read initial terminal dimensions: %w", err)
	}
	if dimensions.Width <= 0 || dimensions.Height <= 0 {
		return fmt.Errorf("read initial terminal dimensions: invalid size %dx%d", dimensions.Width, dimensions.Height)
	}
	model := newBubbleDashboardModel(ctx, control, s, dimensions)
	monitoredOutput := newPresentationOutputMonitor(control.Terminal.Output)
	program := tea.NewProgram(
		model,
		tea.WithContext(ctx),
		tea.WithInput(control.Terminal.Input),
		tea.WithOutput(monitoredOutput.writer()),
		tea.WithWindowSize(dimensions.Width, dimensions.Height),
		tea.WithColorProfile(bubbleColorProfile(model.colorProfile)),
		tea.WithoutSignalHandler(),
	)
	// Bubble Tea v2.0.8 does not shut down when initialization fails after
	// entering raw mode. Kill is idempotent with Run's normal shutdown path.
	defer program.Kill()
	watchStopped := make(chan struct{})
	go func() {
		defer close(watchStopped)
		select {
		case <-monitoredOutput.failed:
			program.Kill()
		case <-s.done:
		}
	}()
	_, err = program.Run()
	started = model.started()
	if outputErr := monitoredOutput.failure(); outputErr != nil {
		resultErr = fmt.Errorf("write terminal presentation: %w", outputErr)
		if restoreErr := restoreDashboardTerminal(control.Terminal.Output); restoreErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("restore terminal presentation: %w", restoreErr))
		}
	} else if ctx.Err() != nil && (err == nil || errors.Is(err, tea.ErrProgramKilled)) {
		resultErr = ctx.Err()
	} else {
		resultErr = err
	}
	// Closing done in the function defer releases this watcher on ordinary
	// completion. An output failure already released it before Program.Run.
	if monitoredOutput.failure() != nil {
		<-watchStopped
	}
	return resultErr
}

// Bubble Tea's Kill path invokes normal shutdown, but the output failure that
// forced it may also prevent teardown controls from being written. Retry those
// controls directly with this intentionally idempotent sequence. CAN first
// returns a terminal parser stranded in an incomplete control string or CSI to
// ground state. Close any active OSC 8 hyperlink and reset SGR before restoring
// terminal modes, the normal screen, and the cursor.
const dashboardTerminalRestoration = "\x18\x1b]8;;\x1b\\\x1b[0m" + ansi.ResetModeSynchronizedOutput + ansi.ResetModeUnicodeCore + "\x1b[>4m\x1b[=0;1u\x1b[?1002l\x1b[?1003l\x1b[?1006l\x1b[?2004l\x1b[?1049l\x1b[?25h"

func restoreDashboardTerminal(output io.Writer) error {
	return writeAll(output, []byte(dashboardTerminalRestoration))
}

type presentationOutputMonitor struct {
	output io.Writer
	failed chan struct{}
	once   sync.Once
	mu     sync.Mutex
	err    error
}

func newPresentationOutputMonitor(output io.Writer) *presentationOutputMonitor {
	return &presentationOutputMonitor{output: output, failed: make(chan struct{})}
}

func (w *presentationOutputMonitor) writer() io.Writer {
	file, ok := w.output.(interface {
		io.ReadWriteCloser
		Fd() uintptr
	})
	if !ok {
		return w
	}
	return presentationTTYOutputMonitor{presentationOutputMonitor: w, file: file}
}

type presentationTTYOutputMonitor struct {
	*presentationOutputMonitor
	file interface {
		io.ReadWriteCloser
		Fd() uintptr
	}
}

func (w presentationTTYOutputMonitor) Read(content []byte) (int, error) {
	return w.file.Read(content)
}

func (w presentationTTYOutputMonitor) Close() error {
	return w.file.Close()
}

func (w presentationTTYOutputMonitor) Fd() uintptr {
	return w.file.Fd()
}

func (w *presentationOutputMonitor) Write(content []byte) (written int, resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = fmt.Errorf("terminal output panic: %v", recovered)
		}
		if resultErr != nil {
			w.recordFailure(resultErr)
		}
	}()
	written, resultErr = w.output.Write(content)
	if resultErr == nil && written != len(content) {
		resultErr = io.ErrShortWrite
	}
	return written, resultErr
}

func (w *presentationOutputMonitor) recordFailure(err error) {
	w.once.Do(func() {
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		close(w.failed)
	})
}

func (w *presentationOutputMonitor) failure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func bubbleColorProfile(profile TerminalColorProfile) colorprofile.Profile {
	switch profile {
	case TerminalColorNone:
		return colorprofile.Ascii
	case TerminalColorANSI256:
		return colorprofile.ANSI256
	case TerminalColorTrueColor:
		return colorprofile.TrueColor
	default:
		return colorprofile.ANSI
	}
}

func (s *bubbleDashboardSession) signalStartup(err error) {
	s.startupOnce.Do(func() { s.startup <- err })
}

func (s *bubbleDashboardSession) waitForStartup(ctx context.Context) error {
	select {
	case err := <-s.startup:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *bubbleDashboardSession) configure(initial state.State, source followStateSource) {
	cloned := cloneDashboardState(initial)
	s.finalMu.Lock()
	s.source = source
	s.finalState = &cloned
	if !s.completionBaselineSet {
		s.initialCompletions = mergedRunIdentities(initial)
		s.completionBaselineSet = true
	}
	s.finalMu.Unlock()
	s.publish(dashboardConfiguredMsg{initial: cloned, source: source})
}

func mergedRunIdentities(current state.State) map[string]struct{} {
	identities := make(map[string]struct{})
	for _, run := range current.Runs {
		if run.Status == scheduler.StatusMerged {
			identities[run.RunID] = struct{}{}
		}
	}
	return identities
}

func (s *bubbleDashboardSession) stateSaved(current state.State) {
	cloned := cloneDashboardState(current)
	s.finalMu.Lock()
	s.finalState = &cloned
	s.finalMu.Unlock()
	s.publish(dashboardStateMsg(cloned))
}

func (s *bubbleDashboardSession) flush(ctx context.Context) error {
	acknowledged := make(chan struct{})
	s.finalMu.Lock()
	naturalExit := s.naturalExit
	s.finalMu.Unlock()
	s.publish(dashboardFlushMsg{acknowledged: acknowledged, naturalExit: naturalExit})
	select {
	case <-acknowledged:
		return nil
	case <-s.done:
		return errors.New("dashboard stopped before pending updates rendered")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *bubbleDashboardSession) captureFinalSummary(current state.State) error {
	cloned := cloneDashboardState(current)
	s.finalMu.Lock()
	s.finalState = &cloned
	s.naturalExit = true
	s.finalMu.Unlock()
	return nil
}

func (s *bubbleDashboardSession) observeOperationalEvent(event runner.OperationalEvent) {
	shutdown, ok := event.(runner.ShutdownEvent)
	if !ok {
		return
	}
	s.finalMu.Lock()
	s.shutdownResult = shutdown.Result
	if shutdown.Stage == runner.ShutdownStageForceStopping {
		s.forceStopping = true
	}
	s.finalMu.Unlock()
}

func (s *bubbleDashboardSession) setResult(ctx context.Context, err error) bool {
	s.finalMu.Lock()
	defer s.finalMu.Unlock()
	s.resultErr = dashboardResultError(ctx, err, s.naturalExit)
	return s.naturalExit
}

func (s *bubbleDashboardSession) printFinalSummary(output io.Writer) error {
	s.finalMu.Lock()
	current := state.State{Version: state.CurrentVersion}
	if s.finalState != nil {
		current = cloneDashboardState(*s.finalState)
	}
	initialCompletions := maps.Clone(s.initialCompletions)
	source := s.source
	outcome := dashboardFinalOutcome(s.naturalExit, s.forceStopping, s.shutdownResult, s.resultErr)
	s.finalMu.Unlock()
	return printRunFinalReport(output, current, source, s.now(), initialCompletions, outcome)
}

func dashboardResultError(ctx context.Context, err error, natural bool) error {
	if err != nil || natural {
		return err
	}
	return context.Cause(ctx)
}

func dashboardFinalOutcome(natural, forceStopping bool, result runner.ShutdownResult, err error) string {
	var presentationFailure *PresentationFailure
	if errors.As(err, &presentationFailure) {
		return "Error: " + presentationFailure.Error()
	}
	if natural {
		if err != nil {
			var intervention *runner.InterventionRequired
			if !errors.As(err, &intervention) {
				return "Error: " + err.Error()
			}
			return "Natural exhaustion with Attention Required"
		}
		return "Natural exhaustion"
	}

	var signalExit *runner.SignalExit
	if errors.As(err, &signalExit) {
		if forceStopping {
			if result == runner.ShutdownResultFailure || signalExit.Cause != nil {
				return "Force stop finished with errors"
			}
			return "Force stop complete"
		}
		if result == runner.ShutdownResultFailure || signalExit.Cause != nil {
			return "Suspension finished with errors"
		}
		return "Suspension complete"
	}
	if err != nil {
		return "Error: " + err.Error()
	}
	return "Drain complete"
}

func (s *bubbleDashboardSession) publish(msg tea.Msg) {
	s.mu.Lock()
	switch msg.(type) {
	case dashboardStateMsg, dashboardConfiguredMsg:
		for index := len(s.updates) - 1; index >= 0; index-- {
			if sameDashboardUpdateKind(s.updates[index], msg) {
				s.updates[index] = msg
				s.mu.Unlock()
				s.wakeUpdate()
				return
			}
		}
	}
	s.updates = append(s.updates, msg)
	if _, optional := msg.(dashboardOutputMsg); optional {
		for dashboardOutputUpdateCount(s.updates) > dashboardOutputUpdateLimit {
			for index, update := range s.updates {
				if _, ok := update.(dashboardOutputMsg); ok {
					s.removeUpdate(index)
					break
				}
			}
		}
	}
	s.mu.Unlock()
	s.wakeUpdate()
}

func sameDashboardUpdateKind(left, right tea.Msg) bool {
	switch left.(type) {
	case dashboardStateMsg:
		_, ok := right.(dashboardStateMsg)
		return ok
	case dashboardConfiguredMsg:
		_, ok := right.(dashboardConfiguredMsg)
		return ok
	default:
		return false
	}
}

func dashboardOutputUpdateCount(updates []tea.Msg) int {
	count := 0
	for _, update := range updates {
		if _, ok := update.(dashboardOutputMsg); ok {
			count++
		}
	}
	return count
}

func (s *bubbleDashboardSession) removeUpdate(index int) {
	copy(s.updates[index:], s.updates[index+1:])
	s.updates[len(s.updates)-1] = nil
	s.updates = s.updates[:len(s.updates)-1]
}

func (s *bubbleDashboardSession) wakeUpdate() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *bubbleDashboardSession) next(ctx context.Context) (tea.Msg, error) {
	for {
		s.mu.Lock()
		if len(s.updates) > 0 {
			msg := s.updates[0]
			s.updates[0] = nil
			s.updates = s.updates[1:]
			s.mu.Unlock()
			return msg, nil
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *bubbleDashboardSession) Write(content []byte) (int, error) {
	s.outputMu.Lock()
	_, _ = s.pending.Write(content)
	for {
		line, err := s.pending.ReadString('\n')
		if err != nil {
			_, _ = s.pending.WriteString(line)
			break
		}
		if line = normalizedDashboardMessage(strings.TrimSuffix(line, "\n")); line != "" {
			s.publish(dashboardOutputMsg(line))
		}
	}
	s.outputMu.Unlock()
	return len(content), nil
}

type bubbleDashboardStore struct {
	state.FileStore
	session *bubbleDashboardSession
}

func (s bubbleDashboardStore) Save(current state.State) error {
	if err := s.FileStore.Save(current); err != nil {
		return err
	}
	s.session.stateSaved(current)
	return nil
}

type bubbleDashboardModel struct {
	ctx          context.Context
	control      PresentationControl
	session      *bubbleDashboardSession
	dashboard    *liveDashboard
	viewport     viewport.Model
	width        int
	height       int
	header       string
	footer       string
	layout       dashboardBodyLayout
	stage        dashboardStage
	colorProfile TerminalColorProfile
	styler       dashboardStyler

	selectedAnchor     string
	attentionKnown     map[string]struct{}
	attentionPending   map[string]struct{}
	expansionOverrides map[string]bool

	interruptsWaiting int
	pendingFlushes    []dashboardFlushMsg
	flushAfter        func(time.Duration) <-chan time.Time
	urlOpenInFlight   bool
	urlDiagnostic     string
	urlDiagnosticID   uint64
	startup           *atomic.Bool
}

func newBubbleDashboardModel(ctx context.Context, control PresentationControl, session *bubbleDashboardSession, dimensions TerminalDimensions) bubbleDashboardModel {
	empty := state.State{Version: state.CurrentVersion}
	view := viewport.New(viewport.WithWidth(dimensions.Width))
	profile := TerminalColorNone
	if control.Terminal.ColorProfile != nil {
		profile = control.Terminal.ColorProfile()
	}
	view.SoftWrap = true
	view.FillHeight = true
	dashboard := newLiveDashboard(io.Discard, nil, empty, control.Terminal.Now)
	dashboard.hyperlinks = true
	model := bubbleDashboardModel{
		ctx: ctx, control: control, session: session,
		dashboard: dashboard,
		viewport:  view, width: dimensions.Width, height: dimensions.Height,
		attentionKnown: make(map[string]struct{}), attentionPending: make(map[string]struct{}),
		colorProfile: profile, styler: newDashboardFallbackStyler(profile),
		expansionOverrides: make(map[string]bool), flushAfter: time.After, startup: &atomic.Bool{},
	}
	model.refreshViewport(dashboardSelection{})
	model.selectViewportAnchor()
	return model
}

func (m bubbleDashboardModel) started() bool {
	return m.startup.Load()
}

func (m bubbleDashboardModel) Init() tea.Cmd {
	m.startup.Store(true)
	m.session.signalStartup(nil)
	commands := []tea.Cmd{m.waitForSessionUpdate(), m.waitForOperationalEvent(), dashboardElapsedTick(), dashboardActivityTick()}
	if m.colorProfile != TerminalColorNone {
		commands = append(commands, tea.RequestBackgroundColor)
	}
	return tea.Batch(commands...)
}

func (m bubbleDashboardModel) waitForSessionUpdate() tea.Cmd {
	return func() tea.Msg {
		msg, err := m.session.next(m.ctx)
		if err != nil {
			return dashboardQueueStoppedMsg{err: err}
		}
		return msg
	}
}

func (m bubbleDashboardModel) waitForOperationalEvent() tea.Cmd {
	return func() tea.Msg {
		event, err := m.control.NextOperationalEvent(m.ctx)
		if err != nil {
			return dashboardQueueStoppedMsg{err: err}
		}
		return dashboardOperationalMsg{event: event}
	}
}

func dashboardElapsedTick() tea.Cmd {
	return tea.Tick(dashboardElapsedInterval, func(now time.Time) tea.Msg { return dashboardElapsedMsg(now) })
}

func dashboardActivityTick() tea.Cmd {
	return tea.Tick(dashboardActivityInterval, func(now time.Time) tea.Msg { return dashboardActivityMsg(now) })
}

func (m bubbleDashboardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	selection := m.currentSelection()
	var commands []tea.Cmd
	trackAttention := false
	configured := false
	keepSelectionVisible := false
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = max(1, msg.Width), max(1, msg.Height)
		keepSelectionVisible = true
	case tea.BackgroundColorMsg:
		m.styler = newDashboardStyler(m.colorProfile, msg.IsDark())
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c":
			m.interruptsWaiting++
			if m.interruptsWaiting == 1 {
				commands = append(commands, m.interrupt())
			}
			return m, tea.Batch(commands...)
		case "d":
			m.dashboard.toggleDiagnostics()
		case "[":
			m.dashboard.moveDiagnosticRecord(-1)
		case "]":
			m.dashboard.moveDiagnosticRecord(1)
		case ",":
			m.dashboard.moveDiagnosticPage(-1)
		case ".":
			m.dashboard.moveDiagnosticPage(1)
		case "o":
			if !m.urlOpenInFlight {
				if command := m.openSelectedURL(selection.identity, dashboardIssueResource); command != nil {
					m.urlOpenInFlight = true
					commands = append(commands, command)
				}
			}
		case "p":
			if !m.urlOpenInFlight {
				if command := m.openSelectedURL(selection.identity, dashboardPullRequestResource); command != nil {
					m.urlOpenInFlight = true
					commands = append(commands, command)
				}
			}
		}
		if msg.String() == "enter" {
			m.toggleExpansion()
		}
	case dashboardOpenURLResultMsg:
		m.urlOpenInFlight = false
		if msg.err != nil {
			m.urlDiagnosticID++
			id := m.urlDiagnosticID
			resource, ok := msg.resource.label()
			if !ok {
				resource = "resource"
			}
			m.urlDiagnostic = fmt.Sprintf("Open %s failed: %s", resource, plainStatusValue(strings.TrimSpace(msg.err.Error())))
			commands = append(commands, tea.Tick(dashboardURLDiagnosticTimeout, func(time.Time) tea.Msg {
				return dashboardURLDiagnosticExpiredMsg{id: id}
			}))
		}
	case dashboardURLDiagnosticExpiredMsg:
		if msg.id == m.urlDiagnosticID {
			m.urlDiagnostic = ""
		}
	case dashboardInterruptResultMsg:
		if m.interruptsWaiting > 0 {
			m.interruptsWaiting--
		}
		if msg.err != nil {
			if m.ctx.Err() != nil {
				return m, tea.Quit
			}
			m.dashboard.recordMessage(msg.err.Error())
		}
		if m.interruptsWaiting > 0 {
			commands = append(commands, m.interrupt())
		}
	case dashboardConfiguredMsg:
		m.dashboard.source = msg.source
		m.dashboard.update(msg.initial)
		configured = true
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardStateMsg:
		m.dashboard.update(state.State(msg))
		trackAttention = true
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardOutputMsg:
		m.dashboard.recordMessage(string(msg))
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardOperationalMsg:
		m.session.observeOperationalEvent(msg.event)
		m.dashboard.operationalEvent(msg.event)
		if m.control.operationalEvents != nil {
			m.control.operationalEvents.complete()
		}
		commands = append(commands, m.waitForOperationalEvent())
		commands = append(commands, m.renderPendingFlushes()...)
	case dashboardElapsedMsg:
		commands = append(commands, dashboardElapsedTick())
	case dashboardActivityMsg:
		commands = append(commands, dashboardActivityTick())
		if !m.dashboard.activityChanged() {
			// Most 100 ms activity ticks observe no filesystem change. Keep the
			// existing viewport instead of rebuilding and wrapping it.
			return m, tea.Batch(commands...)
		}
	case dashboardFlushMsg:
		if msg.naturalExit {
			m.dashboard.markNaturalExit()
		}
		m.pendingFlushes = append(m.pendingFlushes, msg)
		commands = append(commands, m.renderPendingFlushes()...)
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardFlushRenderedMsg:
		close(msg.acknowledged)
		return m, nil
	case dashboardQueueStoppedMsg:
		if m.ctx.Err() != nil || errors.Is(msg.err, context.Canceled) {
			return m, tea.Quit
		}
		m.dashboard.recordMessage(msg.err.Error())
	}

	m.refreshViewportWithVisibility(selection, keepSelectionVisible)
	if configured {
		m.attentionKnown = cloneDashboardIdentities(m.layout.attention)
		clear(m.attentionPending)
	} else if trackAttention {
		markerChanged := m.trackNewAttention()
		if markerChanged {
			m.refreshViewport(m.currentSelection())
		}
	}
	if m.clearVisibleAttention() {
		m.refreshViewport(m.currentSelection())
	}

	previousOffset := m.viewport.YOffset()
	if m.navigateViewport(msg) {
		if !dashboardAttentionJump(msg) {
			if m.selectKeyboardNavigationAnchor(msg, previousOffset) {
				m.refreshViewport(m.currentSelection())
			} else {
				m.selectViewportAnchor()
			}
		}
		if m.clearVisibleAttention() {
			m.refreshViewport(m.currentSelection())
		}
	}
	return m, tea.Batch(commands...)
}

func (m bubbleDashboardModel) interrupt() tea.Cmd {
	return func() tea.Msg { return dashboardInterruptResultMsg{err: m.control.Interrupt(m.ctx)} }
}

func (m bubbleDashboardModel) openSelectedURL(identity string, resource dashboardResourceKind) tea.Cmd {
	resources, exists := m.layout.resources[identity]
	if !exists {
		return nil
	}
	target, validResource := resources.target(resource)
	if !validResource || target == "" {
		return nil
	}
	return func() tea.Msg {
		if m.control.Terminal.OpenURL == nil {
			return dashboardOpenURLResultMsg{resource: resource, err: errors.New("URL opener unavailable")}
		}
		ctx, cancel := context.WithTimeout(m.ctx, dashboardURLOpenTimeout)
		defer cancel()
		return dashboardOpenURLResultMsg{resource: resource, err: m.control.Terminal.OpenURL(ctx, target)}
	}
}

func (m *bubbleDashboardModel) renderPendingFlushes() []tea.Cmd {
	if m.control.operationalEvents != nil && !m.control.operationalEvents.idle() {
		return nil
	}
	commands := make([]tea.Cmd, 0, len(m.pendingFlushes))
	for _, pending := range m.pendingFlushes {
		message := pending
		delay := time.Second / 30
		if pending.naturalExit {
			delay = dashboardFlushDelay(m.dashboard, m.dashboard.now())
		}
		commands = append(commands, func() tea.Msg {
			<-m.flushAfter(delay)
			return dashboardFlushRenderedMsg{acknowledged: message.acknowledged}
		})
	}
	m.pendingFlushes = nil
	return commands
}

func dashboardFlushDelay(dashboard *liveDashboard, now time.Time) time.Duration {
	delay := time.Second / 30
	if remaining := dashboard.recoveryNoticeRemaining(now); remaining > delay {
		return remaining
	}
	return delay
}

type dashboardSelection struct {
	identity string
	relative int
	valid    bool
}

func (m *bubbleDashboardModel) refreshViewport(selection dashboardSelection) {
	m.refreshViewportWithVisibility(selection, false)
}

func (m *bubbleDashboardModel) refreshViewportWithVisibility(selection dashboardSelection, keepSelectionVisible bool) {
	density := dashboardDensityForHeight(m.height)
	options := responsiveDashboardOptions{
		density: density, width: m.width, selected: m.selectedAnchor, expansionOverrides: m.expansionOverrides, styler: m.styler,
	}
	projection, layout, stage := m.dashboard.renderResponsiveParts(m.dashboard.now(), options)
	if m.revealSelectedCompletion(selection, projection, layout) {
		projection, layout, stage = m.dashboard.renderResponsiveParts(m.dashboard.now(), options)
	}
	footer := projection.footer + "\n" + dashboardNavigationHelp
	if m.urlDiagnostic != "" {
		footer += "\n" + m.urlDiagnostic
	}
	if density == dashboardDensityMinimal {
		m.header = minimalDashboardHeader(projection.metadata, len(layout.attention), 0)
		m.footer = minimalDashboardFooter(footer)
	} else {
		m.header = projection.header
		m.footer = footer
	}
	m.layout = layout
	m.stage = stage
	m.resizeViewport()
	m.viewport.SetContent(layout.text)
	if selection.valid {
		if line, exists := m.anchorVisualLine(selection.identity); exists {
			frame := m.dashboardFrame()
			relative := selection.relative
			if keepSelectionVisible && frame.bodyHeight > 0 {
				relative = max(frame.bodyStart, min(relative, frame.bodyStart+frame.bodyHeight-1))
			}
			m.viewport.SetYOffset(frame.bodyStart + line - relative)
			m.selectedAnchor = selection.identity
			return
		}
	}
	m.selectViewportAnchor()
}

func (m *bubbleDashboardModel) revealSelectedCompletion(selection dashboardSelection, projection dashboardProjection, layout dashboardBodyLayout) bool {
	if !selection.valid || !strings.HasPrefix(selection.identity, "run:") {
		return false
	}
	for _, anchor := range layout.anchors {
		if anchor.identity == selection.identity {
			return false
		}
	}
	section := dashboardSectionAnchor("Recent Completions")
	if _, explicitlySet := m.expansionOverrides[section]; explicitlySet {
		return false
	}
	selectedRunID := strings.TrimPrefix(selection.identity, "run:")
	for _, observed := range projection.sections[statusCompletions] {
		if observed.run.RunID == selectedRunID {
			m.expansionOverrides[section] = true
			return true
		}
	}
	return false
}

func (m *bubbleDashboardModel) toggleExpansion() {
	identity := m.selectedAnchor
	if identity == "" {
		return
	}
	options := responsiveDashboardOptions{
		density: dashboardDensityForHeight(m.height), selected: identity, expansionOverrides: m.expansionOverrides,
	}
	m.expansionOverrides[identity] = !options.expanded(identity, strings.HasPrefix(identity, "section:"))
}

func minimalDashboardHeader(metadata dashboardProjectionMetadata, attention, pending int) string {
	health := fmt.Sprintf("%d healthy, %d anomalous", metadata.healthy, metadata.anomalous)
	status := fmt.Sprintf("Health:%s | Attention:%d", health, attention)
	if pending > 0 {
		status += fmt.Sprintf(" | New:%d", pending)
	}
	status += fmt.Sprintf(" | R:%s | %s", metadata.repository, metadata.capacity.compact())
	return "Backlog: " + metadata.repository + "\n" + status
}

func minimalDashboardFooter(footer string) string {
	return strings.Join(compactDashboardFooter(strings.Split(footer, "\n")), "\n")
}

func (m *bubbleDashboardModel) resizeViewport() {
	frame := m.dashboardFrame()
	m.viewport.SetWidth(m.width)
	m.viewport.SetHeight(frame.bodyHeight)
}

func (m bubbleDashboardModel) currentSelection() dashboardSelection {
	if m.selectedAnchor == "" {
		return dashboardSelection{}
	}
	line, exists := m.anchorVisualLine(m.selectedAnchor)
	if !exists {
		return dashboardSelection{}
	}
	return dashboardSelection{
		identity: m.selectedAnchor,
		relative: m.dashboardBodyStart() + line - m.viewport.YOffset(),
		valid:    true,
	}
}

func (m bubbleDashboardModel) dashboardBodyStart() int {
	return m.dashboardFrame().bodyStart
}

func (m bubbleDashboardModel) anchorVisualLine(identity string) (int, bool) {
	for _, anchor := range m.layout.anchors {
		if anchor.identity == identity {
			return dashboardVisualLine(m.layout.text, anchor.line, m.viewport.Width()), true
		}
	}
	return 0, false
}

func dashboardVisualLine(body string, target, width int) int {
	width = max(1, width)
	lines := strings.Split(body, "\n")
	target = min(target, len(lines))
	visual := 0
	for _, line := range lines[:target] {
		visual += max(1, (ansi.StringWidth(line)+width-1)/width)
	}
	return visual
}

func (m *bubbleDashboardModel) selectViewportAnchor() {
	if len(m.layout.anchors) == 0 {
		m.selectedAnchor = ""
		return
	}
	offset := m.viewport.YOffset()
	selected := m.layout.anchors[0].identity
	for _, anchor := range m.layout.anchors {
		if dashboardVisualLine(m.layout.text, anchor.line, m.viewport.Width()) > offset {
			break
		}
		selected = anchor.identity
	}
	m.selectedAnchor = selected
}

func (m *bubbleDashboardModel) selectKeyboardNavigationAnchor(msg tea.Msg, previousOffset int) bool {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok || len(m.layout.anchors) == 0 {
		return false
	}

	current := -1
	for index, anchor := range m.layout.anchors {
		if anchor.identity == m.selectedAnchor {
			current = index
			break
		}
	}

	var target int
	switch key.String() {
	case "home", "g":
		target = 0
	case "end", "G":
		target = len(m.layout.anchors) - 1
	case "down", "j":
		if m.viewport.YOffset() != previousOffset {
			return false
		}
		target = min(len(m.layout.anchors)-1, current+1)
	case "up", "k":
		if m.viewport.YOffset() != previousOffset {
			return false
		}
		if current < 0 {
			target = 0
		} else {
			target = max(0, current-1)
		}
	case "pgdown", "f":
		if m.viewport.YOffset() != previousOffset {
			return false
		}
		target = len(m.layout.anchors) - 1
	case "pgup", "b":
		if m.viewport.YOffset() != previousOffset {
			return false
		}
		target = 0
	default:
		return false
	}
	m.selectedAnchor = m.layout.anchors[target].identity
	return true
}

func (m *bubbleDashboardModel) navigateViewport(msg tea.Msg) bool {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "down", "j":
			m.viewport.ScrollDown(1)
		case "up", "k":
			m.viewport.ScrollUp(1)
		case "pgdown", "f":
			m.viewport.PageDown()
		case "pgup", "b":
			m.viewport.PageUp()
		case "home", "g":
			m.viewport.GotoTop()
		case "end", "G":
			m.viewport.GotoBottom()
		case "a":
			m.jumpToAttention()
		default:
			return false
		}
		return true
	case tea.MouseWheelMsg:
		updated, _ := m.viewport.Update(msg)
		m.viewport = updated
		return true
	default:
		return false
	}
}

func dashboardAttentionJump(msg tea.Msg) bool {
	key, ok := msg.(tea.KeyPressMsg)
	return ok && key.String() == "a"
}

func (m *bubbleDashboardModel) jumpToAttention() {
	identity := dashboardSectionAnchor("Attention Required")
	if len(m.attentionPending) > 0 {
		m.expansionOverrides[identity] = true
		m.refreshViewport(m.currentSelection())
	}
	for _, anchor := range m.layout.anchors {
		if !strings.HasPrefix(anchor.identity, "run:") {
			continue
		}
		runID := strings.TrimPrefix(anchor.identity, "run:")
		if _, pending := m.attentionPending[runID]; pending {
			identity = anchor.identity
			break
		}
	}
	if line, exists := m.anchorVisualLine(identity); exists {
		m.viewport.SetYOffset(line)
		m.selectedAnchor = identity
	}
}

func cloneDashboardIdentities(identities map[string]struct{}) map[string]struct{} {
	cloned := make(map[string]struct{}, len(identities))
	for identity := range identities {
		cloned[identity] = struct{}{}
	}
	return cloned
}

func (m *bubbleDashboardModel) trackNewAttention() bool {
	before := len(m.attentionPending)
	for runID := range m.attentionPending {
		if _, exists := m.layout.attention[runID]; !exists {
			delete(m.attentionPending, runID)
		}
	}
	for runID := range m.layout.attention {
		if _, known := m.attentionKnown[runID]; known {
			continue
		}
		line, exists := m.anchorVisualLine(dashboardRunAnchor(runID))
		if !exists || !m.visualLineVisible(line) {
			m.attentionPending[runID] = struct{}{}
		}
	}
	m.attentionKnown = cloneDashboardIdentities(m.layout.attention)
	return before != len(m.attentionPending)
}

func (m bubbleDashboardModel) visualLineVisible(line int) bool {
	return line >= m.viewport.YOffset() && line < m.viewport.YOffset()+m.viewport.Height()
}

func (m *bubbleDashboardModel) clearVisibleAttention() bool {
	before := len(m.attentionPending)
	for runID := range m.attentionPending {
		if line, exists := m.anchorVisualLine(dashboardRunAnchor(runID)); exists && m.visualLineVisible(line) {
			delete(m.attentionPending, runID)
		}
	}
	return before != len(m.attentionPending)
}

func (m bubbleDashboardModel) View() tea.View {
	// Update owns projection refreshes. View only composes the already wrapped
	// viewport with fixed chrome, avoiding a duplicate full refresh per message.
	frame := m.dashboardFrame()

	lines := make([]string, 0, m.height)
	if frame.titleHeight > 0 {
		lines = append(lines, renderDashboardTitle(frame.title, m.styler))
	}
	lines = append(lines, frame.chrome.top...)
	if frame.bodyHeight > 0 {
		lines = append(lines, strings.Split(m.viewport.View(), "\n")...)
	}
	lines = append(lines, frame.chrome.bottom...)
	for index := range lines {
		lines[index] = fitDashboardLine(lines[index], m.width)
	}
	view := tea.NewView(strings.Join(lines, "\n"))
	view.AltScreen = true
	view.MouseMode = tea.MouseModeCellMotion
	return view
}

type dashboardFrame struct {
	title       string
	titleHeight int
	chrome      dashboardChrome
	bodyStart   int
	bodyHeight  int
}

func (m bubbleDashboardModel) dashboardFrame() dashboardFrame {
	headerLines := strings.Split(m.header, "\n")
	chrome := dashboardChromeLines(headerLines[1:], strings.Split(m.footer, "\n"), len(m.attentionPending), m.stage, m.styler, m.width, m.height)
	chromeHeight := len(chrome.top) + len(chrome.bottom)
	titleHeight := 0
	if chromeHeight+1 < m.height {
		titleHeight = 1
	}
	return dashboardFrame{
		title:       headerLines[0],
		titleHeight: titleHeight,
		chrome:      chrome,
		bodyStart:   titleHeight + len(chrome.top),
		bodyHeight:  max(0, m.height-chromeHeight-titleHeight),
	}
}

func renderDashboardTitle(title string, styler dashboardStyler) string {
	if !styler.enabled {
		return title
	}
	return lipgloss.NewStyle().Bold(true).Render(title)
}

type dashboardChrome struct {
	top    []string
	bottom []string
}

func dashboardChromeLines(header, footer []string, pendingAttention int, stage dashboardStage, styler dashboardStyler, width, height int) dashboardChrome {
	if width <= 0 || height <= 0 {
		return dashboardChrome{}
	}
	chromeLimit := height
	if height >= 3 {
		chromeLimit--
	}
	metadata := styledDashboardHeaderItems(header, pendingAttention, false, styler)
	lifecycle := styledDashboardFooterItems(footer, stage, styler)
	headerGroups := dashboardChromeGroups(metadata)
	footerGroups := dashboardChromeGroups(lifecycle)
	compactNavigation := append([]string(nil), footer...)
	if len(compactNavigation) > 2 {
		compactNavigation[2] = dashboardCompactNavigationHelp
	}
	compactNavigation = styledDashboardFooterItems(compactNavigation, stage, styler)
	compactHeader := styledDashboardHeaderItems(compactDashboardHeader(header), pendingAttention, true, styler)
	compactFooter := styledDashboardFooterItems(compactDashboardFooter(footer), stage, styler)
	candidates := []dashboardChrome{
		{top: wrapDashboardChrome(headerGroups, width), bottom: wrapDashboardChrome(footerGroups, width)},
		{top: wrapDashboardChrome(headerGroups, width), bottom: wrapDashboardChrome(dashboardChromeGroups(compactNavigation), width)},
		{top: wrapDashboardChrome([][]string{metadata}, width), bottom: wrapDashboardChrome([][]string{lifecycle}, width)},
		{top: wrapDashboardChrome([][]string{compactHeader}, width), bottom: wrapDashboardChrome([][]string{compactFooter}, width)},
		// A one-line terminal cannot have distinct header and footer rows. Keep
		// the whole chrome in the footer row, preferring full labels when width
		// permits and compact labels otherwise.
		{bottom: wrapDashboardChrome([][]string{append(append([]string(nil), metadata...), lifecycle...)}, width)},
		{bottom: wrapDashboardChrome([][]string{append(append([]string(nil), compactHeader...), compactFooter...)}, width)},
	}
	for _, candidate := range candidates {
		if len(candidate.top)+len(candidate.bottom) <= chromeLimit {
			return candidate
		}
	}

	// When not all chrome can fit, spend the available rows on the fixed footer
	// before retaining header details. Supplemental footer diagnostics come
	// first so a temporary error remains visible, followed by next-interrupt
	// guidance and navigation in separately allocated rows when space permits.
	next, navigation := compactFooterLine(compactFooter, 1), compactFooterLine(compactFooter, 2)
	supplemental := strings.Join(compactFooter[min(3, len(compactFooter)):], " | ")
	priorityFooter := []string{next, navigation}
	if supplemental != "" {
		priorityFooter = append([]string{supplemental}, priorityFooter...)
	}
	bottom := wrapDashboardChrome([][]string{priorityFooter}, width)
	if len(bottom) > chromeLimit && chromeLimit >= 2 {
		if supplemental == "" {
			bottom = []string{fitDashboardLine(next, width), fitDashboardLine(navigation, width)}
		} else if chromeLimit == 2 {
			bottom = []string{
				fitDashboardLine(supplemental, width),
				fitDashboardLine(strings.Join([]string{next, navigation}, " | "), width),
			}
		} else {
			bottom = []string{
				fitDashboardLine(supplemental, width),
				fitDashboardLine(next, width),
				fitDashboardLine(navigation, width),
			}
		}
	}
	if len(bottom) > chromeLimit {
		bottom = bottom[:chromeLimit]
	}
	remaining := chromeLimit - len(bottom)
	if remaining > 0 {
		top := wrapDashboardChrome([][]string{append(compactHeader, compactFooterLine(compactFooter, 0))}, width)
		if len(top) > remaining {
			top = top[:remaining]
		}
		return dashboardChrome{top: top, bottom: bottom}
	}
	return dashboardChrome{bottom: bottom}
}

func dashboardChromeGroups(lines []string) [][]string {
	groups := make([][]string, 0, len(lines))
	for _, line := range lines {
		groups = append(groups, []string{line})
	}
	return groups
}

func compactDashboardHeader(header []string) []string {
	compact := make([]string, 0, len(header))
	for _, line := range header {
		if strings.HasPrefix(line, "Worker capacity: ") {
			line = compactDashboardCapacity(line)
		} else {
			line = strings.Replace(line, "Repository: ", "R:", 1)
		}
		compact = append(compact, line)
	}
	return compact
}

func compactDashboardFooter(footer []string) []string {
	compact := make([]string, 0, len(footer))
	for _, line := range footer {
		line = strings.Replace(line, "Runner stage: ", "S:", 1)
		line = strings.Replace(line, "Next Ctrl-C: ", "^C:", 1)
		line = strings.Replace(line, dashboardNavigationHelp, dashboardCompactNavigationHelp, 1)
		compact = append(compact, line)
	}
	return compact
}

func compactFooterLine(footer []string, index int) string {
	if index >= len(footer) {
		return ""
	}
	return footer[index]
}

func styledDashboardHeaderItems(items []string, pendingAttention int, compact bool, styler dashboardStyler) []string {
	styled := make([]string, len(items))
	for index, item := range items {
		styled[index] = styler.render(dashboardSemanticMetadata, item)
	}
	if pendingAttention == 0 || len(styled) == 0 {
		return styled
	}
	notice := fmt.Sprintf("NEW ATTENTION (%d): press a", pendingAttention)
	if compact {
		notice = fmt.Sprintf("! (%d): press a", pendingAttention)
	}
	styled[0] += styler.render(dashboardSemanticMetadata, " | ") + styler.render(dashboardSemanticAttention, notice)
	return styled
}

func styledDashboardFooterItems(items []string, stage dashboardStage, styler dashboardStyler) []string {
	styled := make([]string, len(items))
	for index, item := range items {
		semantic := dashboardSemanticMetadata
		if index < 2 {
			semantic = dashboardStageSemantic(stage)
		}
		styled[index] = styler.render(semantic, item)
	}
	return styled
}

func wrapDashboardChrome(groups [][]string, width int) []string {
	var lines []string
	for _, group := range groups {
		wrapped := ansi.Wordwrap(strings.Join(group, " | "), width, "|")
		lines = append(lines, strings.Split(wrapped, "\n")...)
	}
	return lines
}

func compactDashboardCapacity(line string) string {
	line = strings.TrimPrefix(line, "Worker capacity: ")
	if line == "pending configuration" {
		return "W:pending"
	}
	line = strings.Replace(line, " used | ", "u/", 1)
	line = strings.Replace(line, " available | ", "a/", 1)
	line = strings.TrimSuffix(line, " total") + "t"
	return "W:" + line
}

func fitDashboardLine(line string, width int) string {
	if width <= 0 {
		return ""
	}
	line = ansi.Truncate(line, width, "")
	if padding := width - lipgloss.Width(line); padding > 0 {
		line += strings.Repeat(" ", padding)
	}
	return line
}
