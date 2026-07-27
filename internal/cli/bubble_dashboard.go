package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
	"github.com/robinjoseph08/backlog/internal/state"
)

const dashboardUpdateLimit = 64

type dashboardConfiguredMsg struct {
	initial state.State
	source  followStateSource
}

type dashboardStateMsg state.State
type dashboardOutputMsg string
type dashboardOperationalMsg struct{ event runner.OperationalEvent }
type dashboardInterruptResultMsg struct{ err error }
type dashboardElapsedMsg time.Time
type dashboardActivityMsg time.Time
type dashboardQueueStoppedMsg struct{ err error }
type dashboardFlushMsg struct{ acknowledged chan struct{} }
type dashboardFlushRenderedMsg struct{ acknowledged chan struct{} }

// bubbleDashboardSession is the asynchronous boundary between Runner writes
// and Bubble Tea's single Update loop. State updates are coalesced while plain
// operational lines remain bounded independently of Runner progress.
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

	finalMu    sync.Mutex
	finalState *state.State
	source     followStateSource
	now        func() time.Time
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
	program := tea.NewProgram(
		model,
		tea.WithContext(ctx),
		tea.WithInput(control.Terminal.Input),
		tea.WithOutput(control.Terminal.Output),
		tea.WithWindowSize(dimensions.Width, dimensions.Height),
		tea.WithColorProfile(bubbleColorProfile(control.Terminal.ColorProfile())),
		tea.WithoutSignalHandler(),
	)
	_, err = program.Run()
	started = model.started()
	if ctx.Err() != nil && (err == nil || errors.Is(err, tea.ErrProgramKilled)) {
		return ctx.Err()
	}
	return err
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
	s.finalMu.Lock()
	s.source = source
	s.finalMu.Unlock()
	s.publish(dashboardConfiguredMsg{initial: cloneDashboardState(initial), source: source})
}

func (s *bubbleDashboardSession) stateSaved(current state.State) {
	s.publish(dashboardStateMsg(cloneDashboardState(current)))
}

func (s *bubbleDashboardSession) flush(ctx context.Context) error {
	acknowledged := make(chan struct{})
	s.publish(dashboardFlushMsg{acknowledged: acknowledged})
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
	s.finalMu.Unlock()
	return nil
}

func (s *bubbleDashboardSession) printFinalSummary(output io.Writer) error {
	s.finalMu.Lock()
	if s.finalState == nil {
		s.finalMu.Unlock()
		return nil
	}
	current := cloneDashboardState(*s.finalState)
	source := s.source
	s.finalMu.Unlock()
	return printRunFinalSummary(output, current, source, s.now())
}

func (s *bubbleDashboardSession) publish(msg tea.Msg) {
	s.mu.Lock()
	switch msg.(type) {
	case dashboardStateMsg:
		for index := len(s.updates) - 1; index >= 0; index-- {
			if _, ok := s.updates[index].(dashboardStateMsg); ok {
				s.updates[index] = msg
				s.mu.Unlock()
				s.wakeUpdate()
				return
			}
		}
	case dashboardConfiguredMsg:
		for index := len(s.updates) - 1; index >= 0; index-- {
			if _, ok := s.updates[index].(dashboardConfiguredMsg); ok {
				s.updates[index] = msg
				s.mu.Unlock()
				s.wakeUpdate()
				return
			}
		}
	}
	s.updates = append(s.updates, msg)
	if len(s.updates) > dashboardUpdateLimit {
		s.updates[0] = nil
		s.updates = s.updates[1:]
	}
	s.mu.Unlock()
	s.wakeUpdate()
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
	ctx       context.Context
	control   PresentationControl
	session   *bubbleDashboardSession
	dashboard *liveDashboard
	viewport  viewport.Model
	width     int
	height    int

	interruptsWaiting int
	startup           *atomic.Bool
}

func newBubbleDashboardModel(ctx context.Context, control PresentationControl, session *bubbleDashboardSession, dimensions TerminalDimensions) bubbleDashboardModel {
	empty := state.State{Version: state.CurrentVersion}
	view := viewport.New(viewport.WithWidth(dimensions.Width), viewport.WithHeight(max(0, dimensions.Height-5)))
	view.SoftWrap = true
	view.FillHeight = true
	return bubbleDashboardModel{
		ctx: ctx, control: control, session: session,
		dashboard: newLiveDashboard(io.Discard, nil, empty, control.Terminal.Now),
		viewport:  view, width: dimensions.Width, height: dimensions.Height,
		startup: &atomic.Bool{},
	}
}

func (m bubbleDashboardModel) started() bool {
	return m.startup.Load()
}

func (m bubbleDashboardModel) Init() tea.Cmd {
	m.startup.Store(true)
	m.session.signalStartup(nil)
	return tea.Batch(m.waitForSessionUpdate(), dashboardElapsedTick(), dashboardActivityTick())
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

func dashboardElapsedTick() tea.Cmd {
	return tea.Tick(dashboardElapsedInterval, func(now time.Time) tea.Msg { return dashboardElapsedMsg(now) })
}

func dashboardActivityTick() tea.Cmd {
	return tea.Tick(dashboardActivityInterval, func(now time.Time) tea.Msg { return dashboardActivityMsg(now) })
}

func (m bubbleDashboardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var commands []tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = max(1, msg.Width), max(1, msg.Height)
		m.viewport.SetWidth(m.width)
		m.viewport.SetHeight(max(0, m.height-5))
	case tea.KeyPressMsg:
		if msg.String() == "ctrl+c" {
			m.interruptsWaiting++
			if m.interruptsWaiting == 1 {
				commands = append(commands, m.interrupt())
			}
			return m, tea.Batch(commands...)
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
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardStateMsg:
		m.dashboard.update(state.State(msg))
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardOutputMsg:
		m.dashboard.recordMessage(string(msg))
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardOperationalMsg:
		m.dashboard.operationalEvent(msg.event)
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardElapsedMsg:
		commands = append(commands, dashboardElapsedTick())
	case dashboardActivityMsg:
		_ = m.dashboard.activityChanged()
		commands = append(commands, dashboardActivityTick())
	case dashboardFlushMsg:
		commands = append(commands, tea.Tick(time.Second/30, func(time.Time) tea.Msg {
			return dashboardFlushRenderedMsg(msg)
		}))
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardFlushRenderedMsg:
		close(msg.acknowledged)
	case dashboardQueueStoppedMsg:
		if m.ctx.Err() != nil || errors.Is(msg.err, context.Canceled) {
			return m, tea.Quit
		}
		m.dashboard.recordMessage(msg.err.Error())
	}
	_, body, _ := m.dashboard.renderParts(m.dashboard.now())
	m.viewport.SetContent(body)
	updated, command := m.viewport.Update(msg)
	m.viewport = updated
	if command != nil {
		commands = append(commands, command)
	}
	return m, tea.Batch(commands...)
}

func (m bubbleDashboardModel) interrupt() tea.Cmd {
	return func() tea.Msg { return dashboardInterruptResultMsg{err: m.control.Interrupt(m.ctx)} }
}

func (m bubbleDashboardModel) View() tea.View {
	header, body, footer := m.dashboard.renderParts(m.dashboard.now())
	m.viewport.SetContent(body)
	lines := []string{
		lipgloss.NewStyle().Bold(true).Render("Backlog Run Dashboard"),
		strings.TrimPrefix(strings.SplitN(header, "\n", 3)[1], ""),
		strings.SplitN(header, "\n", 3)[2],
	}
	if m.height >= 5 {
		lines = append(lines, strings.Split(m.viewport.View(), "\n")...)
	}
	lines = append(lines, strings.Split(footer, "\n")...)
	if len(lines) > m.height {
		essential := []string{lines[1], lines[2], lines[len(lines)-2], lines[len(lines)-1]}
		if m.height < len(essential) {
			essential = essential[len(essential)-m.height:]
		}
		lines = essential
	}
	for index := range lines {
		lines[index] = fitDashboardLine(lines[index], m.width)
	}
	view := tea.NewView(strings.Join(lines, "\n"))
	view.AltScreen = true
	return view
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
