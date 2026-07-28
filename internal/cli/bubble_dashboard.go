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

const (
	dashboardOutputUpdateLimit = 64
	dashboardNavigationHelp    = "Nav: ↑↓/jk PgUp/Dn/f/b Home/End g/G a:Attention"
)

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
		tea.WithColorProfile(bubbleColorProfile(model.colorProfile)),
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

	selectedAnchor   string
	attentionKnown   map[string]struct{}
	attentionPending map[string]struct{}

	interruptsWaiting int
	pendingFlushes    []dashboardFlushMsg
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
	model := bubbleDashboardModel{
		ctx: ctx, control: control, session: session,
		dashboard: newLiveDashboard(io.Discard, nil, empty, control.Terminal.Now),
		viewport:  view, width: dimensions.Width, height: dimensions.Height,
		attentionKnown: make(map[string]struct{}), attentionPending: make(map[string]struct{}),
		colorProfile: profile, startup: &atomic.Bool{},
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
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = max(1, msg.Width), max(1, msg.Height)
	case tea.BackgroundColorMsg:
		m.styler = newDashboardStyler(m.colorProfile, msg.IsDark())
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
		m.dashboard.operationalEvent(msg.event)
		if m.control.operationalEvents != nil {
			m.control.operationalEvents.complete()
		}
		commands = append(commands, m.waitForOperationalEvent())
		commands = append(commands, m.renderPendingFlushes()...)
	case dashboardElapsedMsg:
		commands = append(commands, dashboardElapsedTick())
	case dashboardActivityMsg:
		_ = m.dashboard.activityChanged()
		commands = append(commands, dashboardActivityTick())
	case dashboardFlushMsg:
		m.pendingFlushes = append(m.pendingFlushes, msg)
		commands = append(commands, m.renderPendingFlushes()...)
		commands = append(commands, m.waitForSessionUpdate())
	case dashboardFlushRenderedMsg:
		close(msg.acknowledged)
	case dashboardQueueStoppedMsg:
		if m.ctx.Err() != nil || errors.Is(msg.err, context.Canceled) {
			return m, tea.Quit
		}
		m.dashboard.recordMessage(msg.err.Error())
	}

	m.refreshViewport(selection)
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

	if m.navigateViewport(msg) {
		m.selectViewportAnchor()
		if m.clearVisibleAttention() {
			m.refreshViewport(m.currentSelection())
		}
	}
	return m, tea.Batch(commands...)
}

func (m bubbleDashboardModel) interrupt() tea.Cmd {
	return func() tea.Msg { return dashboardInterruptResultMsg{err: m.control.Interrupt(m.ctx)} }
}

func (m *bubbleDashboardModel) renderPendingFlushes() []tea.Cmd {
	if m.control.operationalEvents != nil && !m.control.operationalEvents.idle() {
		return nil
	}
	commands := make([]tea.Cmd, 0, len(m.pendingFlushes))
	for _, pending := range m.pendingFlushes {
		message := pending
		commands = append(commands, tea.Tick(time.Second/30, func(time.Time) tea.Msg {
			return dashboardFlushRenderedMsg(message)
		}))
	}
	m.pendingFlushes = nil
	return commands
}

type dashboardSelection struct {
	identity string
	relative int
	valid    bool
}

func (m *bubbleDashboardModel) refreshViewport(selection dashboardSelection) {
	header, layout, footer, stage := m.dashboard.renderPartsWithLayout(m.dashboard.now(), m.styler)
	m.header = dashboardHeaderWithAttention(header, len(m.attentionPending))
	m.footer = footer + "\n" + dashboardNavigationHelp
	m.layout = layout
	m.stage = stage
	m.resizeViewport()
	m.viewport.SetContent(layout.text)
	if selection.valid {
		if line, exists := m.anchorVisualLine(selection.identity); exists {
			m.viewport.SetYOffset(m.dashboardBodyStart() + line - selection.relative)
			m.selectedAnchor = selection.identity
			return
		}
	}
	m.selectViewportAnchor()
}

func dashboardHeaderWithAttention(header string, pending int) string {
	if pending == 0 {
		return header
	}
	lines := strings.Split(header, "\n")
	if len(lines) > 1 {
		lines[1] += fmt.Sprintf(" | NEW ATTENTION (%d): press a", pending)
	}
	return strings.Join(lines, "\n")
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

func (m *bubbleDashboardModel) jumpToAttention() {
	identity := dashboardSectionAnchor("Attention Required")
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
		if line, exists := m.anchorVisualLine(dashboardRunAnchor(runID)); exists && !m.visualLineVisible(line) {
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
	// Keep direct projection updates visible to callers that render without a
	// preceding Bubble Tea message. The program's normal path already refreshes
	// in Update, so this local copy does not alter navigation state.
	m.refreshViewport(m.currentSelection())
	frame := m.dashboardFrame()

	lines := make([]string, 0, m.height)
	if frame.titleHeight > 0 {
		lines = append(lines, renderDashboardTitle(frame.title))
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
	headerLines := strings.SplitN(m.header, "\n", 3)
	chrome := dashboardChromeLines(headerLines[1:], strings.Split(m.footer, "\n"), m.stage, m.styler, m.width, m.height)
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

func renderDashboardTitle(title string) string {
	return lipgloss.NewStyle().Bold(true).Render(title)
}

type dashboardChrome struct {
	top    []string
	bottom []string
}

func dashboardChromeLines(header, footer []string, stage dashboardStage, styler dashboardStyler, width, height int) dashboardChrome {
	if width <= 0 || height <= 0 {
		return dashboardChrome{}
	}
	chromeLimit := height
	if height >= 3 {
		chromeLimit--
	}
	metadata := styledDashboardChromeItems(header, dashboardSemanticMetadata, styler)
	lifecycle := styledDashboardFooterItems(footer, stage, styler)
	headerGroups := dashboardChromeGroups(metadata)
	footerGroups := dashboardChromeGroups(lifecycle)
	compactNavigation := append([]string(nil), footer...)
	if len(compactNavigation) > 2 {
		compactNavigation[2] = "N:jk/fb Pg H/E gG a"
	}
	compactNavigation = styledDashboardFooterItems(compactNavigation, stage, styler)
	compactHeader := styledDashboardChromeItems(compactDashboardHeader(header), dashboardSemanticMetadata, styler)
	compactFooter := styledDashboardFooterItems(compactDashboardFooter(footer), stage, styler)
	candidates := []dashboardChrome{
		{top: wrapDashboardChrome(headerGroups, width), bottom: wrapDashboardChrome(footerGroups, width)},
		{top: wrapDashboardChrome(headerGroups, width), bottom: wrapDashboardChrome(dashboardChromeGroups(compactNavigation), width)},
		{top: wrapDashboardChrome([][]string{metadata}, width), bottom: wrapDashboardChrome([][]string{lifecycle}, width)},
		{top: wrapDashboardChrome([][]string{compactHeader}, width), bottom: wrapDashboardChrome([][]string{compactFooter}, width)},
		// A one-line terminal cannot have distinct header and footer rows. Keep
		// the whole compact chrome in the footer row so lifecycle guidance and
		// navigation are never moved above the scrollable body.
		{bottom: wrapDashboardChrome([][]string{append(append([]string(nil), compactHeader...), compactFooter...)}, width)},
	}
	for _, candidate := range candidates {
		if len(candidate.top)+len(candidate.bottom) <= chromeLimit {
			return candidate
		}
	}

	// When not all chrome can fit, spend the available rows on the fixed footer
	// before retaining header details. Keep next-interrupt guidance and
	// navigation in separately allocated rows whenever the terminal permits it.
	next, navigation := compactFooterLine(compactFooter, 1), compactFooterLine(compactFooter, 2)
	bottom := wrapDashboardChrome([][]string{{next, navigation}}, width)
	if len(bottom) > chromeLimit && chromeLimit >= 2 {
		bottom = []string{fitDashboardLine(next, width), fitDashboardLine(navigation, width)}
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
			line = strings.Replace(line, " | NEW ATTENTION ", " | ! ", 1)
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
		line = strings.Replace(line, dashboardNavigationHelp, "N:jk/fb Pg H/E gG a", 1)
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

func styledDashboardChromeItems(items []string, semantic dashboardSemantic, styler dashboardStyler) []string {
	styled := make([]string, len(items))
	for index, item := range items {
		styled[index] = styler.render(semantic, item)
	}
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
