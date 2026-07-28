package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

const (
	dashboardActivityInterval = 100 * time.Millisecond
	dashboardElapsedInterval  = time.Second
	dashboardMessageLimit     = 12
	dashboardOccurrenceLimit  = dashboardMessageLimit * 2
)

type dashboardStage int

const (
	dashboardRunning dashboardStage = iota
	dashboardDraining
	dashboardSuspending
	dashboardForceStopping
	dashboardDrainComplete
	dashboardDrainFailed
	dashboardDrainIncomplete
	dashboardSuspensionComplete
	dashboardSuspensionIncomplete
	dashboardStopped
	dashboardFinished
	dashboardStageCount
)

// liveDashboard holds the aggregate observation model shared by the Bubble Tea
// dashboard and focused projection tests. It only reads lifecycle state and
// Activity evidence; rendering never writes Worker Activity.
type liveDashboard struct {
	output io.Writer
	source followStateSource
	now    func() time.Time

	mu                  sync.Mutex
	current             state.State
	messages            []dashboardMessage
	messageOccurrences  map[string]int
	shutdownOccurrences map[string]int
	occurrenceOrder     []string
	stage               dashboardStage
	lastActivity        map[string]fileSignature
	observations        map[string]dashboardActivityObservation
	pendingOutput       bytes.Buffer
	err                 error

	updates chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

type dashboardMessage struct {
	text             string
	semantic         dashboardSemantic
	shutdown         bool
	shutdownPriority bool
	plainMatched     bool
}

type fileSignature struct {
	path    string
	size    int64
	modTime time.Time
}

type dashboardActivityObservation struct {
	logPath string
	metrics followMetrics
	source  *normalizedActivitySource
}

func newLiveDashboard(output io.Writer, source followStateSource, initial state.State, now func() time.Time) *liveDashboard {
	if now == nil {
		now = time.Now
	}
	initial = cloneDashboardState(initial)
	return &liveDashboard{
		output: output, source: source, now: now, current: initial,
		messageOccurrences: make(map[string]int), shutdownOccurrences: make(map[string]int), stage: dashboardRunning,
		lastActivity: activitySignatures(initial), observations: make(map[string]dashboardActivityObservation), updates: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

func (d *liveDashboard) start() {
	d.redraw()
	go d.loop()
}

func (d *liveDashboard) loop() {
	defer close(d.done)
	activityTicker := time.NewTicker(dashboardActivityInterval)
	elapsedTicker := time.NewTicker(dashboardElapsedInterval)
	defer activityTicker.Stop()
	defer elapsedTicker.Stop()
	for {
		select {
		case <-d.updates:
			d.redraw()
		case <-activityTicker.C:
			if d.activityChanged() {
				d.redraw()
			}
		case <-elapsedTicker.C:
			d.redraw()
		case <-d.stop:
			return
		}
	}
}

func (d *liveDashboard) update(current state.State) {
	d.mu.Lock()
	d.current = cloneDashboardState(current)
	d.mu.Unlock()
	d.requestRedraw()
}

func cloneDashboardState(current state.State) state.State {
	cloned := current
	cloned.Leases = append([]scheduler.Lease(nil), current.Leases...)
	cloned.Runs = append([]scheduler.Run(nil), current.Runs...)
	for index := range cloned.Runs {
		run := &cloned.Runs[index]
		if run.Continuation != nil {
			continuation := *run.Continuation
			run.Continuation = &continuation
		}
		if run.SuspendingAt != nil {
			value := *run.SuspendingAt
			run.SuspendingAt = &value
		}
		if run.SuspendedAt != nil {
			value := *run.SuspendedAt
			run.SuspendedAt = &value
		}
		if run.CompletedAt != nil {
			value := *run.CompletedAt
			run.CompletedAt = &value
		}
		if run.AcknowledgedAt != nil {
			value := *run.AcknowledgedAt
			run.AcknowledgedAt = &value
		}
	}
	return cloned
}

func (d *liveDashboard) requestRedraw() {
	select {
	case d.updates <- struct{}{}:
	default:
	}
}

// Write receives append-only Runner lifecycle messages. In dashboard mode the
// messages are retained in the dashboard instead of being lost on the next
// redraw. Plain mode never uses this writer.
func (d *liveDashboard) Write(content []byte) (int, error) {
	d.mu.Lock()
	if d.err != nil {
		err := d.err
		d.mu.Unlock()
		return 0, err
	}
	_, _ = d.pendingOutput.Write(content)
	for {
		line, err := d.pendingOutput.ReadString('\n')
		if err != nil {
			_, _ = d.pendingOutput.WriteString(line)
			break
		}
		d.recordMessageLocked(strings.TrimSuffix(line, "\n"))
	}
	d.mu.Unlock()
	d.requestRedraw()
	return len(content), nil
}

func (d *liveDashboard) recordMessage(message string) {
	d.mu.Lock()
	d.recordMessageLocked(message)
	d.mu.Unlock()
}

func (d *liveDashboard) recordMessageLocked(message string) {
	message = normalizedDashboardMessage(message)
	if message == "" {
		return
	}
	d.trackOccurrenceLocked(message, false)
	if d.messageOccurrences[message] <= d.shutdownOccurrences[message] {
		// A typed event arrived first and already inserted this compatible
		// rendering into history.
		for index := range d.messages {
			tracked := &d.messages[index]
			if tracked.text == message && tracked.shutdown && !tracked.plainMatched {
				tracked.plainMatched = true
				break
			}
		}
		d.reconcileOccurrencesLocked(message)
		return
	}
	d.appendMessageLocked(dashboardMessage{text: message})
}

func (d *liveDashboard) trackOccurrenceLocked(message string, shutdown bool) {
	if d.messageOccurrences[message] == 0 && d.shutdownOccurrences[message] == 0 {
		d.occurrenceOrder = append(d.occurrenceOrder, message)
	}
	if shutdown {
		d.shutdownOccurrences[message]++
	} else {
		d.messageOccurrences[message]++
	}
	for len(d.occurrenceOrder) > dashboardOccurrenceLimit {
		pruneIndex := 0
		for index, tracked := range d.occurrenceOrder {
			if !d.hasVisibleUnmatchedShutdownLocked(tracked) {
				pruneIndex = index
				break
			}
		}
		pruned := d.occurrenceOrder[pruneIndex]
		d.occurrenceOrder = append(d.occurrenceOrder[:pruneIndex], d.occurrenceOrder[pruneIndex+1:]...)
		delete(d.messageOccurrences, pruned)
		delete(d.shutdownOccurrences, pruned)
	}
}

func (d *liveDashboard) hasVisibleUnmatchedShutdownLocked(message string) bool {
	for _, tracked := range d.messages {
		if tracked.text == message && tracked.shutdown && !tracked.plainMatched {
			return true
		}
	}
	return false
}

func (d *liveDashboard) reconcileOccurrencesLocked(message string) {
	if d.messageOccurrences[message] != d.shutdownOccurrences[message] {
		return
	}
	delete(d.messageOccurrences, message)
	delete(d.shutdownOccurrences, message)
	for index, tracked := range d.occurrenceOrder {
		if tracked == message {
			d.occurrenceOrder = append(d.occurrenceOrder[:index], d.occurrenceOrder[index+1:]...)
			return
		}
	}
}

func (d *liveDashboard) appendMessageLocked(message dashboardMessage) {
	d.messages = append(d.messages, message)
	if len(d.messages) <= dashboardMessageLimit {
		return
	}
	for index, retained := range d.messages {
		if !retained.shutdownPriority {
			d.messages = append(d.messages[:index], d.messages[index+1:]...)
			return
		}
	}
	d.messages = d.messages[1:]
}

func normalizedDashboardMessage(message string) string {
	return plainStatusValue(strings.TrimSpace(message))
}

func cloneDashboardMessages(messages []dashboardMessage) []dashboardMessage {
	return append([]dashboardMessage(nil), messages...)
}

// operationalEvent receives lifecycle state directly from the Runner. Message
// formatting remains independent, so presentation never infers semantics from
// append-only prose.
func (d *liveDashboard) operationalEvent(event runner.OperationalEvent) {
	message := normalizedDashboardMessage(runner.FormatOperationalEvent(event))
	semantic := dashboardOperationalEventSemantic(event)
	_, shutdownPriority := event.(runner.ShutdownEvent)
	next, hasNext := dashboardStageForOperationalEvent(event)
	if message == "" && !hasNext {
		return
	}

	d.mu.Lock()
	if message != "" {
		d.trackOccurrenceLocked(message, true)
		matched := false
		for index := len(d.messages) - 1; index >= 0; index-- {
			if d.messages[index].text == message && !d.messages[index].shutdown {
				d.messages[index].semantic = semantic
				d.messages[index].shutdown = true
				d.messages[index].shutdownPriority = shutdownPriority
				d.messages[index].plainMatched = true
				matched = true
				break
			}
		}
		if !matched {
			// Event delivery may lag far enough behind output for the matching
			// ordinary line to be evicted. Restore it as typed history. This also
			// represents the line when the event arrives before plain output.
			d.appendMessageLocked(dashboardMessage{text: message, semantic: semantic, shutdown: true, shutdownPriority: shutdownPriority})
		}
		d.reconcileOccurrencesLocked(message)
	}
	if hasNext && next > d.stage {
		d.stage = next
	}
	d.mu.Unlock()
	d.requestRedraw()
}

func dashboardOperationalEventSemantic(event runner.OperationalEvent) dashboardSemantic {
	switch event := event.(type) {
	case runner.CandidateDiscoveryFailed:
		return dashboardSemanticWarning
	case runner.CandidateDiscoveryRecovered:
		return dashboardSemanticActive
	case runner.ShutdownEvent:
		if event.Result == runner.ShutdownResultFailure {
			return dashboardSemanticAttention
		}
		switch event.Stage {
		case runner.ShutdownStageDraining, runner.ShutdownStageSuspending, runner.ShutdownStageSuspensionComplete:
			return dashboardSemanticWarning
		case runner.ShutdownStageForceStopping, runner.ShutdownStageDrainIncomplete, runner.ShutdownStageSuspensionIncomplete:
			return dashboardSemanticAttention
		case runner.ShutdownStageDrainComplete:
			return dashboardSemanticCompletion
		default:
			return dashboardSemanticMetadata
		}
	default:
		return dashboardSemanticMetadata
	}
}

func dashboardStageForOperationalEvent(event runner.OperationalEvent) (dashboardStage, bool) {
	shutdown, ok := event.(runner.ShutdownEvent)
	if !ok {
		return dashboardRunning, false
	}
	switch shutdown.Stage {
	case runner.ShutdownStageDraining:
		return dashboardDraining, true
	case runner.ShutdownStageSuspending:
		return dashboardSuspending, true
	case runner.ShutdownStageForceStopping:
		return dashboardForceStopping, true
	case runner.ShutdownStageDrainComplete:
		if shutdown.Result == runner.ShutdownResultFailure {
			return dashboardDrainFailed, true
		}
		return dashboardDrainComplete, true
	case runner.ShutdownStageDrainIncomplete:
		return dashboardDrainIncomplete, true
	case runner.ShutdownStageSuspensionComplete:
		return dashboardSuspensionComplete, true
	case runner.ShutdownStageSuspensionIncomplete:
		return dashboardSuspensionIncomplete, true
	default:
		return dashboardRunning, false
	}
}

func (d *liveDashboard) activityChanged() bool {
	d.mu.Lock()
	current := d.current
	previous := d.lastActivity
	d.mu.Unlock()
	next := activitySignatures(current)
	if signaturesEqual(previous, next) {
		return false
	}
	d.mu.Lock()
	d.lastActivity = next
	d.mu.Unlock()
	return true
}

func activitySignatures(current state.State) map[string]fileSignature {
	signatures := make(map[string]fileSignature)
	for _, run := range current.Runs {
		if run.LogPath == "" || !scheduler.IsActive(run.Status) {
			continue
		}
		path := activity.PathForLog(run.LogPath)
		info, err := os.Stat(path)
		if err != nil {
			path = run.LogPath
			info, err = os.Stat(path)
		}
		if err == nil {
			signatures[run.RunID] = fileSignature{path: path, size: info.Size(), modTime: info.ModTime()}
		}
	}
	return signatures
}

func signaturesEqual(left, right map[string]fileSignature) bool {
	if len(left) != len(right) {
		return false
	}
	for id, signature := range left {
		if right[id] != signature {
			return false
		}
	}
	return true
}

func (d *liveDashboard) redraw() {
	d.mu.Lock()
	if d.err != nil {
		d.mu.Unlock()
		return
	}
	current := d.current
	messages := cloneDashboardMessages(d.messages)
	stage := d.stage
	d.mu.Unlock()

	body := d.render(current, messages, stage, d.now())
	_, err := d.output.Write([]byte(body))
	if err != nil {
		d.mu.Lock()
		d.err = err
		d.mu.Unlock()
	}
}

type dashboardBodyAnchor struct {
	identity string
	line     int
}

type dashboardBodyLayout struct {
	text      string
	anchors   []dashboardBodyAnchor
	attention map[string]struct{}
}

type dashboardBodyBuilder struct {
	body    strings.Builder
	line    int
	anchors []dashboardBodyAnchor
}

func (b *dashboardBodyBuilder) separate() {
	if b.body.Len() > 0 {
		b.write("\n")
	}
}

func (b *dashboardBodyBuilder) write(content string) {
	b.body.WriteString(content)
	b.line += strings.Count(content, "\n")
}

func (b *dashboardBodyBuilder) anchor(identity string) {
	b.anchors = append(b.anchors, dashboardBodyAnchor{identity: identity, line: b.line})
}

func dashboardSectionAnchor(name string) string { return "section:" + name }
func dashboardRunAnchor(runID string) string    { return "run:" + runID }

func (d *liveDashboard) render(current state.State, messages []dashboardMessage, stage dashboardStage, now time.Time) string {
	header, body, _ := d.renderPartsFor(current, messages, stage, now, dashboardStyler{})
	return header + "\n" + body + "\n\n" + dashboardFooter(stage) + "\n"
}

func (d *liveDashboard) renderPartsWithLayout(now time.Time, styler dashboardStyler) (string, dashboardBodyLayout, string, dashboardStage) {
	d.mu.Lock()
	current := cloneDashboardState(d.current)
	messages := cloneDashboardMessages(d.messages)
	stage := d.stage
	d.mu.Unlock()
	header, layout, footer := d.renderPartsForWithLayout(current, messages, stage, now, styler)
	return header, layout, footer, stage
}

func (d *liveDashboard) renderPartsFor(current state.State, messages []dashboardMessage, stage dashboardStage, now time.Time, styler dashboardStyler) (string, string, string) {
	header, layout, footer := d.renderPartsForWithLayout(current, messages, stage, now, styler)
	return header, layout.text, footer
}

func (d *liveDashboard) renderPartsForWithLayout(current state.State, messages []dashboardMessage, stage dashboardStage, now time.Time, styler dashboardStyler) (string, dashboardBodyLayout, string) {
	sections := d.observeSections(current, now)
	capacity := "Worker capacity: pending configuration"
	if current.MaxConcurrentIssues > 0 {
		used := dashboardUsedCapacity(current)
		available := max(0, current.MaxConcurrentIssues-used)
		capacity = fmt.Sprintf("Worker capacity: %d used | %d available | %d total", used, available, current.MaxConcurrentIssues)
	}

	header := fmt.Sprintf("Backlog Run Dashboard\nRepository: %s\n%s",
		valueOr(plainStatusValue(current.Repo), "not initialized"), capacity)
	body := dashboardBodyBuilder{}
	body.renderSection(statusActive, "Active Runs", sections[statusActive], now, styler)
	body.renderSection(statusAttention, "Attention Required", sections[statusAttention], now, styler)
	body.renderSection(statusOutcomes, "Outcomes to Acknowledge", sections[statusOutcomes], now, styler)
	body.renderCompletions(sections[statusCompletions], now, styler)
	if len(messages) > 0 {
		body.separate()
		body.anchor(dashboardSectionAnchor("Operational messages"))
		body.write(styler.render(dashboardSemanticMetadata, "Operational messages") + "\n")
		for _, message := range messages {
			body.write(styler.render(message.semantic, "  "+message.text) + "\n")
		}
	}
	attention := make(map[string]struct{}, len(sections[statusAttention]))
	for _, observed := range sections[statusAttention] {
		attention[observed.run.RunID] = struct{}{}
	}
	return header, dashboardBodyLayout{text: body.body.String(), anchors: body.anchors, attention: attention}, dashboardFooterParts(stage)
}

// observeSections applies the shared status ownership and history projection.
// Only Activity observation remains dashboard-specific so redraws can reuse an
// open normalized Activity stream.
func (d *liveDashboard) observeSections(current state.State, now time.Time) map[statusSection][]statusRun {
	leasedRuns := make(map[string]struct{}, len(current.Leases))
	for _, lease := range current.Leases {
		leasedRuns[lease.RunID] = struct{}{}
	}
	recentCompletions := selectRecentCompletions(current.Runs)
	sections := map[statusSection][]statusRun{
		statusActive: {}, statusAttention: {}, statusOutcomes: {}, statusCompletions: {},
	}
	for _, run := range current.Runs {
		_, leased := leasedRuns[run.RunID]
		section := statusSectionFor(run, leased)
		if section == statusHistory {
			switch {
			case (run.Status == scheduler.StatusFailed || run.Status == scheduler.StatusNeedsHuman) && run.AcknowledgedAt == nil:
				section = statusOutcomes
			case run.Status == scheduler.StatusMerged && recentCompletions[run.RunID]:
				section = statusCompletions
			default:
				continue
			}
		}
		observation := runObservation{run: run, process: observeFollowRun(d.source, run), observed: now}
		if leased && run.LogPath != "" {
			observation.metrics = d.observeActivity(run)
		}
		sections[section] = append(sections[section], statusRun{run: run, observation: observation})
	}
	sortDashboardActiveRuns(sections[statusActive])
	sortStatusRuns(sections[statusAttention])
	sortStatusRuns(sections[statusOutcomes])
	sortDashboardCompletions(sections[statusCompletions])
	return sections
}

func sortDashboardActiveRuns(runs []statusRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		leftAnomaly := dashboardActiveLivenessAnomaly(runs[i])
		rightAnomaly := dashboardActiveLivenessAnomaly(runs[j])
		if leftAnomaly != rightAnomaly {
			return leftAnomaly
		}
		left, right := runs[i].run, runs[j].run
		if left.StartedAt.Equal(right.StartedAt) {
			return left.RunID < right.RunID
		}
		return left.StartedAt.Before(right.StartedAt)
	})
}

func dashboardActiveLivenessAnomaly(observed statusRun) bool {
	return observed.run.Status == scheduler.StatusRunning &&
		observed.observation.process.workerLivenessState != workerLivenessAlive
}

func sortDashboardCompletions(runs []statusRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		left, right := completionTime(runs[i].run), completionTime(runs[j].run)
		if left.Equal(right) {
			return runs[i].run.RunID > runs[j].run.RunID
		}
		return left.After(right)
	})
}

func (d *liveDashboard) observeActivity(run scheduler.Run) followMetrics {
	cached, exists := d.observations[run.RunID]
	if !exists || cached.logPath != run.LogPath {
		observed, source := observeRunOnce(d.source, run, io.Discard, d.now)
		cached = dashboardActivityObservation{logPath: run.LogPath, metrics: observed.metrics, source: source}
	} else if cached.source != nil {
		consumeActivity(&cached.metrics, cached.source)
	}
	d.observations[run.RunID] = cached
	return cached.metrics
}

func renderDashboardCompletions(output *strings.Builder, runs []statusRun, now time.Time, styler dashboardStyler) {
	body := dashboardBodyBuilder{}
	body.renderCompletions(runs, now, styler)
	_, _ = output.WriteString("\n" + body.body.String())
}

func (b *dashboardBodyBuilder) renderCompletions(runs []statusRun, now time.Time, styler dashboardStyler) {
	b.separate()
	b.anchor(dashboardSectionAnchor("Recent Completions"))
	heading := fmt.Sprintf("Recent Completions (%d)", len(runs))
	b.write(styler.render(dashboardSemanticCompletion, heading) + "\n")
	if len(runs) == 0 {
		b.write(styler.render(dashboardSemanticMetadata, "  none") + "\n")
		return
	}
	visible := min(3, len(runs))
	for _, observed := range runs[:visible] {
		run := observed.run
		b.anchor(dashboardRunAnchor(run.RunID))
		identity := fmt.Sprintf("#%d", run.Issue)
		if title := plainStatusValue(run.IssueTitle); title != "" {
			identity += "  " + title
		}
		progress := summarizeRunProgress(run, followMetrics{}, now)
		line := styler.render(dashboardSemanticCompletion, "  "+identity)
		if run.PullRequest != "" {
			line += styler.render(dashboardSemanticMetadata, " | PR: "+plainStatusValue(run.PullRequest))
		}
		completed := "n/a"
		if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
			completed = displayDuration(now.Sub(*run.CompletedAt)) + " ago"
		}
		line += styler.render(dashboardSemanticMetadata, fmt.Sprintf(" | Elapsed: %s | Completed: %s", progress.elapsed, completed))
		b.write(line + "\n")
	}
	if remainder := len(runs) - visible; remainder > 0 {
		line := fmt.Sprintf("  %d more completions", remainder)
		b.write(styler.render(dashboardSemanticMetadata, line) + "\n")
	}
}

func renderDashboardSection(output *strings.Builder, section statusSection, name string, runs []statusRun, now time.Time, styler dashboardStyler) {
	body := dashboardBodyBuilder{}
	body.renderSection(section, name, runs, now, styler)
	_, _ = output.WriteString("\n" + body.body.String())
}

func (b *dashboardBodyBuilder) renderSection(section statusSection, name string, runs []statusRun, now time.Time, styler dashboardStyler) {
	b.separate()
	b.anchor(dashboardSectionAnchor(name))
	heading := fmt.Sprintf("%s (%d)", name, len(runs))
	b.write(styler.render(dashboardSectionSemantic(section), heading) + "\n")
	if len(runs) == 0 {
		b.write(styler.render(dashboardSemanticMetadata, "  none") + "\n")
		return
	}
	for _, observed := range runs {
		run := observed.run
		b.anchor(dashboardRunAnchor(run.RunID))
		identity := fmt.Sprintf("#%d", run.Issue)
		if title := plainStatusValue(run.IssueTitle); title != "" {
			identity += "  " + title
		}
		progress := summarizeRunProgress(run, observed.observation.metrics, now)
		b.write(styler.render(dashboardRunSemantic(observed, section), "  "+identity) + "\n")
		if run.IssueURL != "" {
			line := "    Issue: " + plainStatusValue(run.IssueURL)
			b.write(styler.render(dashboardSemanticMetadata, line) + "\n")
		}
		state := "    State: " + displayedRunState(run, observed.observation.process)
		line := styler.render(dashboardRunSemantic(observed, section), state)
		line += styler.render(dashboardSemanticMetadata, " | Elapsed: "+progress.elapsed+" | ")
		liveness := "Worker liveness: " + plainStatusValue(observed.observation.process.workerLiveness)
		line += styler.render(dashboardLivenessSemantic(observed), liveness)
		b.write(line + "\n")
		line = fmt.Sprintf("    Activity age: %s | Deepest operation: %s", progress.activityAge, plainStatusValue(progress.deepestOperation))
		b.write(styler.render(dashboardSemanticMetadata, line) + "\n")
		line = fmt.Sprintf("    Turns: Worker %s | Subagent %s | Observed tokens: %s", progress.workerTurns, progress.subagentTurns, progress.observedTokens)
		b.write(styler.render(dashboardSemanticMetadata, line) + "\n")
		if run.Error != "" {
			line = "    Diagnostic: " + plainStatusValue(strings.TrimSpace(run.Error))
			b.write(styler.render(dashboardSemanticAttention, line) + "\n")
		}
	}
}

func dashboardUsedCapacity(current state.State) int {
	runs := make(map[string]scheduler.Run, len(current.Runs))
	for _, run := range current.Runs {
		runs[run.RunID] = run
	}
	used := 0
	for _, lease := range current.Leases {
		run, exists := runs[lease.RunID]
		if exists && scheduler.ConsumesWorkerCapacity(run) {
			used++
		}
	}
	return used
}

type dashboardStagePresentation struct {
	summary       string
	stage         string
	nextInterrupt string
}

type dashboardStageDefinition struct {
	presentation dashboardStagePresentation
	semantic     dashboardSemantic
}

var dashboardStageDefinitions = [dashboardStageCount]dashboardStageDefinition{
	dashboardRunning: {
		presentation: dashboardStagePresentation{
			summary:       "Running: Ctrl-C starts Drain, stopping admission while Owned Workers finish.",
			stage:         "Running",
			nextInterrupt: "start Drain and stop Admission",
		},
		semantic: dashboardSemanticActive,
	},
	dashboardDraining: {
		presentation: dashboardStagePresentation{
			summary:       "Draining: admission is stopped; next Ctrl-C suspends unfinished Runs within the shared deadline.",
			stage:         "Draining",
			nextInterrupt: "suspend unfinished Runs within the shared deadline",
		},
		semantic: dashboardSemanticWarning,
	},
	dashboardSuspending: {
		presentation: dashboardStagePresentation{
			summary:       "Suspending: continuation boundaries are being established; next Ctrl-C force stops remaining verified Worker groups.",
			stage:         "Suspending",
			nextInterrupt: "force stop remaining verified Worker groups",
		},
		semantic: dashboardSemanticWarning,
	},
	dashboardForceStopping: {
		presentation: dashboardStagePresentation{
			summary:       "Force stopping: Worker identities are revalidated before signaling; next Ctrl-C repeats the force-stop request.",
			stage:         "Force stopping",
			nextInterrupt: "repeat the force-stop request after identity checks",
		},
		semantic: dashboardSemanticAttention,
	},
	dashboardDrainComplete: {
		presentation: dashboardStagePresentation{
			summary:       "Drain complete: no Owned Workers remain; no further interrupt is needed.",
			stage:         "Drain complete",
			nextInterrupt: "no effect",
		},
		semantic: dashboardSemanticCompletion,
	},
	dashboardDrainFailed: {
		presentation: dashboardStagePresentation{
			summary:       "Drain complete: no Owned Workers remain, but the Runner is exiting after an operational failure.",
			stage:         "Drain complete after operational failure",
			nextInterrupt: "no effect",
		},
		semantic: dashboardSemanticAttention,
	},
	dashboardDrainIncomplete: {
		presentation: dashboardStagePresentation{
			summary:       "Drain incomplete: Worker liveness remains unverified; no further interrupt has an effect before exit.",
			stage:         "Drain incomplete; Worker liveness is unverified",
			nextInterrupt: "no effect",
		},
		semantic: dashboardSemanticAttention,
	},
	dashboardSuspensionComplete: {
		presentation: dashboardStagePresentation{
			summary:       "Suspension finished: no further interrupt has an effect before exit.",
			stage:         "Suspension finished",
			nextInterrupt: "no effect",
		},
		semantic: dashboardSemanticWarning,
	},
	dashboardSuspensionIncomplete: {
		presentation: dashboardStagePresentation{
			summary:       "Suspension incomplete: one or more shutdown steps failed; review operational messages and Run diagnostics; no further interrupt has an effect before exit.",
			stage:         "Suspension incomplete",
			nextInterrupt: "no effect",
		},
		semantic: dashboardSemanticAttention,
	},
	dashboardStopped: {
		presentation: dashboardStagePresentation{
			summary:       "Stopped: the runner is exiting; interrupts have no further effect.",
			stage:         "Stopped; the Runner is exiting",
			nextInterrupt: "no effect",
		},
		semantic: dashboardSemanticWarning,
	},
	dashboardFinished: {
		presentation: dashboardStagePresentation{
			summary:       "Complete: the runner has exited; interrupts have no further effect.",
			stage:         "Complete; the Runner has exited",
			nextInterrupt: "no effect",
		},
		semantic: dashboardSemanticCompletion,
	},
}

func dashboardStageDefinitionFor(stage dashboardStage) dashboardStageDefinition {
	if stage < dashboardRunning || stage >= dashboardStageCount {
		return dashboardStageDefinitions[dashboardRunning]
	}
	return dashboardStageDefinitions[stage]
}

func dashboardStagePresentationFor(stage dashboardStage) dashboardStagePresentation {
	return dashboardStageDefinitionFor(stage).presentation
}

func dashboardStageSemantic(stage dashboardStage) dashboardSemantic {
	return dashboardStageDefinitionFor(stage).semantic
}

func dashboardFooter(stage dashboardStage) string {
	return dashboardStagePresentationFor(stage).summary
}

func dashboardFooterParts(stage dashboardStage) string {
	presentation := dashboardStagePresentationFor(stage)
	return fmt.Sprintf("Runner stage: %s\nNext Ctrl-C: %s", presentation.stage, presentation.nextInterrupt)
}

func (d *liveDashboard) finalSummary(current state.State) error {
	d.update(current)
	d.stopLoop()
	d.mu.Lock()
	d.stage = dashboardFinished
	messages := cloneDashboardMessages(d.messages)
	d.mu.Unlock()
	body := "Final aggregate summary\n" + d.render(current, messages, dashboardFinished, d.now())
	_, err := d.output.Write([]byte(body))
	return err
}

func (d *liveDashboard) stopLoop() {
	d.once.Do(func() {
		close(d.stop)
		<-d.done
	})
}

func (d *liveDashboard) close() {
	d.stopLoop()
	d.mu.Lock()
	finished := d.stage == dashboardFinished
	if !finished && d.stage < dashboardDrainComplete {
		d.stage = dashboardStopped
	}
	d.mu.Unlock()
	// A state or lifecycle update can be queued when Run returns. Publish the
	// latest complete shutdown frame before restoring the cursor. Natural exit
	// already published its specially labeled final aggregate summary.
	if !finished {
		d.redraw()
	}
}
