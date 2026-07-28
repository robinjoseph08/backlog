package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/x/ansi"
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
	admission           dashboardAdmission
	diagnosticsOpen     bool
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

type dashboardAdmission struct {
	snapshotComplete    bool
	degraded            bool
	consecutiveFailures int
	firstFailure        time.Time
	latestFailure       time.Time
	retryAt             time.Time
	operation           runner.CandidateDiscoveryOperation
	issue               *int
	cause               string
	equivalentFailures  map[string]int
	equivalentOrder     []string
	failures            []runner.CandidateDiscoveryFailed
	recoveredAt         time.Time
	recoveredFailures   int
}

const (
	dashboardDiagnosticLimit          = 20
	admissionAggregationIdentityLimit = 256
	admissionRecoveryNotice           = 10 * time.Second
)

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

// Write receives append-only Runner operational lines that do not have a
// structured dashboard surface. They remain visible instead of being lost on
// the next redraw. Plain mode never uses this writer.
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

func normalizedDashboardDiagnostic(message string) string {
	message = strings.ReplaceAll(message, "\r", "\n")
	state := terminalText
	filtered := filterTerminalControls([]byte(message), &state, true)
	return strings.Join(strings.Fields(string(filtered)), " ")
}

func cloneDashboardMessages(messages []dashboardMessage) []dashboardMessage {
	return append([]dashboardMessage(nil), messages...)
}

// operationalEvent receives lifecycle state directly from the Runner. Message
// formatting remains independent, so presentation never infers Admission or
// other operational semantics from append-only prose.
func (d *liveDashboard) operationalEvent(event runner.OperationalEvent) {
	switch event := event.(type) {
	case runner.CandidateSnapshotCompleted:
		d.mu.Lock()
		d.admission.snapshotComplete = true
		d.admission.degraded = false
		d.admission.recoveredAt = time.Time{}
		d.admission.recoveredFailures = 0
		d.mu.Unlock()
		d.requestRedraw()
		return
	case runner.CandidateDiscoveryFailed:
		d.mu.Lock()
		d.recordAdmissionFailureLocked(event)
		d.mu.Unlock()
		d.requestRedraw()
		return
	case runner.CandidateDiscoveryRecovered:
		d.mu.Lock()
		d.admission.snapshotComplete = true
		d.admission.degraded = false
		d.admission.consecutiveFailures = 0
		d.admission.equivalentFailures = nil
		d.admission.equivalentOrder = nil
		d.admission.recoveredAt = event.OccurredAt
		d.admission.recoveredFailures = event.Failures
		d.mu.Unlock()
		d.requestRedraw()
		return
	}

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

func (d *liveDashboard) recordAdmissionFailureLocked(failure runner.CandidateDiscoveryFailed) {
	cause := normalizedDashboardMessage(failure.Cause)
	if cause == "" && failure.Err != nil {
		cause = normalizedDashboardMessage(failure.Err.Error())
	}
	if cause == "" {
		cause = "unknown error"
	}
	newEpisode := !d.admission.degraded
	if d.admission.degraded && !failure.FirstFailureAt.IsZero() && !d.admission.firstFailure.IsZero() && !failure.FirstFailureAt.Equal(d.admission.firstFailure) {
		newEpisode = true
	}
	if d.admission.degraded && failure.ConsecutiveFailures > 0 && d.admission.consecutiveFailures > 0 && failure.ConsecutiveFailures < d.admission.consecutiveFailures {
		newEpisode = true
	}
	if newEpisode {
		d.admission.firstFailure = failure.FirstFailureAt
		if d.admission.firstFailure.IsZero() {
			d.admission.firstFailure = failure.OccurredAt
		}
		d.admission.equivalentFailures = make(map[string]int)
		d.admission.equivalentOrder = nil
	}
	d.admission.snapshotComplete = false
	d.admission.degraded = true
	d.admission.consecutiveFailures = failure.ConsecutiveFailures
	d.admission.latestFailure = failure.OccurredAt
	d.admission.retryAt = failure.RetryAt
	d.admission.operation = failure.Operation
	d.admission.issue = cloneIssue(failure.Issue)
	d.admission.cause = cause
	d.admission.recoveredAt = time.Time{}
	d.admission.recoveredFailures = 0
	key := string(failure.Operation) + "\x00" + cause
	d.admission.equivalentFailures = retainBoundedAdmissionOccurrences(d.admission.equivalentFailures, &d.admission.equivalentOrder, key, presentationFailureOccurrences(failure))
	d.admission.failures = append(d.admission.failures, failure)
	if len(d.admission.failures) > dashboardDiagnosticLimit {
		d.admission.failures = append([]runner.CandidateDiscoveryFailed(nil), d.admission.failures[len(d.admission.failures)-dashboardDiagnosticLimit:]...)
	}
}

func retainBoundedAdmissionOccurrences(counts map[string]int, order *[]string, key string, occurrences int) map[string]int {
	if counts == nil {
		counts = make(map[string]int, admissionAggregationIdentityLimit)
	}
	if _, exists := counts[key]; exists {
		counts[key] += occurrences
		touchAdmissionIdentity(order, key)
		return counts
	}
	if len(counts) >= admissionAggregationIdentityLimit && len(*order) > 0 {
		delete(counts, (*order)[0])
		*order = (*order)[1:]
	}
	counts[key] = occurrences
	*order = append(*order, key)
	return counts
}

func takeBoundedAdmissionOccurrences(counts map[string]int, order *[]string, key string) int {
	occurrences, exists := counts[key]
	if !exists {
		return 0
	}
	delete(counts, key)
	removeAdmissionIdentity(order, key)
	return occurrences
}

func touchAdmissionIdentity(order *[]string, key string) {
	removeAdmissionIdentity(order, key)
	*order = append(*order, key)
}

func removeAdmissionIdentity(order *[]string, key string) {
	for index, candidate := range *order {
		if candidate != key {
			continue
		}
		copy((*order)[index:], (*order)[index+1:])
		*order = (*order)[:len(*order)-1]
		return
	}
}

func cloneIssue(issue *int) *int {
	if issue == nil {
		return nil
	}
	value := *issue
	return &value
}

func (d *liveDashboard) toggleDiagnostics() {
	d.mu.Lock()
	d.diagnosticsOpen = !d.diagnosticsOpen
	d.mu.Unlock()
	d.requestRedraw()
}

func (d *liveDashboard) markNaturalExit() {
	d.mu.Lock()
	if d.stage == dashboardRunning {
		d.stage = dashboardFinished
	}
	d.mu.Unlock()
	d.requestRedraw()
}

func (d *liveDashboard) recoveryNoticeRemaining(now time.Time) time.Duration {
	d.mu.Lock()
	degraded := d.admission.degraded
	recoveredAt := d.admission.recoveredAt
	d.mu.Unlock()
	if degraded || recoveredAt.IsZero() {
		return 0
	}
	elapsed := now.Sub(recoveredAt)
	if elapsed < 0 {
		elapsed = 0
	}
	remaining := admissionRecoveryNotice - elapsed
	if remaining < 0 {
		return 0
	}
	return remaining
}

func dashboardOperationalEventSemantic(event runner.OperationalEvent) dashboardSemantic {
	switch event := event.(type) {
	case runner.CandidateDiscoveryFailed:
		return dashboardSemanticWarning
	case runner.CandidateSnapshotCompleted, runner.CandidateDiscoveryRecovered:
		return dashboardSemanticActive
	case runner.RunLifecycleEvent:
		switch event.Stage {
		case runner.RunLifecycleClaimed, runner.RunLifecycleStarted, runner.RunLifecycleResumed:
			return dashboardSemanticActive
		case runner.RunLifecycleCleanupCompleted, runner.RunLifecycleMerged:
			return dashboardSemanticCompletion
		case runner.RunLifecycleFailed, runner.RunLifecycleNeedsHuman:
			return dashboardSemanticAttention
		default:
			return dashboardSemanticMetadata
		}
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

type dashboardDensity uint8

const (
	dashboardDensityRoomy dashboardDensity = iota
	dashboardDensityConstrained
	dashboardDensityMinimal
)

type responsiveDashboardOptions struct {
	density            dashboardDensity
	width              int
	selected           string
	expansionOverrides map[string]bool
	styler             dashboardStyler
}

func dashboardDensityForHeight(height int) dashboardDensity {
	switch {
	case height >= 24:
		return dashboardDensityRoomy
	case height >= 12:
		return dashboardDensityConstrained
	default:
		return dashboardDensityMinimal
	}
}

func (o responsiveDashboardOptions) expanded(identity string, section bool) bool {
	if expanded, exists := o.expansionOverrides[identity]; exists {
		return expanded
	}
	if section {
		return identity != dashboardSectionAnchor("Recent Completions") || o.density == dashboardDensityRoomy
	}
	return o.density == dashboardDensityRoomy && identity == o.selected
}

type dashboardCapacity struct {
	configured      bool
	used, available int
	total           int
}

func (c dashboardCapacity) full() string {
	if !c.configured {
		return "Worker capacity: pending configuration"
	}
	return fmt.Sprintf("Worker capacity: %d used | %d available | %d total", c.used, c.available, c.total)
}

func (c dashboardCapacity) compact() string {
	if !c.configured {
		return "W:pending"
	}
	return fmt.Sprintf("W:%du/%da/%dt", c.used, c.available, c.total)
}

type dashboardProjectionMetadata struct {
	repository         string
	capacity           dashboardCapacity
	healthy, anomalous int
}

type dashboardProjection struct {
	metadata  dashboardProjectionMetadata
	header    string
	sections  map[statusSection][]statusRun
	attention map[string]struct{}
	footer    string
}

func (d *liveDashboard) project(current state.State, stage dashboardStage, now time.Time) dashboardProjection {
	sections := d.observeSections(current, now)
	capacity := dashboardCapacity{}
	if current.MaxConcurrentIssues > 0 {
		used := dashboardUsedCapacity(current)
		capacity = dashboardCapacity{configured: true, used: used, available: max(0, current.MaxConcurrentIssues-used), total: current.MaxConcurrentIssues}
	}
	healthy, anomalous := dashboardWorkerHealth(sections[statusActive], sections[statusAttention])
	metadata := dashboardProjectionMetadata{
		repository: valueOr(plainStatusValue(current.Repo), "not initialized"),
		capacity:   capacity, healthy: healthy, anomalous: anomalous,
	}
	header := fmt.Sprintf("Backlog Run Dashboard\nRepository: %s\n%s\nWorker health: %d healthy, %d anomalous",
		metadata.repository, metadata.capacity.full(), metadata.healthy, metadata.anomalous)
	attention := make(map[string]struct{}, len(sections[statusAttention]))
	for _, observed := range sections[statusAttention] {
		attention[observed.run.RunID] = struct{}{}
	}
	return dashboardProjection{
		metadata: metadata, header: header, sections: sections, attention: attention, footer: dashboardFooterParts(stage),
	}
}

func (d *liveDashboard) renderResponsiveParts(now time.Time, options responsiveDashboardOptions) (dashboardProjection, dashboardBodyLayout, dashboardStage) {
	d.mu.Lock()
	current := cloneDashboardState(d.current)
	messages := cloneDashboardMessages(d.messages)
	stage := d.stage
	admission := cloneDashboardAdmission(d.admission)
	diagnosticsOpen := d.diagnosticsOpen
	d.mu.Unlock()

	projection := d.project(current, stage, now)
	body := dashboardBodyBuilder{}
	body.renderResponsiveAdmission(admission, diagnosticsOpen, stage, now, options)
	body.renderResponsiveSection(statusActive, "Active Runs", projection.sections[statusActive], now, options, false)
	body.renderResponsiveSection(statusAttention, "Attention Required", projection.sections[statusAttention], now, options, false)
	body.renderResponsiveSection(statusOutcomes, "Outcomes to Acknowledge", projection.sections[statusOutcomes], now, options, false)
	body.renderResponsiveSection(statusCompletions, "Recent Completions", projection.sections[statusCompletions], now, options, true)
	if len(messages) > 0 {
		body.renderResponsiveMessages(messages, options)
	}
	return projection, dashboardBodyLayout{text: body.body.String(), anchors: body.anchors, attention: projection.attention}, stage
}

func (b *dashboardBodyBuilder) renderResponsiveAdmission(admission dashboardAdmission, diagnosticsOpen bool, stage dashboardStage, now time.Time, options responsiveDashboardOptions) {
	b.separate()
	identity := dashboardSectionAnchor("Admission health")
	b.anchor(identity)
	marker := "  "
	if options.selected == identity {
		marker = "> "
	}
	expanded := options.expanded(identity, true)
	heading := marker + "Admission health"
	if !expanded {
		heading += " [collapsed]"
	}
	semantic := dashboardAdmissionSemantic(admission)
	b.write(options.styler.render(semantic, truncateDashboardContent(heading, options.width)) + "\n")

	var content strings.Builder
	if expanded {
		renderAdmissionDetails(&content, admission, diagnosticsOpen, stage, now, options.styler)
	} else {
		renderCompactAdmissionStatus(&content, admission, stage, now, options)
		renderAdmissionDiagnosticsState(&content, admission.failures, diagnosticsOpen, options.styler, options.width)
	}
	b.write(content.String())
}

func (b *dashboardBodyBuilder) renderResponsiveSection(section statusSection, name string, runs []statusRun, now time.Time, options responsiveDashboardOptions, completions bool) {
	b.separate()
	sectionIdentity := dashboardSectionAnchor(name)
	b.anchor(sectionIdentity)
	marker := "  "
	if options.selected == sectionIdentity {
		marker = "> "
	}
	expanded := options.expanded(sectionIdentity, true)
	heading := fmt.Sprintf("%s%s (%d)", marker, name, len(runs))
	if !expanded {
		heading += " [collapsed]"
	}
	heading = truncateDashboardContent(heading, options.width)
	b.write(options.styler.render(dashboardSectionSemantic(section), heading) + "\n")
	if !expanded {
		return
	}
	if len(runs) == 0 {
		none := truncateDashboardContent("    none", options.width)
		b.write(options.styler.render(dashboardSemanticMetadata, none) + "\n")
		return
	}
	for _, observed := range runs {
		identity := dashboardRunAnchor(observed.run.RunID)
		b.anchor(identity)
		marker := "  "
		if options.selected == identity {
			marker = "> "
		}
		line := truncateDashboardContent(marker+compactDashboardRun(observed, now, completions, options.width-ansi.StringWidth(marker)), options.width)
		b.write(options.styler.render(dashboardRunSemantic(observed, section), line) + "\n")
		if options.expanded(identity, false) {
			details := expandedDashboardRun(observed, now)
			if completions {
				details = expandedDashboardCompletion(observed.run, now)
			}
			b.write(options.styler.render(dashboardSemanticMetadata, details))
		}
	}
}

func (b *dashboardBodyBuilder) renderResponsiveMessages(messages []dashboardMessage, options responsiveDashboardOptions) {
	b.separate()
	identity := dashboardSectionAnchor("Operational messages")
	b.anchor(identity)
	marker := "  "
	if options.selected == identity {
		marker = "> "
	}
	expanded := options.expanded(identity, true)
	heading := fmt.Sprintf("%sOperational messages (%d)", marker, len(messages))
	if !expanded {
		heading += " [collapsed]"
	}
	heading = truncateDashboardContent(heading, options.width)
	b.write(options.styler.render(dashboardSemanticMetadata, heading) + "\n")
	if expanded {
		for _, message := range messages {
			b.write(options.styler.render(message.semantic, "    "+message.text) + "\n")
		}
	}
}

func compactDashboardRun(observed statusRun, now time.Time, completion bool, width int) string {
	run := observed.run
	parts := []string{fmt.Sprintf("#%d", run.Issue)}
	if pullRequest := dashboardPullRequestIdentity(run.PullRequest); pullRequest != "" {
		parts = append(parts, pullRequest)
	}
	progress := summarizeRunProgress(run, observed.observation.metrics, now)
	if !completion {
		parts = append(parts, "State: "+displayedRunState(run, observed.observation.process))
		if progress.elapsed != "n/a" {
			parts = append(parts, "Elapsed: "+progress.elapsed)
		}
	}
	detailReserve := width / 2
	if completion {
		detailReserve = width / 3
	}
	parts[0] = compactDashboardIssueIdentity(run, parts, width, detailReserve)
	if completion {
		if progress.elapsed != "n/a" {
			parts = append(parts, "Elapsed: "+progress.elapsed)
		}
		if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
			parts = append(parts, "Completed: "+displayDuration(now.Sub(*run.CompletedAt))+" ago")
		}
		return strings.Join(parts, " | ")
	}
	parts = append(parts, dashboardLivenessPromotions(observed)...)
	if run.Error != "" {
		parts = append(parts, "Diagnostic: "+plainStatusValue(strings.TrimSpace(run.Error)))
	}
	if progress.deepestOperation != "n/a" {
		parts = append(parts, "Deepest operation: "+plainStatusValue(progress.deepestOperation))
	}
	if progress.activityAge != "n/a" {
		parts = append(parts, "Activity: "+progress.activityAge)
	}
	if progress.workerTurns != "n/a" {
		parts = append(parts, "Turns: "+progress.workerTurns)
	}
	return strings.Join(parts, " | ")
}

func compactDashboardIssueIdentity(run scheduler.Run, mandatory []string, width, detailReserve int) string {
	identity := mandatory[0]
	title := plainStatusValue(run.IssueTitle)
	if title == "" || width <= 0 {
		return identity
	}
	available := width - ansi.StringWidth(strings.Join(mandatory, " | ")) - detailReserve - 2
	if available <= 0 {
		return identity
	}
	return identity + "  " + ansi.Truncate(title, min(30, available), "")
}

func dashboardIssueIdentity(run scheduler.Run) string {
	identity := fmt.Sprintf("#%d", run.Issue)
	if title := plainStatusValue(run.IssueTitle); title != "" {
		identity += "  " + title
	}
	return identity
}

func dashboardPullRequestIdentity(value string) string {
	value = plainStatusValue(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	if index := strings.LastIndex(value, "/pull/"); index >= 0 {
		number := strings.TrimSuffix(value[index+len("/pull/"):], "/")
		if parsed, err := strconv.Atoi(number); err == nil && parsed > 0 {
			return fmt.Sprintf("PR #%d", parsed)
		}
	}
	return "PR: " + value
}

func dashboardRunExpectsWorker(run scheduler.Run) bool {
	return run.Status == scheduler.StatusRunning
}

func dashboardWorkerHealth(groups ...[]statusRun) (healthy, anomalous int) {
	for _, runs := range groups {
		for _, observed := range runs {
			if !dashboardRunExpectsWorker(observed.run) {
				continue
			}
			if len(dashboardLivenessPromotions(observed)) > 0 {
				anomalous++
			} else {
				healthy++
			}
		}
	}
	return healthy, anomalous
}

func dashboardLivenessPromotions(observed statusRun) []string {
	if !dashboardRunExpectsWorker(observed.run) {
		return nil
	}
	var promoted []string
	switch observed.observation.process.workerLivenessState {
	case workerLivenessAbsent:
		promoted = append(promoted, "Liveness: missing")
	case workerLivenessDead:
		promoted = append(promoted, "Liveness: dead")
	case workerLivenessUnknown:
		promoted = append(promoted, "Liveness: uncertain")
	}
	if observed.observation.process.supervision != "SUPERVISED" {
		promoted = append(promoted, "Supervision: unsupervised")
	}
	return promoted
}

func expandedDashboardRun(observed statusRun, now time.Time) string {
	run := observed.run
	progress := summarizeRunProgress(run, observed.observation.metrics, now)
	var details strings.Builder
	fmt.Fprintf(&details, "    Issue: %s\n", dashboardIssueIdentity(run))
	if issueURL := plainStatusValue(run.IssueURL); issueURL != "" {
		fmt.Fprintf(&details, "    Issue URL: %s\n", issueURL)
	}
	if pullRequest := dashboardPullRequestIdentity(run.PullRequest); pullRequest != "" {
		fmt.Fprintf(&details, "    Pull request: %s\n", pullRequest)
	}
	fmt.Fprintf(&details, "    Run: %s\n", plainStatusValue(run.RunID))
	fmt.Fprintf(&details, "    State: %s\n", displayedRunState(run, observed.observation.process))
	fmt.Fprintf(&details, "    Runner supervision: %s\n", plainStatusValue(observed.observation.process.supervision))
	fmt.Fprintf(&details, "    Worker liveness: %s\n", plainStatusValue(observed.observation.process.workerLiveness))
	fmt.Fprintf(&details, "    Elapsed: %s\n", progress.elapsed)
	fmt.Fprintf(&details, "    Activity age: %s\n", progress.activityAge)
	fmt.Fprintf(&details, "    Current Worker operation: %s\n", plainStatusValue(progress.workerOperation))
	fmt.Fprintf(&details, "    Completed Worker turns: %s\n", progress.workerTurns)
	fmt.Fprintf(&details, "    Completed Worker tokens: %s\n", progress.workerTokens)
	fmt.Fprintf(&details, "    Subagents: %d (%d active)\n", len(observed.observation.metrics.subagents), progress.activeSubagents)
	fmt.Fprintf(&details, "    Deepest current operation: %s\n", plainStatusValue(progress.deepestOperation))
	fmt.Fprintf(&details, "    Approximate Subagent turns: %s\n", progress.subagentTurns)
	fmt.Fprintf(&details, "    Approximate Subagent tool uses: %s\n", progress.subagentToolUses)
	fmt.Fprintf(&details, "    Approximate Subagent tokens: %s\n", progress.subagentTokens)
	fmt.Fprintf(&details, "    Observed tokens: %s\n", progress.observedTokens)
	if run.Error != "" {
		fmt.Fprintf(&details, "    Diagnostic: %s\n", plainStatusValue(strings.TrimSpace(run.Error)))
	}
	return details.String()
}

func expandedDashboardCompletion(run scheduler.Run, now time.Time) string {
	progress := summarizeRunProgress(run, followMetrics{}, now)
	completed := "n/a"
	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		completed = displayDuration(now.Sub(*run.CompletedAt)) + " ago"
	}
	var details strings.Builder
	fmt.Fprintf(&details, "    Issue: %s\n", dashboardIssueIdentity(run))
	if pullRequest := dashboardPullRequestIdentity(run.PullRequest); pullRequest != "" {
		fmt.Fprintf(&details, "    Pull request: %s\n", pullRequest)
	}
	fmt.Fprintf(&details, "    Elapsed: %s\n", progress.elapsed)
	fmt.Fprintf(&details, "    Completed: %s\n", completed)
	return details.String()
}

func truncateDashboardContent(content string, width int) string {
	if width <= 0 {
		return ""
	}
	return ansi.Truncate(content, width, "")
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

func (d *liveDashboard) renderParts(now time.Time) (string, string, string) {
	header, layout, footer, _ := d.renderPartsWithLayout(now, dashboardStyler{})
	return header, layout.text, footer
}

func (d *liveDashboard) renderPartsFor(current state.State, messages []dashboardMessage, stage dashboardStage, now time.Time, styler dashboardStyler) (string, string, string) {
	header, layout, footer := d.renderPartsForWithLayout(current, messages, stage, now, styler)
	return header, layout.text, footer
}

func (d *liveDashboard) renderPartsForWithLayout(current state.State, messages []dashboardMessage, stage dashboardStage, now time.Time, styler dashboardStyler) (string, dashboardBodyLayout, string) {
	projection := d.project(current, stage, now)
	body := dashboardBodyBuilder{}
	d.mu.Lock()
	admission := cloneDashboardAdmission(d.admission)
	diagnosticsOpen := d.diagnosticsOpen
	d.mu.Unlock()
	body.renderAdmission(admission, diagnosticsOpen, stage, now, styler)
	body.renderSection(statusActive, "Active Runs", projection.sections[statusActive], now, styler)
	body.renderSection(statusAttention, "Attention Required", projection.sections[statusAttention], now, styler)
	body.renderSection(statusOutcomes, "Outcomes to Acknowledge", projection.sections[statusOutcomes], now, styler)
	body.renderCompletions(projection.sections[statusCompletions], now, styler)
	if len(messages) > 0 {
		body.separate()
		body.anchor(dashboardSectionAnchor("Operational messages"))
		body.write(styler.render(dashboardSemanticMetadata, "Operational messages") + "\n")
		for _, message := range messages {
			body.write(styler.render(message.semantic, "  "+message.text) + "\n")
		}
	}
	return projection.header, dashboardBodyLayout{text: body.body.String(), anchors: body.anchors, attention: projection.attention}, projection.footer
}

func (b *dashboardBodyBuilder) renderAdmission(admission dashboardAdmission, diagnosticsOpen bool, stage dashboardStage, now time.Time, styler dashboardStyler) {
	b.separate()
	b.anchor(dashboardSectionAnchor("Admission health"))
	var admissionBody strings.Builder
	renderAdmissionHealth(&admissionBody, admission, diagnosticsOpen, stage, now, styler)
	b.write(strings.TrimPrefix(admissionBody.String(), "\n"))
}

func cloneDashboardAdmission(admission dashboardAdmission) dashboardAdmission {
	admission.issue = cloneIssue(admission.issue)
	admission.failures = append([]runner.CandidateDiscoveryFailed(nil), admission.failures...)
	admission.equivalentOrder = append([]string(nil), admission.equivalentOrder...)
	if admission.equivalentFailures != nil {
		groups := make(map[string]int, len(admission.equivalentFailures))
		for key, count := range admission.equivalentFailures {
			groups[key] = count
		}
		admission.equivalentFailures = groups
	}
	return admission
}

func renderAdmissionHealth(output *strings.Builder, admission dashboardAdmission, diagnosticsOpen bool, stage dashboardStage, now time.Time, styler dashboardStyler) {
	output.WriteByte('\n')
	writeDashboardStyledLine(output, styler, dashboardAdmissionSemantic(admission), "Admission health")
	renderAdmissionDetails(output, admission, diagnosticsOpen, stage, now, styler)
}

func dashboardAdmissionSemantic(admission dashboardAdmission) dashboardSemantic {
	if admission.degraded {
		return dashboardSemanticWarning
	}
	if admission.snapshotComplete {
		return dashboardSemanticActive
	}
	return dashboardSemanticMetadata
}

func renderAdmissionDetails(output *strings.Builder, admission dashboardAdmission, diagnosticsOpen bool, stage dashboardStage, now time.Time, styler dashboardStyler) {
	if admission.degraded {
		noun := "failures"
		if admission.consecutiveFailures == 1 {
			noun = "failure"
		}
		writeDashboardStyledLine(output, styler, dashboardSemanticWarning, fmt.Sprintf("  Admission: DEGRADED | %d consecutive %s", admission.consecutiveFailures, noun))
		failureTimes := fmt.Sprintf("    First failure: %s | Latest failure: %s", formatAdmissionTime(admission.firstFailure), formatAdmissionTime(admission.latestFailure))
		if stage == dashboardRunning {
			failureTimes += " | Next retry: " + admissionRetryCountdown(admission.retryAt, now)
		} else {
			failureTimes += " | Retry: stopped"
		}
		writeDashboardStyledLine(output, styler, dashboardSemanticMetadata, failureTimes)
		operation := "    Operation: " + plainStatusValue(string(admission.operation))
		if admission.issue != nil {
			operation += fmt.Sprintf(" | Issue: #%d", *admission.issue)
		}
		writeDashboardStyledLine(output, styler, dashboardSemanticMetadata, operation)
		cause := "    Cause: " + plainStatusValue(admission.cause)
		key := string(admission.operation) + "\x00" + admission.cause
		if equivalent := admission.equivalentFailures[key]; equivalent > 1 {
			cause += fmt.Sprintf(" | Equivalent failures: %d", equivalent)
		}
		writeDashboardStyledLine(output, styler, dashboardSemanticWarning, cause)
	} else if !admission.snapshotComplete {
		writeDashboardStyledLine(output, styler, dashboardSemanticMetadata, incompleteAdmissionStatus(stage))
	} else {
		health := "  Admission: healthy" + admissionRecoverySummary(admission, now)
		writeDashboardStyledLine(output, styler, dashboardSemanticActive, health)
	}
	renderAdmissionDiagnosticsState(output, admission.failures, diagnosticsOpen, styler, 0)
}

func renderCompactAdmissionStatus(output *strings.Builder, admission dashboardAdmission, stage dashboardStage, now time.Time, options responsiveDashboardOptions) {
	semantic := dashboardAdmissionSemantic(admission)
	line := incompleteAdmissionStatus(stage)
	if admission.degraded {
		noun := "failures"
		if admission.consecutiveFailures == 1 {
			noun = "failure"
		}
		line = fmt.Sprintf("  Admission: DEGRADED | %d consecutive %s", admission.consecutiveFailures, noun)
		if operation := plainStatusValue(string(admission.operation)); operation != "" {
			line += " | " + operation
			if admission.issue != nil {
				line += fmt.Sprintf(" #%d", *admission.issue)
			}
		}
		if cause := plainStatusValue(admission.cause); cause != "" {
			line += " | Cause: " + cause
		}
		if stage == dashboardRunning {
			line += " | Next retry: " + admissionRetryCountdown(admission.retryAt, now)
		} else {
			line += " | Retry: stopped"
		}
	} else if admission.snapshotComplete {
		line = "  Admission: healthy" + admissionRecoverySummary(admission, now)
	}
	writeDashboardStyledLine(output, options.styler, semantic, truncateDashboardContent(line, options.width))
}

func incompleteAdmissionStatus(stage dashboardStage) string {
	if stage == dashboardRunning {
		return "  Admission: checking | Candidate snapshot not yet complete"
	}
	return "  Admission: stopped | Candidate snapshot not completed"
}

func admissionRecoverySummary(admission dashboardAdmission, now time.Time) string {
	if admission.recoveredAt.IsZero() {
		return ""
	}
	age := now.Sub(admission.recoveredAt)
	if age < 0 {
		age = 0
	}
	if age >= admissionRecoveryNotice {
		return ""
	}
	noun := "failures"
	if admission.recoveredFailures == 1 {
		noun = "failure"
	}
	return fmt.Sprintf(" | Recovered %s ago after %d %s", displayDuration(age), admission.recoveredFailures, noun)
}

func renderAdmissionDiagnosticsState(output *strings.Builder, failures []runner.CandidateDiscoveryFailed, diagnosticsOpen bool, styler dashboardStyler, width int) {
	if diagnosticsOpen {
		renderAdmissionDiagnosticsStyled(output, failures, styler)
		return
	}
	line := fmt.Sprintf("  Diagnostics: closed (d to open; %d recent)", len(failures))
	if width > 0 {
		line = truncateDashboardContent(line, width)
	}
	writeDashboardStyledLine(output, styler, dashboardSemanticMetadata, line)
}

func writeDashboardStyledLine(output *strings.Builder, styler dashboardStyler, semantic dashboardSemantic, line string) {
	output.WriteString(styler.render(semantic, line))
	output.WriteByte('\n')
}

func renderAdmissionDiagnostics(output *strings.Builder, failures []runner.CandidateDiscoveryFailed) {
	renderAdmissionDiagnosticsStyled(output, failures, dashboardStyler{})
}

func renderAdmissionDiagnosticsStyled(output *strings.Builder, failures []runner.CandidateDiscoveryFailed, styler dashboardStyler) {
	noun := "records"
	if len(failures) == 1 {
		noun = "record"
	}
	output.WriteByte('\n')
	writeDashboardStyledLine(output, styler, dashboardSemanticMetadata, fmt.Sprintf("Diagnostics (%d recent Candidate discovery failure %s; d to close)", len(failures), noun))
	if len(failures) == 0 {
		writeDashboardStyledLine(output, styler, dashboardSemanticMetadata, "  none")
		return
	}
	for _, failure := range failures {
		diagnostic := normalizedDashboardDiagnostic(runner.FormatOperationalEvent(failure))
		label := "Full error/command"
		if errors.Is(failure.Err, runner.ErrCandidateDiscoveryDiagnosticExpired) {
			label = "Diagnostic unavailable"
			diagnostic = runner.ErrCandidateDiscoveryDiagnosticExpired.Error()
		}
		line := fmt.Sprintf("  [%s] %s: %s", formatAdmissionTime(failure.OccurredAt), label, diagnostic)
		writeDashboardStyledLine(output, styler, dashboardSemanticMetadata, line)
	}
}

func formatAdmissionTime(value time.Time) string {
	if value.IsZero() {
		return "n/a"
	}
	return value.UTC().Format(time.RFC3339)
}

func admissionRetryCountdown(retryAt, now time.Time) string {
	if retryAt.IsZero() {
		return "n/a"
	}
	remaining := retryAt.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	return displayDuration(remaining)
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
