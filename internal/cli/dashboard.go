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
	dashboardDrainIncomplete
	dashboardSuspensionComplete
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
	text         string
	shutdown     bool
	plainMatched bool
}

type dashboardAdmission struct {
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
	dashboardDiagnosticLimit     = 20
	dashboardAggregationKeyLimit = 20
	admissionRecoveryNotice      = 10 * time.Second
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
		if !retained.shutdown {
			d.messages = append(d.messages[:index], d.messages[index+1:]...)
			return
		}
	}
	d.messages = d.messages[1:]
}

func normalizedDashboardMessage(message string) string {
	return plainStatusValue(strings.TrimSpace(message))
}

func dashboardMessageTexts(messages []dashboardMessage) []string {
	texts := make([]string, len(messages))
	for index, message := range messages {
		texts[index] = message.text
	}
	return texts
}

// operationalEvent receives lifecycle state directly from the Runner. Message
// formatting remains independent, so presentation never infers Admission or
// shutdown state from append-only prose.
func (d *liveDashboard) operationalEvent(event runner.OperationalEvent) {
	switch event := event.(type) {
	case runner.CandidateDiscoveryFailed:
		d.mu.Lock()
		d.recordAdmissionFailureLocked(event)
		d.mu.Unlock()
		d.requestRedraw()
		return
	case runner.CandidateDiscoveryRecovered:
		d.mu.Lock()
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

	shutdown, ok := event.(runner.ShutdownEvent)
	if !ok {
		return
	}
	var next dashboardStage
	switch shutdown.Stage {
	case runner.ShutdownStageDraining:
		next = dashboardDraining
	case runner.ShutdownStageSuspending:
		next = dashboardSuspending
	case runner.ShutdownStageForceStopping:
		next = dashboardForceStopping
	case runner.ShutdownStageDrainComplete:
		next = dashboardDrainComplete
	case runner.ShutdownStageDrainIncomplete:
		next = dashboardDrainIncomplete
	case runner.ShutdownStageSuspensionComplete, runner.ShutdownStageSuspensionIncomplete:
		next = dashboardSuspensionComplete
	default:
		return
	}
	d.mu.Lock()
	if message := normalizedDashboardMessage(shutdown.Message); message != "" {
		d.trackOccurrenceLocked(message, true)
		matched := false
		for index := len(d.messages) - 1; index >= 0; index-- {
			if d.messages[index].text == message && !d.messages[index].shutdown {
				d.messages[index].shutdown = true
				d.messages[index].plainMatched = true
				matched = true
				break
			}
		}
		if !matched {
			// Event delivery may lag far enough behind output for the matching
			// ordinary line to be evicted. Restore it as typed history. This also
			// represents the line when the event arrives before plain output.
			d.appendMessageLocked(dashboardMessage{text: message, shutdown: true})
		}
		d.reconcileOccurrencesLocked(message)
	}
	if next > d.stage {
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
	if !d.admission.degraded {
		d.admission.firstFailure = failure.FirstFailureAt
		if d.admission.firstFailure.IsZero() {
			d.admission.firstFailure = failure.OccurredAt
		}
		d.admission.equivalentFailures = make(map[string]int)
		d.admission.equivalentOrder = nil
	}
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
	d.recordEquivalentFailureLocked(key, presentationFailureOccurrences(failure))
	d.admission.failures = append(d.admission.failures, failure)
	if len(d.admission.failures) > dashboardDiagnosticLimit {
		d.admission.failures = append([]runner.CandidateDiscoveryFailed(nil), d.admission.failures[len(d.admission.failures)-dashboardDiagnosticLimit:]...)
	}
}

func (d *liveDashboard) recordEquivalentFailureLocked(key string, occurrences int) {
	for index, existing := range d.admission.equivalentOrder {
		if existing != key {
			continue
		}
		copy(d.admission.equivalentOrder[index:], d.admission.equivalentOrder[index+1:])
		d.admission.equivalentOrder = d.admission.equivalentOrder[:len(d.admission.equivalentOrder)-1]
		break
	}
	d.admission.equivalentOrder = append(d.admission.equivalentOrder, key)
	d.admission.equivalentFailures[key] += occurrences
	if len(d.admission.equivalentOrder) <= dashboardAggregationKeyLimit {
		return
	}
	oldest := d.admission.equivalentOrder[0]
	delete(d.admission.equivalentFailures, oldest)
	d.admission.equivalentOrder = d.admission.equivalentOrder[1:]
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
	messages := dashboardMessageTexts(d.messages)
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

func (d *liveDashboard) render(current state.State, messages []string, stage dashboardStage, now time.Time) string {
	header, body, _ := d.renderPartsFor(current, messages, stage, now)
	return header + "\n" + body + "\n\n" + dashboardFooter(stage) + "\n"
}

func (d *liveDashboard) renderParts(now time.Time) (string, string, string) {
	d.mu.Lock()
	current := cloneDashboardState(d.current)
	messages := dashboardMessageTexts(d.messages)
	stage := d.stage
	d.mu.Unlock()
	return d.renderPartsFor(current, messages, stage, now)
}

func (d *liveDashboard) renderPartsFor(current state.State, messages []string, stage dashboardStage, now time.Time) (string, string, string) {
	sections := d.observeSections(current, now)
	capacity := "Worker capacity: pending configuration"
	if current.MaxConcurrentIssues > 0 {
		used := dashboardUsedCapacity(current)
		available := max(0, current.MaxConcurrentIssues-used)
		capacity = fmt.Sprintf("Worker capacity: %d used | %d available | %d total", used, available, current.MaxConcurrentIssues)
	}

	header := fmt.Sprintf("Backlog Run Dashboard\nRepository: %s\n%s",
		valueOr(plainStatusValue(current.Repo), "not initialized"), capacity)
	var body strings.Builder
	d.mu.Lock()
	admission := cloneDashboardAdmission(d.admission)
	diagnosticsOpen := d.diagnosticsOpen
	d.mu.Unlock()
	renderAdmissionHealth(&body, admission, diagnosticsOpen, stage, now)
	renderDashboardSection(&body, "Active Runs", sections[statusActive], now)
	renderDashboardSection(&body, "Attention Required", sections[statusAttention], now)
	renderDashboardSection(&body, "Outcomes to Acknowledge", sections[statusOutcomes], now)
	renderDashboardCompletions(&body, sections[statusCompletions], now)
	if len(messages) > 0 {
		body.WriteString("\nOperational messages\n")
		for _, message := range messages {
			fmt.Fprintf(&body, "  %s\n", message)
		}
	}
	return header, strings.TrimPrefix(body.String(), "\n"), dashboardFooterParts(stage)
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

func renderAdmissionHealth(output *strings.Builder, admission dashboardAdmission, diagnosticsOpen bool, stage dashboardStage, now time.Time) {
	output.WriteString("\nAdmission health\n")
	if admission.degraded {
		noun := "failures"
		if admission.consecutiveFailures == 1 {
			noun = "failure"
		}
		fmt.Fprintf(output, "  Admission: DEGRADED | %d consecutive %s\n", admission.consecutiveFailures, noun)
		fmt.Fprintf(output, "    First failure: %s | Latest failure: %s", formatAdmissionTime(admission.firstFailure), formatAdmissionTime(admission.latestFailure))
		if stage == dashboardRunning {
			fmt.Fprintf(output, " | Next retry: %s\n", admissionRetryCountdown(admission.retryAt, now))
		} else {
			output.WriteString(" | Retry: stopped\n")
		}
		fmt.Fprintf(output, "    Operation: %s", plainStatusValue(string(admission.operation)))
		if admission.issue != nil {
			fmt.Fprintf(output, " | Issue: #%d", *admission.issue)
		}
		output.WriteByte('\n')
		fmt.Fprintf(output, "    Cause: %s", plainStatusValue(admission.cause))
		key := string(admission.operation) + "\x00" + admission.cause
		if equivalent := admission.equivalentFailures[key]; equivalent > 1 {
			fmt.Fprintf(output, " | Equivalent failures: %d", equivalent)
		}
		output.WriteByte('\n')
	} else {
		output.WriteString("  Admission: healthy")
		if !admission.recoveredAt.IsZero() {
			age := now.Sub(admission.recoveredAt)
			if age < 0 {
				age = 0
			}
			if age < admissionRecoveryNotice {
				noun := "failures"
				if admission.recoveredFailures == 1 {
					noun = "failure"
				}
				fmt.Fprintf(output, " | Recovered %s ago after %d %s", displayDuration(age), admission.recoveredFailures, noun)
			}
		}
		output.WriteByte('\n')
	}
	if diagnosticsOpen {
		renderAdmissionDiagnostics(output, admission.failures)
	} else {
		fmt.Fprintf(output, "  Diagnostics: closed (d to open; %d recent)\n", len(admission.failures))
	}
}

func renderAdmissionDiagnostics(output *strings.Builder, failures []runner.CandidateDiscoveryFailed) {
	noun := "failures"
	if len(failures) == 1 {
		noun = "failure"
	}
	fmt.Fprintf(output, "\nDiagnostics (%d recent Candidate discovery %s; d to close)\n", len(failures), noun)
	if len(failures) == 0 {
		output.WriteString("  none\n")
		return
	}
	for _, failure := range failures {
		fmt.Fprintf(output, "  [%s] Full error/command: %s\n", formatAdmissionTime(failure.OccurredAt), normalizedDashboardMessage(runner.FormatOperationalEvent(failure)))
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

func renderDashboardCompletions(output *strings.Builder, runs []statusRun, now time.Time) {
	fmt.Fprintf(output, "\nRecent Completions (%d)\n", len(runs))
	if len(runs) == 0 {
		output.WriteString("  none\n")
		return
	}
	visible := min(3, len(runs))
	for _, observed := range runs[:visible] {
		run := observed.run
		identity := fmt.Sprintf("#%d", run.Issue)
		if title := plainStatusValue(run.IssueTitle); title != "" {
			identity += "  " + title
		}
		progress := summarizeRunProgress(run, followMetrics{}, now)
		pullRequest := ""
		if run.PullRequest != "" {
			pullRequest = " | PR: " + plainStatusValue(run.PullRequest)
		}
		completed := "n/a"
		if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
			completed = displayDuration(now.Sub(*run.CompletedAt)) + " ago"
		}
		fmt.Fprintf(output, "  %s%s | Elapsed: %s | Completed: %s\n", identity, pullRequest, progress.elapsed, completed)
	}
	if remainder := len(runs) - visible; remainder > 0 {
		fmt.Fprintf(output, "  %d more completions\n", remainder)
	}
}

func renderDashboardSection(output *strings.Builder, name string, runs []statusRun, now time.Time) {
	fmt.Fprintf(output, "\n%s (%d)\n", name, len(runs))
	if len(runs) == 0 {
		output.WriteString("  none\n")
		return
	}
	for _, observed := range runs {
		run := observed.run
		identity := fmt.Sprintf("#%d", run.Issue)
		if title := plainStatusValue(run.IssueTitle); title != "" {
			identity += "  " + title
		}
		progress := summarizeRunProgress(run, observed.observation.metrics, now)
		fmt.Fprintf(output, "  %s\n", identity)
		if run.IssueURL != "" {
			fmt.Fprintf(output, "    Issue: %s\n", plainStatusValue(run.IssueURL))
		}
		fmt.Fprintf(output, "    State: %s | Elapsed: %s | Worker liveness: %s\n",
			displayedRunState(run, observed.observation.process), progress.elapsed, plainStatusValue(observed.observation.process.workerLiveness))
		fmt.Fprintf(output, "    Activity age: %s | Deepest operation: %s\n",
			progress.activityAge, plainStatusValue(progress.deepestOperation))
		fmt.Fprintf(output, "    Turns: Worker %s | Subagent %s | Observed tokens: %s\n",
			progress.workerTurns, progress.subagentTurns, progress.observedTokens)
		if run.Error != "" {
			fmt.Fprintf(output, "    Diagnostic: %s\n", plainStatusValue(strings.TrimSpace(run.Error)))
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

func dashboardStagePresentationFor(stage dashboardStage) dashboardStagePresentation {
	switch stage {
	case dashboardDraining:
		return dashboardStagePresentation{
			summary:       "Draining: admission is stopped; next Ctrl-C suspends unfinished Runs within the shared deadline.",
			stage:         "Draining",
			nextInterrupt: "suspend unfinished Runs within the shared deadline",
		}
	case dashboardSuspending:
		return dashboardStagePresentation{
			summary:       "Suspending: continuation boundaries are being established; next Ctrl-C force stops remaining verified Worker groups.",
			stage:         "Suspending",
			nextInterrupt: "force stop remaining verified Worker groups",
		}
	case dashboardForceStopping:
		return dashboardStagePresentation{
			summary:       "Force stopping: Worker identities are revalidated before signaling; next Ctrl-C repeats the force-stop request.",
			stage:         "Force stopping",
			nextInterrupt: "repeat the force-stop request after identity checks",
		}
	case dashboardDrainComplete:
		return dashboardStagePresentation{
			summary:       "Drain complete: no Owned Workers remain; no further interrupt is needed.",
			stage:         "Drain complete",
			nextInterrupt: "no effect",
		}
	case dashboardDrainIncomplete:
		return dashboardStagePresentation{
			summary:       "Drain incomplete: Worker liveness remains unverified; no further interrupt has an effect before exit.",
			stage:         "Drain incomplete; Worker liveness is unverified",
			nextInterrupt: "no effect",
		}
	case dashboardSuspensionComplete:
		return dashboardStagePresentation{
			summary:       "Suspension finished: no further interrupt has an effect before exit.",
			stage:         "Suspension finished",
			nextInterrupt: "no effect",
		}
	case dashboardStopped:
		return dashboardStagePresentation{
			summary:       "Stopped: the runner is exiting; interrupts have no further effect.",
			stage:         "Stopped; the Runner is exiting",
			nextInterrupt: "no effect",
		}
	case dashboardFinished:
		return dashboardStagePresentation{
			summary:       "Complete: the runner has exited; interrupts have no further effect.",
			stage:         "Complete; the Runner has exited",
			nextInterrupt: "no effect",
		}
	default:
		return dashboardStagePresentation{
			summary:       "Running: Ctrl-C starts Drain, stopping admission while Owned Workers finish.",
			stage:         "Running",
			nextInterrupt: "start Drain and stop Admission",
		}
	}
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
	messages := dashboardMessageTexts(d.messages)
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
