package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
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
)

// liveDashboard presents the same aggregate observation model as status while
// keeping all terminal control sequences inside the explicitly selected TTY
// path. It only reads lifecycle state and Activity evidence; redraws never
// write Worker Activity.
type liveDashboard struct {
	output io.Writer
	source followStateSource
	now    func() time.Time

	mu                      sync.Mutex
	current                 state.State
	baselineStatuses        map[string]scheduler.Status
	messages                []dashboardMessage
	pendingShutdownMessages map[string]int
	stage                   dashboardStage
	lastActivity            map[string]fileSignature
	observations            map[string]dashboardActivityObservation
	pendingOutput           bytes.Buffer
	err                     error

	updates chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

type dashboardMessage struct {
	text     string
	shutdown bool
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
	baseline := make(map[string]scheduler.Status, len(initial.Runs))
	for _, run := range initial.Runs {
		baseline[run.RunID] = run.Status
	}
	return &liveDashboard{
		output: output, source: source, now: now, current: initial,
		baselineStatuses: baseline, pendingShutdownMessages: make(map[string]int), stage: dashboardRunning,
		lastActivity: activitySignatures(initial), observations: make(map[string]dashboardActivityObservation), updates: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

func (d *liveDashboard) start() {
	_, _ = io.WriteString(d.output, "\x1b[?25l")
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

func (d *liveDashboard) recordMessageLocked(message string) {
	message = normalizedDashboardMessage(message)
	if message == "" {
		return
	}
	shutdown := d.pendingShutdownMessages[message] > 0
	if shutdown {
		d.pendingShutdownMessages[message]--
		if d.pendingShutdownMessages[message] == 0 {
			delete(d.pendingShutdownMessages, message)
		}
	}
	d.messages = append(d.messages, dashboardMessage{text: message, shutdown: shutdown})
	if len(d.messages) > 12 {
		for index, retained := range d.messages {
			if !retained.shutdown {
				d.messages = append(d.messages[:index], d.messages[index+1:]...)
				break
			}
		}
	}
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
// formatting remains independent, so presentation never infers shutdown stage
// from append-only prose.
func (d *liveDashboard) operationalEvent(event runner.OperationalEvent) {
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
		d.pendingShutdownMessages[message]++
	}
	if next > d.stage {
		d.stage = next
	}
	d.mu.Unlock()
	d.requestRedraw()
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
	_, err := fmt.Fprintf(d.output, "\x1b[H\x1b[2J%s", body)
	if err != nil {
		d.mu.Lock()
		d.err = err
		d.mu.Unlock()
	}
}

func (d *liveDashboard) render(current state.State, messages []string, stage dashboardStage, now time.Time) string {
	sections := d.observeSections(current, now)
	active := sections[statusActive]
	attention := sections[statusAttention]
	recent := d.recentlyFinished(current, sections, now)
	used := dashboardUsedCapacity(current)
	available := max(0, current.MaxConcurrentIssues-used)

	var output strings.Builder
	fmt.Fprintf(&output, "Backlog Run Dashboard\nRepository: %s\nWorker capacity: %d used | %d available | %d total\n",
		valueOr(plainStatusValue(current.Repo), "not initialized"), used, available, current.MaxConcurrentIssues)
	renderDashboardSection(&output, "Active Runs", active, now)
	renderDashboardSection(&output, "Attention Required", attention, now)
	renderDashboardSection(&output, "Recently Finished", recent, now)
	if len(messages) > 0 {
		output.WriteString("\nLifecycle messages\n")
		for _, message := range messages {
			fmt.Fprintf(&output, "  %s\n", message)
		}
	}
	fmt.Fprintf(&output, "\n%s\n", dashboardFooter(stage))
	return output.String()
}

func (d *liveDashboard) observeSections(current state.State, now time.Time) map[statusSection][]statusRun {
	leasedRuns := make(map[string]struct{}, len(current.Leases))
	for _, lease := range current.Leases {
		leasedRuns[lease.RunID] = struct{}{}
	}
	sections := map[statusSection][]statusRun{
		statusActive:    {},
		statusAttention: {},
		statusHistory:   {},
	}
	for _, run := range current.Runs {
		_, leased := leasedRuns[run.RunID]
		section := statusSectionFor(run, leased)
		if section == statusHistory {
			continue
		}
		observation := runObservation{run: run, process: observeFollowRun(d.source, run), observed: now}
		if run.LogPath != "" {
			observation.metrics = d.observeActivity(run)
		}
		sections[section] = append(sections[section], statusRun{run: run, observation: observation})
	}
	return sections
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

func (d *liveDashboard) recentlyFinished(current state.State, sections map[statusSection][]statusRun, now time.Time) []statusRun {
	attention := make(map[string]struct{}, len(sections[statusAttention]))
	for _, run := range sections[statusAttention] {
		attention[run.run.RunID] = struct{}{}
	}
	var recent []statusRun
	for _, run := range current.Runs {
		if !scheduler.IsTerminal(run.Status) {
			continue
		}
		if _, needsAttention := attention[run.RunID]; needsAttention {
			continue
		}
		baseline, existed := d.baselineStatuses[run.RunID]
		if existed && scheduler.IsTerminal(baseline) {
			continue
		}
		observation := runObservation{run: run, process: observeFollowRun(d.source, run), observed: now}
		if run.LogPath != "" {
			observation.metrics = d.observeActivity(run)
		}
		recent = append(recent, statusRun{run: run, observation: observation})
	}
	return recent
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

func dashboardFooter(stage dashboardStage) string {
	switch stage {
	case dashboardDraining:
		return "Draining: admission is stopped; next Ctrl-C suspends unfinished Runs within the shared deadline."
	case dashboardSuspending:
		return "Suspending: continuation boundaries are being established; next Ctrl-C force stops remaining verified Worker groups."
	case dashboardForceStopping:
		return "Force stopping: Worker identities are revalidated before signaling; next Ctrl-C repeats the force-stop request."
	case dashboardDrainComplete:
		return "Drain complete: no Owned Workers remain; no further interrupt is needed."
	case dashboardDrainIncomplete:
		return "Drain incomplete: Worker liveness remains unverified; no further interrupt has an effect before exit."
	case dashboardSuspensionComplete:
		return "Suspension finished: no further interrupt has an effect before exit."
	case dashboardStopped:
		return "Stopped: the runner is exiting; interrupts have no further effect."
	case dashboardFinished:
		return "Complete: the runner has exited; interrupts have no further effect."
	default:
		return "Running: Ctrl-C starts Drain, stopping admission while Owned Workers finish."
	}
}

func (d *liveDashboard) finalSummary(current state.State) error {
	d.update(current)
	d.stopLoop()
	d.mu.Lock()
	d.stage = dashboardFinished
	messages := dashboardMessageTexts(d.messages)
	d.mu.Unlock()
	body := "Final aggregate summary\n" + d.render(current, messages, dashboardFinished, d.now())
	_, err := fmt.Fprintf(d.output, "\x1b[H\x1b[2J%s", body)
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
	_, _ = io.WriteString(d.output, "\x1b[?25h\n")
}

type dashboardStore struct {
	state.FileStore
	dashboard *liveDashboard
}

func (s dashboardStore) Save(current state.State) error {
	if err := s.FileStore.Save(current); err != nil {
		return err
	}
	s.dashboard.update(current)
	return nil
}
