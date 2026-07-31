package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

type statusSection int

const (
	statusActive statusSection = iota
	statusAttention
	statusOutcomes
	statusCompletions
	statusHistory
)

const recentCompletionLimit = 10

type statusRun struct {
	run         scheduler.Run
	observation runObservation
}

// statusSectionFor partitions every persisted Run exactly once. A Lease is
// the source of current ownership; leased lifecycle states that Backlog can
// advance or reconcile remain Active, while every other retained Lease needs
// intervention. Runs without a Lease are History regardless of their outcome.
func statusSectionFor(run scheduler.Run, leased bool) statusSection {
	if !leased {
		return statusHistory
	}
	if scheduler.IsActive(run.Status) {
		return statusActive
	}
	return statusAttention
}

func observeStatusSections(current state.State, source followStateSource, now time.Time) map[statusSection][]statusRun {
	leasedRuns := make(map[string]struct{}, len(current.Leases))
	for _, lease := range current.Leases {
		leasedRuns[lease.RunID] = struct{}{}
	}
	sections := newStatusSections()
	for _, run := range current.Runs {
		_, leased := leasedRuns[run.RunID]
		observation := runObservation{run: run, process: observeFollowRun(source, run), observed: now}
		if run.Status == scheduler.StatusRunning {
			observation, _ = observeRunOnce(source, run, io.Discard, func() time.Time { return now })
		}
		section := statusSectionFor(run, leased)
		sections[section] = append(sections[section], statusRun{run: run, observation: observation})
	}
	return sections
}

// classifyPersistedStatusSections projects only durable Run and Lease state.
// Final reports do not need process liveness or Activity telemetry.
func classifyPersistedStatusSections(current state.State) map[statusSection][]statusRun {
	leasedRuns := make(map[string]struct{}, len(current.Leases))
	for _, lease := range current.Leases {
		leasedRuns[lease.RunID] = struct{}{}
	}
	sections := newStatusSections()
	for _, run := range current.Runs {
		_, leased := leasedRuns[run.RunID]
		section := statusSectionFor(run, leased)
		sections[section] = append(sections[section], statusRun{run: run})
	}
	return sections
}

func newStatusSections() map[statusSection][]statusRun {
	return map[statusSection][]statusRun{
		statusActive: {}, statusAttention: {}, statusOutcomes: {}, statusCompletions: {}, statusHistory: {},
	}
}

func printPlainStatus(output io.Writer, current state.State, source followStateSource, now time.Time) error {
	return printPlainStatusProjection(output, current, source, now, false)
}

type statusProjection struct {
	sections           map[statusSection][]statusRun
	displayed          int
	acknowledgedHidden int
}

func projectStatus(current state.State, source followStateSource, now time.Time, showAll bool) statusProjection {
	fullSections := observeStatusSections(current, source, now)
	sections := map[statusSection][]statusRun{
		statusActive: fullSections[statusActive], statusAttention: fullSections[statusAttention],
		statusOutcomes: {}, statusCompletions: {}, statusHistory: {},
	}
	recentCompletions := selectRecentCompletions(current.Runs)
	acknowledgedHidden := 0
	for _, observed := range fullSections[statusHistory] {
		run := observed.run
		if run.AcknowledgedAt != nil {
			acknowledgedHidden++
		}
		if showAll {
			sections[statusHistory] = append(sections[statusHistory], observed)
			continue
		}
		switch {
		case (run.Status == scheduler.StatusFailed || run.Status == scheduler.StatusNeedsHuman) && run.AcknowledgedAt == nil:
			sections[statusOutcomes] = append(sections[statusOutcomes], observed)
		case run.Status == scheduler.StatusMerged && (run.CleanupPending || recentCompletions[run.RunID]):
			sections[statusCompletions] = append(sections[statusCompletions], observed)
		}
	}
	displayed := 0
	for section := range sections {
		sortStatusRuns(sections[section])
		displayed += len(sections[section])
	}
	return statusProjection{sections: sections, displayed: displayed, acknowledgedHidden: acknowledgedHidden}
}

func printPlainStatusProjection(output io.Writer, current state.State, source followStateSource, now time.Time, showAll bool) error {
	projection := projectStatus(current, source, now, showAll)
	printer := statusPrinter{output: output}
	printer.printf("Repository: %s\n", valueOr(plainStatusValue(current.Repo), "not initialized"))
	printer.printf("Runs: %d total | %d displayed\n", len(current.Runs), projection.displayed)
	printer.printf("Acknowledged outcomes hidden by default: %d\n", projection.acknowledgedHidden)
	printer.printf("Active Leases: %d\n", len(current.Leases))
	printer.section("Active", projection.sections[statusActive])
	printer.section("Attention Required", projection.sections[statusAttention])
	if showAll {
		printer.section("History", projection.sections[statusHistory])
	} else {
		printer.section("Outcomes to Acknowledge", projection.sections[statusOutcomes])
		printer.section("Recent Completions", projection.sections[statusCompletions])
	}
	return printer.err
}

func printCompactStatusProjection(output io.Writer, current state.State, source followStateSource, now time.Time, showAll bool, presentation compactPresentation) error {
	projection := projectStatus(current, source, now, showAll)
	printer := compactStatusPrinter{output: output, presentation: presentation, now: now}
	printer.line(dashboardSemanticNone, "Backlog Status")
	summary := fmt.Sprintf("%s | %d runs, %d shown | %d leases",
		valueOr(plainStatusValue(current.Repo), "not initialized"), len(current.Runs), projection.displayed, len(current.Leases))
	if projection.acknowledgedHidden > 0 && !showAll {
		summary += fmt.Sprintf(" | %d acknowledged hidden", projection.acknowledgedHidden)
	}
	printer.line(dashboardSemanticMetadata, summary)
	printer.line(dashboardSemanticNone, "")
	printer.section(statusActive, "Active", projection.sections[statusActive])
	printer.section(statusAttention, "Attention Required", projection.sections[statusAttention])
	if showAll {
		printer.section(statusHistory, "History", projection.sections[statusHistory])
	} else {
		printer.section(statusOutcomes, "Outcomes to Acknowledge", projection.sections[statusOutcomes])
		printer.section(statusCompletions, "Recent Completions", projection.sections[statusCompletions])
	}
	return printer.err
}

func printRunFinalSummary(output io.Writer, current state.State, source followStateSource, now time.Time) error {
	sections := observeStatusSections(current, source, now)
	printer := statusPrinter{output: output}
	printer.printf("\nFinal aggregate summary\n")
	printer.header(current)
	printer.section("Active", sections[statusActive])
	printer.section("Attention Required", sections[statusAttention])
	return printer.err
}

// printRunFinalReport leaves a concise invocation result on the normal screen.
// Completions are limited to Runs that became merged during this invocation;
// transient Admission and operational-message history belongs only to the live
// dashboard and is intentionally omitted.
func printRunFinalReport(output io.Writer, current state.State, _ followStateSource, _ time.Time, initialCompletions map[string]struct{}, outcome string) error {
	sections := classifyPersistedStatusSections(current)
	completions := make([]statusRun, 0)
	for _, observed := range sections[statusHistory] {
		if observed.run.Status != scheduler.StatusMerged {
			continue
		}
		if _, existed := initialCompletions[observed.run.RunID]; existed {
			continue
		}
		completions = append(completions, observed)
	}
	sortStatusRuns(completions)

	printer := statusPrinter{output: output}
	printer.printf("\nFinal aggregate summary\n")
	printer.printf("Final outcome: %s\n", plainStatusValue(outcome))
	printer.header(current)
	printer.finalReportSection("Completions produced", completions)
	printer.finalReportSection("Active", sections[statusActive])
	printer.finalReportSection("Attention Required", sections[statusAttention])
	return printer.err
}

func selectRecentCompletions(runs []scheduler.Run) map[string]bool {
	completions := make([]scheduler.Run, 0)
	for _, run := range runs {
		if run.Status == scheduler.StatusMerged {
			completions = append(completions, run)
		}
	}
	sort.SliceStable(completions, func(i, j int) bool {
		left, right := completionTime(completions[i]), completionTime(completions[j])
		if left.Equal(right) {
			return completions[i].RunID > completions[j].RunID
		}
		return left.After(right)
	})
	selected := make(map[string]bool, min(recentCompletionLimit, len(completions)))
	for _, run := range completions[:min(recentCompletionLimit, len(completions))] {
		selected[run.RunID] = true
	}
	return selected
}

func completionTime(run scheduler.Run) time.Time {
	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		return *run.CompletedAt
	}
	if !run.UpdatedAt.IsZero() {
		return run.UpdatedAt
	}
	return run.StartedAt
}

func sortStatusRuns(runs []statusRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		left, right := lifecycleTime(runs[i].run), lifecycleTime(runs[j].run)
		if left.Equal(right) {
			return runs[i].run.RunID > runs[j].run.RunID
		}
		return left.After(right)
	})
}

func lifecycleTime(run scheduler.Run) time.Time {
	latest := run.StartedAt
	for _, candidate := range []time.Time{run.WorkerStartedAt, run.UpdatedAt} {
		if candidate.After(latest) {
			latest = candidate
		}
	}
	for _, candidate := range []*time.Time{run.SuspendingAt, run.SuspendedAt, run.CompletedAt, run.AcknowledgedAt, run.ResolvedExternallyAt} {
		if candidate != nil && candidate.After(latest) {
			latest = *candidate
		}
	}
	return latest
}

type compactStatusPrinter struct {
	output       io.Writer
	presentation compactPresentation
	now          time.Time
	err          error
}

func (p *compactStatusPrinter) line(semantic dashboardSemantic, text string) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintln(p.output, p.presentation.render(semantic, text))
}

func (p *compactStatusPrinter) section(section statusSection, name string, runs []statusRun) {
	p.line(dashboardSectionSemantic(section), fmt.Sprintf("%s (%d)", name, len(runs)))
	if len(runs) == 0 {
		return
	}
	for _, observed := range runs {
		completion := section == statusCompletions
		row := "  " + compactRunSummary(observed, p.now, completion, p.presentation.width-2)
		p.line(compactStatusRunSemantic(observed, section), row)
		p.details(observed)
	}
}

func (p *compactStatusPrinter) details(observed statusRun) {
	run := observed.run
	progress := summarizeRunProgress(run, observed.observation.metrics, p.now)
	fields := []string{"Run: " + plainStatusValue(run.RunID)}
	if run.Status == scheduler.StatusRunning {
		fields = append(fields,
			"Runner: "+plainStatusValue(observed.observation.process.supervision),
			"Worker: "+plainStatusValue(observed.observation.process.workerLiveness),
			"Activity: "+progress.activityAge,
			"Worker operation: "+plainStatusValue(progress.workerOperation),
			"Deepest: "+plainStatusValue(progress.deepestOperation),
			fmt.Sprintf("Turns: Worker %s, Subagent %s", progress.workerTurns, progress.subagentTurns),
			"Tokens: "+progress.observedTokens,
		)
	}
	if run.Error != "" {
		fields = append(fields, "Diagnostic: "+plainStatusValue(strings.TrimSpace(run.Error)))
	}
	fields = append(fields, dashboardLifecycleDiagnosticParts(run)...)
	if run.CleanupPending {
		fields = append(fields, "Completion cleanup: pending")
	}
	if run.AcknowledgedAt != nil && !run.AcknowledgedAt.IsZero() {
		fields = append(fields, "Acknowledged: "+run.AcknowledgedAt.UTC().Format(time.RFC3339))
	}
	if run.Status == scheduler.StatusResolvedExternally {
		resolvedAt := "n/a"
		if run.ResolvedExternallyAt != nil && !run.ResolvedExternallyAt.IsZero() {
			resolvedAt = run.ResolvedExternallyAt.UTC().Format(time.RFC3339)
		}
		fields = append(fields, "Resolved externally: "+resolvedAt, "GitHub closure reason: "+valueOr(plainStatusValue(run.ClosureReason), "n/a"))
	}
	if run.DiagnosticWarning != "" {
		fields = append(fields, "Diagnostic warning: "+plainStatusValue(run.DiagnosticWarning))
	}
	for _, line := range p.presentation.fieldLines("    ", fields...) {
		p.line(dashboardSemanticMetadata, line)
	}
}

func compactStatusRunSemantic(observed statusRun, section statusSection) dashboardSemantic {
	if section != statusHistory {
		return dashboardRunSemantic(observed, section)
	}
	semantic := dashboardRunLifecycleSemantic(observed)
	if semantic == dashboardSemanticActive || semantic == dashboardSemanticWarning {
		return dashboardSemanticMetadata
	}
	return semantic
}

type statusPrinter struct {
	output io.Writer
	err    error
}

func (p *statusPrinter) printf(format string, values ...any) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintf(p.output, format, values...)
}

func (p *statusPrinter) header(current state.State) {
	p.printf("Repository: %s\n", valueOr(plainStatusValue(current.Repo), "not initialized"))
	p.printf("Runs: %d\n", len(current.Runs))
	p.printf("Active Leases: %d\n", len(current.Leases))
}

func (p *statusPrinter) section(name string, runs []statusRun) {
	p.printf("\n%s (%d)\n", name, len(runs))
	if len(runs) == 0 {
		p.printf("  none\n")
		return
	}
	for _, observed := range runs {
		p.run(observed)
	}
}

// finalReportSection preserves the Run and issue identities operators need
// after the TUI exits without copying live observation telemetry to the normal
// screen.
func (p *statusPrinter) finalReportSection(name string, runs []statusRun) {
	p.printf("\n%s (%d)\n", name, len(runs))
	if len(runs) == 0 {
		p.printf("  none\n")
		return
	}
	for _, observed := range runs {
		run := observed.run
		identity := fmt.Sprintf("#%d", run.Issue)
		if title := plainStatusValue(run.IssueTitle); title != "" {
			identity += "  " + title
		}
		p.printf("  %s  %s\n", identity, run.Status)
		if issueURL := plainStatusValue(run.IssueURL); issueURL != "" {
			p.printf("    Issue: %s\n", issueURL)
		}
		p.printf("    Run: %s | State: %s\n", plainStatusValue(run.RunID), run.Status)
		if run.PullRequest != "" && (run.Status == scheduler.StatusWaitingForMerge || run.Status == scheduler.StatusMerged) {
			p.printf("    Pull request: %s\n", plainStatusValue(run.PullRequest))
		}
		if run.Error != "" && statusSectionFor(run, true) == statusAttention {
			p.printReason(run)
		}
	}
}

func (p *statusPrinter) run(observed statusRun) {
	run := observed.run
	identity := fmt.Sprintf("#%d", run.Issue)
	if title := plainStatusValue(run.IssueTitle); title != "" {
		identity += "  " + title
	}
	p.printf("  %s  %s\n", identity, displayedRunState(run, observed.observation.process))
	if issueURL := plainStatusValue(run.IssueURL); issueURL != "" {
		p.printf("    Issue: %s\n", issueURL)
	}
	p.printf("    Run: %s | State: %s\n", plainStatusValue(run.RunID), run.Status)
	if run.AcknowledgedAt != nil {
		p.printTime("Acknowledged", run.AcknowledgedAt)
	}
	p.printf("    Elapsed: %s\n", summarizeRunProgress(run, observed.observation.metrics, observed.observation.observed).elapsed)

	switch run.Status {
	case scheduler.StatusClaimed:
		p.printf("    Progress: Lease retained; preparing the Run; Worker not started\n")
		p.printBranch(run)
	case scheduler.StatusWorktreeReady:
		p.printf("    Progress: worktree ready; Worker has not been released to begin AFK work\n")
		p.printBranch(run)
	case scheduler.StatusRunning:
		p.printRunning(observed.observation)
		p.printBranch(run)
	case scheduler.StatusWaitingForMerge:
		p.printf("    Progress: waiting for merge reconciliation; Worker not active\n")
		p.printf("    Pull request: %s\n", valueOr(plainStatusValue(run.PullRequest), "n/a"))
	case scheduler.StatusSuspended:
		resume := "continuation telemetry unavailable; Runner will recheck Resume eligibility"
		if run.Continuation != nil && !run.Continuation.VerifiedAt.IsZero() {
			resume = "continuation recorded; Runner will recheck Resume eligibility"
		}
		if run.ResumePending {
			resume = "replacement Worker launch pending; Worker liveness uncertain"
		}
		p.printf("    Progress: suspended; %s\n", resume)
		p.printTime("Suspended", run.SuspendedAt)
		if run.Error != "" {
			p.printReason(run)
		}
	case scheduler.StatusResetting:
		p.printf("    Intervention: Reset is incomplete; rerun backlog reset; Worker not active\n")
		p.printReason(run)
	case scheduler.StatusResolvingExternally:
		p.printf("    Intervention: External Resolution is incomplete; close the issue if needed and rerun backlog resolve, or Reset the Run with backlog reset; a supervising Runner will retry at startup or during watch polling while no Owned Worker is present; Worker not active\n")
		p.printReason(run)
	case scheduler.StatusMerged:
		p.printf("    Completion: verified merged; Worker not active\n")
		p.printf("    Pull request: %s\n", valueOr(plainStatusValue(run.PullRequest), "n/a"))
		p.printTime("Completed", run.CompletedAt)
		if run.CleanupPending {
			p.printf("    Completion cleanup: pending; retry with backlog resolve or the next runner startup\n")
		}
		if run.Error != "" {
			p.printReason(run)
		}
	case scheduler.StatusFailed:
		p.printf("    Outcome: failed; Worker not active\n")
		p.printReason(run)
	case scheduler.StatusNeedsHuman:
		p.printNeedsHuman(observed.observation)
		p.printReason(run)
	case scheduler.StatusReset:
		p.printf("    Outcome: Reset completed; Lease released; Worker not active\n")
		p.printReason(run)
	case scheduler.StatusResolvedExternally:
		p.printf("    Outcome: External Resolution; Lease released; Worker not active\n")
		p.printTime("Resolved externally", run.ResolvedExternallyAt)
		p.printf("    GitHub closure reason: %s\n", valueOr(plainStatusValue(run.ClosureReason), "n/a"))
		p.printReason(run)
		if run.DiagnosticWarning != "" {
			p.printf("    Diagnostic warning: %s\n", plainStatusValue(run.DiagnosticWarning))
		}
	}
}

func (p *statusPrinter) printRunning(observed runObservation) {
	progress := summarizeRunProgress(observed.run, observed.metrics, observed.observed)
	p.printf("    Worker liveness: %s\n", plainStatusValue(observed.process.workerLiveness))
	p.printf("    Activity age: %s\n", progress.activityAge)
	p.printf("    Current deepest operation: %s\n", plainStatusValue(progress.deepestOperation))
	p.printf("    Turns: Worker %s | Subagent %s\n", progress.workerTurns, progress.subagentTurns)
	p.printf("    Observed tokens: %s\n", progress.observedTokens)
}

func (p *statusPrinter) printNeedsHuman(observed runObservation) {
	if observed.process.workerLivenessState == workerLivenessAbsent {
		p.printf("    Intervention: human judgment required; Worker not active\n")
		return
	}
	p.printf("    Intervention: human judgment required; retained Worker liveness: %s\n", plainStatusValue(observed.process.workerLiveness))
}

func (p *statusPrinter) printBranch(run scheduler.Run) {
	p.printf("    Branch: %s\n", valueOr(plainStatusValue(run.Branch), "n/a"))
}

func (p *statusPrinter) printReason(run scheduler.Run) {
	p.printf("    Diagnostic: %s\n", valueOr(strings.TrimSpace(plainStatusValue(run.Error)), "n/a"))
	for _, diagnostic := range dashboardLifecycleDiagnosticParts(run) {
		p.printf("    %s\n", diagnostic)
	}
}

func displayedRunIsSuspending(run scheduler.Run, process followObservation) bool {
	return run.Status == scheduler.StatusRunning && run.SuspendingAt != nil && process.supervision == "SUPERVISED"
}

func displayedRunState(run scheduler.Run, process followObservation) string {
	if displayedRunIsSuspending(run, process) {
		return "suspending"
	}
	return string(run.Status)
}

type terminalTextState uint8

const (
	terminalText terminalTextState = iota
	terminalEscape
	terminalCSI
	terminalControlString
	terminalControlStringEscape
)

type terminalControlWriter struct {
	output io.Writer

	mu    sync.Mutex
	state terminalTextState
}

func (w *terminalControlWriter) Write(content []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	filtered := filterTerminalControls(content, &w.state, true)
	if len(filtered) == 0 {
		return len(content), nil
	}
	written, err := w.output.Write(filtered)
	if err == nil && written != len(filtered) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	return len(content), nil
}

func plainStatusValue(value string) string {
	state := terminalText
	filtered := filterTerminalControls([]byte(value), &state, false)
	return strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return -1
		}
		return character
	}, string(filtered))
}

func filterTerminalControls(content []byte, state *terminalTextState, preserveWhitespace bool) []byte {
	filtered := make([]byte, 0, len(content))
	for _, character := range content {
		switch *state {
		case terminalText:
			switch {
			case character == 0x1b:
				*state = terminalEscape
			case character == 0x9b:
				*state = terminalCSI
			case character < 0x20 || character == 0x7f:
				if preserveWhitespace && (character == '\n' || character == '\t') {
					filtered = append(filtered, character)
				}
			default:
				filtered = append(filtered, character)
			}
		case terminalEscape:
			switch character {
			case '[':
				*state = terminalCSI
			case ']', 'P', 'X', '^', '_':
				*state = terminalControlString
			case 0x1b:
			default:
				*state = terminalText
				if character >= 0x20 && character != 0x7f {
					filtered = append(filtered, character)
				}
			}
		case terminalCSI:
			if character >= 0x40 && character <= 0x7e {
				*state = terminalText
			}
		case terminalControlString:
			switch character {
			case 0x07:
				*state = terminalText
			case 0x1b:
				*state = terminalControlStringEscape
			}
		case terminalControlStringEscape:
			if character == '\\' {
				*state = terminalText
			} else if character != 0x1b {
				*state = terminalControlString
			}
		}
	}
	return filtered
}

func (p *statusPrinter) printTime(label string, value *time.Time) {
	if value == nil || value.IsZero() {
		p.printf("    %s: n/a\n", label)
		return
	}
	p.printf("    %s: %s\n", label, value.UTC().Format(time.RFC3339))
}
