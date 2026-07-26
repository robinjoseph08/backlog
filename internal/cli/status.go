package cli

import (
	"fmt"
	"io"
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
	statusHistory
)

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
	sections := map[statusSection][]statusRun{
		statusActive:    {},
		statusAttention: {},
		statusHistory:   {},
	}
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

func printPlainStatus(output io.Writer, current state.State, source followStateSource, now time.Time) error {
	sections := observeStatusSections(current, source, now)
	printer := statusPrinter{output: output}
	printer.header(current)
	printer.section("Active", sections[statusActive])
	printer.section("Attention Required", sections[statusAttention])
	printer.section("History", sections[statusHistory])
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
	case scheduler.StatusResetting:
		p.printf("    Intervention: Reset is incomplete; rerun backlog reset; Worker not active\n")
		p.printReason(run)
	case scheduler.StatusMerged:
		p.printf("    Completion: verified merged; Worker not active\n")
		p.printf("    Pull request: %s\n", valueOr(plainStatusValue(run.PullRequest), "n/a"))
		p.printTime("Completed", run.CompletedAt)
		if run.CleanupPending {
			p.printf("    Completion cleanup: pending; the next runner startup will retry\n")
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
	}
}

func (p *statusPrinter) printRunning(observed runObservation) {
	progress := summarizeRunProgress(observed.run, observed.metrics, observed.observed)
	workerTurns := "n/a"
	if len(observed.metrics.entries) > 0 && !observed.metrics.turnsUnavailable {
		workerTurns = fmt.Sprintf("%d", observed.metrics.turns)
	}
	p.printf("    Worker liveness: %s\n", plainStatusValue(observed.process.workerLiveness))
	p.printf("    Activity age: %s\n", progress.activityAge)
	p.printf("    Current deepest operation: %s\n", plainStatusValue(progress.deepestOperation))
	p.printf("    Turns: Worker %s | Subagent %s\n", workerTurns, progress.subagentTurns)
	p.printf("    Observed tokens: %s\n", progress.observedTokens)
}

func (p *statusPrinter) printNeedsHuman(observed runObservation) {
	liveness := observed.process.workerLiveness
	if liveness == "absent" {
		p.printf("    Intervention: human judgment required; Worker not active\n")
		return
	}
	p.printf("    Intervention: human judgment required; retained Worker liveness: %s\n", plainStatusValue(liveness))
}

func (p *statusPrinter) printBranch(run scheduler.Run) {
	p.printf("    Branch: %s\n", valueOr(plainStatusValue(run.Branch), "n/a"))
}

func (p *statusPrinter) printReason(run scheduler.Run) {
	p.printf("    Diagnostic: %s\n", valueOr(strings.TrimSpace(plainStatusValue(run.Error)), "n/a"))
}

func displayedRunState(run scheduler.Run, process followObservation) string {
	if run.Status == scheduler.StatusRunning && run.SuspendingAt != nil && process.supervision == "SUPERVISED" {
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
