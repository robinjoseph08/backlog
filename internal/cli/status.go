package cli

import (
	"fmt"
	"io"
	"strings"
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
	switch run.Status {
	case scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge, scheduler.StatusSuspended:
		return statusActive
	default:
		return statusAttention
	}
}

func printPlainStatus(output io.Writer, current state.State, source followStateSource, now time.Time) error {
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
		observation, _ := observeRunOnce(source, run, io.Discard, now)
		section := statusSectionFor(run, leased)
		sections[section] = append(sections[section], statusRun{run: run, observation: observation})
	}

	printer := statusPrinter{output: output}
	printer.printf("Repository: %s\n", valueOr(plainStatusValue(current.Repo), "not initialized"))
	printer.printf("Runs: %d\n", len(current.Runs))
	printer.printf("Active Leases: %d\n", len(current.Leases))
	printer.section("Active", sections[statusActive])
	printer.section("Attention Required", sections[statusAttention])
	printer.section("History", sections[statusHistory])
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
	displayedState := string(run.Status)
	if run.Status == scheduler.StatusRunning && run.SuspendingAt != nil && observed.observation.process.supervision == "SUPERVISED" {
		displayedState = "suspending"
	}
	p.printf("  %s  %s\n", identity, displayedState)
	if issueURL := plainStatusValue(run.IssueURL); issueURL != "" {
		p.printf("    Issue: %s\n", issueURL)
	}
	p.printf("    Run: %s | State: %s\n", plainStatusValue(run.RunID), run.Status)

	switch run.Status {
	case scheduler.StatusClaimed:
		p.printf("    Progress: Lease retained; preparing the Run; Worker not started\n")
		p.printBranch(run)
	case scheduler.StatusWorktreeReady:
		p.printf("    Progress: worktree ready; Worker not started\n")
		p.printBranch(run)
	case scheduler.StatusRunning:
		p.printRunning(observed.observation)
		p.printBranch(run)
	case scheduler.StatusWaitingForMerge:
		p.printf("    Progress: waiting for merge reconciliation; Worker not active\n")
		p.printf("    Pull request: %s\n", valueOr(plainStatusValue(run.PullRequest), "n/a"))
	case scheduler.StatusSuspended:
		resume := "availability n/a (continuation telemetry unavailable)"
		if run.Continuation != nil && !run.Continuation.VerifiedAt.IsZero() {
			resume = "available to a replacement Worker"
		}
		if run.ResumePending {
			resume = "pending"
		}
		p.printf("    Progress: suspended; Worker stopped; Resume %s\n", resume)
		p.printTime("Suspended", run.SuspendedAt)
	case scheduler.StatusResetting:
		p.printf("    Intervention: Reset is incomplete; rerun backlog reset; Worker not active\n")
		p.printReason(run)
	case scheduler.StatusMerged:
		p.printf("    Completion: verified merged; Worker not active\n")
		p.printf("    Pull request: %s\n", valueOr(plainStatusValue(run.PullRequest), "n/a"))
		p.printTime("Completed", run.CompletedAt)
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
	if len(observed.metrics.entries) > 0 {
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

func plainStatusValue(value string) string {
	return strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return -1
		}
		return character
	}, value)
}

func (p *statusPrinter) printTime(label string, value *time.Time) {
	if value == nil || value.IsZero() {
		p.printf("    %s: n/a\n", label)
		return
	}
	p.printf("    %s: %s\n", label, value.UTC().Format(time.RFC3339))
}
