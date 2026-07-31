package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
)

type followRenderer struct {
	output       io.Writer
	presentation compactPresentation
}

func newFollowRenderer(output io.Writer, presentation compactPresentation) followRenderer {
	return followRenderer{output: output, presentation: presentation}
}

func (r followRenderer) summary(run scheduler.Run, metrics followMetrics, observation followObservation, now time.Time) error {
	return r.summaryWithHeading(run, metrics, observation, now, true)
}

func (r followRenderer) finalSummary(run scheduler.Run, metrics followMetrics, observation followObservation, now time.Time) error {
	return r.summaryWithHeading(run, metrics, observation, now, false)
}

func (r followRenderer) summaryWithHeading(run scheduler.Run, metrics followMetrics, observation followObservation, now time.Time, heading bool) error {
	if !r.presentation.enabled {
		return printFollowSummary(r.output, run, metrics, observation, now)
	}
	progress := summarizeRunProgress(run, metrics, now)
	observed := statusRun{run: run, observation: runObservation{
		run: run, metrics: metrics, process: observation, observed: now,
	}}
	if heading {
		if err := r.line(dashboardSemanticNone, "Backlog Follow"); err != nil {
			return err
		}
	}
	row := compactDashboardRun(observed, now, false, r.presentation.width)
	if err := r.line(followRunSemantic(observed), row); err != nil {
		return err
	}
	metadata := fmt.Sprintf("Run: %s | Runner: %s | Worker: %s",
		plainStatusValue(run.RunID), plainStatusValue(observation.supervision), plainStatusValue(observation.workerLiveness))
	if err := r.line(dashboardSemanticMetadata, metadata); err != nil {
		return err
	}
	usage := fmt.Sprintf("Tokens: %s | Subagents: %d (%d active)", progress.observedTokens, len(metrics.subagents), progress.activeSubagents)
	if err := r.line(dashboardSemanticMetadata, usage); err != nil {
		return err
	}
	if run.Status == scheduler.StatusResolvedExternally {
		resolvedAt := "n/a"
		if run.ResolvedExternallyAt != nil && !run.ResolvedExternallyAt.IsZero() {
			resolvedAt = run.ResolvedExternallyAt.UTC().Format(time.RFC3339)
		}
		resolution := fmt.Sprintf("Resolved: %s | GitHub: %s", resolvedAt, valueOr(plainStatusValue(run.ClosureReason), "n/a"))
		if err := r.line(dashboardSemanticCompletion, resolution); err != nil {
			return err
		}
		for _, diagnostic := range []string{run.Error, run.DiagnosticWarning} {
			if diagnostic != "" {
				if err := r.line(dashboardSemanticWarning, "Diagnostic: "+plainStatusValue(diagnostic)); err != nil {
					return err
				}
			}
		}
	}
	for index, id := range metrics.subagentOrder {
		observed := metrics.subagents[id]
		subagent := observed.snapshot
		line := fmt.Sprintf("  Subagent %d %s | %s | %s | turns %s | tools %s | %s | tokens %s",
			index+1, valueOr(plainStatusValue(subagent.Description), "n/a"), valueOr(plainStatusValue(subagent.Status), "n/a"),
			valueOr(plainStatusValue(subagent.Activity), "n/a"), approximateInt(subagent.Turns), approximateInt(subagent.ToolUses),
			displaySubagentDuration(subagent.DurationMillis, subagent.Active, observed.latest, now), approximateInt64(subagent.ApproxTokens))
		if err := r.line(followSubagentSemantic(subagent), line); err != nil {
			return err
		}
	}
	return nil
}

func (r followRenderer) initialActivity(entries []activity.Entry) error {
	if !r.presentation.enabled {
		return printInitialActivity(r.output, entries)
	}
	if err := r.write("\n"); err != nil {
		return err
	}
	if err := r.line(dashboardSemanticActive, "Activity (latest 20)"); err != nil {
		return err
	}
	start := max(0, len(entries)-20)
	if start == len(entries) {
		return r.line(dashboardSemanticMetadata, "  none")
	}
	for _, entry := range entries[start:] {
		if err := r.activityEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (r followRenderer) activityEntry(entry activity.Entry) error {
	if !r.presentation.enabled {
		return printActivityEntry(r.output, entry)
	}
	observed := "--:--:--"
	if !entry.ObservedAt.IsZero() {
		observed = entry.ObservedAt.Local().Format("15:04:05")
	}
	description := plainStatusValue(entry.Description)
	if entry.Subagent != nil {
		description += " | " + displaySubagentDuration(entry.Subagent.DurationMillis, false, time.Time{}, time.Time{})
	}
	line := r.presentation.renderSpans(
		compactSpan{semantic: dashboardSemanticMetadata, text: "  " + observed + "  "},
		compactSpan{semantic: followActivitySemantic(entry), text: description},
	)
	_, err := fmt.Fprintln(r.output, line)
	return err
}

func (r followRenderer) activeSubagentSummary(metrics followMetrics) error {
	active, deepest := metrics.activeSubagentSummary()
	if !r.presentation.enabled {
		_, err := fmt.Fprintf(r.output, "  Subagent summary: %d (%d active) | Deepest current operation: %s\n", len(metrics.subagents), active, deepest)
		return err
	}
	line := fmt.Sprintf("  Subagents: %d (%d active) | Deepest: %s", len(metrics.subagents), active, plainStatusValue(deepest))
	return r.line(dashboardSemanticActive, line)
}

func (r followRenderer) terminalHeading() error {
	if !r.presentation.enabled {
		_, err := fmt.Fprintln(r.output, "\nTerminal Run summary:")
		return err
	}
	if err := r.write("\n"); err != nil {
		return err
	}
	return r.line(dashboardSemanticCompletion, "Final")
}

func (r followRenderer) line(semantic dashboardSemantic, text string) error {
	_, err := fmt.Fprintln(r.output, r.presentation.render(semantic, text))
	return err
}

func (r followRenderer) write(text string) error {
	_, err := io.WriteString(r.output, text)
	return err
}

func followRunSemantic(observed statusRun) dashboardSemantic {
	switch observed.run.Status {
	case scheduler.StatusMerged, scheduler.StatusResolvedExternally, scheduler.StatusReset:
		return dashboardSemanticCompletion
	case scheduler.StatusFailed, scheduler.StatusNeedsHuman, scheduler.StatusResolvingExternally:
		return dashboardSemanticAttention
	case scheduler.StatusWaitingForMerge, scheduler.StatusSuspended, scheduler.StatusResetting:
		return dashboardSemanticWarning
	case scheduler.StatusRunning:
		if observed.observation.process.workerLivenessState != workerLivenessAlive {
			return dashboardSemanticWarning
		}
	}
	return dashboardSemanticActive
}

func followSubagentSemantic(snapshot activity.SubagentSnapshot) dashboardSemantic {
	switch {
	case snapshot.Completed:
		return dashboardSemanticCompletion
	case snapshot.Active:
		return dashboardSemanticActive
	default:
		return dashboardSemanticMetadata
	}
}

func followActivitySemantic(entry activity.Entry) dashboardSemantic {
	switch entry.Kind {
	case "retry", "compaction":
		return dashboardSemanticWarning
	case "subagent":
		if entry.Subagent != nil {
			return followSubagentSemantic(*entry.Subagent)
		}
	}
	return dashboardSemanticActive
}
