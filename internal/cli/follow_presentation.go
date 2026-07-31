package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/charmbracelet/x/ansi"
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
	row := compactRunSummary(observed, now, false, r.presentation.width)
	if err := r.line(followRunSemantic(observed), row); err != nil {
		return err
	}
	if err := r.fields(dashboardSemanticMetadata, "",
		"Run: "+plainStatusValue(run.RunID),
		"Runner: "+plainStatusValue(observation.supervision),
		"Worker: "+plainStatusValue(observation.workerLiveness),
	); err != nil {
		return err
	}
	if err := r.fields(dashboardSemanticMetadata, "",
		"Activity: "+progress.activityAge,
		"Worker operation: "+plainStatusValue(progress.workerOperation),
		fmt.Sprintf("Subagents: %d (%d active)", len(metrics.subagents), progress.activeSubagents),
		"Deepest: "+plainStatusValue(progress.deepestOperation),
	); err != nil {
		return err
	}
	if err := r.fields(dashboardSemanticMetadata, "",
		fmt.Sprintf("Turns: Worker %s, Subagent %s", progress.workerTurns, progress.subagentTurns),
		"Subagent tools: "+progress.subagentToolUses,
		fmt.Sprintf("Tokens: Worker %s, Subagent %s, Total %s", progress.workerTokens, progress.subagentTokens, progress.observedTokens),
	); err != nil {
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
		if err := r.fields(followSubagentSemantic(subagent), "  ",
			fmt.Sprintf("Subagent %d: %s", index+1, valueOr(plainStatusValue(subagent.Description), "n/a")),
			"Status: "+valueOr(plainStatusValue(subagent.Status), "n/a"),
			"Operation: "+valueOr(plainStatusValue(subagent.Activity), "n/a"),
			"Turns: "+approximateInt(subagent.Turns),
			"Tools: "+approximateInt(subagent.ToolUses),
			"Duration: "+displaySubagentDuration(subagent.DurationMillis, subagent.Active, observed.latest, now),
			"Tokens: "+approximateInt64(subagent.ApproxTokens),
		); err != nil {
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
	return r.activityEntrySemantic(entry, followActivitySemantic(entry))
}

func (r followRenderer) activityEntrySemantic(entry activity.Entry, semantic dashboardSemantic) error {
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
	prefix := ansi.Truncate("  "+observed+"  ", r.presentation.width, "")
	remaining := max(0, r.presentation.width-ansi.StringWidth(prefix))
	description = ansi.Truncate(description, remaining, "")
	line := r.presentation.styler.render(dashboardSemanticMetadata, prefix) +
		r.presentation.styler.render(semantic, description)
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

func (r followRenderer) fields(semantic dashboardSemantic, indent string, fields ...string) error {
	for _, line := range r.presentation.fieldLines(indent, fields...) {
		if err := r.line(semantic, line); err != nil {
			return err
		}
	}
	return nil
}

func (r followRenderer) terminalHeading(semantic dashboardSemantic) error {
	if !r.presentation.enabled {
		_, err := fmt.Fprintln(r.output, "\nTerminal Run summary:")
		return err
	}
	if err := r.write("\n"); err != nil {
		return err
	}
	return r.line(semantic, "Final")
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
	return dashboardRunLifecycleSemantic(observed)
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

func followObservationSemantic(observation followObservation) dashboardSemantic {
	if observation.supervision != "SUPERVISED" || observation.workerLivenessState == workerLivenessDead || observation.workerLivenessState == workerLivenessUnknown {
		return dashboardSemanticWarning
	}
	return dashboardSemanticActive
}

func followLivenessSemantic(observation followObservation) dashboardSemantic {
	if observation.workerLivenessState == workerLivenessDead || observation.workerLivenessState == workerLivenessUnknown || observation.workerLivenessState == workerLivenessAbsent {
		return dashboardSemanticWarning
	}
	return dashboardSemanticActive
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
