package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

type acknowledgmentSelection struct {
	index int
	run   scheduler.Run
}

func acknowledgeCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("acknowledge", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: backlog acknowledge <run-id|positive-issue-number>... [flags]")
		fmt.Fprintln(stderr, "       backlog acknowledge --all [flags]")
		flags.PrintDefaults()
	}
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the Runs")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable used to identify the repository root")
	all := flags.Bool("all", false, "acknowledge every currently eligible Historical Run outcome")
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return flags.Parse([]string{arg})
		}
	}
	selectors, flagArgs := splitAcknowledgeArguments(args)
	if err := flags.Parse(flagArgs); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected acknowledge arguments: %s", strings.Join(flags.Args(), " "))
	}
	if *all && len(selectors) != 0 {
		return errors.New("acknowledge --all cannot be combined with selectors")
	}
	if !*all && len(selectors) == 0 {
		return errors.New("usage: backlog acknowledge <run-id|positive-issue-number>... or backlog acknowledge --all")
	}

	resolved, commonDirectory, err := resolveStateFromFlags(ctx, *repoDir, *stateDir, *gitExecutable)
	if err != nil {
		return err
	}
	lock, err := acquireRepositoryLock(commonDirectory)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()
	if err := bindStateDirectory(commonDirectory, resolved); err != nil {
		return err
	}
	store := state.FileStore{Path: filepath.Join(resolved, "state.json")}
	current, migrationRequired, err := store.Preview()
	if err != nil {
		return err
	}
	selected, err := resolveAcknowledgmentSelections(current, selectors, *all)
	if err != nil {
		return err
	}

	acknowledgedAt := time.Now().UTC()
	changed := make([]acknowledgmentSelection, 0, len(selected))
	already := make([]acknowledgmentSelection, 0, len(selected))
	for _, selection := range selected {
		if current.Runs[selection.index].AcknowledgedAt != nil {
			already = append(already, selection)
			continue
		}
		current.Runs[selection.index].AcknowledgedAt = &acknowledgedAt
		selection.run.AcknowledgedAt = &acknowledgedAt
		changed = append(changed, selection)
	}
	if len(changed) != 0 || migrationRequired {
		if err := store.Save(current); err != nil {
			return fmt.Errorf("persist state for Outcome Acknowledgment: %w", err)
		}
	}
	return printAcknowledgmentResult(stdout, changed, already)
}

func splitAcknowledgeArguments(args []string) ([]string, []string) {
	selectors := make([]string, 0, len(args))
	flagArgs := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := args[index]
		name := strings.TrimLeft(strings.SplitN(arg, "=", 2)[0], "-")
		if strings.HasPrefix(arg, "-") {
			flagArgs = append(flagArgs, arg)
			if (name == "repo-dir" || name == "state-dir" || name == "git") && !strings.Contains(arg, "=") && index+1 < len(args) {
				index++
				flagArgs = append(flagArgs, args[index])
			}
			continue
		}
		selectors = append(selectors, arg)
	}
	return selectors, flagArgs
}

func resolveAcknowledgmentSelections(current state.State, selectors []string, all bool) ([]acknowledgmentSelection, error) {
	leased := make(map[string]bool, len(current.Leases))
	for _, lease := range current.Leases {
		leased[lease.RunID] = true
	}
	selected := make(map[string]acknowledgmentSelection)
	add := func(index int) {
		run := current.Runs[index]
		selected[run.RunID] = acknowledgmentSelection{index: index, run: run}
	}
	if all {
		for index, run := range current.Runs {
			if acknowledgmentEligible(run, leased[run.RunID]) {
				add(index)
			}
		}
		return sortedAcknowledgmentSelections(selected), nil
	}

	for _, selector := range selectors {
		exact := -1
		for index := range current.Runs {
			if current.Runs[index].RunID == selector {
				exact = index
				break
			}
		}
		if exact >= 0 {
			run := current.Runs[exact]
			if !acknowledgmentEligible(run, leased[run.RunID]) {
				return nil, ineligibleAcknowledgmentError(run, leased[run.RunID])
			}
			add(exact)
			continue
		}
		issue, err := strconv.Atoi(selector)
		if err != nil || issue <= 0 {
			return nil, fmt.Errorf("Run %q was not found", selector)
		}
		issueFound := false
		for index, run := range current.Runs {
			if run.Issue != issue {
				continue
			}
			issueFound = true
			if acknowledgmentEligible(run, leased[run.RunID]) {
				add(index)
			}
		}
		if !issueFound {
			return nil, fmt.Errorf("issue #%d has no Run history", issue)
		}
	}
	return sortedAcknowledgmentSelections(selected), nil
}

func acknowledgmentEligible(run scheduler.Run, leased bool) bool {
	return !leased && (run.Status == scheduler.StatusFailed || run.Status == scheduler.StatusNeedsHuman)
}

func ineligibleAcknowledgmentError(run scheduler.Run, leased bool) error {
	if leased {
		return fmt.Errorf("Run %q is not an eligible Historical Run outcome because it retains a Lease", run.RunID)
	}
	return fmt.Errorf("Run %q is not eligible for Outcome Acknowledgment in state %q", run.RunID, run.Status)
}

func sortedAcknowledgmentSelections(selected map[string]acknowledgmentSelection) []acknowledgmentSelection {
	result := make([]acknowledgmentSelection, 0, len(selected))
	for _, selection := range selected {
		result = append(result, selection)
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := lifecycleTime(result[i].run), lifecycleTime(result[j].run)
		if left.Equal(right) {
			return result[i].run.RunID > result[j].run.RunID
		}
		return left.After(right)
	})
	return result
}

func printAcknowledgmentResult(output io.Writer, changed, already []acknowledgmentSelection) error {
	lines := make([]string, 0, 2+len(changed)+len(already))
	if len(changed) == 0 && len(already) == 0 {
		lines = append(lines, "No eligible Historical Run outcomes to acknowledge.")
	} else if len(changed) == 0 {
		lines = append(lines, "No additional Historical Run outcomes acknowledged.")
	} else {
		lines = append(lines, fmt.Sprintf("Acknowledged %d Historical Run outcome(s):", len(changed)))
		for _, selection := range changed {
			lines = append(lines, acknowledgmentResultLine(selection))
		}
	}
	if len(already) != 0 {
		lines = append(lines, fmt.Sprintf("Already acknowledged %d Historical Run outcome(s):", len(already)))
		for _, selection := range already {
			lines = append(lines, acknowledgmentResultLine(selection))
		}
	}
	return writeAll(output, []byte(strings.Join(lines, "\n")+"\n"))
}

func acknowledgmentResultLine(selection acknowledgmentSelection) string {
	return fmt.Sprintf("  %s (issue #%d) at %s", plainStatusValue(selection.run.RunID), selection.run.Issue, selection.run.AcknowledgedAt.Format(time.RFC3339Nano))
}
