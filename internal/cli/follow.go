package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/robinjoseph08/backlog/internal/activity"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

const followPollInterval = 50 * time.Millisecond

type followStateSource interface {
	Preview() (state.State, bool, error)
}

type repositoryFollowSource struct {
	followStateSource
	commonDirectory string
}

func (s repositoryFollowSource) RunnerSupervised() (bool, error) {
	return runnerSupervised(s.commonDirectory)
}

func followCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("follow", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: backlog follow <run-id|positive-issue-number> [--raw] [flags]")
		flags.PrintDefaults()
	}
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the Run")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable used to identify the repository root")
	raw := flags.Bool("raw", false, "print the Worker's raw JSONL")
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return flags.Parse([]string{arg})
		}
	}
	selector, flagArgs, err := splitFollowArguments(args)
	if err != nil {
		return err
	}
	if err := flags.Parse(flagArgs); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected follow arguments: %s", strings.Join(flags.Args(), " "))
	}
	resolved, commonDirectory, err := resolveStateFromFlags(ctx, *repoDir, *stateDir, *gitExecutable)
	if err != nil {
		return err
	}
	source := repositoryFollowSource{
		followStateSource: state.FileStore{Path: filepath.Join(resolved, "state.json")},
		commonDirectory:   commonDirectory,
	}
	runID, err := resolveFollowSelector(source, selector)
	if err != nil {
		return err
	}
	if *raw {
		selected, err := loadFollowRun(source, runID)
		if err != nil {
			return err
		}
		observation := observeFollowRun(source, selected)
		if _, err := fmt.Fprintf(stderr, "Run: %s\nRunner supervision: %s\nWorker liveness: %s\n", runID, observation.supervision, observation.worker); err != nil {
			return err
		}
		return followRaw(ctx, source, runID, stdout, followPollInterval)
	}
	return followNormalized(ctx, source, runID, stdout, stderr, followPollInterval, time.Now)
}

func splitFollowArguments(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("usage: backlog follow <run-id|positive-issue-number> [--raw] [flags]")
	}
	if !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:], nil
	}
	for index, value := range args {
		if !strings.HasPrefix(value, "-") && (index == 0 || !followFlagTakesValue(args[index-1])) {
			remaining := append([]string{}, args[:index]...)
			remaining = append(remaining, args[index+1:]...)
			return value, remaining, nil
		}
	}
	return "", nil, errors.New("follow requires a Run ID or positive issue number")
}

func followFlagTakesValue(name string) bool {
	if strings.Contains(name, "=") {
		return false
	}
	name = strings.TrimLeft(name, "-")
	return name == "repo-dir" || name == "state-dir" || name == "git"
}

func followRaw(ctx context.Context, source followStateSource, runID string, output io.Writer, pollInterval time.Duration) error {
	selected, err := loadFollowRun(source, runID)
	if err != nil {
		return err
	}
	for selected.LogPath == "" {
		if selected.Status != scheduler.StatusClaimed && selected.Status != scheduler.StatusWorktreeReady {
			return fmt.Errorf("Run %q has no Worker log available", runID)
		}
		if !waitToFollow(ctx, pollInterval) {
			return nil
		}
		selected, err = loadFollowRun(source, runID)
		if err != nil {
			return err
		}
	}

	logPath := selected.LogPath
	logFile, err := os.Open(logPath)
	if err != nil {
		return fmt.Errorf("Run %q Worker log %q is unavailable: %w", runID, logPath, err)
	}
	defer logFile.Close()

	stream := rawLogStream{file: logFile, output: output}
	for {
		if err := stream.emitAvailable(ctx); err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return nil
			}
			return fmt.Errorf("follow Run %q Worker log %q: %w", runID, logPath, err)
		}
		selected, err = loadFollowRun(source, runID)
		if err != nil {
			return err
		}
		if selected.LogPath != logPath {
			return fmt.Errorf("Run %q Worker log changed from %q to %q", runID, logPath, selected.LogPath)
		}
		if scheduler.IsTerminal(selected.Status) && !selected.WorkerLogOpen {
			if err := stream.emitAvailable(ctx); err != nil {
				if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
					return nil
				}
				return fmt.Errorf("finish following Run %q Worker log %q: %w", runID, logPath, err)
			}
			return nil
		}
		if !waitToFollow(ctx, pollInterval) {
			return nil
		}
	}
}

func resolveFollowSelector(source followStateSource, selector string) (string, error) {
	current, _, err := source.Preview()
	if err != nil {
		return "", fmt.Errorf("resolve Follow selector %q: read runner state: %w", selector, err)
	}
	for _, run := range current.Runs {
		if run.RunID == selector {
			return run.RunID, nil
		}
	}
	issue, err := strconv.Atoi(selector)
	if err != nil || issue <= 0 {
		return "", fmt.Errorf("Run %q was not found", selector)
	}
	for _, lease := range current.Leases {
		if lease.Issue == issue {
			return lease.RunID, nil
		}
	}
	var latest *scheduler.Run
	for index := range current.Runs {
		run := &current.Runs[index]
		if run.Issue != issue {
			continue
		}
		if latest == nil || run.StartedAt.After(latest.StartedAt) || run.StartedAt.Equal(latest.StartedAt) && run.RunID > latest.RunID {
			latest = run
		}
	}
	if latest == nil {
		return "", fmt.Errorf("issue #%d has no Run to Follow", issue)
	}
	return latest.RunID, nil
}

func loadFollowRun(source followStateSource, runID string) (scheduler.Run, error) {
	current, _, err := source.Preview()
	if err != nil {
		return scheduler.Run{}, fmt.Errorf("follow Run %q: read runner state: %w", runID, err)
	}
	for _, run := range current.Runs {
		if run.RunID == runID {
			return run, nil
		}
	}
	return scheduler.Run{}, fmt.Errorf("Run %q was not found", runID)
}

type followObservation struct {
	supervision string
	worker      string
}

type followSupervisionSource interface {
	RunnerSupervised() (bool, error)
}

func observeFollowRun(source followStateSource, run scheduler.Run) followObservation {
	observation := followObservation{worker: followWorkerLiveness(run)}
	if scheduler.IsTerminal(run.Status) {
		observation.supervision = "n/a (terminal Run)"
		return observation
	}
	supervised := false
	var err error
	if observer, ok := source.(followSupervisionSource); ok {
		supervised, err = observer.RunnerSupervised()
	}
	switch {
	case err != nil:
		observation.supervision = "UNKNOWN (coordination ownership could not be observed)"
	case supervised:
		observation.supervision = "SUPERVISED"
	default:
		observation.supervision = "UNSUPERVISED"
	}
	return observation
}

func followWorkerLiveness(run scheduler.Run) string {
	if run.PID <= 0 || run.ProcessIdentity == "" {
		return "absent"
	}
	alive, err := signalZero(run.PID)
	if err != nil {
		return "unknown (PID liveness could not be verified)"
	}
	if !alive {
		return fmt.Sprintf("dead (recorded PID %d is absent)", run.PID)
	}
	identity, err := pidStartIdentity(run.PID)
	if err != nil {
		return fmt.Sprintf("unknown (PID %d process-start identity could not be verified)", run.PID)
	}
	if identity != run.ProcessIdentity {
		return fmt.Sprintf("dead (stale PID %d has a different process-start identity)", run.PID)
	}
	return fmt.Sprintf("alive (PID %d and process-start identity verified)", run.PID)
}

func waitToFollow(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type followMetrics struct {
	entries             []activity.Entry
	latest              time.Time
	operation           string
	turns               int
	tokens              int64
	tokensKnown         bool
	usageMissing        bool
	subagents           map[string]followSubagent
	subagentOrder       []string
	pendingSubagentFeed map[string]activity.Entry
	sequence            int
}

type followSubagent struct {
	snapshot activity.SubagentSnapshot
	latest   time.Time
	sequence int
}

func (m *followMetrics) apply(entry activity.Entry) {
	if entry.SuppressFeed && entry.Subagent != nil {
		if m.pendingSubagentFeed == nil {
			m.pendingSubagentFeed = make(map[string]activity.Entry)
		}
		m.pendingSubagentFeed[entry.Subagent.ID] = entry
	} else {
		m.appendFeedEntry(entry)
		if entry.Subagent != nil {
			delete(m.pendingSubagentFeed, entry.Subagent.ID)
		}
	}
	if !entry.ObservedAt.IsZero() && entry.ObservedAt.After(m.latest) {
		m.latest = entry.ObservedAt
	}
	if entry.OperationChanged {
		m.operation = entry.Operation
	}
	m.turns += entry.TurnDelta
	if entry.ResponseCompleted {
		if !entry.TokensKnown || entry.TokenDelta < 0 || entry.TokenDelta > math.MaxInt64-m.tokens {
			m.usageMissing = true
			m.tokensKnown = false
		} else {
			m.tokens += entry.TokenDelta
			m.tokensKnown = !m.usageMissing
		}
	}
	if entry.Subagent != nil {
		if m.subagents == nil {
			m.subagents = make(map[string]followSubagent)
		}
		if _, exists := m.subagents[entry.Subagent.ID]; !exists {
			m.subagentOrder = append(m.subagentOrder, entry.Subagent.ID)
		}
		m.sequence++
		m.subagents[entry.Subagent.ID] = followSubagent{snapshot: *entry.Subagent, latest: entry.ObservedAt, sequence: m.sequence}
	}
}

func (m *followMetrics) appendFeedEntry(entry activity.Entry) {
	m.entries = append(m.entries, entry)
	if len(m.entries) > 20 {
		m.entries = append([]activity.Entry(nil), m.entries[len(m.entries)-20:]...)
	}
}

func (m *followMetrics) clearPendingSubagentFeed() {
	m.pendingSubagentFeed = nil
}

type completeRecordReader struct {
	path    string
	offset  int64
	pending []byte
}

func (r *completeRecordReader) read() ([][]byte, error) {
	file, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < r.offset {
		return nil, errors.New("Activity source was truncated while being followed")
	}
	if _, err := file.Seek(r.offset, io.SeekStart); err != nil {
		return nil, err
	}
	appended, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	r.offset += int64(len(appended))
	r.pending = append(r.pending, appended...)
	lastNewline := bytes.LastIndexByte(r.pending, '\n')
	if lastNewline < 0 {
		return nil, nil
	}
	complete := r.pending[:lastNewline]
	r.pending = append([]byte(nil), r.pending[lastNewline+1:]...)
	if len(complete) == 0 {
		return nil, nil
	}
	lines := bytes.Split(complete, []byte{'\n'})
	for index := range lines {
		lines[index] = bytes.TrimSuffix(lines[index], []byte{'\r'})
	}
	return lines, nil
}

type normalizedActivitySource struct {
	reader         completeRecordReader
	rawPath        string
	projectionPath string
	projected      bool
	projector      activity.Projector
	now            func() time.Time
	stderr         io.Writer
	initial        bool
	semanticRead   int
}

func openNormalizedActivitySource(run scheduler.Run, stderr io.Writer, now func() time.Time) (*normalizedActivitySource, error) {
	if run.LogPath == "" {
		return nil, nil
	}
	projectionPath := activity.PathForLog(run.LogPath)
	projectionUsable := true
	if diagnostic, err := os.ReadFile(activity.UnavailablePath(projectionPath)); err == nil {
		projectionUsable = false
		fmt.Fprintf(stderr, "Follow diagnostic for Run %q: Activity projection unavailable: %s", run.RunID, diagnostic)
	} else if !errors.Is(err, os.ErrNotExist) {
		projectionUsable = false
		fmt.Fprintf(stderr, "Follow diagnostic for Run %q: Activity projection diagnostic unavailable: %v\n", run.RunID, err)
	}
	if projectionUsable {
		if _, err := os.Stat(projectionPath); err == nil {
			return &normalizedActivitySource{
				reader: completeRecordReader{path: projectionPath}, rawPath: run.LogPath, projectionPath: projectionPath,
				projected: true, now: now, stderr: stderr, initial: true,
			}, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(stderr, "Follow diagnostic for Run %q: Activity projection unavailable: %v\n", run.RunID, err)
		}
	}
	if _, err := os.Stat(run.LogPath); err != nil {
		return nil, fmt.Errorf("Run %q Worker Activity is unavailable: %w", run.RunID, err)
	}
	fmt.Fprintf(stderr, "Follow diagnostic for Run %q: Activity projection unavailable; replayed Activity age is n/a\n", run.RunID)
	return &normalizedActivitySource{
		reader: completeRecordReader{path: run.LogPath}, rawPath: run.LogPath, projectionPath: projectionPath,
		now: now, stderr: stderr, initial: true,
	}, nil
}

func (s *normalizedActivitySource) read() ([]activity.Entry, int, bool) {
	replayed, reset := s.fallbackIfProjectionFailed()
	lines, err := s.reader.read()
	if err != nil {
		if s.projected {
			fmt.Fprintf(s.stderr, "Follow diagnostic: Activity projection unavailable: %v; replaying raw Worker Activity\n", err)
			fmt.Fprintln(s.stderr, "Follow diagnostic: replayed Activity age is n/a")
			replayed = s.switchToRaw()
			replayedEntries, _, _ := s.read()
			return replayedEntries, replayed, true
		}
		fmt.Fprintf(s.stderr, "Follow diagnostic: Worker Activity unavailable: %v\n", err)
		return nil, replayed, reset
	}
	entries := make([]activity.Entry, 0, len(lines))
	for _, line := range lines {
		if s.projected {
			var entry activity.Entry
			if err := json.Unmarshal(line, &entry); err != nil || entry.Version != activity.CurrentVersion || entry.Kind == "" || entry.Description == "" {
				fmt.Fprintln(s.stderr, "Follow diagnostic: unusable Activity projection record; replaying raw Worker Activity")
				fmt.Fprintln(s.stderr, "Follow diagnostic: replayed Activity age is n/a")
				replayed = s.switchToRaw()
				replayedEntries, _, _ := s.read()
				return replayedEntries, replayed, true
			}
			entries = append(entries, entry)
			continue
		}
		observedAt := time.Time{}
		if !s.initial {
			observedAt = s.now()
		}
		entry, semantic, err := s.projector.Observe(line, observedAt)
		if err != nil {
			fmt.Fprintf(s.stderr, "Follow diagnostic: ignored unusable Worker telemetry: %v\n", err)
			continue
		}
		if semantic {
			entries = append(entries, entry)
		}
	}
	s.initial = false
	s.semanticRead += len(entries)
	return entries, replayed, reset
}

func (s *normalizedActivitySource) fallbackIfProjectionFailed() (int, bool) {
	if !s.projected {
		return 0, false
	}
	diagnostic, err := os.ReadFile(activity.UnavailablePath(s.projectionPath))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false
	}
	if err == nil {
		fmt.Fprintf(s.stderr, "Follow diagnostic: Activity projection unavailable: %s", diagnostic)
	} else {
		fmt.Fprintf(s.stderr, "Follow diagnostic: Activity projection diagnostic unavailable: %v\n", err)
	}
	fmt.Fprintln(s.stderr, "Follow diagnostic: replayed Activity age is n/a")
	return s.switchToRaw(), true
}

func (s *normalizedActivitySource) switchToRaw() int {
	replayed := s.semanticRead
	s.reader = completeRecordReader{path: s.rawPath}
	s.projected = false
	s.projector = activity.Projector{}
	s.initial = true
	s.semanticRead = 0
	return replayed
}

func consumeActivity(metrics *followMetrics, source *normalizedActivitySource) []activity.Entry {
	entries, replayed, reset := source.read()
	if !reset {
		for _, entry := range entries {
			metrics.apply(entry)
		}
		return entries
	}
	*metrics = followMetrics{}
	for _, entry := range entries {
		metrics.apply(entry)
	}
	return entries[min(replayed, len(entries)):]
}

func printNewActivity(output io.Writer, metrics *followMetrics, source *normalizedActivitySource) error {
	showSubagentSummary := false
	for _, entry := range consumeActivity(metrics, source) {
		if entry.SuppressFeed {
			continue
		}
		if err := printActivityEntry(output, entry); err != nil {
			return err
		}
		showSubagentSummary = showSubagentSummary || entry.Subagent != nil
	}
	flushed, err := flushPendingSubagentActivity(output, metrics, source.now())
	if err != nil {
		return err
	}
	if showSubagentSummary || flushed {
		return printActiveSubagentSummary(output, *metrics)
	}
	return nil
}

func flushPendingSubagentActivity(output io.Writer, metrics *followMetrics, now time.Time) (bool, error) {
	flushed := false
	for _, id := range metrics.subagentOrder {
		entry, pending := metrics.pendingSubagentFeed[id]
		if !pending || entry.ObservedAt.IsZero() || now.Sub(entry.ObservedAt) < time.Second {
			continue
		}
		if err := printActivityEntry(output, entry); err != nil {
			return false, err
		}
		entry.SuppressFeed = false
		metrics.appendFeedEntry(entry)
		delete(metrics.pendingSubagentFeed, id)
		flushed = true
	}
	return flushed, nil
}

func printActiveSubagentSummary(output io.Writer, metrics followMetrics) error {
	active, deepest := metrics.activeSubagentSummary()
	_, err := fmt.Fprintf(output, "  Subagent summary: %d (%d active) | Deepest current operation: %s\n", len(metrics.subagents), active, deepest)
	return err
}

func followNormalized(
	ctx context.Context,
	source followStateSource,
	runID string,
	output, diagnostics io.Writer,
	pollInterval time.Duration,
	now func() time.Time,
) error {
	selected, err := loadFollowRun(source, runID)
	if err != nil {
		return err
	}
	activitySource, err := openNormalizedActivitySource(selected, diagnostics, now)
	if err != nil {
		fmt.Fprintln(diagnostics, "Follow diagnostic:", err)
	}
	metrics := followMetrics{}
	if activitySource != nil {
		consumeActivity(&metrics, activitySource)
	}
	lastObservation := observeFollowRun(source, selected)
	if err := printFollowSummary(output, selected, metrics, lastObservation, now()); err != nil {
		return err
	}
	if err := printInitialActivity(output, metrics.entries); err != nil {
		return err
	}
	metrics.clearPendingSubagentFeed()
	if scheduler.IsTerminal(selected.Status) && !selected.WorkerLogOpen {
		return nil
	}

	lastStatus := selected.Status
	lastLogPath := selected.LogPath
	for {
		if activitySource != nil {
			if err := printNewActivity(output, &metrics, activitySource); err != nil {
				return err
			}
		}
		selected, err = loadFollowRun(source, runID)
		if err != nil {
			return err
		}
		if lastLogPath != "" && selected.LogPath != lastLogPath {
			return fmt.Errorf("Run %q Worker log changed from %q to %q", runID, lastLogPath, selected.LogPath)
		}
		if activitySource == nil && selected.LogPath != "" {
			activitySource, err = openNormalizedActivitySource(selected, diagnostics, now)
			if err != nil {
				fmt.Fprintln(diagnostics, "Follow diagnostic:", err)
			} else {
				lastLogPath = selected.LogPath
				consumeActivity(&metrics, activitySource)
				for _, entry := range metrics.entries {
					if err := printActivityEntry(output, entry); err != nil {
						return err
					}
				}
				if len(metrics.subagents) > 0 {
					if err := printActiveSubagentSummary(output, metrics); err != nil {
						return err
					}
				}
				metrics.clearPendingSubagentFeed()
			}
		}
		if selected.Status != lastStatus {
			entry := activity.Entry{
				Version: activity.CurrentVersion, ObservedAt: now().UTC(), Kind: "lifecycle",
				Description: "Run state changed to " + string(selected.Status),
			}
			if err := printActivityEntry(output, entry); err != nil {
				return err
			}
			lastStatus = selected.Status
		}
		observation := observeFollowRun(source, selected)
		if observation.supervision != lastObservation.supervision {
			if err := printActivityEntry(output, activity.Entry{ObservedAt: now().UTC(), Description: "Runner supervision changed to " + observation.supervision}); err != nil {
				return err
			}
		}
		if observation.worker != lastObservation.worker {
			if err := printActivityEntry(output, activity.Entry{ObservedAt: now().UTC(), Description: "Worker liveness changed to " + observation.worker}); err != nil {
				return err
			}
		}
		lastObservation = observation
		if scheduler.IsTerminal(selected.Status) && !selected.WorkerLogOpen {
			if activitySource != nil {
				if err := printNewActivity(output, &metrics, activitySource); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintln(output, "\nTerminal Run summary:"); err != nil {
				return err
			}
			return printFollowSummary(output, selected, metrics, lastObservation, now())
		}
		if !waitToFollow(ctx, pollInterval) {
			return nil
		}
	}
}

func printFollowSummary(output io.Writer, run scheduler.Run, metrics followMetrics, observation followObservation, now time.Time) error {
	issue := fmt.Sprintf("#%d", run.Issue)
	if run.IssueTitle != "" {
		issue += "  " + run.IssueTitle
	}
	if run.IssueURL != "" {
		issue += "  " + run.IssueURL
	}
	elapsedEnd := now
	if run.CompletedAt != nil {
		elapsedEnd = *run.CompletedAt
	} else if scheduler.IsTerminal(run.Status) && !run.UpdatedAt.IsZero() {
		elapsedEnd = run.UpdatedAt
	}
	elapsed := "n/a"
	if !run.StartedAt.IsZero() {
		elapsed = displayDuration(elapsedEnd.Sub(run.StartedAt))
	}
	age := "n/a"
	if !metrics.latest.IsZero() {
		age = displayDuration(now.Sub(metrics.latest))
	}
	operation := valueOr(metrics.operation, "n/a")
	workerTokens := "n/a"
	if metrics.tokensKnown {
		workerTokens = fmt.Sprintf("%d", metrics.tokens)
	}
	activeSubagents, deepest := metrics.activeSubagentSummary()
	subagentTurns, subagentToolUses, subagentTokens := metrics.subagentTotals()
	observedTokens := "n/a"
	if len(metrics.subagents) == 0 && metrics.tokensKnown {
		observedTokens = fmt.Sprintf("%d", metrics.tokens)
	} else if approximate, known := metrics.totalSubagentTokens(); metrics.tokensKnown && known && approximate <= math.MaxInt64-metrics.tokens {
		observedTokens = fmt.Sprintf("~%d", metrics.tokens+approximate)
	}
	if _, err := fmt.Fprintf(output, "Run: %s\nIssue: %s\nState: %s\nRunner supervision: %s\nWorker liveness: %s\nElapsed: %s\nActivity age: %s\nCurrent Worker operation: %s\nCompleted Worker turns: %d\nCompleted Worker tokens: %s\nSubagents: %d (%d active)\nDeepest current operation: %s\nApproximate Subagent turns: %s\nApproximate Subagent tool uses: %s\nApproximate Subagent tokens: %s\nObserved tokens: %s\n",
		run.RunID, issue, run.Status, observation.supervision, observation.worker, elapsed, age, operation, metrics.turns, workerTokens, len(metrics.subagents), activeSubagents,
		deepest, subagentTurns, subagentToolUses, subagentTokens, observedTokens); err != nil {
		return err
	}
	for index, id := range metrics.subagentOrder {
		observed := metrics.subagents[id]
		subagent := observed.snapshot
		if _, err := fmt.Fprintf(output, "Subagent %d [%s]: %s | status: %s | operation: %s | turns: %s | tool uses: %s | duration: %s | tokens: %s\n",
			index+1, shortSubagentID(id), valueOr(subagent.Description, "n/a"), valueOr(subagent.Status, "n/a"),
			valueOr(subagent.Activity, "n/a"), approximateInt(subagent.Turns), approximateInt(subagent.ToolUses),
			displaySubagentDuration(subagent.DurationMillis, subagent.Active, observed.latest, now), approximateInt64(subagent.ApproxTokens)); err != nil {
			return err
		}
	}
	return nil
}

func (m followMetrics) activeSubagentSummary() (int, string) {
	active := 0
	latestSequence := -1
	deepest := "n/a"
	for _, subagent := range m.subagents {
		if !subagent.snapshot.Active {
			continue
		}
		active++
		if subagent.sequence > latestSequence {
			latestSequence = subagent.sequence
			operation := valueOr(subagent.snapshot.Activity, valueOr(subagent.snapshot.Status, "n/a"))
			deepest = fmt.Sprintf("Subagent %q: %s", valueOr(subagent.snapshot.Description, "n/a"), operation)
		}
	}
	return active, deepest
}

func (m followMetrics) subagentTotals() (string, string, string) {
	if len(m.subagents) == 0 {
		return "n/a", "n/a", "n/a"
	}
	return totalIntIfKnown(m.subagents, func(snapshot activity.SubagentSnapshot) *int { return snapshot.Turns }),
		totalIntIfKnown(m.subagents, func(snapshot activity.SubagentSnapshot) *int { return snapshot.ToolUses }),
		totalInt64IfKnown(m.subagents, func(snapshot activity.SubagentSnapshot) *int64 { return snapshot.ApproxTokens })
}

func totalIntIfKnown(subagents map[string]followSubagent, field func(activity.SubagentSnapshot) *int) string {
	var total int64
	for _, subagent := range subagents {
		value := field(subagent.snapshot)
		if value == nil || *value < 0 || int64(*value) > math.MaxInt64-total {
			return "n/a"
		}
		total += int64(*value)
	}
	return fmt.Sprintf("~%d", total)
}

func totalInt64IfKnown(subagents map[string]followSubagent, field func(activity.SubagentSnapshot) *int64) string {
	total, known := totalInt64(subagents, field)
	if !known {
		return "n/a"
	}
	return fmt.Sprintf("~%d", total)
}

func totalInt64(subagents map[string]followSubagent, field func(activity.SubagentSnapshot) *int64) (int64, bool) {
	var total int64
	for _, subagent := range subagents {
		value := field(subagent.snapshot)
		if value == nil || *value < 0 || *value > math.MaxInt64-total {
			return 0, false
		}
		total += *value
	}
	return total, true
}

func (m followMetrics) totalSubagentTokens() (int64, bool) {
	if len(m.subagents) == 0 {
		return 0, false
	}
	return totalInt64(m.subagents, func(snapshot activity.SubagentSnapshot) *int64 { return snapshot.ApproxTokens })
}

func approximateInt(value *int) string {
	if value == nil {
		return "n/a"
	}
	return fmt.Sprintf("~%d", *value)
}

func approximateInt64(value *int64) string {
	if value == nil {
		return "n/a"
	}
	return fmt.Sprintf("~%d", *value)
}

func displaySubagentDuration(milliseconds *int64, active bool, observedAt, now time.Time) string {
	if milliseconds == nil || *milliseconds < 0 || *milliseconds > math.MaxInt64/int64(time.Millisecond) {
		return "n/a"
	}
	duration := time.Duration(*milliseconds) * time.Millisecond
	if active && !observedAt.IsZero() && now.After(observedAt) {
		elapsed := now.Sub(observedAt)
		if elapsed > time.Duration(math.MaxInt64)-duration {
			return "n/a"
		}
		duration += elapsed
	}
	return duration.String()
}

func shortSubagentID(id string) string {
	if len(id) <= 16 {
		return id
	}
	return id[:16]
}

func printInitialActivity(output io.Writer, entries []activity.Entry) error {
	if _, err := fmt.Fprintln(output, "\nRun Activity (latest 20):"); err != nil {
		return err
	}
	start := max(0, len(entries)-20)
	if start == len(entries) {
		_, err := fmt.Fprintln(output, "  n/a")
		return err
	}
	for _, entry := range entries[start:] {
		if err := printActivityEntry(output, entry); err != nil {
			return err
		}
	}
	return nil
}

func printActivityEntry(output io.Writer, entry activity.Entry) error {
	observed := "time n/a"
	if !entry.ObservedAt.IsZero() {
		observed = entry.ObservedAt.Format(time.RFC3339)
	}
	_, err := fmt.Fprintf(output, "  %s  %s\n", observed, entry.Description)
	return err
}

func displayDuration(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	return duration.Truncate(time.Second).String()
}

type rawLogStream struct {
	file          *os.File
	output        io.Writer
	scannedOffset int64
	emittedOffset int64
}

func (s *rawLogStream) emitAvailable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("inspect raw JSONL: %w", err)
	}
	remaining := info.Size() - s.scannedOffset
	buffer := make([]byte, 32*1024)
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		readSize := min(int64(len(buffer)), remaining)
		count, readErr := s.file.Read(buffer[:readSize])
		if count > 0 {
			s.scannedOffset += int64(count)
			remaining -= int64(count)
			if newline := bytes.LastIndexByte(buffer[:count], '\n'); newline >= 0 {
				completeOffset := s.scannedOffset - int64(count-newline-1)
				if err := s.emitThrough(ctx, completeOffset, buffer); err != nil {
					return err
				}
			}
		}
		if readErr != nil {
			return fmt.Errorf("read raw JSONL: %w", readErr)
		}
		if count == 0 {
			return fmt.Errorf("read raw JSONL: %w", io.ErrNoProgress)
		}
	}
	return nil
}

func (s *rawLogStream) emitThrough(ctx context.Context, completeOffset int64, buffer []byte) error {
	reader := io.NewSectionReader(s.file, s.emittedOffset, completeOffset-s.emittedOffset)
	remaining := completeOffset - s.emittedOffset
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, err := reader.Read(buffer)
		if count > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if writeErr := writeAllContext(ctx, s.output, buffer[:count]); writeErr != nil {
				return fmt.Errorf("write raw JSONL: %w", writeErr)
			}
			remaining -= int64(count)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read complete raw JSONL: %w", err)
		}
		if count == 0 {
			return fmt.Errorf("read complete raw JSONL: %w", io.ErrUnexpectedEOF)
		}
	}
	s.emittedOffset = completeOffset
	return nil
}

func writeAll(writer io.Writer, data []byte) error {
	return writeAllContext(context.Background(), writer, data)
}

func writeAllContext(ctx context.Context, writer io.Writer, data []byte) error {
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
