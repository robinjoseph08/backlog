package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

type Config struct {
	Repo                string
	DefaultBranch       string
	MaxConcurrentIssues int
	PollInterval        time.Duration
	MaxWorkerAge        time.Duration
	Watch               bool
	SessionsDir         string
}

type GitHub interface {
	Candidates(context.Context, string) ([]scheduler.Candidate, error)
	Completion(context.Context, string, int, string) (ghadapter.CompletionOutcome, error)
}

type Store interface {
	Load() (state.State, error)
	Save(state.State) error
}

type Worktrees interface {
	Plan(int, string) (worktree.Assignment, error)
	Prepare(context.Context, worktree.Assignment) error
	Cleanup(context.Context, worktree.Assignment) error
	Exists(worktree.Assignment) bool
}

type WorkerProcess interface {
	PID() int
	Release() error
	Abort() error
	Wait() worker.Result
	Close() worker.Result
}

type Workers interface {
	Start(context.Context, worker.Request) (WorkerProcess, error)
	Release(runID string) error
}

type Runner struct {
	Config    Config
	GitHub    GitHub
	Store     Store
	Worktrees Worktrees
	Workers   Workers
	Output    io.Writer

	Now         func() time.Time
	NewRunID    func(issue int) string
	PIDAlive    func(pid int) bool
	PIDIdentity func(pid int) (string, error)
}

type workerCompletion struct {
	issue  int
	result worker.Result
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	runCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	current, err := r.Store.Load()
	if err != nil {
		return fmt.Errorf("load runner state: %w", err)
	}
	if err := r.initializeState(&current); err != nil {
		return err
	}
	if err := r.Store.Save(current); err != nil {
		return fmt.Errorf("initialize runner state: %w", err)
	}
	if err := r.reconcile(ctx, &current, nil); err != nil {
		return err
	}

	completions := make(chan workerCompletion, r.Config.MaxConcurrentIssues)
	localWorkers := make(map[int]WorkerProcess)
	poll := time.NewTicker(r.Config.PollInterval)
	defer poll.Stop()

	for {
		if ctx.Err() != nil {
			return r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped; worker was terminated and its worktree was retained")
		}

		candidates, err := r.GitHub.Candidates(ctx, r.Config.Repo)
		if err != nil {
			shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a backlog reconciliation error; worktree retained")
			if ctx.Err() != nil {
				return shutdownErr
			}
			return errors.Join(fmt.Errorf("reconcile GitHub backlog: %w", err), shutdownErr)
		}
		plan := scheduler.Plan(scheduler.Snapshot{Candidates: candidates, Runs: current.Runs, Leases: current.Leases}, r.Config.MaxConcurrentIssues)
		for _, candidate := range plan.Starts {
			process, err := r.start(ctx, runCtx, &current, candidate)
			if err != nil {
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a worker launch error; worktree retained")
				return errors.Join(err, shutdownErr)
			}
			if process == nil {
				continue
			}
			localWorkers[candidate.Number] = process
			go func(issue int, process WorkerProcess) {
				completions <- workerCompletion{issue: issue, result: process.Wait()}
			}(candidate.Number, process)
		}

		if len(plan.Starts) > 0 {
			continue
		}
		if unfinishedRunCount(&current) == 0 && !r.Config.Watch {
			return nil
		}

		select {
		case completion := <-completions:
			process := localWorkers[completion.issue]
			if process == nil {
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after an unknown worker completion; worktree retained")
				return errors.Join(fmt.Errorf("worker completed for unowned issue #%d", completion.issue), shutdownErr)
			}
			closedBeforeReconciliation := false
			var closed worker.Result
			if !completion.result.Settled {
				if completion.result.StreamErr == nil {
					completion.result.StreamErr = errors.New("Pi RPC worker ended without agent_settled")
					completion.result.Err = errors.Join(completion.result.Err, completion.result.StreamErr)
				}
				if err := process.Abort(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					completion.result.Err = errors.Join(completion.result.Err, fmt.Errorf("stop invalid Pi RPC worker: %w", err))
				}
				closed = process.Close()
				closedBeforeReconciliation = true
				completion.result.ExitCode = closed.ExitCode
				completion.result.Err = errors.Join(completion.result.Err, closed.Err)
			}
			runID := findActiveRun(&current, completion.issue).RunID
			if err := r.handleWorkerCompletion(ctx, &current, completion); err != nil {
				_ = process.Abort()
				_ = process.Close()
				delete(localWorkers, completion.issue)
				persisted, reloadErr := r.Store.Load()
				if reloadErr == nil {
					current = persisted
				}
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a completion error; worktree retained")
				return errors.Join(err, reloadErr, shutdownErr)
			}
			// Reconciliation and its durable state write happen while the idle RPC
			// process is still alive. EOF is sent only after that write succeeds.
			if !closedBeforeReconciliation {
				closed = process.Close()
			}
			delete(localWorkers, completion.issue)
			if err := r.finalizeSettledWorker(ctx, &current, runID, closed.Err, completion.result.Settled); err != nil {
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after an RPC finalization error; worktree retained")
				return errors.Join(err, shutdownErr)
			}
		case <-poll.C:
			if err := r.reconcile(ctx, &current, localWorkers); err != nil {
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a reconciliation error; worktree retained")
				return errors.Join(err, shutdownErr)
			}
		case <-ctx.Done():
			continue
		}
	}
}

func (r *Runner) validate() error {
	if r.Config.Repo == "" || r.Config.DefaultBranch == "" || r.Config.SessionsDir == "" {
		return errors.New("runner repository configuration is incomplete")
	}
	if r.Config.MaxConcurrentIssues <= 0 {
		return fmt.Errorf("max concurrent issues must be positive")
	}
	if r.Config.PollInterval <= 0 {
		return fmt.Errorf("poll interval must be positive")
	}
	if r.Config.MaxWorkerAge <= 0 {
		r.Config.MaxWorkerAge = 7 * 24 * time.Hour
	}
	if r.GitHub == nil || r.Store == nil || r.Worktrees == nil || r.Workers == nil {
		return errors.New("runner adapters are incomplete")
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.NewRunID == nil {
		r.NewRunID = defaultRunID
	}
	if r.PIDAlive == nil {
		r.PIDAlive = pidAlive
	}
	if r.PIDIdentity == nil {
		r.PIDIdentity = pidIdentity
	}
	if r.Output == nil {
		r.Output = io.Discard
	}
	return nil
}

func (r *Runner) initializeState(current *state.State) error {
	if current.Repo != "" && current.Repo != r.Config.Repo {
		return fmt.Errorf("state belongs to %s, not %s", current.Repo, r.Config.Repo)
	}
	current.Repo = r.Config.Repo
	if current.DefaultBranch != "" && current.DefaultBranch != r.Config.DefaultBranch {
		return fmt.Errorf("repository default branch changed from %s to %s; move or reset runner state", current.DefaultBranch, r.Config.DefaultBranch)
	}
	current.DefaultBranch = r.Config.DefaultBranch
	current.MaxConcurrentIssues = r.Config.MaxConcurrentIssues
	return nil
}

func (r *Runner) start(ctx context.Context, workerCtx context.Context, current *state.State, candidate scheduler.Candidate) (WorkerProcess, error) {
	now := r.Now().UTC()
	runID := r.NewRunID(candidate.Number)
	run := scheduler.Run{
		Issue: candidate.Number, RunID: runID, Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModeRPC,
		SessionName: fmt.Sprintf("afk #%d", candidate.Number), SessionID: "backlog-" + runID,
		SessionDir: filepath.Join(r.Config.SessionsDir, runID), StartedAt: now, UpdatedAt: now,
	}
	current.Runs = append(current.Runs, run)
	current.Leases = append(current.Leases, scheduler.Lease{LeaseID: runID, Issue: candidate.Number, RunID: runID})
	if err := r.Store.Save(*current); err != nil {
		return nil, fmt.Errorf("persist lease for issue #%d: %w", candidate.Number, err)
	}
	r.logf("claimed issue #%d as %s", candidate.Number, runID)

	assignment, err := r.Worktrees.Plan(candidate.Number, runID)
	if err != nil {
		r.failRun(current, candidate.Number, fmt.Sprintf("plan worktree: %v", err))
		return nil, r.saveAfterFailure(*current, candidate.Number)
	}
	run = findActiveRun(current, candidate.Number)
	run.Worktree = assignment.Path
	run.Branch = assignment.Branch
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	if err := r.Store.Save(*current); err != nil {
		return nil, fmt.Errorf("persist planned worktree for issue #%d: %w", candidate.Number, err)
	}
	if err := r.Worktrees.Prepare(ctx, assignment); err != nil {
		r.failRun(current, candidate.Number, fmt.Sprintf("prepare worktree: %v", err))
		return nil, r.saveAfterFailure(*current, candidate.Number)
	}
	run = findActiveRun(current, candidate.Number)
	transitionStatus(&run, scheduler.StatusWorktreeReady)
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	if err := r.Store.Save(*current); err != nil {
		return nil, fmt.Errorf("persist prepared worktree for issue #%d: %w", candidate.Number, err)
	}

	process, err := r.Workers.Start(workerCtx, worker.Request{
		Issue: candidate.Number, RunID: runID, Worktree: assignment.Path, SessionName: run.SessionName,
		SessionID: run.SessionID, SessionDir: run.SessionDir,
	})
	if err != nil {
		r.failRun(current, candidate.Number, fmt.Sprintf("start Pi worker: %v", err))
		return nil, r.saveAfterFailure(*current, candidate.Number)
	}
	identity, err := r.PIDIdentity(process.PID())
	if err != nil {
		_ = process.Abort()
		_ = process.Close()
		r.failRun(current, candidate.Number, fmt.Sprintf("record Pi worker identity: %v", err))
		return nil, r.saveAfterFailure(*current, candidate.Number)
	}
	run = findActiveRun(current, candidate.Number)
	transitionStatus(&run, scheduler.StatusRunning)
	run.PID = process.PID()
	run.ProcessIdentity = identity
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	if err := r.Store.Save(*current); err != nil {
		_ = process.Abort()
		_ = process.Close()
		r.failRun(current, candidate.Number, fmt.Sprintf("persist worker identity before release: %v", err))
		persistErr := r.Store.Save(*current)
		return nil, errors.Join(
			fmt.Errorf("persist worker for issue #%d before release: %w", candidate.Number, err),
			persistErr,
		)
	}
	if err := process.Release(); err != nil {
		_ = process.Abort()
		_ = process.Close()
		r.failRun(current, candidate.Number, fmt.Sprintf("release Pi worker: %v", err))
		return nil, r.saveAfterFailure(*current, candidate.Number)
	}
	r.logf("started issue #%d in %s (pid %d)", candidate.Number, assignment.Path, process.PID())
	return process, nil
}

func (r *Runner) handleWorkerCompletion(ctx context.Context, current *state.State, completion workerCompletion) error {
	run := findActiveRun(current, completion.issue)
	if run.Issue == 0 {
		return fmt.Errorf("worker completed for unleased issue #%d", completion.issue)
	}
	outcome, err := r.GitHub.Completion(ctx, r.Config.Repo, run.Issue, run.Branch)
	if err != nil {
		r.needsHuman(current, run.Issue, fmt.Sprintf("verify worker outcome: %v", err))
	} else {
		if completion.result.StreamErr != nil && outcome.Merged && outcome.IssueClosed {
			r.needsHuman(current, run.Issue, fmt.Sprintf("GitHub completion verified but Pi RPC stream was invalid; worktree retained: %v", completion.result.StreamErr))
			updated := findActiveRun(current, run.Issue)
			updated.PullRequest = outcome.PullRequest
			replaceRun(current, updated)
		} else {
			r.applyOutcome(ctx, current, run, outcome, completion.result.ExitCode == 0 && completion.result.Err == nil, false)
		}
		if updated := findActiveRun(current, run.Issue); updated.Status == scheduler.StatusFailed && completion.result.Err != nil {
			updated.Error = completion.result.Err.Error()
			updated.UpdatedAt = r.Now().UTC()
			replaceRun(current, updated)
		}
	}
	resultRun := findRun(current.Runs, run.RunID)
	if resultRun.LogPath == "" {
		resultRun.LogPath = completion.result.LogPath
	}
	if resultRun.StderrPath == "" {
		resultRun.StderrPath = completion.result.StderrPath
	}
	replaceRun(current, resultRun)
	if err := r.Store.Save(*current); err != nil {
		return fmt.Errorf("persist completion for issue #%d: %w", run.Issue, err)
	}
	return nil
}

func (r *Runner) reconcile(ctx context.Context, current *state.State, owned map[int]WorkerProcess) error {
	changed := false
	for _, lease := range append([]scheduler.Lease(nil), current.Leases...) {
		run := findRun(current.Runs, lease.RunID)
		if run.Issue == 0 || run.Issue != lease.Issue {
			return fmt.Errorf("active Lease %q has an invalid Run reference", lease.LeaseID)
		}
		if _, isOwned := owned[run.Issue]; isOwned {
			continue
		}
		switch run.Status {
		case scheduler.StatusMerged, scheduler.StatusFailed, scheduler.StatusNeedsHuman:
			continue
		case scheduler.StatusRunning:
			if run.PID > 0 && r.PIDAlive(run.PID) {
				identity, err := r.PIDIdentity(run.PID)
				if err != nil || identity != run.ProcessIdentity {
					detail := "identity changed"
					if err != nil {
						detail = err.Error()
					}
					r.needsHuman(current, run.Issue, fmt.Sprintf("recorded worker PID no longer matches its process identity: %s", detail))
					changed = true
					continue
				}
				if r.Now().Sub(run.StartedAt) > r.Config.MaxWorkerAge {
					r.needsHuman(current, run.Issue, "recorded worker exceeded the maximum age; verify process identity before retrying")
					changed = true
					continue
				}
				if run.WorkerMode == scheduler.WorkerModeRPC {
					r.needsHumanWithLiveWorker(current, run.Issue, "recovered live RPC Worker cannot restore its prompt and event channels; Worker retained for intervention")
					changed = true
					continue
				}
				if err := r.Workers.Release(run.RunID); err != nil {
					r.needsHuman(current, run.Issue, fmt.Sprintf("release recovered Pi worker: %v", err))
					changed = true
				}
				continue
			}
		case scheduler.StatusWaitingForMerge:
			// Always verify waiting runs.
		case scheduler.StatusClaimed:
			if run.Branch == "" {
				r.failRun(current, run.Issue, "runner stopped before planning the issue worktree")
			} else if r.Worktrees.Exists(worktree.Assignment{Path: run.Worktree, Branch: run.Branch}) {
				r.failRun(current, run.Issue, "runner stopped after creating the issue worktree; retained for diagnosis")
			} else {
				r.failRun(current, run.Issue, "runner stopped before creating the planned issue worktree")
			}
			changed = true
			continue
		default:
			// Worktree-ready runs cannot still be supervised after restart.
		}

		outcome, err := r.GitHub.Completion(ctx, r.Config.Repo, run.Issue, run.Branch)
		if err != nil {
			r.needsHuman(current, run.Issue, fmt.Sprintf("reconcile GitHub outcome: %v", err))
			changed = true
			continue
		}
		allowWaiting := run.Status == scheduler.StatusWaitingForMerge || run.Status == scheduler.StatusRunning
		r.applyOutcome(ctx, current, run, outcome, allowWaiting, true)
		changed = true
	}
	if changed {
		if err := r.Store.Save(*current); err != nil {
			return fmt.Errorf("persist reconciled state: %w", err)
		}
	}
	return nil
}

func (r *Runner) applyOutcome(ctx context.Context, current *state.State, run scheduler.Run, outcome ghadapter.CompletionOutcome, allowWaiting, cleanupMerged bool) {
	run.PullRequest = outcome.PullRequest
	run.UpdatedAt = r.Now().UTC()
	switch {
	case outcome.Merged && outcome.IssueClosed:
		if cleanupMerged {
			assignment := worktree.Assignment{Path: run.Worktree, Branch: run.Branch}
			if assignment.Path != "" && assignment.Branch != "" {
				if err := r.Worktrees.Cleanup(ctx, assignment); err != nil {
					transitionStatus(&run, scheduler.StatusNeedsHuman)
					run.Error = fmt.Sprintf("completion verified but worktree cleanup failed: %v", err)
					break
				}
			}
		}
		now := r.Now().UTC()
		transitionStatus(&run, scheduler.StatusMerged)
		run.CompletedAt = &now
		run.PID = 0
		run.Error = ""
		removeLease(current, run.RunID)
		r.logf("verified merged completion for issue #%d", run.Issue)
	case outcome.Merged && !outcome.IssueClosed:
		transitionStatus(&run, scheduler.StatusNeedsHuman)
		run.Error = "pull request merged but issue remains open"
	case outcome.PRFound && allowWaiting && outcome.AutoMergeArmed:
		transitionStatus(&run, scheduler.StatusWaitingForMerge)
		run.PID = 0
		run.Error = ""
	case outcome.PRFound:
		transitionStatus(&run, scheduler.StatusNeedsHuman)
		run.PID = 0
		run.Error = "worker stopped with an unmerged pull request"
	default:
		transitionStatus(&run, scheduler.StatusFailed)
		run.PID = 0
		run.Error = "worker stopped without creating a pull request"
	}
	replaceRun(current, run)
}

func (r *Runner) finalizeSettledWorker(ctx context.Context, current *state.State, runID string, closeErr error, settled bool) error {
	if !settled {
		return nil
	}
	run := findRun(current.Runs, runID)
	if run.RunID == "" {
		return fmt.Errorf("finalize unknown Run %q", runID)
	}
	if closeErr != nil {
		if run.Status == scheduler.StatusMerged || run.Status == scheduler.StatusWaitingForMerge {
			r.retainProvisionalCompletion(current, &run, fmt.Sprintf("Pi RPC stream or process-group shutdown failed after settlement: %v", closeErr))
			if err := r.Store.Save(*current); err != nil {
				return fmt.Errorf("persist fail-closed RPC shutdown for issue #%d: %w", run.Issue, err)
			}
		}
		return nil
	}
	if run.Status != scheduler.StatusMerged {
		return nil
	}
	assignment := worktree.Assignment{Path: run.Worktree, Branch: run.Branch}
	if assignment.Path == "" || assignment.Branch == "" {
		return nil
	}
	if err := r.Worktrees.Cleanup(ctx, assignment); err != nil {
		r.retainProvisionalCompletion(current, &run, fmt.Sprintf("completion verified but worktree cleanup failed: %v", err))
		if saveErr := r.Store.Save(*current); saveErr != nil {
			return errors.Join(fmt.Errorf("cleanup issue #%d worktree: %w", run.Issue, err), fmt.Errorf("persist retained completion: %w", saveErr))
		}
	}
	return nil
}

func (r *Runner) retainProvisionalCompletion(current *state.State, run *scheduler.Run, message string) {
	run.Status = scheduler.StatusNeedsHuman
	run.CompletedAt = nil
	run.PID = 0
	run.Error = message
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, *run)
	if findActiveRun(current, run.Issue).RunID == "" {
		current.Leases = append(current.Leases, scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID})
	}
	r.logf("issue #%d needs human attention: %s", run.Issue, message)
}

func (r *Runner) failRun(current *state.State, issue int, message string) {
	run := findActiveRun(current, issue)
	transitionStatus(&run, scheduler.StatusFailed)
	run.PID = 0
	run.Error = message
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	r.logf("issue #%d failed: %s", issue, message)
}

func (r *Runner) needsHuman(current *state.State, issue int, message string) {
	run := findActiveRun(current, issue)
	transitionStatus(&run, scheduler.StatusNeedsHuman)
	run.PID = 0
	run.Error = message
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	r.logf("issue #%d needs human attention: %s", issue, message)
}

func (r *Runner) needsHumanWithLiveWorker(current *state.State, issue int, message string) {
	run := findActiveRun(current, issue)
	transitionStatus(&run, scheduler.StatusNeedsHuman)
	run.Error = message
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	r.logf("issue #%d needs human attention: %s", issue, message)
}

func (r *Runner) saveAfterFailure(current state.State, issue int) error {
	if err := r.Store.Save(current); err != nil {
		return fmt.Errorf("persist failure for issue #%d: %w", issue, err)
	}
	return nil
}

func (r *Runner) shutdownOwned(
	cancelWorkers context.CancelFunc,
	current *state.State,
	local map[int]WorkerProcess,
	completions <-chan workerCompletion,
	reason string,
) error {
	cancelWorkers()
	issues := make([]int, 0, len(local))
	var shutdownErrors []error
	for issue, process := range local {
		issues = append(issues, issue)
		if err := process.Abort(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("abort issue #%d worker: %w", issue, err))
		}
	}
	for range issues {
		completion := <-completions
		if process := local[completion.issue]; process != nil {
			closed := process.Close()
			if closed.Err != nil && !errors.Is(closed.Err, context.Canceled) {
				shutdownErrors = append(shutdownErrors, fmt.Errorf("close issue #%d worker: %w", completion.issue, closed.Err))
			}
		}
		delete(local, completion.issue)
	}
	for _, issue := range issues {
		r.failRun(current, issue, reason)
	}
	if err := r.Store.Save(*current); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("persist interrupted runs: %w", err))
	}
	return errors.Join(shutdownErrors...)
}

func (r *Runner) logf(format string, args ...any) {
	fmt.Fprintf(r.Output, format+"\n", args...)
}

func transitionStatus(run *scheduler.Run, next scheduler.Status) {
	if !scheduler.CanTransition(run.Status, next) {
		previous := run.Status
		run.Status = scheduler.StatusNeedsHuman
		run.Error = fmt.Sprintf("invalid internal run transition %s -> %s", previous, next)
		return
	}
	run.Status = next
}

func findRun(runs []scheduler.Run, runID string) scheduler.Run {
	for _, run := range runs {
		if run.RunID == runID {
			return run
		}
	}
	return scheduler.Run{}
}

func findActiveRun(current *state.State, issue int) scheduler.Run {
	for _, lease := range current.Leases {
		if lease.Issue == issue {
			return findRun(current.Runs, lease.RunID)
		}
	}
	return scheduler.Run{}
}

func replaceRun(current *state.State, replacement scheduler.Run) {
	for index := range current.Runs {
		if current.Runs[index].RunID == replacement.RunID {
			current.Runs[index] = replacement
			return
		}
	}
}

func removeLease(current *state.State, runID string) {
	for index := range current.Leases {
		if current.Leases[index].RunID == runID {
			current.Leases = append(current.Leases[:index], current.Leases[index+1:]...)
			return
		}
	}
}

func unfinishedRunCount(current *state.State) int {
	count := 0
	for _, lease := range current.Leases {
		run := findRun(current.Runs, lease.RunID)
		if scheduler.RequiresLease(run.Status) {
			count++
		}
	}
	return count
}

func defaultRunID(issue int) string {
	return fmt.Sprintf("%s-%d", time.Now().UTC().Format("20060102T150405.000000000"), issue)
}

func pidIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid %d", pid)
	}
	command := exec.Command("ps", "-p", fmt.Sprint(pid), "-o", "lstart=")
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("inspect pid %d start time: %w", pid, err)
	}
	started := strings.TrimSpace(string(output))
	if started == "" {
		return "", fmt.Errorf("inspect pid %d start time: empty output", pid)
	}
	return fmt.Sprintf("%d:%s", pid, started), nil
}

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, os.ErrPermission)
}
