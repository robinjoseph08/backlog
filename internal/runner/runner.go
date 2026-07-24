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
	"sync"
	"sync/atomic"
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
	SuspensionTimeout   time.Duration
}

type GitHub interface {
	// Candidates returns a complete admission snapshot. When err is non-nil,
	// callers must not create Leases from any returned candidates.
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
	Suspend(context.Context, worker.ContinuationRequest) (worker.Continuation, error)
	Wait() worker.Result
	Close() worker.Result
	CloseWithForceContext(context.Context, func() error) worker.Result
	CloseContext(context.Context, func() error) worker.Result
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
	Signals   <-chan os.Signal

	Now               func() time.Time
	NewRunID          func(issue int) string
	PIDAlive          func(pid int) bool
	ProcessGroupAlive func(pid int) (bool, error)
	PIDIdentity       func(context.Context, int) (string, error)

	suspensionExit       atomic.Int32
	suspensionFailed     atomic.Bool
	forceStopRequested   atomic.Bool
	suspensionMu         sync.Mutex
	suspensionDeadline   time.Time
	suspensionCancel     context.CancelFunc
	suspensionEventReady chan struct{}
}

type workerCompletion struct {
	issue  int
	result worker.Result
}

type workerStart struct {
	process WorkerProcess
	err     error
}

// SignalExit requests the conventional shell exit status for suspension or
// force stopping initiated by a signal. Cause records any Run that had to fail
// closed without changing the signal-derived process status.
type SignalExit struct {
	Code  int
	Cause error
}

func (e *SignalExit) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("signal shutdown (%d): %v", e.Code, e.Cause)
	}
	return fmt.Sprintf("signal shutdown (%d)", e.Code)
}

func (e *SignalExit) Unwrap() error { return e.Cause }

// admissionGate serializes the in-memory Drain transition with the complete
// durable Lease write. Once stop returns, no later commit can call Store.Save.
type admissionGate struct {
	mu       sync.Mutex
	draining bool
}

func (g *admissionGate) commit(save func() error) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return false, nil
	}
	return true, save()
}

func (g *admissionGate) stop() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	first := !g.draining
	g.draining = true
	return first
}

func (g *admissionGate) stopped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.draining
}

type signalEvent struct {
	signal     os.Signal
	firstDrain bool
	forceStop  bool
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	operationCtx, cancelOperations := context.WithCancel(ctx)
	defer cancelOperations()
	stopWorkerCancellation := context.AfterFunc(ctx, cancelWorkers)
	defer func() {
		stopWorkerCancellation()
		cancelWorkers()
	}()
	admissionCtx, cancelAdmission := context.WithCancel(ctx)
	defer cancelAdmission()
	admission := &admissionGate{}
	r.suspensionEventReady = make(chan struct{})
	signalCtx, stopSignals := context.WithCancel(context.Background())
	defer stopSignals()
	signalEvents := r.observeSignals(signalCtx, admission, cancelAdmission, cancelOperations)
	draining := false

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
	if admission.stopped() {
		event := <-signalEvents
		draining = r.handleSignal(event, 0)
	} else if err := r.reconcile(admissionCtx, &current, nil); err != nil {
		switch {
		case admissionCtx.Err() != nil && ctx.Err() == nil:
			event := <-signalEvents
			draining = r.handleSignal(event, 0)
		case ctx.Err() != nil:
			return nil
		default:
			return err
		}
	}

	completions := make(chan workerCompletion, r.Config.MaxConcurrentIssues)
	localWorkers := make(map[int]WorkerProcess)
	poll := time.NewTicker(r.Config.PollInterval)
	defer poll.Stop()
	var candidateRetryTimer *time.Timer
	var candidateRetry <-chan time.Time
	defer func() {
		if candidateRetryTimer != nil {
			candidateRetryTimer.Stop()
		}
	}()

	for {
		select {
		case event := <-signalEvents:
			draining = r.handleSignal(event, len(localWorkers)) || draining
		default:
		}
		if code := int(r.suspensionExit.Load()); code != 0 {
			// requestSuspension changes the stage before publishing its event so
			// blocked operations are canceled promptly. Wait for publication and
			// report all queued stage changes before entering suspension.
			<-r.suspensionEventReady
			for {
				select {
				case event := <-signalEvents:
					r.handleSignal(event, len(localWorkers))
				default:
					return r.suspendOwned(&current, localWorkers, code)
				}
			}
		}
		if ctx.Err() != nil {
			return r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped; worker was terminated and its worktree was retained")
		}

		if !draining && candidateRetry == nil {
			candidates, err := r.GitHub.Candidates(admissionCtx, r.Config.Repo)
			if err != nil {
				if admissionCtx.Err() != nil && ctx.Err() == nil {
					select {
					case event := <-signalEvents:
						draining = r.handleSignal(event, len(localWorkers))
						continue
					case <-ctx.Done():
						continue
					}
				}
				if ctx.Err() != nil {
					continue
				}
				r.logf("candidate discovery failed; admission paused; retry due in %s: %v", r.Config.PollInterval, err)
				if candidateRetryTimer == nil {
					candidateRetryTimer = time.NewTimer(r.Config.PollInterval)
				} else {
					candidateRetryTimer.Reset(r.Config.PollInterval)
				}
				candidateRetry = candidateRetryTimer.C
			} else {
				plan := scheduler.Plan(scheduler.Snapshot{Candidates: candidates, Runs: current.Runs, Leases: current.Leases}, r.Config.MaxConcurrentIssues)
				startedWorker := false
				for _, candidate := range plan.Starts {
					process, startedDraining, err := r.startWhileObservingSignals(workerCtx, operationCtx, admission, &current, candidate, signalEvents, len(localWorkers))
					draining = startedDraining || draining
					if err != nil {
						shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a worker launch error; worktree retained")
						return errors.Join(err, shutdownErr)
					}
					if process != nil {
						localWorkers[candidate.Number] = process
						startedWorker = true
						go func(issue int, process WorkerProcess) {
							completions <- workerCompletion{issue: issue, result: process.Wait()}
						}(candidate.Number, process)
					}
					if draining {
						break
					}
				}

				if startedWorker {
					continue
				}
				if unfinishedRunCount(&current) == 0 && !r.Config.Watch {
					if admission.stopped() {
						if r.suspensionExit.Load() != 0 {
							continue
						}
						event := <-signalEvents
						draining = r.handleSignal(event, len(localWorkers)) || draining
						continue
					}
					return nil
				}
			}
		} else if draining && len(localWorkers) == 0 {
			r.logf("Drain complete: 0 Workers remaining; exiting successfully")
			return nil
		}

		select {
		case event := <-signalEvents:
			draining = r.handleSignal(event, len(localWorkers)) || draining
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
			startedDraining, err := r.handleWorkerCompletionWhileObservingSignals(operationCtx, &current, completion, signalEvents, len(localWorkers))
			draining = startedDraining || draining
			if err != nil && operationCtx.Err() != nil && r.suspensionExit.Load() != 0 {
				persisted, reloadErr := r.Store.Load()
				if reloadErr != nil {
					return errors.Join(err, fmt.Errorf("reload state after interrupted completion reconciliation: %w", reloadErr))
				}
				current = persisted
				continue
			}
			if err != nil {
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
				var closeDraining bool
				closed, closeDraining = r.closeSettledWhileObservingSignals(process, runID, signalEvents, len(localWorkers))
				draining = closeDraining || draining
			}
			if closedBeforeReconciliation || closed.GroupExited {
				delete(localWorkers, completion.issue)
			}
			if draining && len(localWorkers) > 0 {
				r.logf("Drain: %s remaining; next SIGINT will be recorded as a suspension request", workerSummary(len(localWorkers)))
			}
			if !closed.GroupExited && r.suspensionExit.Load() != 0 {
				continue
			}
			if closed.ForceStopped && r.suspensionExit.Load() != 0 {
				if err := r.finalizeForceStoppedSettledWorker(&current, runID, closed.Err); err != nil {
					r.suspensionFailed.Store(true)
					r.logf("Force stop: durable terminal outcome preserved, but post-stop cleanup failed: %v", err)
				}
				continue
			}
			if err := r.finalizeSettledWorker(ctx, &current, runID, closed.Err, completion.result.Settled); err != nil {
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after an RPC finalization error; worktree retained")
				return errors.Join(err, shutdownErr)
			}
		case <-candidateRetry:
			candidateRetry = nil
		case <-poll.C:
			if draining {
				continue
			}
			if err := r.reconcile(admissionCtx, &current, localWorkers); err != nil {
				if admissionCtx.Err() != nil && ctx.Err() == nil {
					event := <-signalEvents
					draining = r.handleSignal(event, len(localWorkers)) || draining
					continue
				}
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a reconciliation error; worktree retained")
				if ctx.Err() != nil {
					return shutdownErr
				}
				return errors.Join(err, shutdownErr)
			}
		case <-ctx.Done():
			continue
		}
	}
}

func (r *Runner) observeSignals(ctx context.Context, admission *admissionGate, cancelAdmission, cancelOperations context.CancelFunc) <-chan signalEvent {
	if r.Signals == nil {
		return nil
	}
	events := make(chan signalEvent, 16)
	go func() {
		for {
			select {
			case signal, ok := <-r.Signals:
				if !ok {
					return
				}
				first := admission.stop()
				if first {
					cancelAdmission()
				}
				startedSuspension := false
				forceStop := false
				if signal == syscall.SIGTERM {
					startedSuspension = r.requestSuspension(143, cancelOperations)
					forceStop = !startedSuspension
				} else if !first {
					startedSuspension = r.requestSuspension(130, cancelOperations)
					forceStop = !startedSuspension
				}
				select {
				case events <- signalEvent{signal: signal, firstDrain: first, forceStop: forceStop}:
					if startedSuspension {
						close(r.suspensionEventReady)
					}
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return events
}

func (r *Runner) requestSuspension(exitCode int32, cancelOperations context.CancelFunc) bool {
	r.suspensionMu.Lock()
	defer r.suspensionMu.Unlock()
	if r.suspensionExit.Load() != 0 {
		// A signal received while suspension is already active requests the
		// same force-stop path used when its shared deadline expires.
		r.forceStopRequested.Store(true)
		if r.suspensionCancel != nil {
			r.suspensionCancel()
		}
		return false
	}
	r.suspensionDeadline = time.Now().Add(r.Config.SuspensionTimeout)
	r.suspensionExit.Store(exitCode)
	cancelOperations()
	return true
}

func (r *Runner) suspensionContext() (context.Context, context.CancelFunc) {
	r.suspensionMu.Lock()
	defer r.suspensionMu.Unlock()
	deadline := r.suspensionDeadline
	if deadline.IsZero() {
		deadline = time.Now().Add(r.Config.SuspensionTimeout)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	r.suspensionCancel = cancel
	if r.forceStopRequested.Load() {
		cancel()
	}
	return ctx, cancel
}

func (r *Runner) handleSignal(event signalEvent, workers int) bool {
	if event.forceStop {
		return true
	}
	if event.signal == syscall.SIGTERM {
		r.logf("Suspension: SIGTERM accepted; %s share one %s deadline", workerSummary(workers), r.Config.SuspensionTimeout)
		return true
	}
	if event.firstDrain {
		r.logf("Drain: admission stopped; %s remaining; next SIGINT will be recorded as a suspension request", workerSummary(workers))
		return true
	}
	r.logf("Drain: additional %s recorded as a suspension request; %s remaining", event.signal, workerSummary(workers))
	return false
}

func (r *Runner) startWhileObservingSignals(workerCtx, operationCtx context.Context, admission *admissionGate, current *state.State, candidate scheduler.Candidate, signalEvents <-chan signalEvent, workers int) (WorkerProcess, bool, error) {
	admissionResult := make(chan bool, 1)
	result := make(chan workerStart, 1)
	go func() {
		process, err := r.start(workerCtx, operationCtx, admission, current, candidate, admissionResult)
		result <- workerStart{process: process, err: err}
	}()

	draining := false
	admitted := false
	admissionKnown := false
	for {
		select {
		case admitted = <-admissionResult:
			admissionKnown = true
		case event := <-signalEvents:
			if !admissionKnown {
				admitted = <-admissionResult
				admissionKnown = true
			}
			starting := 0
			if admitted {
				starting = 1
			}
			draining = r.handleSignal(event, workers+starting) || draining
		case started := <-result:
			return started.process, draining, started.err
		}
	}
}

func workerSummary(count int) string {
	if count == 1 {
		return "1 Worker"
	}
	return fmt.Sprintf("%d Workers", count)
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
	if r.Config.SuspensionTimeout <= 0 {
		r.Config.SuspensionTimeout = 60 * time.Second
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
	if r.ProcessGroupAlive == nil {
		r.ProcessGroupAlive = processGroupAlive
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

func (r *Runner) start(workerCtx, operationCtx context.Context, admission *admissionGate, current *state.State, candidate scheduler.Candidate, admissionResult chan<- bool) (WorkerProcess, error) {
	now := r.Now().UTC()
	runID := r.NewRunID(candidate.Number)
	run := scheduler.Run{
		Issue: candidate.Number, RunID: runID, Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModeRPC,
		SessionName: fmt.Sprintf("afk #%d", candidate.Number), SessionID: "backlog-" + runID,
		SessionDir: filepath.Join(r.Config.SessionsDir, runID), StartedAt: now, UpdatedAt: now,
	}
	admitted, err := admission.commit(func() error {
		next := *current
		next.Runs = append(append([]scheduler.Run(nil), current.Runs...), run)
		next.Leases = append(append([]scheduler.Lease(nil), current.Leases...), scheduler.Lease{LeaseID: runID, Issue: candidate.Number, RunID: runID})
		if err := r.Store.Save(next); err != nil {
			return err
		}
		*current = next
		return nil
	})
	if err != nil {
		admissionResult <- false
		return nil, fmt.Errorf("persist lease for issue #%d: %w", candidate.Number, err)
	}
	admissionResult <- admitted
	if !admitted {
		return nil, nil
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
	if err := r.Worktrees.Prepare(operationCtx, assignment); err != nil {
		if operationCtx.Err() != nil && r.suspensionExit.Load() != 0 {
			r.needsHuman(current, candidate.Number, "suspension interrupted Worker preparation before a continuation boundary could be established")
			r.suspensionFailed.Store(true)
		} else {
			r.failRun(current, candidate.Number, fmt.Sprintf("prepare worktree: %v", err))
		}
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
	identity, err := r.PIDIdentity(operationCtx, process.PID())
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

func (r *Runner) handleWorkerCompletionWhileObservingSignals(ctx context.Context, current *state.State, completion workerCompletion, signalEvents <-chan signalEvent, workers int) (bool, error) {
	result := make(chan error, 1)
	go func() { result <- r.handleWorkerCompletion(ctx, current, completion) }()
	draining := false
	for {
		select {
		case err := <-result:
			return draining, err
		case event := <-signalEvents:
			draining = r.handleSignal(event, workers) || draining
		}
	}
}

func (r *Runner) closeSettledWhileObservingSignals(process WorkerProcess, runID string, signalEvents <-chan signalEvent, workers int) (worker.Result, bool) {
	forceCtx, cancelForce := context.WithCancel(context.Background())
	defer cancelForce()
	closed := make(chan worker.Result, 1)
	go func() {
		closed <- process.CloseWithForceContext(forceCtx, r.authorizeSettledKill(runID, process))
	}()

	draining := false
	var deadlineTimer *time.Timer
	var deadline <-chan time.Time
	startDeadline := func() {
		if deadline != nil || r.suspensionExit.Load() == 0 {
			return
		}
		r.suspensionMu.Lock()
		suspensionDeadline := r.suspensionDeadline
		r.suspensionMu.Unlock()
		delay := time.Until(suspensionDeadline)
		if delay < 0 {
			delay = 0
		}
		deadlineTimer = time.NewTimer(delay)
		deadline = deadlineTimer.C
	}
	defer func() {
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
	}()
	startDeadline()
	if r.forceStopRequested.Load() {
		r.logf("Force stop: requesting force stop for %s after settlement; identity will be revalidated before signaling", workerSummary(workers))
		cancelForce()
	}

	for {
		select {
		case result := <-closed:
			return result, draining
		case event := <-signalEvents:
			draining = r.handleSignal(event, workers) || draining
			startDeadline()
			if event.forceStop {
				r.logf("Force stop: additional signal accepted; requesting force stop for %s after settlement; identity will be revalidated before signaling", workerSummary(workers))
				cancelForce()
			}
		case <-deadline:
			deadline = nil
			r.logf("Force stop: suspension deadline expired; requesting force stop for %s after settlement; identity will be revalidated before signaling", workerSummary(workers))
			cancelForce()
		}
	}
}

func (r *Runner) authorizeSettledKill(runID string, process WorkerProcess) func() error {
	return func() error {
		persisted, err := r.Store.Load()
		if err != nil {
			return fmt.Errorf("reload settled Run before force stopping Worker: %w", err)
		}
		run := findRun(persisted.Runs, runID)
		active := findActiveRun(&persisted, run.Issue)
		if run.RunID == "" || active.RunID != "" && active.RunID != runID || run.Status == scheduler.StatusRunning {
			return errors.New("durable settled Run state no longer authorizes Worker force stop")
		}
		if !r.PIDAlive(process.PID()) {
			return errors.New("settled Worker PID is not live before force stop")
		}
		identityCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		identity, err := r.PIDIdentity(identityCtx, process.PID())
		if err != nil {
			return fmt.Errorf("recheck settled Worker identity before force stop: %w", err)
		}
		if identity != run.ProcessIdentity {
			return fmt.Errorf("settled Worker identity changed from %q to %q before force stop", run.ProcessIdentity, identity)
		}
		return nil
	}
}

func (r *Runner) handleWorkerCompletion(ctx context.Context, current *state.State, completion workerCompletion) error {
	run := findActiveRun(current, completion.issue)
	if run.Issue == 0 {
		return fmt.Errorf("worker completed for unleased issue #%d", completion.issue)
	}
	outcome, err := r.GitHub.Completion(ctx, r.Config.Repo, run.Issue, run.Branch)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.needsHuman(current, run.Issue, fmt.Sprintf("verify worker outcome: %v", err))
	} else {
		if completion.result.StreamErr != nil && outcome.Merged && outcome.IssueClosed {
			r.needsHuman(current, run.Issue, fmt.Sprintf("GitHub completion verified but Pi RPC stream was invalid; worktree retained: %v", completion.result.StreamErr))
			updated := findActiveRun(current, run.Issue)
			updated.PullRequest = outcome.PullRequest
			replaceRun(current, updated)
		} else {
			if err := r.applyOutcome(ctx, current, run, outcome, completion.result.ExitCode == 0 && completion.result.Err == nil, false); err != nil {
				return err
			}
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
		recoverableContinuation := false
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
				identity, err := r.PIDIdentity(ctx, run.PID)
				if err != nil && ctx.Err() != nil {
					return ctx.Err()
				}
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
			if run.Continuation != nil {
				if run.PID <= 0 {
					r.needsHuman(current, run.Issue, "persisted continuation has no Worker PID for process-group absence verification")
					changed = true
					continue
				}
				groupAlive, err := r.ProcessGroupAlive(run.PID)
				if err != nil {
					r.needsHumanWithLiveWorker(current, run.Issue, fmt.Sprintf("verify recovered Worker process-group absence: %v", err))
					changed = true
					continue
				}
				if groupAlive {
					r.needsHumanWithLiveWorker(current, run.Issue, "recovered Worker leader is absent but its process group may still be live; Worker retained for intervention")
					changed = true
					continue
				}
				recoverableContinuation = true
			}
		case scheduler.StatusWaitingForMerge, scheduler.StatusSuspended:
			// Always verify waiting and suspended Runs without making retained
			// continuation state eligible for normal admission.
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
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.needsHuman(current, run.Issue, fmt.Sprintf("reconcile GitHub outcome: %v", err))
			changed = true
			continue
		}
		allowWaiting := run.Status == scheduler.StatusWaitingForMerge || run.Status == scheduler.StatusRunning || run.Status == scheduler.StatusSuspended
		if run.Status == scheduler.StatusSuspended && !outcome.Merged && !(outcome.PRFound && outcome.AutoMergeArmed) {
			continue
		}
		if recoverableContinuation && !outcome.Merged && !(outcome.PRFound && outcome.AutoMergeArmed) {
			transitionStatus(&run, scheduler.StatusSuspended)
			run.PID = 0
			run.ProcessIdentity = ""
			run.Error = ""
			run.UpdatedAt = r.Now().UTC()
			replaceRun(current, run)
			r.logf("recovered suspended Run for issue #%d from its persisted continuation", run.Issue)
			changed = true
			continue
		}
		if err := r.applyOutcome(ctx, current, run, outcome, allowWaiting, true); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		if err := r.Store.Save(*current); err != nil {
			return fmt.Errorf("persist reconciled state: %w", err)
		}
	}
	return nil
}

func (r *Runner) applyOutcome(ctx context.Context, current *state.State, run scheduler.Run, outcome ghadapter.CompletionOutcome, allowWaiting, cleanupMerged bool) error {
	run.PullRequest = outcome.PullRequest
	run.UpdatedAt = r.Now().UTC()
	switch {
	case outcome.Merged && outcome.IssueClosed:
		if cleanupMerged {
			assignment := worktree.Assignment{Path: run.Worktree, Branch: run.Branch}
			if assignment.Path != "" && assignment.Branch != "" {
				if err := r.Worktrees.Cleanup(ctx, assignment); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
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
	return nil
}

func (r *Runner) finalizeForceStoppedSettledWorker(current *state.State, runID string, closeErr error) error {
	if closeErr != nil {
		return fmt.Errorf("force close settled Worker: %w", closeErr)
	}
	run := findRun(current.Runs, runID)
	if run.RunID == "" {
		return fmt.Errorf("finalize unknown force-stopped Run %q", runID)
	}
	if run.Status != scheduler.StatusMerged {
		return nil
	}
	assignment := worktree.Assignment{Path: run.Worktree, Branch: run.Branch}
	if assignment.Path == "" || assignment.Branch == "" {
		return nil
	}
	r.suspensionMu.Lock()
	deadline := r.suspensionDeadline
	r.suspensionMu.Unlock()
	if deadline.IsZero() {
		deadline = time.Now().Add(r.Config.SuspensionTimeout)
	}
	cleanupCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if err := r.Worktrees.Cleanup(cleanupCtx, assignment); err != nil {
		return fmt.Errorf("cleanup force-stopped issue #%d worktree: %w", run.Issue, err)
	}
	return nil
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

type suspensionBoundaryResult struct {
	issue    int
	boundary worker.Continuation
	err      error
}

type suspensionGitHubResult struct {
	issue   int
	outcome ghadapter.CompletionOutcome
	err     error
}

type suspensionCloseResult struct {
	issue  int
	result worker.Result
}

func (r *Runner) authorizeSuspensionKill(runID string, process WorkerProcess) func() error {
	return func() error {
		persisted, err := r.Store.Load()
		if err != nil {
			return fmt.Errorf("reload Run before force stopping Worker: %w", err)
		}
		run := findRun(persisted.Runs, runID)
		active := findActiveRun(&persisted, run.Issue)
		if run.RunID == "" || active.RunID != runID || run.Status != scheduler.StatusRunning || run.PID != process.PID() {
			return errors.New("durable active Run state and Worker PID no longer match before force stop")
		}
		if !r.PIDAlive(run.PID) {
			return errors.New("recorded Worker PID is not live before force stop")
		}
		identityCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		identity, err := r.PIDIdentity(identityCtx, run.PID)
		if err != nil {
			return fmt.Errorf("recheck Worker identity before force stop: %w", err)
		}
		if identity != run.ProcessIdentity {
			return fmt.Errorf("Worker identity changed from %q to %q before force stop", run.ProcessIdentity, identity)
		}
		return nil
	}
}

func (r *Runner) suspendOwned(current *state.State, local map[int]WorkerProcess, exitCode int) error {
	if len(local) == 0 {
		if r.suspensionFailed.Load() {
			return &SignalExit{Code: exitCode, Cause: errors.New("suspension could not establish a continuation boundary for every admitted Run")}
		}
		r.logf("Suspension complete: 0 Workers remaining")
		return &SignalExit{Code: exitCode}
	}
	ctx, cancel := r.suspensionContext()
	defer cancel()
	r.logf("Suspension: establishing continuation boundaries for %s; one %s deadline; next SIGINT will force stop remaining verified Worker groups", workerSummary(len(local)), r.Config.SuspensionTimeout)

	workerCount := len(local)
	var forceStopRemaining atomic.Int64
	forceStopRemaining.Store(int64(workerCount))
	go func() {
		<-ctx.Done()
		remaining := workerSummary(int(forceStopRemaining.Load()))
		switch {
		case r.forceStopRequested.Load():
			r.logf("Force stop: additional signal accepted; requesting force stop for %s; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request", remaining)
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			r.logf("Force stop: suspension deadline expired; requesting force stop for %s; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request", remaining)
		}
	}()
	boundaries := make(chan suspensionBoundaryResult, workerCount)
	runIDs := make(map[int]string, workerCount)
	for issue, process := range local {
		run := findActiveRun(current, issue)
		runIDs[issue] = run.RunID
		go func(issue int, process WorkerProcess, run scheduler.Run) {
			if run.Status != scheduler.StatusRunning || run.PID != process.PID() || !r.PIDAlive(run.PID) {
				boundaries <- suspensionBoundaryResult{issue: issue, err: errors.New("Run state and live Worker PID no longer match")}
				return
			}
			identity, err := r.PIDIdentity(ctx, run.PID)
			if err != nil || identity != run.ProcessIdentity {
				if err == nil {
					err = fmt.Errorf("process identity changed from %q to %q", run.ProcessIdentity, identity)
				}
				boundaries <- suspensionBoundaryResult{issue: issue, err: fmt.Errorf("recheck Worker process identity: %w", err)}
				return
			}
			boundary, err := process.Suspend(ctx, worker.ContinuationRequest{
				SessionID: run.SessionID, SessionDir: run.SessionDir, Worktree: run.Worktree,
			})
			boundaries <- suspensionBoundaryResult{issue: issue, boundary: boundary, err: err}
		}(issue, process, run)
	}

	closeResults := make(chan suspensionCloseResult, workerCount)
	githubResults := make(chan suspensionGitHubResult, workerCount)
	failureReasons := make(map[int]string)
	reconciliationCount := 0
	clean := !r.suspensionFailed.Load()
	closeProcess := func(issue int, process WorkerProcess) {
		go func() {
			result := process.CloseContext(ctx, r.authorizeSuspensionKill(runIDs[issue], process))
			if result.GroupExited && !result.ForceStopped {
				forceStopRemaining.Add(-1)
			}
			closeResults <- suspensionCloseResult{issue: issue, result: result}
		}()
	}
	for completed := 0; completed < workerCount; completed++ {
		result := <-boundaries
		process := local[result.issue]
		run := findActiveRun(current, result.issue)
		if result.err != nil {
			clean = false
			failureReasons[result.issue] = fmt.Sprintf("establish verified continuation boundary: %v", result.err)
			closeProcess(result.issue, process)
			continue
		}

		now := r.Now().UTC()
		run.Continuation = &scheduler.ContinuationBoundary{
			SessionID: result.boundary.SessionID, SessionFile: result.boundary.SessionFile,
			Worktree: result.boundary.Worktree, LeafID: result.boundary.LeafID,
			EntryCount: result.boundary.EntryCount, SHA256: result.boundary.SHA256, VerifiedAt: now,
		}
		if run.LogPath == "" {
			run.LogPath = result.boundary.LogPath
		}
		if run.StderrPath == "" {
			run.StderrPath = result.boundary.StderrPath
		}
		run.UpdatedAt = now
		replaceRun(current, run)
		// This write is the continuation marker. RPC input remains open until it
		// succeeds, preserving the evidence needed by later recovery logic.
		if err := r.Store.Save(*current); err != nil {
			clean = false
			failureReasons[result.issue] = fmt.Sprintf("persist continuation marker: %v", err)
			run.Continuation = nil
			replaceRun(current, run)
			closeProcess(result.issue, process)
			continue
		}

		reconciliationCount++
		go func(issue int, run scheduler.Run, process WorkerProcess) {
			outcome, err := r.GitHub.Completion(ctx, r.Config.Repo, run.Issue, run.Branch)
			closeProcess(issue, process)
			githubResults <- suspensionGitHubResult{issue: issue, outcome: outcome, err: err}
		}(result.issue, run, process)
	}

	verifiedOutcomes := make(map[int]ghadapter.CompletionOutcome, reconciliationCount)
	for completed := 0; completed < reconciliationCount; completed++ {
		result := <-githubResults
		if result.err != nil {
			clean = false
			failureReasons[result.issue] = fmt.Sprintf("reconcile GitHub before suspension: %v", result.err)
		} else if result.outcome.Merged || (result.outcome.PRFound && result.outcome.AutoMergeArmed) {
			verifiedOutcomes[result.issue] = result.outcome
		}
	}

	var persistenceErrors []error
	for completed := 0; completed < workerCount; completed++ {
		closed := <-closeResults
		if closed.result.GroupExited {
			delete(local, closed.issue)
		}

		// Reload after CloseContext's immediate pre-signal check. A concurrently
		// persisted terminal outcome is authoritative and must not be replaced by
		// stale in-memory suspension state.
		persisted, err := r.Store.Load()
		if err != nil {
			clean = false
			persistenceErrors = append(persistenceErrors, fmt.Errorf("reload issue #%d after Worker close: %w", closed.issue, err))
			r.logf("Suspension: %s remaining", workerSummary(len(local)))
			continue
		}
		*current = persisted
		run := findRun(current.Runs, runIDs[closed.issue])
		if run.RunID == "" {
			clean = false
			persistenceErrors = append(persistenceErrors, fmt.Errorf("reload issue #%d after Worker close: Run %q disappeared", closed.issue, runIDs[closed.issue]))
			r.logf("Suspension: %s remaining", workerSummary(len(local)))
			continue
		}

		if run.Status != scheduler.StatusRunning {
			// Merged, waiting-for-merge, cleanly suspended, and other durable
			// outcomes win over force escalation. Authorization already refused
			// to signal a process for a non-running Run.
			if !closed.result.GroupExited || closed.result.Err != nil {
				clean = false
			}
			r.logf("Suspension: %s remaining", workerSummary(len(local)))
			continue
		}

		if closed.result.GroupExited && closed.result.Err != nil {
			clean = false
			reason := fmt.Sprintf("close RPC Worker after continuation verification: %v", closed.result.Err)
			if previous := failureReasons[closed.issue]; previous != "" {
				reason = previous + "; " + reason
			}
			failureReasons[closed.issue] = reason
		}
		if !closed.result.GroupExited {
			clean = false
			message := "Worker process-group exit was not verified before force escalation"
			if closed.result.Err != nil {
				message += ": " + closed.result.Err.Error()
			}
			run.PID = local[closed.issue].PID()
			run.Status = scheduler.StatusNeedsHuman
			run.CompletedAt = nil
			run.Error = message
			run.UpdatedAt = r.Now().UTC()
			replaceRun(current, run)
		} else {
			if outcome, exists := verifiedOutcomes[closed.issue]; exists && failureReasons[closed.issue] == "" {
				if err := r.applyOutcome(ctx, current, run, outcome, true, false); err != nil {
					clean = false
					failureReasons[closed.issue] = fmt.Sprintf("apply GitHub outcome after Worker exit: %v", err)
				}
				run = findRun(current.Runs, run.RunID)
			}
			run.PID = 0
			run.ProcessIdentity = ""
			run.UpdatedAt = r.Now().UTC()
			if reason := failureReasons[closed.issue]; reason != "" {
				run.Status = scheduler.StatusNeedsHuman
				run.CompletedAt = nil
				run.Error = reason
			} else if run.Status == scheduler.StatusRunning && run.Continuation == nil {
				clean = false
				run.Status = scheduler.StatusNeedsHuman
				run.CompletedAt = nil
				run.Error = "Worker exited without a persisted continuation marker"
			} else if run.Status == scheduler.StatusRunning {
				transitionStatus(&run, scheduler.StatusSuspended)
				run.Error = ""
			}
			replaceRun(current, run)
		}
		if closed.result.GroupExited && run.Status == scheduler.StatusMerged {
			// Force escalation cancels the shared suspension context. Cleanup gets
			// a fresh bounded context and completes before merged state becomes
			// durable, so cancellation cannot overwrite a terminal outcome.
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), r.Config.SuspensionTimeout)
			err := r.finalizeSettledWorker(cleanupCtx, current, run.RunID, nil, true)
			cancelCleanup()
			if err != nil {
				clean = false
				persistenceErrors = append(persistenceErrors, err)
			} else if findRun(current.Runs, run.RunID).Status != scheduler.StatusMerged {
				clean = false
			}
		}
		if err := r.Store.Save(*current); err != nil {
			clean = false
			persistenceErrors = append(persistenceErrors, fmt.Errorf("persist suspended issue #%d: %w", closed.issue, err))
		}
		r.logf("Suspension: %s remaining", workerSummary(len(local)))
	}

	var cause error
	if len(persistenceErrors) != 0 {
		cause = errors.Join(persistenceErrors...)
	} else if !clean && len(local) != 0 {
		cause = fmt.Errorf("suspension could not verify or stop %s; one or more Runs require human verification", workerSummary(len(local)))
	} else if !clean {
		cause = errors.New("suspension stopped all Workers but one or more Runs require human verification")
	}
	if cause == nil {
		r.logf("Suspension complete: 0 Workers remaining")
	} else {
		r.logf("Suspension incomplete: %v", cause)
	}
	return &SignalExit{Code: exitCode, Cause: cause}
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

func pidIdentity(ctx context.Context, pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid %d", pid)
	}
	// #nosec G204: pid is validated as positive and passed as a single argument.
	command := exec.CommandContext(ctx, "ps", "-p", fmt.Sprint(pid), "-o", "lstart=")
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

func processGroupAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, fmt.Errorf("invalid process-group leader pid %d", pid)
	}
	err := syscall.Kill(-pid, syscall.Signal(0))
	switch {
	case err == nil, errors.Is(err, syscall.EPERM):
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	default:
		return false, err
	}
}
