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
	IssueState(context.Context, string, int) (ghadapter.IssueState, error)
}

type Store interface {
	Load() (state.State, error)
	Save(state.State) error
}

type Worktrees interface {
	Plan(int, string) (worktree.Assignment, error)
	Prepare(context.Context, worktree.Assignment) error
	Verify(context.Context, worktree.Assignment) error
	Cleanup(context.Context, worktree.Assignment) error
	Exists(worktree.Assignment) bool
}

type WorkerProcess interface {
	PID() int
	LogPaths() (string, string)
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

	// OnOperationalEvent receives typed Admission and shutdown lifecycle events.
	// Delivery is asynchronous, ordered, and isolated from callback panics so
	// presentation cannot block Runner control paths or compatible plain output.
	OnOperationalEvent func(OperationalEvent)

	// SuppressOperationalEventOutput lets a structured presentation avoid the
	// compatible line-oriented copies. Plain mode leaves this false.
	SuppressOperationalEventOutput bool

	// FinalSummary presents the aggregate state immediately before natural
	// one-shot exhaustion. Signal shutdown and watch mode do not call it.
	FinalSummary func(state.State) error

	Now               func() time.Time
	NewRunID          func(issue int) string
	PIDAlive          func(pid int) bool
	ProcessGroupAlive func(pid int) (bool, error)
	PIDIdentity       func(context.Context, int) (string, error)
	Lstat             func(string) (os.FileInfo, error)

	suspensionExit       atomic.Int32
	suspensionFailed     atomic.Bool
	forceStopRequested   atomic.Bool
	forceStopping        atomic.Bool
	suspensionMu         sync.Mutex
	suspensionDeadline   time.Time
	suspensionCancel     context.CancelFunc
	suspensionEventReady chan struct{}

	operationalEventOnce     sync.Once
	operationalEventMu       sync.Mutex
	operationalEventWake     chan struct{}
	operationalEventStop     chan struct{}
	operationalEventDone     chan struct{}
	operationalEventStopping bool
	operationalEvents        []OperationalEvent
}

const operationalAdmissionFailureLimit = 20

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

// InterventionRequired reports the aggregate result of natural one-shot
// exhaustion when retained Leases cannot advance autonomously. Diagnostics
// for the individual Run outcomes remain persisted in state.
type InterventionRequired struct {
	Count int
}

func (e *InterventionRequired) Error() string {
	noun := "Runs"
	if e.Count == 1 {
		noun = "Run"
	}
	return fmt.Sprintf("natural exhaustion left %d Intervention-required %s", e.Count, noun)
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

// whileActive linearizes Admission reporting with Drain acceptance. The
// registration callback must not perform output: it runs under the gate only
// long enough to order the report before a concurrent Drain transition.
func (g *admissionGate) whileActive(register func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return false
	}
	register()
	return true
}

// finishNatural linearizes one-shot exhaustion with Drain acceptance. A
// signal accepted first prevents natural-exhaustion presentation and policy;
// otherwise the complete final decision precedes any later Drain request.
func (g *admissionGate) finishNatural(finish func() error) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return false, nil
	}
	return true, finish()
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
	defer r.stopOperationalEventDelivery()
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
	var drainOperationalErr error

	current, err := r.Store.Load()
	if err != nil {
		return fmt.Errorf("load runner state: %w", err)
	}
	if err := r.initializeState(&current); err != nil {
		return err
	}
	if admission.stopped() {
		event := <-signalEvents
		draining = r.handleSignal(event, 0)
	} else {
		if err := r.reconcile(admissionCtx, &current, nil); err != nil {
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
		if err := r.Store.Save(current); err != nil {
			return fmt.Errorf("initialize reconciled runner state: %w", err)
		}
	}

	completions := make(chan workerCompletion, r.Config.MaxConcurrentIssues)
	localWorkers := make(map[int]WorkerProcess)
	poll := time.NewTicker(r.Config.PollInterval)
	defer poll.Stop()
	var candidateRetryTimer *time.Timer
	var candidateRetry <-chan time.Time
	candidateDiscoveryFailures := 0
	var candidateDiscoveryFirstFailure time.Time
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

		resumedWorker := false
		for !draining && persistedWorkerCount(&current) < r.Config.MaxConcurrentIssues {
			run := nextSuspendedRun(&current)
			if run.RunID == "" {
				break
			}
			process, startedDraining, err := r.resumeWhileObservingSignals(workerCtx, operationCtx, &current, run, signalEvents, len(localWorkers))
			draining = startedDraining || draining
			if err != nil && operationCtx.Err() != nil && r.suspensionExit.Load() != 0 {
				persisted, reloadErr := r.Store.Load()
				if reloadErr != nil {
					return errors.Join(err, fmt.Errorf("reload state after interrupted Worker Resume: %w", reloadErr))
				}
				current = persisted
				break
			}
			if err != nil {
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a Worker Resume error; worktree retained")
				return errors.Join(err, shutdownErr)
			}
			if process != nil {
				localWorkers[run.Issue] = process
				resumedWorker = true
				go func(issue int, process WorkerProcess) {
					completions <- workerCompletion{issue: issue, result: process.Wait()}
				}(run.Issue, process)
			}
		}
		if resumedWorker {
			continue
		}

		if !draining && candidateRetry == nil {
			candidates, err := r.GitHub.Candidates(admissionCtx, r.Config.Repo)
			if admissionCtx.Err() != nil {
				if ctx.Err() != nil {
					continue
				}
				select {
				case event := <-signalEvents:
					draining = r.handleSignal(event, len(localWorkers))
					continue
				case <-ctx.Done():
					continue
				}
			}
			if err != nil {
				candidateDiscoveryFailures++
				if candidateRetryTimer == nil {
					candidateRetryTimer = time.NewTimer(r.Config.PollInterval)
				} else {
					candidateRetryTimer.Reset(r.Config.PollInterval)
				}
				candidateRetry = candidateRetryTimer.C
				occurredAt := r.Now().UTC()
				if candidateDiscoveryFailures == 1 {
					candidateDiscoveryFirstFailure = occurredAt
				}
				operation := CandidateDiscoverySnapshot
				cause := conciseDiscoveryCause(err)
				var issue *int
				var discoveryErr *ghadapter.CandidateDiscoveryError
				if errors.As(err, &discoveryErr) {
					if discoveryErr.Cause != "" {
						cause = discoveryErr.Cause
					}
					switch discoveryErr.Operation {
					case ghadapter.CandidateDiscoveryList:
						operation = CandidateDiscoveryList
					case ghadapter.CandidateDiscoveryInspect:
						operation = CandidateDiscoveryInspect
						if discoveryErr.Issue > 0 {
							identity := discoveryErr.Issue
							issue = &identity
						}
					}
				}
				r.emitWhileAdmissionActive(admission, CandidateDiscoveryFailed{
					Operation: operation, Issue: issue, Err: err, Cause: cause,
					FirstFailureAt: candidateDiscoveryFirstFailure, OccurredAt: occurredAt, RetryAt: occurredAt.Add(r.Config.PollInterval),
					ConsecutiveFailures: candidateDiscoveryFailures,
				})
			} else {
				if candidateDiscoveryFailures > 0 {
					failures := candidateDiscoveryFailures
					if !r.emitWhileAdmissionActive(admission, CandidateDiscoveryRecovered{OccurredAt: r.Now().UTC(), Failures: failures}) {
						continue
					}
					candidateDiscoveryFailures = 0
					candidateDiscoveryFirstFailure = time.Time{}
				}
				plan := scheduler.Plan(scheduler.Snapshot{Candidates: candidates, Runs: current.Runs, Leases: current.Leases}, r.Config.MaxConcurrentIssues)
				startedWorker := false
				for _, candidate := range plan.Starts {
					process, startedDraining, err := r.startWhileObservingSignals(workerCtx, operationCtx, admission, &current, candidate, signalEvents, len(localWorkers))
					draining = startedDraining || draining
					if err != nil && operationCtx.Err() != nil && r.suspensionExit.Load() != 0 {
						persisted, reloadErr := r.Store.Load()
						if reloadErr != nil {
							return errors.Join(err, fmt.Errorf("reload state after interrupted Worker launch: %w", reloadErr))
						}
						current = persisted
						break
					}
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
				if activeRunCount(&current) == 0 && !r.Config.Watch {
					finished, exhaustionErr := admission.finishNatural(func() error {
						if r.FinalSummary != nil {
							if err := r.FinalSummary(current); err != nil {
								return fmt.Errorf("print final aggregate summary: %w", err)
							}
						}
						if count := interventionRequiredCount(&current); count > 0 {
							return &InterventionRequired{Count: count}
						}
						return nil
					})
					if finished {
						return exhaustionErr
					}
					if r.suspensionExit.Load() != 0 {
						continue
					}
					event := <-signalEvents
					draining = r.handleSignal(event, len(localWorkers)) || draining
					continue
				}
			}
		} else if draining && len(localWorkers) == 0 {
			if drainOperationalErr != nil {
				if unverified := persistedWorkerCount(&current); unverified > 0 {
					r.shutdownEvent(ShutdownStageDrainIncomplete, "retaining Workers with unverified liveness", 0, NextInterruptNone, "Drain incomplete: 0 supervised Workers remaining; %s retained with unverified liveness", workerSummary(unverified))
				} else {
					r.shutdownEvent(ShutdownStageDrainComplete, "exiting after an operational failure", 0, NextInterruptNone, "Drain complete: 0 Workers remaining; exiting after an operational failure")
				}
				return drainOperationalErr
			}
			r.shutdownEvent(ShutdownStageDrainComplete, "exiting successfully", 0, NextInterruptNone, "Drain complete: 0 Workers remaining; exiting successfully")
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
			completedRun := findActiveRun(&current, completion.issue)
			runID := completedRun.RunID
			closedBeforeReconciliation := false
			var closed worker.Result
			var workerControlErr error
			if !completion.result.Settled {
				if completion.result.StreamErr == nil {
					completion.result.StreamErr = errors.New("Pi RPC worker ended without agent_settled")
					completion.result.Err = errors.Join(completion.result.Err, completion.result.StreamErr)
				}
				if err := process.Abort(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					workerControlErr = errors.Join(workerControlErr, fmt.Errorf("stop invalid Pi RPC worker: %w", err))
					completion.result.Err = errors.Join(completion.result.Err, workerControlErr)
				}
				closed = process.Close()
				workerControlErr = errors.Join(workerControlErr, closed.ControlErr)
				if !closed.GroupExited {
					workerControlErr = errors.Join(workerControlErr, fmt.Errorf("invalid Pi RPC Worker process-group exit was not verified for issue #%d", completion.issue))
				}
				closedBeforeReconciliation = true
				completion.result.ExitCode = closed.ExitCode
				completion.result.Err = errors.Join(completion.result.Err, closed.Err)
			}
			if closedBeforeReconciliation && workerLogIsClosed(closed) {
				markWorkerLogClosed(&current, runID)
			}
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
				abortErr := process.Abort()
				closedAfterError := process.Close()
				delete(localWorkers, completion.issue)
				persisted, reloadErr := r.Store.Load()
				var recoverySaveErr error
				if reloadErr == nil {
					current = persisted
					if draining {
						message := fmt.Sprintf("completion reconciliation failed during Drain: %v", err)
						if closedAfterError.GroupExited {
							r.needsHuman(&current, completion.issue, message)
						} else {
							r.needsHumanWithLiveWorker(&current, completion.issue, message)
						}
						if workerLogIsClosed(closedAfterError) {
							markWorkerLogClosed(&current, runID)
						}
						recoverySaveErr = r.Store.Save(current)
					}
				}
				var unverifiedExitErr error
				if !closedAfterError.GroupExited {
					unverifiedExitErr = fmt.Errorf("completion-error Worker process-group exit was not verified for issue #%d", completion.issue)
				}
				completionErr := errors.Join(err, abortErr, closedAfterError.Err, unverifiedExitErr, reloadErr, recoverySaveErr)
				if draining {
					drainOperationalErr = errors.Join(drainOperationalErr, completionErr)
					if len(localWorkers) > 0 {
						r.shutdownEvent(ShutdownStageDraining, "waiting for Owned Workers", len(localWorkers), NextInterruptSuspends, "Drain: %s remaining; next SIGINT will be recorded as a suspension request", workerSummary(len(localWorkers)))
					}
					continue
				}
				shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after a completion error; worktree retained")
				return errors.Join(completionErr, shutdownErr)
			}
			if draining && workerControlErr != nil {
				var retainErr error
				if !closed.GroupExited {
					retainErr = r.retainUnverifiedWorker(&current, completedRun, workerControlErr.Error(), closed)
				}
				drainOperationalErr = errors.Join(drainOperationalErr, workerControlErr, retainErr)
			}
			// Reconciliation and its durable state write happen while the idle RPC
			// process is still alive. EOF is sent only after that write succeeds.
			if !closedBeforeReconciliation {
				var closeDraining bool
				closed, closeDraining = r.closeSettledWhileObservingSignals(process, runID, signalEvents, len(localWorkers))
				draining = closeDraining || draining
				if workerLogIsClosed(closed) {
					markWorkerLogClosed(&current, runID)
					if err := r.Store.Save(current); err != nil {
						markerErr := fmt.Errorf("persist closed Worker log for Run %s: %w", runID, err)
						var completedShutdownErr error
						groupExited := closed.GroupExited
						if !groupExited {
							if abortErr := process.Abort(); abortErr != nil && !errors.Is(abortErr, os.ErrProcessDone) {
								completedShutdownErr = errors.Join(completedShutdownErr, fmt.Errorf("abort completed issue #%d Worker: %w", completion.issue, abortErr))
							}
							reclosed := process.Close()
							groupExited = reclosed.GroupExited
							if reclosed.Err != nil && !errors.Is(reclosed.Err, context.Canceled) {
								completedShutdownErr = errors.Join(completedShutdownErr, fmt.Errorf("close completed issue #%d Worker: %w", completion.issue, reclosed.Err))
							}
							if !groupExited {
								completedShutdownErr = errors.Join(completedShutdownErr, fmt.Errorf("completed issue #%d Worker process group did not exit", completion.issue))
							}
						}
						completedFailure := errors.Join(closed.Err, completedShutdownErr)
						if completion.result.Settled {
							if completedFailure == nil {
								completedShutdownErr = errors.Join(completedShutdownErr, r.finalizeSettledWithinLifecycle(ctx, &current, runID, nil, true))
							} else {
								completed := findRun(current.Runs, runID)
								if completed.Status == scheduler.StatusMerged || completed.Status == scheduler.StatusWaitingForMerge {
									r.retainProvisionalCompletion(&current, &completed, fmt.Sprintf("Pi RPC stream or process-group shutdown failed after settlement: %v", completedFailure))
									if !groupExited {
										completed.PID = completedRun.PID
										completed.ProcessIdentity = completedRun.ProcessIdentity
										replaceRun(&current, completed)
									}
								}
							}
						}
						delete(localWorkers, completion.issue)
						if draining {
							drainOperationalErr = errors.Join(drainOperationalErr, markerErr, completedShutdownErr)
							if len(localWorkers) > 0 {
								r.shutdownEvent(ShutdownStageDraining, "waiting for Owned Workers", len(localWorkers), NextInterruptSuspends, "Drain: %s remaining; next SIGINT will be recorded as a suspension request", workerSummary(len(localWorkers)))
							}
							continue
						}
						shutdownErr := r.shutdownOwned(cancelWorkers, &current, localWorkers, completions, "scheduler stopped after Worker log closure persistence failed; worktree retained")
						return errors.Join(markerErr, completedShutdownErr, shutdownErr)
					}
				}
			}
			if !closedBeforeReconciliation && draining {
				settledControlErr := closed.ControlErr
				if !closed.GroupExited && r.suspensionExit.Load() == 0 {
					settledControlErr = errors.Join(settledControlErr, fmt.Errorf("settled Worker process-group exit was not verified for issue #%d", completion.issue))
					settledControlErr = errors.Join(settledControlErr, r.retainUnverifiedWorker(&current, completedRun, settledControlErr.Error(), closed))
				}
				if settledControlErr != nil {
					drainOperationalErr = errors.Join(drainOperationalErr, settledControlErr)
				}
			}
			if closedBeforeReconciliation || closed.GroupExited || draining && r.suspensionExit.Load() == 0 {
				delete(localWorkers, completion.issue)
			}
			if draining && len(localWorkers) > 0 {
				r.shutdownEvent(ShutdownStageDraining, "waiting for Owned Workers", len(localWorkers), NextInterruptSuspends, "Drain: %s remaining; next SIGINT will be recorded as a suspension request", workerSummary(len(localWorkers)))
			}
			if !closed.GroupExited && r.suspensionExit.Load() != 0 {
				continue
			}
			if closed.ForceStopped && r.suspensionExit.Load() != 0 {
				if err := r.finalizeForceStoppedSettledWorker(&current, runID, closed.Err); err != nil {
					r.suspensionFailed.Store(true)
					r.shutdownEvent(ShutdownStageForceStopping, "preserving terminal outcome after cleanup failure", len(localWorkers), NextInterruptRepeatsForceStop, "Force stop: durable terminal outcome preserved, but post-stop cleanup failed: %v", err)
				}
				continue
			}
			if err := r.finalizeSettledWithinLifecycle(ctx, &current, runID, closed.Err, completion.result.Settled); err != nil {
				if r.suspensionExit.Load() != 0 {
					r.suspensionFailed.Store(true)
					r.suspensionProgressEvent("stopping merged cleanup at the shared deadline", len(localWorkers), "Suspension: merged cleanup stopped at the shared deadline: %v", err)
					continue
				}
				if draining {
					drainOperationalErr = errors.Join(drainOperationalErr, err)
					continue
				}
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
		r.forceStopping.Store(true)
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
		r.shutdownEvent(ShutdownStageSuspending, "suspension requested by SIGTERM", workers, NextInterruptForceStops, "Suspension: SIGTERM accepted; %s share one %s deadline", workerSummary(workers), r.Config.SuspensionTimeout)
		return true
	}
	if event.firstDrain {
		r.shutdownEvent(ShutdownStageDraining, "admission stopped", workers, NextInterruptSuspends, "Drain: admission stopped; %s remaining; next SIGINT will be recorded as a suspension request", workerSummary(workers))
		return true
	}
	r.shutdownEvent(ShutdownStageSuspending, "suspension requested by additional interrupt", workers, NextInterruptForceStops, "Drain: additional %s recorded as a suspension request; %s remaining", event.signal, workerSummary(workers))
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

func (r *Runner) resumeWhileObservingSignals(workerCtx, operationCtx context.Context, current *state.State, run scheduler.Run, signalEvents <-chan signalEvent, workers int) (WorkerProcess, bool, error) {
	result := make(chan workerStart, 1)
	go func() {
		process, err := r.resume(workerCtx, operationCtx, current, run)
		result <- workerStart{process: process, err: err}
	}()
	draining := false
	for {
		select {
		case event := <-signalEvents:
			draining = r.handleSignal(event, workers+1) || draining
		case resumed := <-result:
			return resumed.process, draining, resumed.err
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
	if r.ProcessGroupAlive == nil {
		r.ProcessGroupAlive = processGroupAlive
	}
	if r.Lstat == nil {
		r.Lstat = os.Lstat
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
	// The CLI holds the exclusive repository scheduling lock before Run starts.
	// Any persisted open marker therefore belongs to a previous Runner whose
	// in-process Worker log writer is already closed.
	for index := range current.Runs {
		current.Runs[index].WorkerLogOpen = false
	}
	return nil
}

func (r *Runner) start(workerCtx, operationCtx context.Context, admission *admissionGate, current *state.State, candidate scheduler.Candidate, admissionResult chan<- bool) (WorkerProcess, error) {
	now := r.Now().UTC()
	runID := r.NewRunID(candidate.Number)
	run := scheduler.Run{
		Issue: candidate.Number, IssueTitle: candidate.Title, IssueURL: candidate.URL,
		RunID: runID, Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModeRPC,
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
	logPath, stderrPath := process.LogPaths()
	if logPath == "" || stderrPath == "" {
		return nil, r.failAfterWorkerStart(current, candidate.Number, process, "record Pi worker logs: Worker omitted a JSONL or standard-error log identity")
	}
	run = findActiveRun(current, candidate.Number)
	run.LogPath = logPath
	run.StderrPath = stderrPath
	run.WorkerLogOpen = true
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	if err := r.Store.Save(*current); err != nil {
		failureErr := r.failAfterWorkerStart(current, candidate.Number, process, fmt.Sprintf("persist Pi worker logs before identity inspection: %v", err))
		return nil, errors.Join(
			fmt.Errorf("persist worker logs for issue #%d before identity inspection: %w", candidate.Number, err),
			failureErr,
		)
	}
	identity, err := r.PIDIdentity(operationCtx, process.PID())
	if err != nil {
		return nil, r.failAfterWorkerStart(current, candidate.Number, process, fmt.Sprintf("record Pi worker identity: %v", err))
	}
	run = findActiveRun(current, candidate.Number)
	transitionStatus(&run, scheduler.StatusRunning)
	run.PID = process.PID()
	run.ProcessIdentity = identity
	run.WorkerStartedAt = r.Now().UTC()
	run.UpdatedAt = run.WorkerStartedAt
	replaceRun(current, run)
	if err := r.Store.Save(*current); err != nil {
		failureErr := r.failAfterWorkerStart(current, candidate.Number, process, fmt.Sprintf("persist worker identity before release: %v", err))
		return nil, errors.Join(
			fmt.Errorf("persist worker for issue #%d before release: %w", candidate.Number, err),
			failureErr,
		)
	}
	if err := process.Release(); err != nil {
		return nil, r.failAfterWorkerStart(current, candidate.Number, process, fmt.Sprintf("release Pi worker: %v", err))
	}
	r.logf("started issue #%d in %s (pid %d)", candidate.Number, assignment.Path, process.PID())
	return process, nil
}

func (r *Runner) resume(workerCtx, operationCtx context.Context, current *state.State, original scheduler.Run) (WorkerProcess, error) {
	run := findActiveRun(current, original.Issue)
	if run.RunID != original.RunID || run.Status != scheduler.StatusSuspended {
		return nil, r.rejectResume(current, original.Issue, "durable Run or Lease identity changed before Resume")
	}
	outcome, err := r.GitHub.Completion(operationCtx, r.Config.Repo, run.Issue, run.Branch)
	if err != nil {
		if operationCtx.Err() != nil {
			return nil, operationCtx.Err()
		}
		return nil, r.rejectResume(current, run.Issue, fmt.Sprintf("verify GitHub Completion before Resume: %v", err))
	}
	if outcome.Merged || (outcome.PRFound && outcome.AutoMergeArmed) {
		if err := r.applyOutcome(operationCtx, current, run, outcome, true, true); err != nil {
			return nil, err
		}
		if err := r.Store.Save(*current); err != nil {
			return nil, fmt.Errorf("persist GitHub outcome before Resume for issue #%d: %w", run.Issue, err)
		}
		return nil, nil
	}
	if outcome.PRFound {
		return nil, r.rejectResume(current, run.Issue, "GitHub state changed before Resume: an unmerged pull request is not armed for auto-merge")
	}

	issue, err := r.GitHub.IssueState(operationCtx, r.Config.Repo, run.Issue)
	if err != nil {
		if operationCtx.Err() != nil {
			return nil, operationCtx.Err()
		}
		return nil, r.rejectResume(current, run.Issue, fmt.Sprintf("verify issue state and managed labels before Resume: %v", err))
	}
	if err := verifyResumeLabels(issue); err != nil {
		return nil, r.rejectResume(current, run.Issue, err.Error())
	}
	if run.PID != 0 || run.ProcessIdentity != "" {
		return nil, r.rejectResume(current, run.Issue, "old Worker absence is not proven before Resume")
	}
	if err := verifyContinuationArtifacts(run); err != nil {
		return nil, r.rejectResume(current, run.Issue, err.Error())
	}
	assignment := worktree.Assignment{Path: run.Worktree, Branch: run.Branch}
	if err := r.Worktrees.Verify(operationCtx, assignment); err != nil {
		if operationCtx.Err() != nil {
			return nil, operationCtx.Err()
		}
		return nil, r.rejectResume(current, run.Issue, fmt.Sprintf("verify retained branch and worktree before Resume: %v", err))
	}
	boundary := run.Continuation
	run.ResumePending = true
	run.UpdatedAt = r.Now().UTC()
	replaceRun(current, run)
	if err := r.Store.Save(*current); err != nil {
		return nil, fmt.Errorf("persist pending replacement Worker before Resume for issue #%d: %w", run.Issue, err)
	}
	process, err := r.Workers.Start(workerCtx, worker.Request{
		Issue: run.Issue, RunID: run.RunID, Worktree: run.Worktree, SessionName: run.SessionName,
		SessionID: run.SessionID, SessionDir: run.SessionDir, SessionFile: boundary.SessionFile, Resume: true,
	})
	if err != nil {
		run = findActiveRun(current, run.Issue)
		run.ResumePending = false
		replaceRun(current, run)
		return nil, r.rejectResume(current, run.Issue, fmt.Sprintf("start replacement Pi Worker: %v", err))
	}
	identity, err := r.PIDIdentity(operationCtx, process.PID())
	if err != nil {
		_ = process.Abort()
		closed := process.Close()
		if operationCtx.Err() != nil {
			r.suspensionFailed.Store(true)
		}
		run = findActiveRun(current, run.Issue)
		run.ResumePending = false
		if !closed.GroupExited {
			run.PID = process.PID()
			run.ProcessIdentity = ""
		}
		replaceRun(current, run)
		return nil, r.rejectResume(current, run.Issue, fmt.Sprintf("record replacement Pi Worker identity: %v; close: %v", err, closed.Err))
	}
	now := r.Now().UTC()
	transitionStatus(&run, scheduler.StatusRunning)
	run.ResumePending = false
	run.PID = process.PID()
	run.ProcessIdentity = identity
	run.WorkerStartedAt = now
	run.UpdatedAt = now
	run.Error = ""
	replaceRun(current, run)
	if err := r.Store.Save(*current); err != nil {
		abortErr := process.Abort()
		closed := process.Close()
		run = findActiveRun(current, run.Issue)
		if closed.GroupExited {
			run.PID = 0
			run.ProcessIdentity = ""
			replaceRun(current, run)
		}
		failureErr := r.rejectResume(current, run.Issue, fmt.Sprintf("persist replacement Worker identity before release: %v; abort: %v; close: %v", err, abortErr, closed.Err))
		return nil, errors.Join(
			fmt.Errorf("persist replacement Worker identity before release for issue #%d: %w", run.Issue, err),
			failureErr,
		)
	}
	if err := verifyContinuationArtifacts(run); err != nil {
		_ = process.Abort()
		closed := process.Close()
		run = findActiveRun(current, run.Issue)
		if closed.GroupExited {
			run.PID = 0
			run.ProcessIdentity = ""
			replaceRun(current, run)
		}
		return nil, r.rejectResume(current, run.Issue, fmt.Sprintf("reverify Pi continuation before replacement Worker release: %v; close: %v", err, closed.Err))
	}
	if err := process.Release(); err != nil {
		_ = process.Abort()
		closed := process.Close()
		run = findActiveRun(current, run.Issue)
		if closed.GroupExited {
			run.PID = 0
			run.ProcessIdentity = ""
			replaceRun(current, run)
		}
		return nil, r.rejectResume(current, run.Issue, fmt.Sprintf("release replacement Pi Worker: %v; close: %v", err, closed.Err))
	}
	r.logf("resumed issue #%d in %s (pid %d, Run %s)", run.Issue, run.Worktree, process.PID(), run.RunID)
	return process, nil
}

func verifyContinuationArtifacts(run scheduler.Run) error {
	if run.WorkerMode != scheduler.WorkerModeRPC {
		return errors.New("legacy print-mode Run cannot Resume automatically")
	}
	if run.SessionID == "" || run.SessionDir == "" || run.Branch == "" || run.Worktree == "" || run.Continuation == nil {
		return errors.New("continuation artifacts are incomplete before Resume")
	}
	boundary := run.Continuation
	if boundary.VerifiedAt.IsZero() {
		return errors.New("continuation verification timestamp is missing before Resume")
	}
	if err := worker.VerifyContinuation(worker.ContinuationRequest{
		SessionID: run.SessionID, SessionDir: run.SessionDir, Worktree: run.Worktree,
	}, worker.Continuation{
		SessionID: boundary.SessionID, SessionFile: boundary.SessionFile, Worktree: boundary.Worktree,
		LeafID: boundary.LeafID, EntryCount: boundary.EntryCount, SHA256: boundary.SHA256,
	}); err != nil {
		return fmt.Errorf("verify Pi continuation before Resume: %w", err)
	}
	return nil
}

func verifyResumeLabels(issue ghadapter.IssueState) error {
	if !issue.Open {
		return errors.New("issue is not open before Resume")
	}
	labels := make(map[string]struct{}, len(issue.Labels))
	for _, label := range issue.Labels {
		labels[label] = struct{}{}
	}
	for _, human := range []string{"needs-triage", "needs-info", "ready-for-human", "wontfix"} {
		if _, exists := labels[human]; exists {
			return fmt.Errorf("human workflow label %q blocks Resume", human)
		}
	}
	if _, exists := labels["in-progress"]; !exists {
		return errors.New("managed label in-progress is missing before Resume")
	}
	if _, exists := labels["ready-for-agent"]; exists {
		return errors.New("managed label ready-for-agent is unexpectedly present before Resume")
	}
	return nil
}

func (r *Runner) rejectResume(current *state.State, issue int, message string) error {
	run := findActiveRun(current, issue)
	if run.PID != 0 || run.ProcessIdentity != "" {
		r.needsHumanWithLiveWorker(current, issue, message)
	} else {
		r.needsHuman(current, issue, message)
	}
	if err := r.Store.Save(*current); err != nil {
		return fmt.Errorf("persist unsafe Resume for issue #%d: %w", issue, err)
	}
	return nil
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
		r.shutdownEvent(ShutdownStageForceStopping, "requesting force stop after settlement", workers, NextInterruptRepeatsForceStop, "Force stop: requesting force stop for %s after settlement; identity will be revalidated before signaling", workerSummary(workers))
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
				r.shutdownEvent(ShutdownStageForceStopping, "requesting force stop after settlement", workers, NextInterruptRepeatsForceStop, "Force stop: additional signal accepted; requesting force stop for %s after settlement; identity will be revalidated before signaling", workerSummary(workers))
				cancelForce()
			}
		case <-deadline:
			deadline = nil
			r.shutdownEvent(ShutdownStageForceStopping, "requesting force stop after suspension deadline", workers, NextInterruptRepeatsForceStop, "Force stop: suspension deadline expired; requesting force stop for %s after settlement; identity will be revalidated before signaling", workerSummary(workers))
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
	for _, historical := range append([]scheduler.Run(nil), current.Runs...) {
		if !historical.CleanupPending {
			continue
		}
		assignment := worktree.Assignment{Path: historical.Worktree, Branch: historical.Branch}
		if assignment.Path != "" && assignment.Branch != "" {
			if err := r.verifyAndCleanupWorktree(ctx, assignment); err != nil {
				return fmt.Errorf("retry pending Completion cleanup for issue #%d: %w", historical.Issue, err)
			}
		}
		historical.CleanupPending = false
		historical.Error = ""
		historical.UpdatedAt = r.Now().UTC()
		replaceRun(current, historical)
		changed = true
		r.logf("completed pending worktree cleanup for merged issue #%d", historical.Issue)
	}
	for _, lease := range append([]scheduler.Lease(nil), current.Leases...) {
		run := findRun(current.Runs, lease.RunID)
		if run.Issue == 0 || run.Issue != lease.Issue {
			return fmt.Errorf("active Lease %q has an invalid Run reference", lease.LeaseID)
		}
		if _, isOwned := owned[run.Issue]; isOwned {
			continue
		}
		switch run.Status {
		case scheduler.StatusMerged, scheduler.StatusFailed, scheduler.StatusNeedsHuman, scheduler.StatusResetting, scheduler.StatusReset:
			continue
		case scheduler.StatusRunning:
			if run.PID > 0 && r.PIDAlive(run.PID) {
				identity, err := r.PIDIdentity(ctx, run.PID)
				if err != nil && ctx.Err() != nil {
					return ctx.Err()
				}
				if err != nil {
					r.needsHumanWithLiveWorker(current, run.Issue, fmt.Sprintf("recorded worker process identity is uncertain: %v", err))
					changed = true
					continue
				}
				if identity != run.ProcessIdentity {
					r.needsHuman(current, run.Issue, "recorded worker PID no longer matches its process identity: identity changed")
					changed = true
					continue
				}
				workerStartedAt := run.WorkerStartedAt
				if workerStartedAt.IsZero() {
					workerStartedAt = run.StartedAt
				}
				if r.Now().Sub(workerStartedAt) > r.Config.MaxWorkerAge {
					r.needsHumanWithLiveWorker(current, run.Issue, "recorded worker exceeded the maximum age; verify process identity before Reset")
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
					r.needsHumanWithLiveWorker(current, run.Issue, fmt.Sprintf("recovered Worker process-group absence is uncertain: %v", err))
					changed = true
					continue
				}
				if groupAlive {
					r.needsHumanWithLiveWorker(current, run.Issue, "recovered Worker leader is absent but its process group may still be live; Worker retained for intervention")
					changed = true
					continue
				}
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
		if run.ResumePending {
			r.needsHuman(current, run.Issue, "replacement Worker launch was interrupted before its process identity became durable")
			changed = true
			continue
		}
		if run.Status == scheduler.StatusSuspended && (run.PID != 0 || run.ProcessIdentity != "") {
			r.needsHumanWithLiveWorker(current, run.Issue, "old Worker absence is not proven before applying GitHub Completion")
			changed = true
			continue
		}
		allowWaiting := run.Status == scheduler.StatusWaitingForMerge || run.Status == scheduler.StatusRunning || run.Status == scheduler.StatusSuspended
		if run.Status == scheduler.StatusRunning && run.WorkerMode == scheduler.WorkerModeRPC && (run.SessionID == "" || run.SessionDir == "") && !outcome.Merged && !outcome.PRFound {
			r.needsHuman(current, run.Issue, "recovered RPC Run has missing durable session identity or storage")
			changed = true
			continue
		}
		recoverableMarker := run.Status == scheduler.StatusRunning && run.WorkerMode == scheduler.WorkerModeRPC && run.Continuation != nil
		if (run.Status == scheduler.StatusSuspended || recoverableMarker) && !outcome.Merged && !outcome.PRFound {
			if run.Status == scheduler.StatusSuspended {
				if err := verifyContinuationArtifacts(run); err != nil {
					r.needsHuman(current, run.Issue, err.Error())
					changed = true
					continue
				}
			}
			if recoverableMarker {
				if err := verifyContinuationArtifacts(run); err != nil {
					r.needsHuman(current, run.Issue, fmt.Sprintf("verify persisted Pi continuation after interrupted suspension: %v", err))
					changed = true
					continue
				}
				run.PID = 0
				run.ProcessIdentity = ""
				r.transitionToSuspended(&run)
				replaceRun(current, run)
				changed = true
			}
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
				if err := r.verifyAndCleanupWorktree(ctx, assignment); err != nil {
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

func (r *Runner) finalizeSettledWithinLifecycle(ctx context.Context, current *state.State, runID string, closeErr error, settled bool) error {
	cleanupCtx, cancel := r.suspensionAwareCleanupContext(ctx)
	defer cancel()
	return r.finalizeSettledWorker(cleanupCtx, current, runID, closeErr, settled)
}

func (r *Runner) suspensionAwareCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	if r.suspensionExit.Load() != 0 {
		return r.suspensionCleanupContext()
	}
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-r.suspensionEventReady:
			r.suspensionMu.Lock()
			deadline := r.suspensionDeadline
			r.suspensionMu.Unlock()
			if delay := time.Until(deadline); delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return
				}
			}
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (r *Runner) suspensionCleanupContext() (context.Context, context.CancelFunc) {
	r.suspensionMu.Lock()
	deadline := r.suspensionDeadline
	r.suspensionMu.Unlock()
	if deadline.IsZero() {
		deadline = time.Now().Add(r.Config.SuspensionTimeout)
	}
	return context.WithDeadline(context.Background(), deadline)
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
	cleanupCtx, cancel := r.suspensionCleanupContext()
	defer cancel()
	var cleanupErr error
	if run.Continuation != nil {
		cleanupErr = r.verifyAndCleanupWorktree(cleanupCtx, assignment)
	} else {
		cleanupErr = r.Worktrees.Cleanup(cleanupCtx, assignment)
	}
	if cleanupErr != nil {
		return fmt.Errorf("cleanup force-stopped issue #%d worktree: %w", run.Issue, cleanupErr)
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
	var cleanupErr error
	if run.Continuation != nil {
		cleanupErr = r.verifyAndCleanupWorktree(ctx, assignment)
	} else {
		cleanupErr = r.Worktrees.Cleanup(ctx, assignment)
	}
	if cleanupErr != nil {
		if ctx.Err() != nil {
			run.CleanupPending = true
			run.Error = fmt.Sprintf("completion verified; worktree cleanup remains pending after lifecycle deadline: %v", ctx.Err())
			run.UpdatedAt = r.Now().UTC()
			replaceRun(current, run)
			saveErr := r.Store.Save(*current)
			return errors.Join(fmt.Errorf("cleanup issue #%d worktree within lifecycle deadline: %w", run.Issue, ctx.Err()), saveErr)
		}
		r.retainProvisionalCompletion(current, &run, fmt.Sprintf("completion verified but worktree cleanup failed: %v", cleanupErr))
		if saveErr := r.Store.Save(*current); saveErr != nil {
			return errors.Join(fmt.Errorf("cleanup issue #%d worktree: %w", run.Issue, cleanupErr), fmt.Errorf("persist retained completion: %w", saveErr))
		}
	}
	return nil
}

func (r *Runner) verifyAndCleanupWorktree(ctx context.Context, assignment worktree.Assignment) error {
	lstat := r.Lstat
	if lstat == nil {
		lstat = os.Lstat
	}
	if _, err := lstat(assignment.Path); err == nil {
		if err := r.Worktrees.Verify(ctx, assignment); err != nil {
			return fmt.Errorf("verify retained worktree before cleanup: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect retained worktree before cleanup: %w", err)
	}
	return r.Worktrees.Cleanup(ctx, assignment)
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

func (r *Runner) retainUnverifiedWorker(current *state.State, original scheduler.Run, message string, closed worker.Result) error {
	run := findRun(current.Runs, original.RunID)
	if run.RunID == "" {
		return fmt.Errorf("retain unknown unverified Worker Run %q", original.RunID)
	}
	run.Status = scheduler.StatusNeedsHuman
	run.CompletedAt = nil
	run.PID = original.PID
	run.ProcessIdentity = original.ProcessIdentity
	run.SuspendingAt = nil
	run.Error = message
	run.UpdatedAt = r.Now().UTC()
	if workerLogIsClosed(closed) {
		run.WorkerLogOpen = false
	}
	replaceRun(current, run)
	if findActiveRun(current, run.Issue).RunID == "" {
		current.Leases = append(current.Leases, scheduler.Lease{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID})
	}
	r.logf("issue #%d needs human attention: %s", run.Issue, message)
	if err := r.Store.Save(*current); err != nil {
		return fmt.Errorf("persist unverified Worker for issue #%d: %w", run.Issue, err)
	}
	return nil
}

func (r *Runner) failAfterWorkerStart(current *state.State, issue int, process WorkerProcess, message string) error {
	abortErr := process.Abort()
	closed := process.Close()
	run := findActiveRun(current, issue)
	if workerLogIsClosed(closed) {
		markWorkerLogClosed(current, run.RunID)
	}
	if closed.GroupExited {
		r.failRun(current, issue, message)
		return r.saveAfterFailure(*current, issue)
	}

	run = findActiveRun(current, issue)
	run.PID = process.PID()
	replaceRun(current, run)
	cleanupErr := errors.Join(
		errors.New("Worker process-group exit was not verified after startup failed"),
		abortErr,
		closed.Err,
	)
	r.needsHumanWithLiveWorker(current, issue, fmt.Sprintf("%s; stop Worker after startup failure: %v", message, cleanupErr))
	return errors.Join(
		fmt.Errorf("stop Worker for issue #%d after startup failure: %w", issue, cleanupErr),
		r.saveAfterFailure(*current, issue),
	)
}

func workerLogIsClosed(result worker.Result) bool {
	return result.LogClosed || result.GroupExited
}

func markWorkerLogClosed(current *state.State, runID string) {
	run := findRun(current.Runs, runID)
	if run.RunID == "" {
		return
	}
	run.WorkerLogOpen = false
	replaceRun(current, run)
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
			cause := errors.New("suspension could not establish a continuation boundary for every admitted Run")
			r.shutdownEvent(ShutdownStageSuspensionIncomplete, "exiting with incomplete suspension", 0, NextInterruptNone, "Suspension incomplete: %v", cause)
			return &SignalExit{Code: exitCode, Cause: cause}
		}
		r.shutdownEvent(ShutdownStageSuspensionComplete, "exiting after suspension", 0, NextInterruptNone, "Suspension complete: 0 Workers remaining")
		return &SignalExit{Code: exitCode}
	}
	ctx, cancel := r.suspensionContext()
	defer cancel()
	r.shutdownEvent(ShutdownStageSuspending, "establishing continuation boundaries", len(local), NextInterruptForceStops, "Suspension: establishing continuation boundaries for %s; one %s deadline; next SIGINT will force stop remaining verified Worker groups", workerSummary(len(local)), r.Config.SuspensionTimeout)

	suspendingAt := r.Now().UTC()
	for issue := range local {
		run := findActiveRun(current, issue)
		if run.Status != scheduler.StatusRunning {
			continue
		}
		run.SuspendingAt = &suspendingAt
		run.UpdatedAt = suspendingAt
		replaceRun(current, run)
	}
	var suspendingPersistenceErr error
	if err := r.Store.Save(*current); err != nil {
		suspendingPersistenceErr = fmt.Errorf("persist suspending Runs: %w", err)
	}

	workerCount := len(local)
	var forceStopRemaining atomic.Int64
	forceStopRemaining.Store(int64(workerCount))
	var reportForceStopOnce sync.Once
	reportForceStop := func() {
		if ctx.Err() == nil {
			return
		}
		reportForceStopOnce.Do(func() {
			r.forceStopping.Store(true)
			remainingWorkers := int(forceStopRemaining.Load())
			remaining := workerSummary(remainingWorkers)
			if r.forceStopRequested.Load() {
				r.shutdownEvent(ShutdownStageForceStopping, "requesting force stop", remainingWorkers, NextInterruptRepeatsForceStop, "Force stop: additional signal accepted; requesting force stop for %s; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request", remaining)
				return
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				r.shutdownEvent(ShutdownStageForceStopping, "requesting force stop after suspension deadline", remainingWorkers, NextInterruptRepeatsForceStop, "Force stop: suspension deadline expired; requesting force stop for %s; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request", remaining)
			}
		})
	}
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
			if result.GroupExited && !result.ForceStopped && ctx.Err() == nil {
				forceStopRemaining.Add(-1)
			}
			closeResults <- suspensionCloseResult{issue: issue, result: result}
		}()
	}
	for completed := 0; completed < workerCount; completed++ {
		result := <-boundaries
		reportForceStop()
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
		reportForceStop()
		if result.err != nil {
			clean = false
			failureReasons[result.issue] = fmt.Sprintf("reconcile GitHub before suspension: %v", result.err)
		} else if result.outcome.Merged || (result.outcome.PRFound && result.outcome.AutoMergeArmed) {
			verifiedOutcomes[result.issue] = result.outcome
		}
	}

	var persistenceErrors []error
	if suspendingPersistenceErr != nil {
		clean = false
		persistenceErrors = append(persistenceErrors, suspendingPersistenceErr)
	}
	for completed := 0; completed < workerCount; completed++ {
		closed := <-closeResults
		reportForceStop()
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
			r.suspensionProgressEvent("waiting for suspension persistence", len(local), "Suspension: %s remaining", workerSummary(len(local)))
			continue
		}
		*current = persisted
		run := findRun(current.Runs, runIDs[closed.issue])
		if run.RunID == "" {
			clean = false
			persistenceErrors = append(persistenceErrors, fmt.Errorf("reload issue #%d after Worker close: Run %q disappeared", closed.issue, runIDs[closed.issue]))
			r.suspensionProgressEvent("waiting for suspension persistence", len(local), "Suspension: %s remaining", workerSummary(len(local)))
			continue
		}

		if run.Status != scheduler.StatusRunning {
			// Merged, waiting-for-merge, cleanly suspended, and other durable
			// outcomes win over force escalation. Authorization already refused
			// to signal a process for a non-running Run.
			if !closed.result.GroupExited || closed.result.Err != nil {
				clean = false
			}
			run.SuspendingAt = nil
			if workerLogIsClosed(closed.result) {
				run.WorkerLogOpen = false
			}
			replaceRun(current, run)
			if err := r.Store.Save(*current); err != nil {
				clean = false
				persistenceErrors = append(persistenceErrors, fmt.Errorf("persist terminal suspension outcome for issue #%d: %w", closed.issue, err))
			}
			r.suspensionProgressEvent("waiting for suspension completion", len(local), "Suspension: %s remaining", workerSummary(len(local)))
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
			run.SuspendingAt = nil
			run.UpdatedAt = r.Now().UTC()
			replaceRun(current, run)
		} else {
			if workerLogIsClosed(closed.result) {
				run.WorkerLogOpen = false
			}
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
				r.transitionToSuspended(&run)
			}
			run.SuspendingAt = nil
			replaceRun(current, run)
		}
		if closed.result.GroupExited && run.Status == scheduler.StatusMerged {
			// Force escalation cancels Worker operations, but merged cleanup still
			// uses the original absolute suspension deadline. Every Run therefore
			// remains inside one wall-clock bound without allowing cancellation to
			// overwrite a verified terminal outcome.
			cleanupCtx, cancelCleanup := r.suspensionCleanupContext()
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
		r.suspensionProgressEvent("waiting for suspension completion", len(local), "Suspension: %s remaining", workerSummary(len(local)))
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
		r.shutdownEvent(ShutdownStageSuspensionComplete, "exiting after suspension", 0, NextInterruptNone, "Suspension complete: 0 Workers remaining")
	} else {
		r.shutdownEvent(ShutdownStageSuspensionIncomplete, "exiting with incomplete suspension", len(local), NextInterruptNone, "Suspension incomplete: %v", cause)
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
			run := findActiveRun(current, completion.issue)
			if workerLogIsClosed(closed) {
				markWorkerLogClosed(current, run.RunID)
			}
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

func (r *Runner) emit(event OperationalEvent) {
	if r.OnOperationalEvent != nil {
		r.enqueueOperationalEvent(event)
	}
	r.writeOperationalEvent(event)
}

// emitWhileAdmissionActive orders an Admission report with Drain without
// holding the Admission gate across compatible plain-output backpressure.
func (r *Runner) emitWhileAdmissionActive(admission *admissionGate, event OperationalEvent) bool {
	active := admission.whileActive(func() {
		if r.OnOperationalEvent != nil {
			r.enqueueOperationalEvent(event)
		}
	})
	if !active {
		return false
	}
	r.writeOperationalEvent(event)
	return true
}

func (r *Runner) writeOperationalEvent(event OperationalEvent) {
	if r.SuppressOperationalEventOutput {
		return
	}
	if message := FormatOperationalEvent(event); message != "" {
		r.logf("%s", message)
	}
}

func conciseDiscoveryCause(err error) string {
	cause := err
	for errors.Unwrap(cause) != nil {
		cause = errors.Unwrap(cause)
	}
	if cause == nil {
		return "unknown error"
	}
	return cause.Error()
}

func (r *Runner) enqueueOperationalEvent(event OperationalEvent) {
	r.operationalEventOnce.Do(func() {
		r.operationalEventWake = make(chan struct{}, 1)
		r.operationalEventStop = make(chan struct{})
		r.operationalEventDone = make(chan struct{})
		go r.deliverOperationalEvents(r.OnOperationalEvent)
	})
	r.operationalEventMu.Lock()
	r.operationalEvents = append(r.operationalEvents, event)
	for operationalAdmissionFailureCount(r.operationalEvents) > operationalAdmissionFailureLimit {
		r.operationalEvents = removeOperationalEvent(r.operationalEvents, oldestOperationalAdmissionFailure(r.operationalEvents))
	}
	r.operationalEventMu.Unlock()
	select {
	case r.operationalEventWake <- struct{}{}:
	default:
	}
}

func operationalAdmissionFailureCount(events []OperationalEvent) int {
	count := 0
	for _, event := range events {
		if _, ok := event.(CandidateDiscoveryFailed); ok {
			count++
		}
	}
	return count
}

func oldestOperationalAdmissionFailure(events []OperationalEvent) int {
	for index, event := range events {
		if _, ok := event.(CandidateDiscoveryFailed); ok {
			return index
		}
	}
	return 0
}

func removeOperationalEvent(events []OperationalEvent, index int) []OperationalEvent {
	copy(events[index:], events[index+1:])
	events[len(events)-1] = nil
	return events[:len(events)-1]
}

func (r *Runner) deliverOperationalEvents(deliver func(OperationalEvent)) {
	defer close(r.operationalEventDone)
	for {
		select {
		case <-r.operationalEventWake:
		case <-r.operationalEventStop:
		}
		for {
			r.operationalEventMu.Lock()
			if len(r.operationalEvents) == 0 {
				stopping := r.operationalEventStopping
				r.operationalEventMu.Unlock()
				if stopping {
					return
				}
				break
			}
			event := r.operationalEvents[0]
			r.operationalEvents = r.operationalEvents[1:]
			r.operationalEventMu.Unlock()
			invokeOperationalEvent(deliver, event)
		}
	}
}

func (r *Runner) stopOperationalEventDelivery() {
	r.operationalEventMu.Lock()
	defer r.operationalEventMu.Unlock()
	if r.operationalEventStop == nil || r.operationalEventStopping {
		return
	}
	r.operationalEventStopping = true
	close(r.operationalEventStop)
}

// WaitForOperationalEventDelivery waits for callbacks queued by Run to finish.
// Run itself never waits, so callback latency remains isolated from Runner
// control paths. Presentation adapters can call this after Run returns before
// tearing down callback-owned state.
func (r *Runner) WaitForOperationalEventDelivery() {
	r.operationalEventMu.Lock()
	done := r.operationalEventDone
	r.operationalEventMu.Unlock()
	if done != nil {
		<-done
	}
}

func invokeOperationalEvent(deliver func(OperationalEvent), event OperationalEvent) {
	defer func() { _ = recover() }()
	deliver(event)
}

func (r *Runner) shutdownEvent(stage ShutdownStage, action string, workers int, next NextInterruptBehavior, format string, args ...any) {
	if stage == ShutdownStageForceStopping {
		r.forceStopping.Store(true)
	}
	r.emit(ShutdownEvent{
		Stage: stage, Action: action, RemainingWorkers: workers, NextInterrupt: next,
		Message: fmt.Sprintf(format, args...),
	})
}

func (r *Runner) suspensionProgressEvent(action string, workers int, format string, args ...any) {
	stage, next := ShutdownStageSuspending, NextInterruptForceStops
	if r.forceStopping.Load() {
		stage, next = ShutdownStageForceStopping, NextInterruptRepeatsForceStop
	}
	r.shutdownEvent(stage, action, workers, next, format, args...)
}

func (r *Runner) logf(format string, args ...any) {
	fmt.Fprintf(r.Output, format+"\n", args...)
}

func (r *Runner) transitionToSuspended(run *scheduler.Run) {
	transitionStatus(run, scheduler.StatusSuspended)
	suspendedAt := r.Now().UTC()
	run.SuspendedAt = &suspendedAt
	run.Error = ""
	run.UpdatedAt = suspendedAt
}

func transitionStatus(run *scheduler.Run, next scheduler.Status) {
	run.SuspendingAt = nil
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

func nextSuspendedRun(current *state.State) scheduler.Run {
	for _, lease := range current.Leases {
		run := findRun(current.Runs, lease.RunID)
		if run.Status == scheduler.StatusSuspended && !run.ResumePending {
			return run
		}
	}
	return scheduler.Run{}
}

func persistedWorkerCount(current *state.State) int {
	count := 0
	for _, lease := range current.Leases {
		run := findRun(current.Runs, lease.RunID)
		if run.PID > 0 {
			count++
		}
	}
	return count
}

func activeRunCount(current *state.State) int {
	count := 0
	for _, lease := range current.Leases {
		run := findRun(current.Runs, lease.RunID)
		if scheduler.IsActive(run.Status) {
			count++
		}
	}
	return count
}

func interventionRequiredCount(current *state.State) int {
	count := 0
	for _, lease := range current.Leases {
		run := findRun(current.Runs, lease.RunID)
		if scheduler.RequiresIntervention(run.Status) {
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
