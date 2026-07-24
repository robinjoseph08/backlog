package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

func TestRunnerFillsSlotsAndImmediatelyRefillsAfterCompletion(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{
		{Number: 1, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Number: 2, CreatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{Number: 3, CreatedAt: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)},
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 2)

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 1, 2)
	github.setCompletion(1, mergedOutcome(1))
	workers.complete(1, worker.Result{ExitCode: 0})
	workers.waitForStarts(t, 3)
	workers.complete(2, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	github.setCompletion(3, mergedOutcome(3))
	workers.complete(3, worker.Result{ExitCode: 0})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop after runnable backlog was exhausted")
	}
	if got := workers.maxRunning(); got != 2 {
		t.Fatalf("max running = %d, want 2", got)
	}
	if got := store.runStatus(2); got != scheduler.StatusFailed {
		t.Fatalf("issue 2 status = %q, want failed", got)
	}
}

func TestRunnerRetainsMergedWorkWhenPiEventStreamIsMalformed(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 9, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 9)
	github.setCompletion(9, mergedOutcome(9))
	streamErr := errors.New("malformed Pi RPC JSON on line 2")
	workers.complete(9, worker.Result{ExitCode: 0, StreamErr: streamErr, Err: streamErr})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := store.runStatus(9); got != scheduler.StatusNeedsHuman {
		t.Fatalf("issue 9 status = %q, want needs-human", got)
	}
}

func TestRunnerStopsInvalidWorkerBeforeGitHubReconciliation(t *testing.T) {
	t.Parallel()

	workers := newFakeWorkers()
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 6, CreatedAt: time.Now()}}}
	github.completionCheck = func(int) error {
		if workers.runningCount() != 0 {
			return errors.New("GitHub reconciled while invalid Worker was alive")
		}
		return nil
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 6)
	streamErr := errors.New("malformed Pi RPC JSON")
	workers.complete(6, worker.Result{ExitCode: -1, StreamErr: streamErr, Err: streamErr})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := store.runStatus(6); got != scheduler.StatusFailed {
		t.Fatalf("issue 6 status = %q, want failed after stopped-Worker reconciliation", got)
	}
}

func TestRunnerFailsClosedWhenGitHubReconciliationFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{
		candidates:     []scheduler.Candidate{{Number: 8, CreatedAt: time.Now()}},
		completionErrs: map[int]error{8: errors.New("GitHub unavailable")},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	worktrees := runner.Worktrees.(*fakeWorktrees)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 8)
	workers.complete(8, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := store.runStatus(8); got != scheduler.StatusNeedsHuman {
		t.Fatalf("issue 8 status = %q, want needs-human", got)
	}
	if worktrees.cleanupCount() != 0 {
		t.Fatalf("cleanup count = %d, want worktree retained", worktrees.cleanupCount())
	}
}

func TestRunnerClosesWorkerAndRetainsLeaseWhenCompletionSaveFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 12, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workerReleased := make(chan struct{})
	workers.onRelease = func(int) { close(workerReleased) }
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	worktrees := runner.Worktrees.(*fakeWorktrees)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 12)
	select {
	case <-workerReleased:
	case <-time.After(time.Second):
		t.Fatal("Worker was not released after its identity became durable")
	}
	github.setCompletion(12, mergedOutcome(12))
	store.failNext()
	workers.complete(12, worker.Result{ExitCode: 0})
	if err := <-done; err == nil || !strings.Contains(err.Error(), "persist completion") {
		t.Fatalf("run error = %v, want completion persistence failure", err)
	}
	if workers.runningCount() != 0 {
		t.Fatalf("running workers = %d, want Worker closed", workers.runningCount())
	}
	if worktrees.cleanupCount() != 0 {
		t.Fatalf("cleanup count = %d, want worktree retained", worktrees.cleanupCount())
	}
	got := store.LoadValue()
	if len(got.Leases) != 1 || got.Runs[0].Status != scheduler.StatusRunning {
		t.Fatalf("state after failed completion save = %#v", got)
	}
}

func TestRunnerFailsClosedWhenRPCOutputBreaksAfterSettlement(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 10, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	worktrees := runner.Worktrees.(*fakeWorktrees)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 10)
	github.setCompletion(10, mergedOutcome(10))
	workers.setCloseResult(10, worker.Result{Err: errors.New("message followed agent_settled")})
	workers.complete(10, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := store.runStatus(10); got != scheduler.StatusNeedsHuman {
		t.Fatalf("issue 10 status = %q, want needs-human", got)
	}
	if worktrees.cleanupCount() != 0 {
		t.Fatalf("cleanup count = %d, want worktree retained", worktrees.cleanupCount())
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Leases) != 1 || got.Leases[0].Issue != 10 {
		t.Fatalf("Leases = %#v, want issue 10 retained", got.Leases)
	}
}

func TestRunnerRetainsFailedWorktree(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 11, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	worktrees := runner.Worktrees.(*fakeWorktrees)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 11)
	workers.complete(11, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if worktrees.cleanupCount() != 0 {
		t.Fatalf("cleanup count = %d, want failed worktree retained", worktrees.cleanupCount())
	}
}

func TestRunnerPersistsWorkerIdentityBeforeRelease(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 7, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	released := make(chan error, 1)
	workers.onRelease = func(issue int) {
		current, err := store.Load()
		if err != nil {
			released <- err
			return
		}
		run := findActiveRun(&current, issue)
		if run.Status != scheduler.StatusRunning || run.PID != 1000+issue || run.ProcessIdentity == "" {
			released <- fmt.Errorf("Run at release = %#v", run)
			return
		}
		released <- nil
	}
	runner := testRunner(github, workers, store, 1)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	workers.complete(7, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunnerDoesNotCommitInMemoryLeaseWhenAdmissionSaveFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 15, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 2}
	runner := testRunner(github, workers, store, 1)

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "persist lease for issue #15") {
		t.Fatalf("run error = %v, want Lease persistence failure", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 0 || len(got.Leases) != 0 {
		t.Fatalf("state after failed admission = %#v, want no Run or Lease", got)
	}
	if workers.wasStarted(15) {
		t.Fatal("Worker started after Lease persistence failed")
	}
}

func TestRunnerStopsGatedWorkerWhenPIDPersistenceFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 6, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 5}
	runner := testRunner(github, workers, store, 1)

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "persist worker") {
		t.Fatalf("run error = %v, want PID persistence failure", err)
	}
	if workers.runningCount() != 0 {
		t.Fatalf("running workers = %d, want gated worker stopped", workers.runningCount())
	}
	if workers.releaseCount() != 0 {
		t.Fatalf("release count = %d, want gated worker unreleased", workers.releaseCount())
	}
}

func TestRunnerNeverStartsBlockedCandidate(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{
		{Number: 1, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Blockers: []scheduler.Blocker{{Number: 9}}},
		{Number: 2, CreatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 2)
	workers.complete(2, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if workers.wasStarted(1) {
		t.Fatal("blocked issue #1 was started")
	}
}

func TestAdmissionGateSerializesDrainAcceptanceWithLeasePersistence(t *testing.T) {
	t.Parallel()

	gate := &admissionGate{}
	saveStarted := make(chan struct{})
	finishSave := make(chan struct{})
	commitDone := make(chan bool, 1)
	go func() {
		admitted, err := gate.commit(func() error {
			close(saveStarted)
			<-finishSave
			return nil
		})
		if err != nil {
			panic(err)
		}
		commitDone <- admitted
	}()
	<-saveStarted

	stopDone := make(chan bool, 1)
	go func() { stopDone <- gate.stop() }()
	select {
	case <-stopDone:
		t.Fatal("Drain was accepted before the in-progress Lease persistence finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(finishSave)
	if admitted := <-commitDone; !admitted {
		t.Fatal("Lease persistence that preceded Drain acceptance was rejected")
	}
	if firstDrain := <-stopDone; !firstDrain {
		t.Fatal("first Drain transition was not accepted")
	}

	savedAfterDrain := false
	admitted, err := gate.commit(func() error {
		savedAfterDrain = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if admitted || savedAfterDrain {
		t.Fatal("Lease persisted after Drain was accepted")
	}
}

func TestRunnerStartRejectsLeaseAfterDrainIsAccepted(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	gate := &admissionGate{}
	if first := gate.stop(); !first {
		t.Fatal("first Drain transition was not accepted")
	}
	current := store.LoadValue()
	admissionResult := make(chan bool, 1)
	process, err := runner.start(context.Background(), context.Background(), gate, &current, scheduler.Candidate{Number: 17}, admissionResult)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if admitted := <-admissionResult; admitted {
		t.Fatal("production start path admitted a Lease after Drain")
	}
	if process != nil || len(current.Runs) != 0 || len(current.Leases) != 0 || len(store.LoadValue().Runs) != 0 {
		t.Fatalf("state after rejected start = process %#v, memory %#v, store %#v", process, current, store.LoadValue())
	}
}

func TestRunnerAcceptsIdleDrainWhileInitialReconciliationIsBlocked(t *testing.T) {
	t.Parallel()

	reconciliationStarted := make(chan struct{})
	github := &fakeGitHub{completionFunc: func(ctx context.Context, _ int, _ string) (ghadapter.CompletionOutcome, error) {
		close(reconciliationStarted)
		<-ctx.Done()
		return ghadapter.CompletionOutcome{}, ctx.Err()
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: 16, RunID: "run-16", Status: scheduler.StatusWaitingForMerge,
			Branch: "agent/issue-16-run-16", Worktree: "/tmp/run-16",
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-16", Issue: 16, RunID: "run-16"}},
	}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-reconciliationStarted
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 0 Workers remaining; next SIGINT will be recorded as a suspension request")
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 1 || got.Runs[0].Status != scheduler.StatusWaitingForMerge || len(got.Leases) != 1 {
		t.Fatalf("state after idle Drain = %#v, want waiting Run and Lease unchanged", got)
	}
	output.waitFor(t, "Drain complete: 0 Workers remaining; exiting successfully")
}

func TestRunnerDoesNotMisclassifyMergedRunWhenDrainCancelsCleanup(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{21: mergedOutcome(21)}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: 21, RunID: "run-21", Status: scheduler.StatusWaitingForMerge,
			Branch: "agent/issue-21-run-21", Worktree: "/tmp/run-21",
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-21", Issue: 21, RunID: "run-21"}},
	}}
	cleanupStarted := make(chan struct{})
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Worktrees = &blockingCleanupWorktrees{cleanupStarted: cleanupStarted}
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-cleanupStarted
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 0 Workers remaining; next SIGINT will be recorded as a suspension request")
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 1 || got.Runs[0].Status != scheduler.StatusWaitingForMerge || len(got.Leases) != 1 {
		t.Fatalf("state after canceled completion cleanup = %#v", got)
	}
}

func TestRunnerAcceptsIdleDrainWhilePeriodicReconciliationIsBlocked(t *testing.T) {
	t.Parallel()

	reconciliationStarted := make(chan struct{})
	completionCalls := 0
	github := &fakeGitHub{completionFunc: func(ctx context.Context, _ int, _ string) (ghadapter.CompletionOutcome, error) {
		completionCalls++
		if completionCalls == 1 {
			return ghadapter.CompletionOutcome{PRFound: true, AutoMergeArmed: true}, nil
		}
		close(reconciliationStarted)
		<-ctx.Done()
		return ghadapter.CompletionOutcome{}, ctx.Err()
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: 20, RunID: "run-20", Status: scheduler.StatusWaitingForMerge,
			Branch: "agent/issue-20-run-20", Worktree: "/tmp/run-20",
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-20", Issue: 20, RunID: "run-20"}},
	}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Config.Watch = true
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-reconciliationStarted
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 0 Workers remaining; next SIGINT will be recorded as a suspension request")
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 1 || got.Runs[0].Status != scheduler.StatusWaitingForMerge || len(got.Leases) != 1 {
		t.Fatalf("state after periodic reconciliation Drain = %#v", got)
	}
}

func TestRunnerNeverPersistsLeaseAfterDrainIsAccepted(t *testing.T) {
	t.Parallel()

	candidateLookupStarted := make(chan struct{})
	github := &fakeGitHub{candidatesFunc: func(ctx context.Context) ([]scheduler.Candidate, error) {
		close(candidateLookupStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 2)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Config.Watch = true
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-candidateLookupStarted
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 0 Workers remaining; next SIGINT will be recorded as a suspension request")
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if len(got.Leases) != 0 || len(got.Runs) != 0 {
		t.Fatalf("state after Drain = %#v, want no admitted Run or Lease", got)
	}
	if len(workers.startedSnapshot()) != 0 {
		t.Fatalf("started Workers = %v, want none", workers.startedSnapshot())
	}
}

func TestRunnerFinishesLeaseCommittedBeforeDrainAndObservesRepeatedSignals(t *testing.T) {
	t.Parallel()

	var candidateCtx context.Context
	candidateReturned := make(chan struct{})
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{{Number: 14, CreatedAt: time.Now()}},
		candidatesFunc: func(ctx context.Context) ([]scheduler.Candidate, error) {
			candidateCtx = ctx
			close(candidateReturned)
			return []scheduler.Candidate{{Number: 14, CreatedAt: time.Now()}}, nil
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	prepareStarted := make(chan struct{})
	finishPrepare := make(chan struct{})
	signals := make(chan os.Signal, 2)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output
	runner.Worktrees = &blockingWorktrees{prepareStarted: prepareStarted, finishPrepare: finishPrepare}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-candidateReturned
	<-prepareStarted
	if got := store.LoadValue(); len(got.Leases) != 1 || got.Leases[0].Issue != 14 {
		t.Fatalf("state before Drain = %#v, want committed Lease for issue 14", got)
	}
	signals <- os.Interrupt
	select {
	case <-candidateCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Drain was not accepted while the committed Run was preparing")
	}
	close(finishPrepare)
	workers.waitForStarts(t, 14)
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining; next SIGINT will be recorded as a suspension request")
	signals <- os.Interrupt
	output.waitFor(t, "Drain: additional interrupt recorded as a suspension request; 1 Worker remaining")
	github.setCompletion(14, mergedOutcome(14))
	workers.complete(14, worker.Result{ExitCode: 0})
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 1 || got.Runs[0].Continuation == nil ||
		(got.Runs[0].Status != scheduler.StatusSuspended && got.Runs[0].Status != scheduler.StatusMerged) {
		t.Fatalf("state after suspension request during startup = %#v", got)
	}
	if got.Runs[0].Status == scheduler.StatusSuspended && len(got.Leases) != 1 || got.Runs[0].Status == scheduler.StatusMerged && len(got.Leases) != 0 {
		t.Fatalf("Lease did not match reconciled state: %#v", got)
	}
	output.waitFor(t, "Suspension complete: 0 Workers remaining")
}

func TestRunnerReportsDrainWhileSettledWorkerReconciliationIsBlocked(t *testing.T) {
	t.Parallel()

	completionStarted := make(chan struct{})
	finishCompletion := make(chan struct{})
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{{Number: 22, CreatedAt: time.Now()}},
		completionFunc: func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
			close(completionStarted)
			<-finishCompletion
			return mergedOutcome(22), nil
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 2)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 22)
	workers.complete(22, worker.Result{ExitCode: 0})
	<-completionStarted
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining; next SIGINT will be recorded as a suspension request")
	signals <- os.Interrupt
	output.waitFor(t, "Drain: additional interrupt recorded as a suspension request; 1 Worker remaining")
	close(finishCompletion)
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 1 || got.Runs[0].Status != scheduler.StatusMerged || len(got.Leases) != 0 {
		t.Fatalf("state after blocked reconciliation and suspension request = %#v", got)
	}
}

func TestRunnerSuspensionCancelsBlockedCompletionReconciliation(t *testing.T) {
	completionStarted := make(chan struct{})
	var completionMu sync.Mutex
	completionCalls := 0
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{{Number: 24, CreatedAt: time.Now()}},
		completionFunc: func(ctx context.Context, _ int, _ string) (ghadapter.CompletionOutcome, error) {
			completionMu.Lock()
			completionCalls++
			call := completionCalls
			completionMu.Unlock()
			if call == 1 {
				close(completionStarted)
				<-ctx.Done()
				return ghadapter.CompletionOutcome{}, ctx.Err()
			}
			return ghadapter.CompletionOutcome{}, nil
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 100 * time.Millisecond
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 24)
	workers.complete(24, worker.Result{ExitCode: 0})
	<-completionStarted
	started := time.Now()
	signals <- syscall.SIGTERM
	if err := <-done; !isSignalExit(err, 143) {
		t.Fatalf("run: %v, want signal exit 143", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("completion reconciliation outlived suspension bound: %s", elapsed)
	}
	if got := store.LoadValue().Runs[0].Status; got != scheduler.StatusSuspended {
		t.Fatalf("Run status = %q, want suspended", got)
	}
}

func TestRunnerDrainsEveryOwnedWorkerAndReportsProgress(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{
		{Number: 18, CreatedAt: time.Now()},
		{Number: 19, CreatedAt: time.Now().Add(time.Second)},
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 2)
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 18, 19)
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 2 Workers remaining; next SIGINT will be recorded as a suspension request")
	github.setCompletion(18, mergedOutcome(18))
	workers.complete(18, worker.Result{ExitCode: 0})
	output.waitFor(t, "Drain: 1 Worker remaining; next SIGINT will be recorded as a suspension request")
	select {
	case err := <-done:
		t.Fatalf("runner exited before every Worker settled: %v", err)
	default:
	}
	github.setCompletion(19, mergedOutcome(19))
	workers.complete(19, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 2 || got.Runs[0].Status != scheduler.StatusMerged || got.Runs[1].Status != scheduler.StatusMerged || len(got.Leases) != 0 {
		t.Fatalf("state after draining two Workers = %#v", got)
	}
	output.waitFor(t, "Drain complete: 0 Workers remaining; exiting successfully")
}

func TestRunnerWaitsForOwnedWorkerBeforePersistingShutdown(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 4, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	workers.waitForStarts(t, 4)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not wait for and stop its owned worker")
	}
	if got := store.runStatus(4); got != scheduler.StatusFailed {
		t.Fatalf("issue 4 status = %q, want failed", got)
	}
	if workers.runningCount() != 0 {
		t.Fatalf("running workers = %d, want zero before shutdown returned", workers.runningCount())
	}
}

func TestRunnerRetriesCandidateDiscoveryAfterFinalWorkerSettles(t *testing.T) {
	t.Parallel()

	pollInterval := 60 * time.Millisecond
	transientErr := errors.New("native blockers: TLS handshake timeout")
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{candidates: []scheduler.Candidate{{Number: 4, CreatedAt: time.Now()}}},
			{candidates: []scheduler.Candidate{{Number: 12, CreatedAt: time.Now()}}, err: transientErr},
			{},
		},
		candidateChanged: make(chan struct{}, 4),
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 2)
	runner.Config.PollInterval = pollInterval
	output := newDiagnosticWriter(transientErr.Error())
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 4)
	github.waitForCandidateCalls(t, 2)

	select {
	case <-output.seen:
	case err := <-done:
		t.Fatalf("runner stopped after transient candidate discovery error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not report the transient candidate discovery error")
	}
	if got := workers.runningCount(); got != 1 {
		t.Fatalf("running Workers = %d, want 1", got)
	}
	if got := store.runStatus(4); got != scheduler.StatusRunning {
		t.Fatalf("issue 4 status = %q, want running", got)
	}
	if workers.wasStarted(12) {
		t.Fatal("issue #12 was started from a failed candidate discovery pass")
	}
	if got := len(store.LoadValue().Leases); got != 1 {
		t.Fatalf("Lease count = %d, want 1", got)
	}

	github.setCompletion(4, mergedOutcome(4))
	workers.complete(4, worker.Result{ExitCode: 0})
	github.waitForCandidateCalls(t, 3)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	calls := github.candidateCallSnapshot()
	if elapsed := calls[2].Sub(calls[1]); elapsed < pollInterval {
		t.Fatalf("candidate discovery retry happened after %s, want no sooner than %s", elapsed, pollInterval)
	}
	if got := store.runStatus(4); got != scheduler.StatusMerged {
		t.Fatalf("issue 4 status = %q, want merged", got)
	}
	if !strings.Contains(output.String(), transientErr.Error()) {
		t.Fatalf("runner output = %q, want candidate discovery diagnostic", output.String())
	}
}

func TestRunnerRetriesCandidateDiscoveryAfterWaitingRunReconciles(t *testing.T) {
	t.Parallel()

	pollInterval := 50 * time.Millisecond
	transientErr := errors.New("candidate discovery unavailable")
	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: transientErr}, {err: transientErr}, {}},
		candidateChanged: make(chan struct{}, 4),
		completions: map[int]ghadapter.CompletionOutcome{
			4: {PRFound: true, PullRequest: "https://example.test/pr/4", AutoMergeArmed: true},
		},
	}
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 4, RunID: "waiting", Status: scheduler.StatusWaitingForMerge, WorkerMode: scheduler.WorkerModeRPC,
			Branch: "agent/issue-4-waiting", Worktree: "/tmp/waiting",
		}},
		Leases: []scheduler.Lease{{LeaseID: "waiting", Issue: 4, RunID: "waiting"}},
	}}
	runner := testRunner(github, newFakeWorkers(), store, 1)
	runner.Config.PollInterval = pollInterval

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	github.waitForCandidateCalls(t, 1)
	github.setCompletion(4, mergedOutcome(4))

	github.waitForCandidateCalls(t, 3)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	calls := github.candidateCallSnapshot()
	for i := 1; i < len(calls); i++ {
		if elapsed := calls[i].Sub(calls[i-1]); elapsed < pollInterval {
			t.Fatalf("candidate discovery retry %d happened after %s, want no sooner than %s", i, elapsed, pollInterval)
		}
	}
	if got := store.runStatus(4); got != scheduler.StatusMerged {
		t.Fatalf("issue 4 status = %q, want merged", got)
	}
}

func TestRunnerRetriesCandidateDiscoveryAfterPollIntervalAndResumesAdmission(t *testing.T) {
	t.Parallel()

	pollInterval := 80 * time.Millisecond
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{candidates: []scheduler.Candidate{{Number: 4, CreatedAt: time.Now()}}},
			{candidates: []scheduler.Candidate{{Number: 5, CreatedAt: time.Now()}}, err: errors.New("candidate discovery unavailable")},
			{candidates: []scheduler.Candidate{{Number: 6, CreatedAt: time.Now()}}, err: errors.New("candidate discovery still unavailable")},
			{candidates: []scheduler.Candidate{{Number: 4, CreatedAt: time.Now()}, {Number: 5, CreatedAt: time.Now()}}},
		},
		candidateChanged: make(chan struct{}, 8),
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 2)
	runner.Config.PollInterval = pollInterval
	runner.Config.Watch = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	workers.waitForStarts(t, 4)
	github.waitForCandidateCalls(t, 2)
	assertOnlyIssueFourIsLeased(t, workers, store)

	github.waitForCandidateCalls(t, 3)
	assertOnlyIssueFourIsLeased(t, workers, store)

	github.waitForCandidateCalls(t, 4)
	workers.waitForStarts(t, 5)
	calls := github.candidateCallSnapshot()
	for i := 2; i < 4; i++ {
		if elapsed := calls[i].Sub(calls[i-1]); elapsed < pollInterval {
			t.Fatalf("candidate discovery retry %d happened after %s, want no sooner than %s", i-1, elapsed, pollInterval)
		}
	}
	if got := workers.startCount(4); got != 1 {
		t.Fatalf("issue #4 start count = %d, want 1", got)
	}
	if got := store.runStatus(4); got != scheduler.StatusRunning {
		t.Fatalf("issue 4 status = %q, want running after discovery retries", got)
	}
	if workers.wasStarted(6) {
		t.Fatal("issue #6 was started from a failed candidate discovery pass")
	}

	workers.complete(4, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	workers.complete(5, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	workers.waitForNoRunningWorkers(t)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerWatchRetriesCandidateDiscoveryWithoutActiveRun(t *testing.T) {
	t.Parallel()

	pollInterval := 40 * time.Millisecond
	transientErr := errors.New("GitHub unavailable")
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{err: transientErr},
			{candidates: []scheduler.Candidate{{Number: 7, CreatedAt: time.Now()}}},
		},
		candidateChanged: make(chan struct{}, 4),
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	runner.Config.PollInterval = pollInterval
	runner.Config.Watch = true
	output := newDiagnosticWriter(transientErr.Error())
	runner.Output = output
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	workers.waitForStarts(t, 7)
	calls := github.candidateCallSnapshot()
	if elapsed := calls[1].Sub(calls[0]); elapsed < pollInterval {
		t.Fatalf("idle candidate discovery retried after %s, want no sooner than %s", elapsed, pollInterval)
	}
	if text := output.String(); !strings.Contains(text, transientErr.Error()) {
		t.Fatalf("runner output = %q, want candidate discovery diagnostic", text)
	}
	workers.complete(7, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	workers.waitForNoRunningWorkers(t)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerRetriesInitialCandidateDiscoveryAndResumesAdmission(t *testing.T) {
	t.Parallel()

	pollInterval := 50 * time.Millisecond
	firstErr := errors.New("list candidates: i/o timeout")
	secondErr := errors.New("native blockers: TLS handshake timeout")
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{candidates: []scheduler.Candidate{{Number: 8, CreatedAt: time.Now()}}, err: firstErr},
			{candidates: []scheduler.Candidate{{Number: 9, CreatedAt: time.Now()}}, err: secondErr},
			{candidates: []scheduler.Candidate{{Number: 7, CreatedAt: time.Now()}}},
		},
		candidateChanged: make(chan struct{}, 4),
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	runner.Config.PollInterval = pollInterval
	output := newDiagnosticWriter(firstErr.Error())
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	github.waitForCandidateCalls(t, 2)
	if got := len(store.LoadValue().Leases); got != 0 {
		t.Fatalf("Lease count during failed discovery passes = %d, want 0", got)
	}
	if workers.wasStarted(8) || workers.wasStarted(9) {
		t.Fatalf("Workers started from failed snapshots: %v", workers.startedSnapshot())
	}

	workers.waitForStarts(t, 7)
	calls := github.candidateCallSnapshot()
	for i := 1; i < 3; i++ {
		if elapsed := calls[i].Sub(calls[i-1]); elapsed < pollInterval {
			t.Fatalf("candidate discovery retry %d happened after %s, want no sooner than %s", i, elapsed, pollInterval)
		}
	}
	if text := output.String(); !strings.Contains(text, firstErr.Error()) || !strings.Contains(text, secondErr.Error()) {
		t.Fatalf("runner output = %q, want both candidate discovery diagnostics", text)
	}

	workers.complete(7, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerWaitsPollIntervalAfterCandidateDiscoveryFailureReturns(t *testing.T) {
	t.Parallel()

	pollInterval := 40 * time.Millisecond
	failureReturned := make(chan time.Time, 1)
	calls := 0
	github := &fakeGitHub{
		candidateChanged: make(chan struct{}, 2),
		candidatesFunc: func(context.Context) ([]scheduler.Candidate, error) {
			calls++
			if calls == 1 {
				time.Sleep(pollInterval + 20*time.Millisecond)
				failureReturned <- time.Now()
				return nil, errors.New("list candidates: slow timeout")
			}
			return []scheduler.Candidate{{Number: 7, CreatedAt: time.Now()}}, nil
		},
	}
	workers := newFakeWorkers()
	runner := testRunner(github, workers, &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = pollInterval

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 7)

	candidateCalls := github.candidateCallSnapshot()
	if elapsed := candidateCalls[1].Sub(<-failureReturned); elapsed < pollInterval {
		t.Fatalf("candidate discovery retry happened %s after failure returned, want no sooner than %s", elapsed, pollInterval)
	}
	workers.complete(7, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerRetriesInitialCandidateDiscoveryUntilEmptySnapshotSucceeds(t *testing.T) {
	t.Parallel()

	pollInterval := 40 * time.Millisecond
	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: errors.New("list candidates: connection reset")}, {}},
		candidateChanged: make(chan struct{}, 2),
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, newFakeWorkers(), store, 1)
	runner.Config.PollInterval = pollInterval

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	calls := github.candidateCallSnapshot()
	if len(calls) != 2 {
		t.Fatalf("candidate discovery calls = %d, want 2", len(calls))
	}
	if elapsed := calls[1].Sub(calls[0]); elapsed < pollInterval {
		t.Fatalf("candidate discovery retry happened after %s, want no sooner than %s", elapsed, pollInterval)
	}
	got := store.LoadValue()
	if len(got.Runs) != 0 || len(got.Leases) != 0 {
		t.Fatalf("state after successful empty snapshot = %#v", got)
	}
}

func TestRunnerCancellationInterruptsCandidateDiscoveryRetryWait(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: errors.New("list candidates: timeout")}},
		candidateChanged: make(chan struct{}, 1),
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, newFakeWorkers(), store, 1)
	runner.Config.PollInterval = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	github.waitForCandidateCalls(t, 1)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runner did not stop promptly during candidate discovery retry wait")
	}
	if got := len(github.candidateCallSnapshot()); got != 1 {
		t.Fatalf("candidate discovery calls = %d, want 1", got)
	}
	got := store.LoadValue()
	if len(got.Runs) != 0 || len(got.Leases) != 0 {
		t.Fatalf("state after cancellation during retry wait = %#v", got)
	}
}

func TestRunnerSignalShutdownInterruptsCandidateDiscoveryRetryWait(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: errors.New("list candidates: timeout")}},
		candidateChanged: make(chan struct{}, 1),
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, newFakeWorkers(), store, 1)
	runner.Config.PollInterval = time.Second
	runner.Signals = signals
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	github.waitForCandidateCalls(t, 1)

	signals <- os.Interrupt
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runner did not stop promptly after Drain began during candidate discovery retry wait")
	}
	if got := len(github.candidateCallSnapshot()); got != 1 {
		t.Fatalf("candidate discovery calls = %d, want 1", got)
	}
	got := store.LoadValue()
	if len(got.Runs) != 0 || len(got.Leases) != 0 {
		t.Fatalf("state after signal shutdown during retry wait = %#v", got)
	}
}

func TestRunnerSchedulesDependentAfterBlockerCloses(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{
		Number: 2, CreatedAt: time.Now(), Blockers: []scheduler.Blocker{{Number: 1}},
	}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	runner.Config.Watch = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	if workers.wasStarted(2) {
		t.Fatal("dependent started while blocker was open")
	}
	github.setCandidates([]scheduler.Candidate{{Number: 2, CreatedAt: time.Now()}})
	workers.waitForStarts(t, 2)
	workers.complete(2, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	for deadline := time.Now().Add(time.Second); store.runStatus(2) != scheduler.StatusFailed && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerDoesNotDuplicatePersistedLiveWorker(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 1, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 1, RunID: "live", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, PID: 1234,
			ProcessIdentity: "identity-1234", Branch: "agent/issue-1-live", Worktree: "/tmp/live",
			StartedAt: time.Date(2026, 7, 2, 2, 0, 0, 0, time.UTC),
		}},
		Leases: []scheduler.Lease{{LeaseID: "live", Issue: 1, RunID: "live"}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(pid int) bool { return pid == 1234 }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if workers.wasStarted(1) {
		t.Fatal("persisted live issue #1 was launched again")
	}
	if got := store.runStatus(1); got != scheduler.StatusRunning {
		t.Fatalf("live orphan status = %q, want running", got)
	}
}

func TestRunnerFailsClosedInsteadOfReleasingRecoveredRPCWorker(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 2, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 2, RunID: "rpc-live", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC, PID: 1235,
			ProcessIdentity: "identity-1235", Branch: "agent/issue-2-rpc-live", Worktree: "/tmp/rpc-live",
			SessionID: "backlog-rpc-live", SessionDir: "/state/sessions/rpc-live",
			StartedAt: time.Date(2026, 7, 2, 2, 0, 0, 0, time.UTC),
		}},
		Leases: []scheduler.Lease{{LeaseID: "rpc-live", Issue: 2, RunID: "rpc-live"}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(pid int) bool { return pid == 1235 }
	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 1235 || len(got.Leases) != 1 {
		t.Fatalf("recovered RPC Run/Lease = %#v/%#v", got.Runs[0], got.Leases)
	}
	if workers.recoveredReleaseCount() != 0 {
		t.Fatalf("recovered release count = %d, want zero", workers.recoveredReleaseCount())
	}
}

func TestRunnerDoesNotTrustRecoveredPIDIndefinitely(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 1, RunID: "stale", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, PID: 1234,
			ProcessIdentity: "identity-1234", Branch: "agent/issue-1-stale", Worktree: "/tmp/stale",
			StartedAt: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		}},
		Leases: []scheduler.Lease{{LeaseID: "stale", Issue: 1, RunID: "stale"}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(int) bool { return true }
	runner.Config.MaxWorkerAge = 24 * time.Hour

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := store.runStatus(1); got != scheduler.StatusNeedsHuman {
		t.Fatalf("stale PID status = %q, want needs-human", got)
	}
}

func TestRunnerDrainCancellationDoesNotEraseRecoveredWorkerPID(t *testing.T) {
	const issue = 78
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: issue, RunID: "run-78", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
			PID: 1078, ProcessIdentity: "identity-1078", Branch: "agent/issue-78-run-78", Worktree: "/tmp/run-78",
			StartedAt: time.Now(),
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-78", Issue: issue, RunID: "run-78"}},
	}}
	signals := make(chan os.Signal, 1)
	identityStarted := make(chan struct{})
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.Signals = signals
	runner.PIDAlive = func(int) bool { return true }
	runner.PIDIdentity = func(ctx context.Context, _ int) (string, error) {
		close(identityStarted)
		<-ctx.Done()
		return "", ctx.Err()
	}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-identityStarted
	signals <- os.Interrupt
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusRunning || got.Runs[0].PID != 1078 || got.Runs[0].ProcessIdentity != "identity-1078" || len(got.Leases) != 1 {
		t.Fatalf("recovered Worker changed during Drain cancellation = %#v", got)
	}
}

func TestRunnerReconcilesClaimedRunWithoutLookingUpAnEmptyBranch(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 1, RunID: "claimed", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "claimed", Issue: 1, RunID: "claimed"}},
	}}
	runner := testRunner(github, workers, store, 1)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := store.runStatus(1); got != scheduler.StatusFailed {
		t.Fatalf("claimed run status = %q, want failed", got)
	}
	if branches := github.completionBranchSnapshot(); len(branches) != 0 {
		t.Fatalf("completion looked up branches %q, want no lookup with an empty branch", branches)
	}
}

func TestRunnerReconcilesDeadWorkerWithArmedAutoMergeAsWaiting(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{
		3: {PRFound: true, PullRequest: "https://example.test/pr/3", AutoMergeArmed: true},
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 3, RunID: "old", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, PID: 999999,
			Branch: "agent/issue-3-old", Worktree: "/tmp/old",
		}},
		Leases: []scheduler.Lease{{LeaseID: "old", Issue: 3, RunID: "old"}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(int) bool { return false }
	runner.Config.Watch = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	for deadline := time.Now().Add(time.Second); store.runStatus(3) != scheduler.StatusWaitingForMerge && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := store.runStatus(3); got != scheduler.StatusWaitingForMerge {
		t.Fatalf("issue 3 status = %q, want waiting-for-merge", got)
	}
}

func TestRunnerReconcilesSuspendedRunWithArmedAutoMergeAsWaiting(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{
		23: {PRFound: true, PullRequest: "https://example.test/pr/23", AutoMergeArmed: true},
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 23, RunID: "suspended", Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
			Branch: "agent/issue-23-suspended", Worktree: "/tmp/suspended", SessionID: "session-23", SessionDir: "/tmp/sessions/23",
			Continuation: &scheduler.ContinuationBoundary{SessionID: "session-23", SessionFile: "/tmp/sessions/23/session.jsonl", Worktree: "/tmp/suspended", LeafID: "leaf", EntryCount: 1, SHA256: strings.Repeat("a", 64), VerifiedAt: time.Now()},
		}},
		Leases: []scheduler.Lease{{LeaseID: "suspended", Issue: 23, RunID: "suspended"}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.Config.Watch = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	for deadline := time.Now().Add(time.Second); store.runStatus(23) != scheduler.StatusWaitingForMerge && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusWaitingForMerge || got.Runs[0].Error != "" || len(got.Leases) != 1 {
		t.Fatalf("reconciled suspended Run = %#v", got)
	}
}

func TestRunnerReconcilesSuspendedMergedOpenIssueAsNeedsHuman(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{
		25: {PRFound: true, PullRequest: "https://example.test/pr/25", Merged: true, IssueClosed: false},
	}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 25, RunID: "suspended-open", Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
			Branch: "agent/issue-25-suspended", Worktree: "/tmp/suspended-25", SessionID: "session-25", SessionDir: "/tmp/sessions/25",
			Continuation: &scheduler.ContinuationBoundary{SessionID: "session-25", SessionFile: "/tmp/sessions/25/session.jsonl", Worktree: "/tmp/suspended-25", LeafID: "leaf", EntryCount: 1, SHA256: strings.Repeat("a", 64), VerifiedAt: time.Now()},
		}},
		Leases: []scheduler.Lease{{LeaseID: "suspended-open", Issue: 25, RunID: "suspended-open"}},
	}}
	runner := testRunner(github, workers, store, 1)
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "issue remains open") || len(got.Leases) != 1 {
		t.Fatalf("reconciled merged-open suspended Run = %#v", got)
	}
}

func TestRunnerReconcilesMergedRunWithoutLaunchingDuplicate(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 1, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{1: mergedOutcome(1)},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version:       state.CurrentVersion,
		Repo:          "acme/widgets",
		DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 1, RunID: "old", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, PID: 999999,
			Branch: "agent/issue-1-old", Worktree: "/tmp/old",
		}},
		Leases: []scheduler.Lease{{LeaseID: "old", Issue: 1, RunID: "old"}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(int) bool { return false }

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if workers.wasStarted(1) {
		t.Fatal("reconciled issue #1 was launched again")
	}
	if got := store.runStatus(1); got != scheduler.StatusMerged {
		t.Fatalf("issue 1 status = %q, want merged", got)
	}
}

func TestRunnerReconcilesOnlyTheLeasedRunWhenIssueHasHistory(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{1: mergedOutcome(1)}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{
			{Issue: 1, RunID: "historical", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "keep this diagnostic"},
			{Issue: 1, RunID: "active", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, PID: 999999,
				Branch: "agent/issue-1-active", Worktree: "/tmp/active"},
		},
		Leases: []scheduler.Lease{{LeaseID: "active", Issue: 1, RunID: "active"}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(int) bool { return false }

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 2 || got.Runs[0].Status != scheduler.StatusFailed || got.Runs[0].Error != "keep this diagnostic" {
		t.Fatalf("historical Run changed: %#v", got.Runs)
	}
	if got.Runs[1].Status != scheduler.StatusMerged || len(got.Leases) != 0 {
		t.Fatalf("active Run/Lease = %#v/%#v, want merged without Lease", got.Runs[1], got.Leases)
	}
}

func TestRunnerSuspendsOnSecondSIGINTAfterPersistingBoundary(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 40, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	workers.onCloseContext = func(issue int) error {
		persisted := store.LoadValue()
		run := findActiveRun(&persisted, issue)
		if run.Continuation == nil || run.Status != scheduler.StatusRunning || run.PID == 0 {
			return fmt.Errorf("RPC closed before running continuation marker was persisted: %#v", run)
		}
		return nil
	}
	signals := make(chan os.Signal, 2)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 40)
	signals <- os.Interrupt
	signals <- os.Interrupt
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}
	got := store.LoadValue()
	run := got.Runs[0]
	if run.Status != scheduler.StatusSuspended || run.PID != 0 || run.ProcessIdentity != "" || run.Continuation == nil {
		t.Fatalf("suspended Run = %#v", run)
	}
	if len(got.Leases) != 1 || run.Branch == "" || run.Worktree == "" || run.SessionID == "" {
		t.Fatalf("retained Run artifacts/Lease = %#v/%#v", run, got.Leases)
	}
}

func TestRunnerThirdSIGINTExpeditesSettledWorkerCloseThroughVerifiedForceStop(t *testing.T) {
	const issue = 79
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{issue: mergedOutcome(issue)},
	}
	workers := newFakeWorkers()
	workers.authorizeClose = true
	workers.blockSettledClose = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 3)
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 5 * time.Second
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	workers.complete(issue, worker.Result{ExitCode: 0})
	<-workers.settledCloseStarted
	started := time.Now()
	signals <- os.Interrupt
	signals <- os.Interrupt
	signals <- os.Interrupt
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("third SIGINT did not expedite settled Worker close: %s", elapsed)
	}
	if got := workers.authorizedForceStopCount(); got != 1 {
		t.Fatalf("authorized force stops = %d, want 1", got)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusMerged || got.Runs[0].CompletedAt == nil || len(got.Leases) != 0 {
		t.Fatalf("settled terminal outcome after force close = %#v", got)
	}
}

func TestRunnerThirdSIGINTBoundsSettledWorkerCleanupAndPreservesMergedOutcome(t *testing.T) {
	const issue = 80
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{issue: mergedOutcome(issue)},
	}
	workers := newFakeWorkers()
	workers.authorizeClose = true
	workers.blockSettledClose = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 3)
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 100 * time.Millisecond
	runner.Signals = signals
	cleanupStarted := make(chan struct{})
	runner.Worktrees = &blockingCleanupWorktrees{cleanupStarted: cleanupStarted}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	workers.complete(issue, worker.Result{ExitCode: 0})
	<-workers.settledCloseStarted
	started := time.Now()
	signals <- os.Interrupt
	signals <- os.Interrupt
	signals <- os.Interrupt
	<-cleanupStarted
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want bounded signal exit 130", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("settled Worker cleanup was not bounded by suspension deadline: %s", elapsed)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusMerged || got.Runs[0].CompletedAt == nil || len(got.Leases) != 0 {
		t.Fatalf("merged outcome after bounded cleanup failure = %#v", got)
	}
}

func TestRunnerThirdSIGINTAndTimeoutUseTheSameVerifiedForceStopPath(t *testing.T) {
	tests := []struct {
		name      string
		trigger   func(chan os.Signal, <-chan int)
		timeout   time.Duration
		exitCode  int
		immediate bool
		forceLog  string
	}{
		{
			name: "third SIGINT",
			trigger: func(signals chan os.Signal, closeStarted <-chan int) {
				signals <- os.Interrupt
				signals <- os.Interrupt
				<-closeStarted
				signals <- os.Interrupt
			},
			timeout: 5 * time.Second, exitCode: 130, immediate: true,
			forceLog: "Force stop: additional signal accepted; requesting force stop for 1 Worker; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request",
		},
		{
			name: "three queued SIGINTs",
			trigger: func(signals chan os.Signal, _ <-chan int) {
				signals <- os.Interrupt
				signals <- os.Interrupt
				signals <- os.Interrupt
			},
			timeout: 5 * time.Second, exitCode: 130, immediate: true,
			forceLog: "Force stop: additional signal accepted; requesting force stop for 1 Worker; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request",
		},
		{
			name: "SIGTERM followed by force SIGINT",
			trigger: func(signals chan os.Signal, closeStarted <-chan int) {
				signals <- syscall.SIGTERM
				<-closeStarted
				signals <- os.Interrupt
			},
			timeout: 5 * time.Second, exitCode: 143, immediate: true,
			forceLog: "Force stop: additional signal accepted; requesting force stop for 1 Worker; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request",
		},
		{
			name: "suspension timeout",
			trigger: func(signals chan os.Signal, _ <-chan int) {
				signals <- syscall.SIGTERM
			},
			timeout: 30 * time.Millisecond, exitCode: 143,
			forceLog: "Force stop: suspension deadline expired; requesting force stop for 1 Worker; each identity will be revalidated before signaling; next SIGINT will repeat the force-stop request",
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issue := 60 + index
			github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
			workers := newFakeWorkers()
			workers.authorizeClose = true
			workers.waitForForce = true
			store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
			signals := make(chan os.Signal, 3)
			runner := testRunner(github, workers, store, 1)
			runner.Config.SuspensionTimeout = test.timeout
			runner.Signals = signals
			output := newSynchronizedOutput()
			runner.Output = output
			var processCheckMu sync.Mutex
			identityChecks := 0
			livenessChecks := 0
			runner.PIDAlive = func(int) bool {
				processCheckMu.Lock()
				defer processCheckMu.Unlock()
				livenessChecks++
				return true
			}
			runner.PIDIdentity = func(_ context.Context, pid int) (string, error) {
				processCheckMu.Lock()
				defer processCheckMu.Unlock()
				identityChecks++
				return fmt.Sprintf("identity-%d", pid), nil
			}

			done := make(chan error, 1)
			go func() { done <- runner.Run(context.Background()) }()
			workers.waitForStarts(t, issue)
			started := time.Now()
			test.trigger(signals, workers.closeContextStarted)
			err := <-done
			if !isSignalExit(err, test.exitCode) {
				t.Fatalf("run: %v, want signal exit %d", err, test.exitCode)
			}
			output.waitFor(t, test.forceLog)
			if test.immediate && time.Since(started) > time.Second {
				t.Fatalf("force signal did not bypass suspension deadline: %s", time.Since(started))
			}
			if got := workers.authorizedForceStopCount(); got != 1 {
				t.Fatalf("authorized force stops = %d, want 1", got)
			}
			processCheckMu.Lock()
			gotIdentityChecks := identityChecks
			gotLivenessChecks := livenessChecks
			processCheckMu.Unlock()
			if gotIdentityChecks != 3 {
				t.Fatalf("identity checks = %d, want start, suspension, and immediate pre-signal checks", gotIdentityChecks)
			}
			if gotLivenessChecks != 2 {
				t.Fatalf("liveness checks = %d, want suspension and immediate pre-signal checks", gotLivenessChecks)
			}
			got := store.LoadValue()
			if got.Runs[0].Status != scheduler.StatusSuspended || got.Runs[0].Continuation == nil || got.Runs[0].PID != 0 || len(got.Leases) != 1 {
				t.Fatalf("Run after force stop = %#v", got)
			}
		})
	}
}

func TestRunnerForceEscalationPreservesDurableTerminalOutcomes(t *testing.T) {
	tests := []scheduler.Status{scheduler.StatusMerged, scheduler.StatusWaitingForMerge, scheduler.StatusSuspended}
	for index, status := range tests {
		t.Run(string(status), func(t *testing.T) {
			issue := 70 + index
			github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
			workers := newFakeWorkers()
			workers.authorizeClose = true
			workers.waitForForce = true
			store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
			signals := make(chan os.Signal, 3)
			runner := testRunner(github, workers, store, 1)
			runner.Config.SuspensionTimeout = 5 * time.Second
			runner.Signals = signals

			done := make(chan error, 1)
			go func() { done <- runner.Run(context.Background()) }()
			workers.waitForStarts(t, issue)
			signals <- os.Interrupt
			signals <- os.Interrupt
			<-workers.closeContextStarted

			persisted := store.LoadValue()
			run := persisted.Runs[0]
			run.Status = status
			run.PID = 0
			run.ProcessIdentity = ""
			if status == scheduler.StatusMerged {
				now := time.Now()
				run.CompletedAt = &now
				persisted.Leases = nil
			}
			persisted.Runs[0] = run
			if err := store.Save(persisted); err != nil {
				t.Fatalf("persist concurrent outcome: %v", err)
			}
			expected := store.LoadValue()
			signals <- os.Interrupt
			if err := <-done; !isSignalExit(err, 130) {
				t.Fatalf("run: %v, want signal exit 130", err)
			}
			got := store.LoadValue()
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("terminal state changed during escalation:\n got: %#v\nwant: %#v", got, expected)
			}
			if got := workers.authorizedForceStopCount(); got != 0 {
				t.Fatalf("authorized force stops = %d, want 0 for terminal Run", got)
			}
		})
	}
}

func TestRunnerForceEscalationCleansBeforePersistingNewMergedOutcome(t *testing.T) {
	const issue = 73
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{issue: mergedOutcome(issue)},
	}
	workers := newFakeWorkers()
	workers.authorizeClose = true
	workers.waitForForce = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 3)
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 5 * time.Second
	runner.Signals = signals
	worktrees := &liveContextWorktrees{store: store, runID: fmt.Sprintf("run-%d", issue)}
	runner.Worktrees = worktrees

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- os.Interrupt
	signals <- os.Interrupt
	<-workers.closeContextStarted
	signals <- os.Interrupt
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusMerged || got.Runs[0].CompletedAt == nil || len(got.Leases) != 0 {
		t.Fatalf("merged Run after force escalation = %#v", got)
	}
	if got := worktrees.cleanupCount(); got != 1 {
		t.Fatalf("worktree cleanup count = %d, want 1", got)
	}
}

func TestRunnerDoesNotPersistBoundaryAfterMarkerWriteFails(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 44, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 6}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 44)
	signals <- syscall.SIGTERM
	if err := <-done; err == nil || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want failed marker suspension", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].Continuation != nil || got.Runs[0].PID != 0 || len(got.Leases) != 1 {
		t.Fatalf("Run after failed marker write = %#v", got)
	}
}

func TestRunnerRecoversPersistedContinuationAfterFinalSuspensionSaveFails(t *testing.T) {
	const issue = 76
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 7}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- syscall.SIGTERM
	if err := <-done; !isSignalExit(err, 143) || !strings.Contains(err.Error(), "persist suspended") {
		t.Fatalf("run: %v, want final suspension persistence failure with signal exit 143", err)
	}
	durable := store.LoadValue()
	if durable.Runs[0].Status != scheduler.StatusRunning || durable.Runs[0].Continuation == nil || durable.Runs[0].PID == 0 {
		t.Fatalf("durable crash-window state = %#v", durable)
	}

	recoveryGitHub := &fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{
		issue: {PRFound: true, PullRequest: "https://example.test/pr/76"},
	}}
	restarted := testRunner(recoveryGitHub, newFakeWorkers(), store, 1)
	restarted.Output = io.Discard
	restarted.PIDAlive = func(int) bool { return false }
	restarted.ProcessGroupAlive = func(int) (bool, error) { return false, nil }
	current := store.LoadValue()
	if err := restarted.reconcile(context.Background(), &current, map[int]WorkerProcess{}); err != nil {
		t.Fatalf("reconcile restart: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusSuspended || got.Runs[0].PID != 0 || got.Runs[0].ProcessIdentity != "" || got.Runs[0].Continuation == nil || len(got.Leases) != 1 {
		t.Fatalf("recovered continuation = %#v", got)
	}
}

func TestRunnerFailsClosedWhenRecoveredContinuationProcessGroupMayBeLive(t *testing.T) {
	const issue = 77
	continuation := &scheduler.ContinuationBoundary{
		SessionID: "backlog-run-77", SessionFile: "/tmp/sessions/run-77/session.jsonl", Worktree: "/tmp/run-77",
		LeafID: "leaf", EntryCount: 1, SHA256: "hash", VerifiedAt: time.Now(),
	}
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: issue, RunID: "run-77", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
			PID: 1077, ProcessIdentity: "identity-1077", Branch: "agent/issue-77-run-77", Worktree: "/tmp/run-77",
			Continuation: continuation,
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-77", Issue: issue, RunID: "run-77"}},
	}}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.Output = io.Discard
	runner.PIDAlive = func(int) bool { return false }
	runner.ProcessGroupAlive = func(int) (bool, error) { return true, nil }
	current := store.LoadValue()
	if err := runner.reconcile(context.Background(), &current, map[int]WorkerProcess{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 1077 || got.Runs[0].Continuation == nil || len(got.Leases) != 1 {
		t.Fatalf("uncertain recovered process group = %#v", got)
	}
}

func TestRunnerSuspensionRequestBoundsCommittedWorkerPreparation(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 47, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	prepareStarted := make(chan struct{})
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 40 * time.Millisecond
	runner.Signals = signals
	runner.Worktrees = &blockingWorktrees{prepareStarted: prepareStarted, finishPrepare: make(chan struct{})}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-prepareStarted
	started := time.Now()
	signals <- syscall.SIGTERM
	if err := <-done; err == nil || !strings.Contains(err.Error(), "continuation boundary") {
		t.Fatalf("run: %v, want failed-closed interrupted preparation", err)
	}
	if elapsed := time.Since(started); elapsed > 120*time.Millisecond {
		t.Fatalf("suspension request did not bound Worker preparation: %s", elapsed)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 0 || len(got.Leases) != 1 {
		t.Fatalf("Run after interrupted preparation = %#v", got)
	}
}

func TestRunnerSuspendsDirectlyOnSIGTERMAndUsesOneDeadline(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{
		{Number: 41, CreatedAt: time.Now()}, {Number: 42, CreatedAt: time.Now().Add(time.Second)},
	}}
	workers := newFakeWorkers()
	deadlines := make(chan time.Time, 2)
	workers.suspendFunc = func(ctx context.Context, _ int, _ worker.ContinuationRequest) (worker.Continuation, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return worker.Continuation{}, errors.New("suspension context has no deadline")
		}
		deadlines <- deadline
		<-ctx.Done()
		return worker.Continuation{}, ctx.Err()
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 2)
	runner.Config.SuspensionTimeout = 40 * time.Millisecond
	runner.Signals = signals
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 41, 42)
	started := time.Now()
	signals <- syscall.SIGTERM
	if err := <-done; err == nil || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want failed-closed suspension", err)
	}
	if elapsed := time.Since(started); elapsed > 120*time.Millisecond {
		t.Fatalf("two Workers used sequential deadlines: %s", elapsed)
	}
	firstDeadline, secondDeadline := <-deadlines, <-deadlines
	if !firstDeadline.Equal(secondDeadline) {
		t.Fatalf("Workers received different suspension deadlines: %s and %s", firstDeadline, secondDeadline)
	}
	got := store.LoadValue()
	for _, run := range got.Runs {
		if run.Status != scheduler.StatusNeedsHuman || run.PID != 0 {
			t.Fatalf("timed-out Run = %#v", run)
		}
	}
	if len(got.Leases) != 2 {
		t.Fatalf("Leases = %#v, want both retained", got.Leases)
	}
}

func TestRunnerPipelinesHealthyWorkerWhileAnotherBoundaryTimesOut(t *testing.T) {
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{
			{Number: 53, CreatedAt: time.Now()}, {Number: 54, CreatedAt: time.Now().Add(time.Second)},
		},
		completionFunc: func(ctx context.Context, _ int, _ string) (ghadapter.CompletionOutcome, error) {
			select {
			case <-ctx.Done():
				return ghadapter.CompletionOutcome{}, ctx.Err()
			default:
				return ghadapter.CompletionOutcome{}, nil
			}
		},
	}
	workers := newFakeWorkers()
	workers.suspendFunc = func(ctx context.Context, issue int, request worker.ContinuationRequest) (worker.Continuation, error) {
		if issue == 53 {
			<-ctx.Done()
			return worker.Continuation{}, ctx.Err()
		}
		return worker.Continuation{
			SessionID: request.SessionID, SessionFile: request.SessionDir + "/session.jsonl", Worktree: request.Worktree,
			LeafID: "leaf", EntryCount: 1, SHA256: "hash",
		}, nil
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 2)
	runner.Config.SuspensionTimeout = 200 * time.Millisecond
	runner.Signals = signals
	output := newSynchronizedOutput()
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 53, 54)
	signals <- syscall.SIGTERM
	if err := <-done; err == nil || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want one failed-closed suspension", err)
	}
	output.waitFor(t, "Force stop: suspension deadline expired; requesting force stop for 1 Worker; each identity will be revalidated before signaling")
	got := store.LoadValue()
	statuses := map[int]scheduler.Status{}
	for _, run := range got.Runs {
		statuses[run.Issue] = run.Status
	}
	if statuses[53] != scheduler.StatusNeedsHuman || statuses[54] != scheduler.StatusSuspended {
		t.Fatalf("mixed-speed suspension statuses = %v, want needs-human/suspended", statuses)
	}
	if len(got.Leases) != 2 {
		t.Fatalf("Leases = %#v, want both retained", got.Leases)
	}
}

func TestRunnerGitHubCompletionWinsOverSuspension(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 45, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{45: mergedOutcome(45)},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	workers.onCloseContext = func(issue int) error {
		persisted := store.LoadValue()
		run := findActiveRun(&persisted, issue)
		if run.Status != scheduler.StatusRunning || run.PID == 0 || run.Continuation == nil || len(persisted.Leases) != 1 {
			return fmt.Errorf("GitHub outcome was persisted before process-group exit: %#v", persisted)
		}
		return nil
	}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 45)
	signals <- syscall.SIGTERM
	if err := <-done; !isSignalExit(err, 143) {
		t.Fatalf("run: %v, want signal exit 143", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusMerged || len(got.Leases) != 0 {
		t.Fatalf("Completion after suspension boundary = %#v", got)
	}
	if runner.Worktrees.(*fakeWorktrees).cleanupCount() != 1 {
		t.Fatal("completed suspension did not clean its worktree after Worker exit")
	}
}

func TestRunnerRetainsPIDAndLeaseWhenSuspensionCannotVerifyProcessGroupExit(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 46, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{46: mergedOutcome(46)},
	}
	workers := newFakeWorkers()
	workers.onCloseContext = func(int) error { return errors.New("process group still exists") }
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	output := newSynchronizedOutput()
	runner.Output = output
	runner.Signals = signals
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 46)
	signals <- syscall.SIGTERM
	if err := <-done; err == nil || !strings.Contains(err.Error(), "could not verify or stop 1 Worker") {
		t.Fatalf("run: %v, want unverified-exit failure", err)
	}
	output.waitFor(t, "Suspension: 1 Worker remaining")
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID == 0 || got.Runs[0].ProcessIdentity == "" || len(got.Leases) != 1 {
		t.Fatalf("unverified process-group exit = %#v", got)
	}
}

func TestRunnerRechecksWorkerIdentityImmediatelyBeforeTimeoutForceStop(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 48, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.authorizeClose = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	var identityMu sync.Mutex
	identityChecks := 0
	runner.PIDIdentity = func(_ context.Context, pid int) (string, error) {
		identityMu.Lock()
		defer identityMu.Unlock()
		identityChecks++
		if identityChecks >= 3 {
			return "reused-process", nil
		}
		return fmt.Sprintf("identity-%d", pid), nil
	}
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 48)
	signals <- syscall.SIGTERM
	err := <-done
	if !isSignalExit(err, 143) || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want failed-closed signal exit 143", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID == 0 || len(got.Leases) != 1 {
		t.Fatalf("Run after force-stop identity mismatch = %#v", got)
	}
}

func TestRunnerBoundsImmediatePreSignalIdentityRevalidation(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 74, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.authorizeClose = true
	workers.waitForForce = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 20 * time.Millisecond
	var identityMu sync.Mutex
	identityChecks := 0
	runner.PIDIdentity = func(ctx context.Context, pid int) (string, error) {
		identityMu.Lock()
		identityChecks++
		check := identityChecks
		identityMu.Unlock()
		if check >= 3 {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return fmt.Sprintf("identity-%d", pid), nil
	}
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 74)
	started := time.Now()
	signals <- syscall.SIGTERM
	err := <-done
	if !isSignalExit(err, 143) || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want bounded failed-closed signal exit 143", err)
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("identity revalidation was not bounded: %s", elapsed)
	}
	if got := workers.authorizedForceStopCount(); got != 0 {
		t.Fatalf("authorized force stops = %d, want 0", got)
	}
}

func TestRunnerRefusesForceStopWhenDurablePIDChanges(t *testing.T) {
	const issue = 75
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.authorizeClose = true
	workers.waitForForce = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 3)
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 5 * time.Second
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- os.Interrupt
	signals <- os.Interrupt
	<-workers.closeContextStarted
	persisted := store.LoadValue()
	persisted.Runs[0].PID++
	persisted.Runs[0].ProcessIdentity = fmt.Sprintf("identity-%d", persisted.Runs[0].PID)
	if err := store.Save(persisted); err != nil {
		t.Fatalf("persist changed Worker PID: %v", err)
	}
	signals <- os.Interrupt
	err := <-done
	if !isSignalExit(err, 130) || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want failed-closed signal exit 130", err)
	}
	if got := workers.authorizedForceStopCount(); got != 0 {
		t.Fatalf("authorized force stops = %d, want 0 for durable PID mismatch", got)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID == 0 || len(got.Leases) != 1 {
		t.Fatalf("Run after durable PID mismatch = %#v", got)
	}
}

func TestRunnerTreatsCloseErrorAsFailedSuspensionAfterVerifiedExit(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 49, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 49)
	workers.setCloseResult(49, worker.Result{Err: errors.New("truncated post-settlement RPC output")})
	signals <- syscall.SIGTERM
	if err := <-done; err == nil || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want failed close validation", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 0 || !strings.Contains(got.Runs[0].Error, "close RPC Worker") || len(got.Leases) != 1 {
		t.Fatalf("Run after close validation error = %#v", got)
	}
}

func TestRunnerDoesNotAbortBeforeIdentityAuthorizedCloseAfterBoundaryFailure(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 52, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.authorizeClose = true
	workers.suspendFunc = func(context.Context, int, worker.ContinuationRequest) (worker.Continuation, error) {
		return worker.Continuation{}, errors.New("boundary verification failed")
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	var identityMu sync.Mutex
	identityChecks := 0
	runner.PIDIdentity = func(_ context.Context, pid int) (string, error) {
		identityMu.Lock()
		defer identityMu.Unlock()
		identityChecks++
		if identityChecks >= 3 {
			return "reused-process", nil
		}
		return fmt.Sprintf("identity-%d", pid), nil
	}
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 52)
	signals <- syscall.SIGTERM
	err := <-done
	if !isSignalExit(err, 143) || !strings.Contains(err.Error(), "could not verify or stop") {
		t.Fatalf("run: %v, want failed-closed signal exit 143", err)
	}
	if got := workers.abortedCount(); got != 0 {
		t.Fatalf("Abort called %d times before identity-authorized close", got)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID == 0 || len(got.Leases) != 1 {
		t.Fatalf("Run after boundary failure identity mismatch = %#v", got)
	}
}

func TestRunnerGitHubReconciliationErrorPreventsCleanSuspension(t *testing.T) {
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{{Number: 50, CreatedAt: time.Now()}},
		completionFunc: func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
			return ghadapter.CompletionOutcome{}, errors.New("GitHub unavailable")
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 50)
	signals <- syscall.SIGTERM
	if err := <-done; err == nil || !strings.Contains(err.Error(), "require human") {
		t.Fatalf("run: %v, want failed GitHub reconciliation", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 0 || got.Runs[0].Continuation == nil || !strings.Contains(got.Runs[0].Error, "GitHub unavailable") || len(got.Leases) != 1 {
		t.Fatalf("Run after GitHub reconciliation error = %#v", got)
	}
}

func TestRunnerMergedOpenIssueWinsOverSuspension(t *testing.T) {
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{{Number: 51, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{51: {
			PRFound: true, PullRequest: "https://example.test/51", Merged: true, IssueClosed: false,
		}},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 51)
	signals <- syscall.SIGTERM
	if err := <-done; !isSignalExit(err, 143) {
		t.Fatalf("run: %v, want signal exit 143", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 0 || !strings.Contains(got.Runs[0].Error, "issue remains open") || len(got.Leases) != 1 {
		t.Fatalf("merged-open outcome after suspension = %#v", got)
	}
}

func TestRunnerGitHubWaitingOutcomeWinsOverSuspension(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 43, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{43: {PRFound: true, PullRequest: "https://example.test/43", AutoMergeArmed: true}},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 43)
	signals <- syscall.SIGTERM
	if err := <-done; !isSignalExit(err, 143) {
		t.Fatalf("run: %v, want signal exit 143", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusWaitingForMerge || got.Runs[0].PID != 0 || got.Runs[0].Continuation == nil || len(got.Leases) != 1 {
		t.Fatalf("waiting outcome after suspension = %#v", got)
	}
}

func isSignalExit(err error, code int) bool {
	var exit *SignalExit
	return errors.As(err, &exit) && exit.Code == code
}

func assertOnlyIssueFourIsLeased(t *testing.T, workers *fakeWorkers, store *memoryStore) {
	t.Helper()
	if got := store.runStatus(4); got != scheduler.StatusRunning {
		t.Fatalf("issue 4 status = %q, want running during discovery failure", got)
	}
	if workers.wasStarted(5) || workers.wasStarted(6) {
		t.Fatalf("Workers started from failed snapshots: %v", workers.startedSnapshot())
	}
	leases := store.LoadValue().Leases
	if len(leases) != 1 || leases[0].Issue != 4 || leases[0].RunID != "run-4" {
		t.Fatalf("Leases = %#v, want only issue #4 Run run-4", leases)
	}
}

type diagnosticWriter struct {
	mu    sync.Mutex
	text  bytes.Buffer
	match string
	seen  chan struct{}
	once  sync.Once
}

func newDiagnosticWriter(match string) *diagnosticWriter {
	return &diagnosticWriter{match: match, seen: make(chan struct{})}
}

func (w *diagnosticWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.text.Write(p)
	if strings.Contains(w.text.String(), w.match) {
		w.once.Do(func() { close(w.seen) })
	}
	return n, err
}

func (w *diagnosticWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.text.String()
}

func testRunner(github *fakeGitHub, workers *fakeWorkers, store *memoryStore, maxWorkers int) *Runner {
	return &Runner{
		Config:            Config{Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: maxWorkers, PollInterval: 5 * time.Millisecond, SessionsDir: "/tmp/backlog-sessions"},
		GitHub:            github,
		Store:             store,
		Worktrees:         &fakeWorktrees{},
		Workers:           workers,
		Now:               func() time.Time { return time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC) },
		NewRunID:          func(issue int) string { return fmt.Sprintf("run-%d", issue) },
		PIDAlive:          func(int) bool { return true },
		ProcessGroupAlive: func(int) (bool, error) { return false, nil },
		PIDIdentity:       func(_ context.Context, pid int) (string, error) { return fmt.Sprintf("identity-%d", pid), nil },
	}
}

type memoryStore struct {
	mu           sync.Mutex
	value        state.State
	saveCount    int
	failAtSave   int
	failNextSave bool
}

func (s *memoryStore) Load() (state.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneState(s.value), nil
}
func (s *memoryStore) Save(value state.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCount++
	if s.failNextSave || s.failAtSave > 0 && s.saveCount == s.failAtSave {
		s.failNextSave = false
		return errors.New("injected state save failure")
	}
	s.value = cloneState(value)
	return nil
}
func (s *memoryStore) failNext() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNextSave = true
}
func (s *memoryStore) runStatus(issue int) scheduler.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, run := range s.value.Runs {
		if run.Issue == issue {
			return run.Status
		}
	}
	return ""
}
func (s *memoryStore) LoadValue() state.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneState(s.value)
}
func cloneState(value state.State) state.State {
	value.Runs = append([]scheduler.Run(nil), value.Runs...)
	value.Leases = append([]scheduler.Lease(nil), value.Leases...)
	return value
}

type candidateResult struct {
	candidates []scheduler.Candidate
	err        error
}

type fakeGitHub struct {
	mu                 sync.Mutex
	candidates         []scheduler.Candidate
	candidateResults   []candidateResult
	candidateCallTimes []time.Time
	candidateChanged   chan struct{}
	candidatesFunc     func(context.Context) ([]scheduler.Candidate, error)
	completions        map[int]ghadapter.CompletionOutcome
	completionFunc     func(context.Context, int, string) (ghadapter.CompletionOutcome, error)
	completionErrs     map[int]error
	completionCheck    func(int) error
	completionBranches []string
}

func (g *fakeGitHub) Candidates(ctx context.Context, _ string) ([]scheduler.Candidate, error) {
	g.mu.Lock()
	call := len(g.candidateCallTimes)
	g.candidateCallTimes = append(g.candidateCallTimes, time.Now())
	if g.candidateChanged != nil {
		select {
		case g.candidateChanged <- struct{}{}:
		default:
		}
	}
	custom := g.candidatesFunc
	if custom != nil {
		g.mu.Unlock()
		return custom(ctx)
	}
	defer g.mu.Unlock()
	if call < len(g.candidateResults) {
		result := g.candidateResults[call]
		return append([]scheduler.Candidate(nil), result.candidates...), result.err
	}
	return append([]scheduler.Candidate(nil), g.candidates...), nil
}
func (g *fakeGitHub) Completion(ctx context.Context, _ string, issue int, branch string) (ghadapter.CompletionOutcome, error) {
	g.mu.Lock()
	custom := g.completionFunc
	if custom != nil {
		g.mu.Unlock()
		return custom(ctx, issue, branch)
	}
	defer g.mu.Unlock()
	g.completionBranches = append(g.completionBranches, branch)
	if g.completionCheck != nil {
		if err := g.completionCheck(issue); err != nil {
			return ghadapter.CompletionOutcome{}, err
		}
	}
	if err := g.completionErrs[issue]; err != nil {
		return ghadapter.CompletionOutcome{}, err
	}
	outcome := g.completions[issue]
	if outcome.Merged && outcome.IssueClosed {
		remaining := g.candidates[:0]
		for _, candidate := range g.candidates {
			if candidate.Number != issue {
				remaining = append(remaining, candidate)
			}
		}
		g.candidates = remaining
	}
	return outcome, nil
}
func (g *fakeGitHub) setCompletion(issue int, outcome ghadapter.CompletionOutcome) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.completions == nil {
		g.completions = make(map[int]ghadapter.CompletionOutcome)
	}
	g.completions[issue] = outcome
}
func (g *fakeGitHub) setCandidates(candidates []scheduler.Candidate) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.candidates = append([]scheduler.Candidate(nil), candidates...)
}
func (g *fakeGitHub) completionBranchSnapshot() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.completionBranches...)
}
func (g *fakeGitHub) waitForCandidateCalls(t *testing.T, count int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if len(g.candidateCallSnapshot()) >= count {
			return
		}
		select {
		case <-g.candidateChanged:
		case <-deadline:
			t.Fatalf("candidate calls = %d, want at least %d", len(g.candidateCallSnapshot()), count)
		}
	}
}
func (g *fakeGitHub) candidateCallSnapshot() []time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]time.Time(nil), g.candidateCallTimes...)
}
func mergedOutcome(issue int) ghadapter.CompletionOutcome {
	return ghadapter.CompletionOutcome{PRFound: true, PullRequest: fmt.Sprintf("https://example.test/pr/%d", issue), Merged: true, IssueClosed: true}
}

type fakeWorktrees struct {
	mu      sync.Mutex
	cleaned []worktree.Assignment
}

type blockingWorktrees struct {
	fakeWorktrees
	prepareStarted chan struct{}
	finishPrepare  chan struct{}
}

type blockingCleanupWorktrees struct {
	fakeWorktrees
	cleanupStarted chan struct{}
}

type liveContextWorktrees struct {
	fakeWorktrees
	store *memoryStore
	runID string
}

func (w *liveContextWorktrees) Cleanup(ctx context.Context, assignment worktree.Assignment) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("cleanup started with canceled context: %w", err)
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("cleanup context has no deadline")
	}
	persisted := w.store.LoadValue()
	if run := findRun(persisted.Runs, w.runID); run.Status != scheduler.StatusRunning {
		return fmt.Errorf("merged outcome became durable before cleanup: %#v", run)
	}
	return w.fakeWorktrees.Cleanup(ctx, assignment)
}

func (w *blockingCleanupWorktrees) Cleanup(ctx context.Context, _ worktree.Assignment) error {
	close(w.cleanupStarted)
	<-ctx.Done()
	return ctx.Err()
}

func (w *blockingWorktrees) Prepare(ctx context.Context, _ worktree.Assignment) error {
	close(w.prepareStarted)
	select {
	case <-w.finishPrepare:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*fakeWorktrees) Plan(issue int, runID string) (worktree.Assignment, error) {
	return worktree.Assignment{Path: fmt.Sprintf("/tmp/%s", runID), Branch: fmt.Sprintf("agent/issue-%d-%s", issue, runID)}, nil
}
func (*fakeWorktrees) Prepare(context.Context, worktree.Assignment) error { return nil }
func (w *fakeWorktrees) Cleanup(_ context.Context, assignment worktree.Assignment) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cleaned = append(w.cleaned, assignment)
	return nil
}
func (*fakeWorktrees) Exists(worktree.Assignment) bool { return true }
func (w *fakeWorktrees) cleanupCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.cleaned)
}

type fakeProcess struct {
	issue       int
	owner       *fakeWorkers
	done        chan worker.Result
	closeResult worker.Result
	closeOnce   sync.Once
}

func (p *fakeProcess) PID() int { return 1000 + p.issue }
func (p *fakeProcess) Release() error {
	p.owner.released(p.issue)
	return nil
}
func (p *fakeProcess) Abort() error {
	p.owner.recordAbort()
	select {
	case p.done <- worker.Result{ExitCode: -1, Err: context.Canceled}:
	default:
	}
	return nil
}
func (p *fakeProcess) Suspend(ctx context.Context, request worker.ContinuationRequest) (worker.Continuation, error) {
	p.owner.mu.Lock()
	suspendFunc := p.owner.suspendFunc
	p.owner.mu.Unlock()
	if suspendFunc != nil {
		return suspendFunc(ctx, p.issue, request)
	}
	select {
	case p.done <- worker.Result{ExitCode: 0, Settled: true}:
	default:
	}
	return worker.Continuation{
		SessionID: request.SessionID, SessionFile: request.SessionDir + "/session.jsonl", Worktree: request.Worktree,
		LeafID: "leaf", EntryCount: 1, SHA256: "hash",
	}, nil
}
func (p *fakeProcess) Wait() worker.Result {
	return <-p.done
}
func (p *fakeProcess) Close() worker.Result {
	p.closeOnce.Do(func() { p.owner.finished(p.issue) })
	return p.closeResult
}
func (p *fakeProcess) CloseWithForceContext(ctx context.Context, authorizeKill func() error) worker.Result {
	p.owner.mu.Lock()
	blockSettledClose := p.owner.blockSettledClose
	settledCloseStarted := p.owner.settledCloseStarted
	authorizeClose := p.owner.authorizeClose
	p.owner.mu.Unlock()
	if !blockSettledClose {
		result := p.Close()
		result.GroupExited = true
		return result
	}
	settledCloseStarted <- p.issue
	<-ctx.Done()
	result := p.Close()
	if authorizeClose {
		if err := authorizeKill(); err != nil {
			result.Err = err
			return result
		}
		p.owner.recordAuthorizedForceStop()
		result.ForceStopped = true
	}
	result.GroupExited = true
	return result
}

func (p *fakeProcess) CloseContext(ctx context.Context, authorizeKill func() error) worker.Result {
	p.owner.mu.Lock()
	onClose := p.owner.onCloseContext
	authorizeClose := p.owner.authorizeClose
	waitForForce := p.owner.waitForForce
	closeContextStarted := p.owner.closeContextStarted
	p.owner.mu.Unlock()
	if waitForForce {
		closeContextStarted <- p.issue
		<-ctx.Done()
	}
	result := p.Close()
	if authorizeClose {
		if err := authorizeKill(); err != nil {
			result.Err = err
			return result
		}
		p.owner.recordAuthorizedForceStop()
		result.ForceStopped = true
	}
	if onClose != nil {
		if err := onClose(p.issue); err != nil {
			result.Err = err
			return result
		}
	}
	result.GroupExited = true
	return result
}

type fakeWorkers struct {
	mu                   sync.Mutex
	started              []int
	processes            map[int]*fakeProcess
	running              int
	maximum              int
	releases             int
	recoveredReleases    int
	onRelease            func(int)
	onCloseContext       func(int) error
	authorizeClose       bool
	waitForForce         bool
	blockSettledClose    bool
	authorizedForceStops int
	abortCount           int
	suspendFunc          func(context.Context, int, worker.ContinuationRequest) (worker.Continuation, error)
	startChanged         chan struct{}
	closeContextStarted  chan int
	settledCloseStarted  chan int
}

func newFakeWorkers() *fakeWorkers {
	return &fakeWorkers{
		processes: make(map[int]*fakeProcess), startChanged: make(chan struct{}, 20),
		closeContextStarted: make(chan int, 20), settledCloseStarted: make(chan int, 20),
	}
}
func (w *fakeWorkers) Start(_ context.Context, request worker.Request) (WorkerProcess, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	process := &fakeProcess{issue: request.Issue, owner: w, done: make(chan worker.Result, 1)}
	w.processes[request.Issue] = process
	w.started = append(w.started, request.Issue)
	w.running++
	if w.running > w.maximum {
		w.maximum = w.running
	}
	w.startChanged <- struct{}{}
	return process, nil
}
func (w *fakeWorkers) Release(string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.recoveredReleases++
	return nil
}
func (w *fakeWorkers) recordAbort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.abortCount++
}
func (w *fakeWorkers) recordAuthorizedForceStop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.authorizedForceStops++
}
func (w *fakeWorkers) authorizedForceStopCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.authorizedForceStops
}
func (w *fakeWorkers) abortedCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.abortCount
}
func (w *fakeWorkers) released(issue int) {
	w.mu.Lock()
	w.releases++
	onRelease := w.onRelease
	w.mu.Unlock()
	if onRelease != nil {
		onRelease(issue)
	}
}
func (w *fakeWorkers) complete(issue int, result worker.Result) {
	w.mu.Lock()
	process := w.processes[issue]
	w.mu.Unlock()
	if result.Err == nil && result.StreamErr == nil {
		result.Settled = true
	}
	process.done <- result
}
func (w *fakeWorkers) setCloseResult(issue int, result worker.Result) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.processes[issue].closeResult = result
}
func (w *fakeWorkers) finished(int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.running--
}
func (w *fakeWorkers) waitForStarts(t *testing.T, issues ...int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		all := true
		for _, issue := range issues {
			if !w.wasStarted(issue) {
				all = false
			}
		}
		if all {
			return
		}
		select {
		case <-w.startChanged:
		case <-deadline:
			t.Fatalf("started %v, want %v", w.startedSnapshot(), issues)
		}
	}
}
func (w *fakeWorkers) waitForNoRunningWorkers(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for w.runningCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := w.runningCount(); got != 0 {
		t.Fatalf("running Workers = %d, want zero", got)
	}
}
func (w *fakeWorkers) wasStarted(issue int) bool {
	return w.startCount(issue) > 0
}
func (w *fakeWorkers) startCount(issue int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	count := 0
	for _, started := range w.started {
		if started == issue {
			count++
		}
	}
	return count
}
func (w *fakeWorkers) startedSnapshot() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]int(nil), w.started...)
}
func (w *fakeWorkers) maxRunning() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maximum
}
func (w *fakeWorkers) runningCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}
func (w *fakeWorkers) releaseCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.releases
}
func (w *fakeWorkers) recoveredReleaseCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.recoveredReleases
}

type synchronizedOutput struct {
	mu      sync.Mutex
	content strings.Builder
	changed chan struct{}
}

func newSynchronizedOutput() *synchronizedOutput {
	return &synchronizedOutput{changed: make(chan struct{}, 20)}
}

func (w *synchronizedOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	written, err := w.content.Write(data)
	select {
	case w.changed <- struct{}{}:
	default:
	}
	return written, err
}

func (w *synchronizedOutput) waitFor(t *testing.T, text string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		w.mu.Lock()
		found := strings.Contains(w.content.String(), text)
		w.mu.Unlock()
		if found {
			return
		}
		select {
		case <-w.changed:
		case <-deadline:
			w.mu.Lock()
			output := w.content.String()
			w.mu.Unlock()
			t.Fatalf("output did not contain %q:\n%s", text, output)
		}
	}
}
