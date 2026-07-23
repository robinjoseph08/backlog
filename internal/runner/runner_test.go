package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	worktrees := runner.Worktrees.(*fakeWorktrees)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 12)
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
	output.waitFor(t, "Drain: admission stopped; 0 Workers remaining; next SIGINT will request suspension")
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
	signals <- os.Interrupt
	close(finishPrepare)
	workers.waitForStarts(t, 14)
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining; next SIGINT will request suspension")
	output.waitFor(t, "Drain: additional interrupt observed; 1 Worker remaining; next SIGINT will request suspension")
	github.setCompletion(14, mergedOutcome(14))
	workers.complete(14, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 1 || got.Runs[0].Status != scheduler.StatusMerged || len(got.Leases) != 0 {
		t.Fatalf("state after drained Worker settlement = %#v", got)
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

func TestRunnerKeepsActiveWorkerRunningThroughCandidateDiscoveryFailure(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("native blockers: TLS handshake timeout")
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{candidates: []scheduler.Candidate{{Number: 4, CreatedAt: time.Now()}}},
			{candidates: []scheduler.Candidate{{Number: 12, CreatedAt: time.Now()}}, err: transientErr},
		},
		candidateChanged: make(chan struct{}, 4),
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 2)
	runner.Config.PollInterval = 200 * time.Millisecond
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
	if err := <-done; err == nil || !strings.Contains(err.Error(), transientErr.Error()) {
		t.Fatalf("run error = %v, want useful candidate discovery error after Worker completion", err)
	}
	if got := store.runStatus(4); got != scheduler.StatusMerged {
		t.Fatalf("issue 4 status = %q, want merged", got)
	}
	if !strings.Contains(output.String(), transientErr.Error()) {
		t.Fatalf("runner output = %q, want candidate discovery diagnostic", output.String())
	}
}

func TestRunnerReturnsCandidateDiscoveryErrorAfterWaitingRunReconciles(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("candidate discovery unavailable")
	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: transientErr}, {err: transientErr}},
		candidateChanged: make(chan struct{}, 2),
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
	runner.Config.PollInterval = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	github.waitForCandidateCalls(t, 1)
	github.setCompletion(4, mergedOutcome(4))

	if err := <-done; err == nil || !strings.Contains(err.Error(), transientErr.Error()) {
		t.Fatalf("run error = %v, want candidate discovery error after waiting Run reconciliation", err)
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
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{err: errors.New("GitHub unavailable")},
			{candidates: []scheduler.Candidate{{Number: 7, CreatedAt: time.Now()}}},
		},
		candidateChanged: make(chan struct{}, 4),
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	runner.Config.PollInterval = pollInterval
	runner.Config.Watch = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	workers.waitForStarts(t, 7)
	calls := github.candidateCallSnapshot()
	if elapsed := calls[1].Sub(calls[0]); elapsed < pollInterval {
		t.Fatalf("idle candidate discovery retried after %s, want no sooner than %s", elapsed, pollInterval)
	}
	workers.complete(7, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	workers.waitForNoRunningWorkers(t)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerReturnsCandidateDiscoveryErrorWithoutWatchOrUnfinishedRun(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: errors.New("native blockers: TLS handshake timeout")}},
		candidateChanged: make(chan struct{}, 1),
	}
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "reconcile GitHub backlog: native blockers: TLS handshake timeout") {
		t.Fatalf("run error = %v, want candidate discovery failure", err)
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

func testRunner(github *fakeGitHub, workers *fakeWorkers, store *memoryStore, max int) *Runner {
	return &Runner{
		Config:      Config{Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: max, PollInterval: 5 * time.Millisecond, SessionsDir: "/tmp/backlog-sessions"},
		GitHub:      github,
		Store:       store,
		Worktrees:   &fakeWorktrees{},
		Workers:     workers,
		Now:         func() time.Time { return time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC) },
		NewRunID:    func(issue int) string { return fmt.Sprintf("run-%d", issue) },
		PIDAlive:    func(int) bool { return true },
		PIDIdentity: func(pid int) (string, error) { return fmt.Sprintf("identity-%d", pid), nil },
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
func (g *fakeGitHub) Completion(_ context.Context, _ string, issue int, branch string) (ghadapter.CompletionOutcome, error) {
	g.mu.Lock()
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
	select {
	case p.done <- worker.Result{ExitCode: -1, Err: context.Canceled}:
	default:
	}
	return nil
}
func (p *fakeProcess) Wait() worker.Result {
	return <-p.done
}
func (p *fakeProcess) Close() worker.Result {
	p.closeOnce.Do(func() { p.owner.finished(p.issue) })
	return p.closeResult
}

type fakeWorkers struct {
	mu                sync.Mutex
	started           []int
	processes         map[int]*fakeProcess
	running           int
	maximum           int
	releases          int
	recoveredReleases int
	onRelease         func(int)
	startChanged      chan struct{}
}

func newFakeWorkers() *fakeWorkers {
	return &fakeWorkers{processes: make(map[int]*fakeProcess), startChanged: make(chan struct{}, 20)}
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
			t.Fatalf("output did not contain %q", text)
		}
	}
}
