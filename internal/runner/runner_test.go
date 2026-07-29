package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

func TestRunnerNaturalExhaustionReportsStartupAttentionAndIgnoresReleasedHistory(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{
			{Issue: 1, RunID: "retained-failure", Status: scheduler.StatusFailed},
			{Issue: 2, RunID: "released-failure", Status: scheduler.StatusFailed},
			{Issue: 3, RunID: "incomplete-reset", Status: scheduler.StatusResetting},
		},
		Leases: []scheduler.Lease{
			{LeaseID: "retained-failure", Issue: 1, RunID: "retained-failure"},
			{LeaseID: "incomplete-reset", Issue: 3, RunID: "incomplete-reset"},
		},
	}}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	var summary state.State
	runner.FinalSummary = func(current state.State) error {
		summary = current
		return nil
	}

	err := runner.Run(context.Background())
	assertInterventionRequired(t, err, 2)
	if len(summary.Runs) != 3 || len(summary.Leases) != 2 {
		t.Fatalf("final summary state = %#v, want complete aggregate state", summary)
	}
}

func TestRunnerNaturalExhaustionSucceedsWithoutAttention(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs:    []scheduler.Run{{Issue: 1, RunID: "released-failure", Status: scheduler.StatusFailed}},
	}}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	summaries := 0
	runner.FinalSummary = func(state.State) error {
		summaries++
		return nil
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("natural exhaustion: %v", err)
	}
	if summaries != 1 {
		t.Fatalf("final summaries = %d, want 1", summaries)
	}
}

func TestRunnerNaturalExhaustionReturnsSummaryFailureAsOperationalError(t *testing.T) {
	t.Parallel()

	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.FinalSummary = func(state.State) error { return errors.New("output unavailable") }
	err := runner.Run(context.Background())
	var intervention *InterventionRequired
	if err == nil || errors.As(err, &intervention) || !strings.Contains(err.Error(), "print final aggregate summary: output unavailable") {
		t.Fatalf("natural exhaustion error = %v, want distinguishable summary output failure", err)
	}
}

func TestRunnerWatchDoesNotExhaustWhenAttentionRemains(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs:    []scheduler.Run{{Issue: 1, RunID: "retained-failure", Status: scheduler.StatusFailed}},
		Leases:  []scheduler.Lease{{LeaseID: "retained-failure", Issue: 1, RunID: "retained-failure"}},
	}}
	github := &fakeGitHub{candidateChanged: make(chan struct{}, 2)}
	runner := testRunner(github, newFakeWorkers(), store, 1)
	runner.Config.Watch = true
	summaries := 0
	runner.FinalSummary = func(state.State) error {
		summaries++
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	github.waitForCandidateCalls(t, 1)
	select {
	case err := <-done:
		t.Fatalf("watch exited with Attention Required: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch cancellation: %v", err)
	}
	if summaries != 0 {
		t.Fatalf("watch printed %d final summaries, want none", summaries)
	}
}

func TestRunnerStartupClosesOrphanedWorkerLogMarkers(t *testing.T) {
	t.Parallel()

	completedAt := time.Now()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: 27, RunID: "run-27", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC,
			SessionID: "backlog-run-27", SessionDir: "/tmp/backlog-sessions/run-27",
			LogPath: "/tmp/run-27.jsonl", WorkerLogOpen: true, CompletedAt: &completedAt,
		}},
	}}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("restart Runner: %v", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 1 || got.Runs[0].WorkerLogOpen {
		t.Fatalf("state after Runner restart = %#v, want orphaned Worker log closed", got)
	}
}

func TestRunnerStartupRetriesPendingMergedCleanup(t *testing.T) {
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: 28, RunID: "run-28", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModeRPC,
			Branch: "agent/issue-28-run-28", Worktree: "/tmp/run-28", CleanupPending: true,
			Error: "completion verified; worktree cleanup remains pending",
		}},
	}}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	worktrees := runner.Worktrees.(*fakeWorktrees)
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("restart Runner: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].CleanupPending || got.Runs[0].Error != "" || worktrees.cleanupCount() != 1 {
		t.Fatalf("pending merged cleanup after restart = %#v, cleanup count = %d", got, worktrees.cleanupCount())
	}
}

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
		assertInterventionRequired(t, err, 1)
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
	assertInterventionRequired(t, <-done, 1)
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
	assertInterventionRequired(t, <-done, 1)
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
	assertInterventionRequired(t, <-done, 1)
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

func TestRunnerShutsDownWorkersWhenLogClosureSaveFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		completedClose          worker.Result
		abortClosesProcessGroup bool
		wantStatus              scheduler.Status
		wantPID                 int
		wantAborts              int
	}{
		{
			name: "completed process group exits after abort", completedClose: worker.Result{LogClosed: true},
			abortClosesProcessGroup: true, wantStatus: scheduler.StatusMerged, wantAborts: 2,
		},
		{
			name: "completed close reports an error", completedClose: worker.Result{LogClosed: true, GroupExited: true, Err: errors.New("close failed")},
			wantStatus: scheduler.StatusNeedsHuman, wantAborts: 1,
		},
		{
			name: "completed process group remains live", completedClose: worker.Result{LogClosed: true},
			wantStatus: scheduler.StatusNeedsHuman, wantPID: 1012, wantAborts: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			github := &fakeGitHub{candidates: []scheduler.Candidate{
				{Number: 12, CreatedAt: time.Now()},
				{Number: 13, CreatedAt: time.Now().Add(time.Second)},
			}}
			workers := newFakeWorkers()
			workers.startupCloseResult = worker.Result{LogClosed: true, GroupExited: true}
			workers.settledCloseLeavesGroup = true
			workers.abortClosesProcessGroup = test.abortClosesProcessGroup
			// Startup writes once, then each Worker writes its Lease, planned and
			// prepared worktree, log paths, and process identity. The completed Run
			// is save 12 and its log closure marker is save 13.
			store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 13}
			runner := testRunner(github, workers, store, 2)
			done := make(chan error, 1)
			go func() { done <- runner.Run(context.Background()) }()

			workers.waitForStarts(t, 12, 13)
			workers.setCloseResult(12, test.completedClose)
			github.setCompletion(12, mergedOutcome(12))
			workers.complete(12, worker.Result{ExitCode: 0})

			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "persist closed Worker log") {
					t.Fatalf("run error = %v, want Worker log closure persistence failure", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Runner did not shut down after Worker log closure persistence failed")
			}
			if got := workers.abortedCount(); got != test.wantAborts {
				t.Fatalf("aborted Workers = %d, want %d", got, test.wantAborts)
			}
			if got := workers.runningCount(); got != 0 {
				t.Fatalf("running Workers = %d, want all Workers closed", got)
			}
			got := store.LoadValue()
			completed := findRun(got.Runs, "run-12")
			failed := findRun(got.Runs, "run-13")
			if completed.Status != test.wantStatus || completed.WorkerLogOpen || completed.PID != test.wantPID {
				t.Fatalf("completed Run after closure save failure = %#v, want status %s and PID %d", completed, test.wantStatus, test.wantPID)
			}
			if test.wantStatus == scheduler.StatusNeedsHuman && (len(got.Leases) != 2 || completed.Error == "") {
				t.Fatalf("unverified completed Run did not retain its Lease and diagnostic: %#v", got)
			}
			if failed.Status != scheduler.StatusFailed || failed.WorkerLogOpen || !strings.Contains(failed.Error, "log closure persistence failed") {
				t.Fatalf("other Run after closure save failure = %#v", failed)
			}
		})
	}
}

func TestRunnerFailsClosedWhenRPCOutputBreaksAfterSettlement(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 10, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		return true, errors.New("External Resolution must not override failed RPC stream validation")
	})
	worktrees := runner.Worktrees.(*fakeWorktrees)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 10)
	github.setCompletion(10, mergedOutcome(10))
	workers.setCloseResult(10, worker.Result{Err: errors.New("message followed agent_settled")})
	workers.complete(10, worker.Result{ExitCode: 0})
	assertInterventionRequired(t, <-done, 1)
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
	if resolutionCalls.Load() != 0 || len(github.completionBranchSnapshot()) != 1 {
		t.Fatalf("unsafe post-settlement reconciliation: resolutions=%d Completion checks=%d", resolutionCalls.Load(), len(github.completionBranchSnapshot()))
	}
}

func TestRunnerPostSettlementCloseFailureSkipsExternalResolution(t *testing.T) {
	var completionCalls atomic.Int32
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{{Number: 14, CreatedAt: time.Now()}},
		completionFunc: func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
			if completionCalls.Add(1) == 1 {
				return ghadapter.CompletionOutcome{IssueClosed: true, PRFound: true, PullRequest: "https://example.test/14", AutoMergeArmed: true}, nil
			}
			return ghadapter.CompletionOutcome{IssueClosed: true}, nil
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(_ context.Context, run scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		persisted := store.LoadValue()
		resolved := findRun(persisted.Runs, run.RunID)
		resolved.Status = scheduler.StatusResolvedExternally
		replaceRun(&persisted, resolved)
		removeLease(&persisted, run.RunID)
		return true, store.Save(persisted)
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 14)
	workers.setCloseResult(14, worker.Result{LogClosed: true, GroupExited: true, Err: errors.New("RPC close validation failed")})
	workers.complete(14, worker.Result{ExitCode: 0})
	assertInterventionRequired(t, <-done, 1)

	got := store.LoadValue()
	run := findRun(got.Runs, "run-14")
	if completionCalls.Load() != 1 || resolutionCalls.Load() != 0 || run.Status != scheduler.StatusNeedsHuman || len(got.Leases) != 1 {
		t.Fatalf("failed RPC close state = %#v, Completion checks=%d resolutions=%d", got, completionCalls.Load(), resolutionCalls.Load())
	}
	if runner.Worktrees.(*fakeWorktrees).cleanupCount() != 0 {
		t.Fatal("failed RPC close started cleanup")
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
	assertInterventionRequired(t, <-done, 1)
	if worktrees.cleanupCount() != 0 {
		t.Fatalf("cleanup count = %d, want failed worktree retained", worktrees.cleanupCount())
	}
	run := store.LoadValue().Runs[0]
	if run.LogPath != "/logs/run-11.jsonl" || run.StderrPath != "/logs/run-11.stderr.log" {
		t.Fatalf("failed Run lost startup log identities: %#v", run)
	}
}

func TestRunnerPersistsObservableWorkerContextBeforeRelease(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{
		Number: 7, Title: "Make Runs observable", URL: "https://github.com/acme/widgets/issues/7", CreatedAt: time.Now(),
	}}}
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
		if run.Status != scheduler.StatusRunning || run.PID != 1000+issue || run.ProcessIdentity == "" || !run.WorkerLogOpen ||
			run.IssueTitle != "Make Runs observable" || run.IssueURL != "https://github.com/acme/widgets/issues/7" ||
			run.LogPath != "/logs/run-7.jsonl" || run.StderrPath != "/logs/run-7.stderr.log" {
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
	assertInterventionRequired(t, <-done, 1)
	var admitted scheduler.Run
	for _, saved := range store.SaveHistory() {
		if len(saved.Runs) == 1 && len(saved.Leases) == 1 && saved.Runs[0].Status == scheduler.StatusClaimed {
			admitted = saved.Runs[0]
			break
		}
	}
	if admitted.RunID == "" || admitted.IssueTitle != "Make Runs observable" || admitted.IssueURL != "https://github.com/acme/widgets/issues/7" {
		t.Fatalf("Run admission did not durably snapshot issue context: %#v", admitted)
	}
}

func TestRunnerPersistsLogIdentitiesBeforeInspectingWorkerPID(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{
		Number: 21, Title: "Observe startup", URL: "https://github.com/acme/widgets/issues/21", CreatedAt: time.Now(),
	}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	identityStarted := make(chan struct{})
	inspectIdentity := make(chan struct{})
	runner := testRunner(github, workers, store, 1)
	runner.PIDIdentity = func(_ context.Context, pid int) (string, error) {
		close(identityStarted)
		<-inspectIdentity
		return fmt.Sprintf("identity-%d", pid), nil
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 21)
	<-identityStarted

	current := store.LoadValue()
	run := findActiveRun(&current, 21)
	if run.Status != scheduler.StatusWorktreeReady || run.PID != 0 || run.ProcessIdentity != "" ||
		run.IssueTitle != "Observe startup" || run.IssueURL != "https://github.com/acme/widgets/issues/21" ||
		run.LogPath != "/logs/run-21.jsonl" || run.StderrPath != "/logs/run-21.stderr.log" {
		t.Fatalf("Run while PID inspection is blocked = %#v", run)
	}
	if workers.releaseCount() != 0 {
		t.Fatal("Worker was released before startup context became durable")
	}

	close(inspectIdentity)
	workers.complete(21, worker.Result{ExitCode: 1, Err: errors.New("failed")})
	assertInterventionRequired(t, <-done, 1)
}

func TestRunnerDoesNotCommitInMemoryLeaseWhenAdmissionSaveFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 15, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 2}
	runner := testRunner(github, workers, store, 1)

	err := runner.Run(context.Background())
	var intervention *InterventionRequired
	if err == nil || errors.As(err, &intervention) || !strings.Contains(err.Error(), "persist lease for issue #15") {
		t.Fatalf("run error = %v, want distinct Lease persistence failure", err)
	}
	got := store.LoadValue()
	if len(got.Runs) != 0 || len(got.Leases) != 0 {
		t.Fatalf("state after failed admission = %#v, want no Run or Lease", got)
	}
	if workers.wasStarted(15) {
		t.Fatal("Worker started after Lease persistence failed")
	}
}

func TestRunnerStopsGatedWorkerWhenLogPersistenceFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 6, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.startupCloseResult.GroupExited = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 5}
	runner := testRunner(github, workers, store, 1)

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "persist worker logs") {
		t.Fatalf("run error = %v, want log persistence failure", err)
	}
	if workers.runningCount() != 0 {
		t.Fatalf("running workers = %d, want gated worker stopped", workers.runningCount())
	}
	if workers.releaseCount() != 0 {
		t.Fatalf("release count = %d, want gated worker unreleased", workers.releaseCount())
	}
	run := store.LoadValue().Runs[0]
	if run.LogPath != "/logs/run-6.jsonl" || run.StderrPath != "/logs/run-6.stderr.log" {
		t.Fatalf("log persistence failure lost recoverable log identities: %#v", run)
	}
}

func TestRunnerStopsGatedWorkerWhenPIDPersistenceFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 16, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.startupCloseResult.GroupExited = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 6}
	runner := testRunner(github, workers, store, 1)

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "persist worker for issue #16") {
		t.Fatalf("run error = %v, want PID persistence failure", err)
	}
	if workers.runningCount() != 0 || workers.releaseCount() != 0 {
		t.Fatalf("running/released workers = %d/%d, want gated Worker stopped and unreleased", workers.runningCount(), workers.releaseCount())
	}
	run := store.LoadValue().Runs[0]
	if run.LogPath != "/logs/run-16.jsonl" || run.StderrPath != "/logs/run-16.stderr.log" {
		t.Fatalf("PID persistence failure lost startup log identities: %#v", run)
	}
}

func TestRunnerRetainsLogIdentitiesWhenPIDInspectionFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 17, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.startupCloseResult.GroupExited = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDIdentity = func(context.Context, int) (string, error) { return "", errors.New("identity unavailable") }

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	run := store.LoadValue().Runs[0]
	if run.Status != scheduler.StatusFailed || run.LogPath != "/logs/run-17.jsonl" || run.StderrPath != "/logs/run-17.stderr.log" {
		t.Fatalf("Run after PID inspection failure = %#v", run)
	}
}

func TestRunnerRetainsLogIdentitiesWhenWorkerReleaseFails(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 18, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.releaseErr = errors.New("prompt unavailable")
	workers.startupCloseResult.GroupExited = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	run := store.LoadValue().Runs[0]
	if run.Status != scheduler.StatusFailed || run.LogPath != "/logs/run-18.jsonl" || run.StderrPath != "/logs/run-18.stderr.log" {
		t.Fatalf("Run after Worker release failure = %#v", run)
	}
	if workers.runningCount() != 0 {
		t.Fatalf("running Workers = %d, want released Worker stopped", workers.runningCount())
	}
}

func TestRunnerFailsRunWhenWorkerOmitsLogIdentity(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 20, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.omitLogPaths = true
	workers.startupCloseResult.GroupExited = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	run := store.LoadValue().Runs[0]
	if run.Status != scheduler.StatusFailed || !strings.Contains(run.Error, "omitted a JSONL or standard-error log identity") {
		t.Fatalf("Run after incomplete Worker startup = %#v", run)
	}
	if workers.runningCount() != 0 || workers.releaseCount() != 0 {
		t.Fatalf("running/released workers = %d/%d, want gated Worker stopped and unreleased", workers.runningCount(), workers.releaseCount())
	}
}

func TestRunnerRetainsLiveWorkerIdentityWhenGatedShutdownCannotBeVerified(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 22, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.omitLogPaths = true
	workers.startupCloseResult = worker.Result{Err: errors.New("process group still alive")}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "process-group exit was not verified") {
		t.Fatalf("run error = %v, want unverified gated Worker shutdown", err)
	}
	run := store.LoadValue().Runs[0]
	if run.Status != scheduler.StatusNeedsHuman || run.PID != 1022 || !strings.Contains(run.Error, "stop Worker after startup failure") {
		t.Fatalf("Run after unverified gated Worker shutdown = %#v", run)
	}
	if workers.releaseCount() != 0 {
		t.Fatalf("release count = %d, want gated Worker unreleased", workers.releaseCount())
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
	assertInterventionRequired(t, <-done, 1)
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

func TestAdmissionGateSerializesNaturalExhaustionWithDrain(t *testing.T) {
	t.Parallel()

	gate := &admissionGate{}
	if first := gate.stop(); !first {
		t.Fatal("Drain was not accepted")
	}
	called := false
	finished, err := gate.finishNatural(func() error {
		called = true
		return nil
	})
	if err != nil || finished || called {
		t.Fatalf("natural exhaustion after Drain = finished %t, called %t, err %v", finished, called, err)
	}

	gate = &admissionGate{}
	finishStarted := make(chan struct{})
	releaseFinish := make(chan struct{})
	finishDone := make(chan bool, 1)
	go func() {
		finished, finishErr := gate.finishNatural(func() error {
			close(finishStarted)
			<-releaseFinish
			return nil
		})
		if finishErr != nil {
			panic(finishErr)
		}
		finishDone <- finished
	}()
	<-finishStarted
	stopDone := make(chan bool, 1)
	go func() { stopDone <- gate.stop() }()
	select {
	case <-stopDone:
		t.Fatal("Drain was accepted before the natural-exhaustion decision completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFinish)
	if finished := <-finishDone; !finished {
		t.Fatal("natural exhaustion lost after it was accepted first")
	}
	if firstDrain := <-stopDone; !firstDrain {
		t.Fatal("later Drain request was not the first signal transition")
	}
}

func TestRunnerNaturalExhaustionAcceptedBeforeDrainWins(t *testing.T) {
	t.Parallel()

	signals := make(chan os.Signal)
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Signals = signals
	summaryStarted := make(chan struct{})
	releaseSummary := make(chan struct{})
	runner.FinalSummary = func(state.State) error {
		close(summaryStarted)
		<-releaseSummary
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-summaryStarted
	signalReceived := make(chan struct{})
	go func() {
		signals <- os.Interrupt
		close(signalReceived)
	}()
	<-signalReceived
	close(releaseSummary)
	if err := <-done; err != nil {
		t.Fatalf("natural exhaustion accepted before Drain: %v", err)
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
	summaries := 0
	runner.FinalSummary = func(state.State) error {
		summaries++
		return nil
	}

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
	if summaries != 0 {
		t.Fatalf("Drain printed %d natural-exhaustion summaries, want none", summaries)
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

func TestRunnerDrainResolvesIssueClosedDuringSettledWorkerClose(t *testing.T) {
	const issue = 16
	var issueClosed atomic.Bool
	var completionCalls atomic.Int32
	var issueStateCalls atomic.Int32
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completionFunc: func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
			completionCalls.Add(1)
			return ghadapter.CompletionOutcome{
				PullRequest: "https://example.test/pr/16", PRFound: true, AutoMergeArmed: true,
				IssueClosed: issueClosed.Load(),
			}, nil
		},
		issueStateFunc: func(int) (ghadapter.IssueState, error) {
			issueStateCalls.Add(1)
			return ghadapter.IssueState{Open: !issueClosed.Load()}, nil
		},
	}
	workers := newFakeWorkers()
	workers.onSettledClose = func(int) error {
		if completionCalls.Load() != 1 {
			return fmt.Errorf("Worker closed after %d initial Completion checks, want 1", completionCalls.Load())
		}
		issueClosed.Store(true)
		return nil
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(_ context.Context, run scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		persisted := store.LoadValue()
		settled := findRun(persisted.Runs, run.RunID)
		if workers.runningCount() != 0 || settled.WorkerLogOpen {
			return true, fmt.Errorf("Drain resolution started before Worker exit and durable log closure: running=%d run=%#v", workers.runningCount(), settled)
		}
		settled.Status = scheduler.StatusResolvedExternally
		settledAt := runner.Now().UTC()
		settled.ResolvedExternallyAt = &settledAt
		settled.ClosureReason = "completed"
		replaceRun(&persisted, settled)
		removeLease(&persisted, run.RunID)
		return true, store.Save(persisted)
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining")
	workers.complete(issue, worker.Result{ExitCode: 0})

	if err := <-done; err != nil {
		t.Fatalf("Drain late-closure External Resolution: %v", err)
	}
	got := store.LoadValue()
	run := findRun(got.Runs, fmt.Sprintf("run-%d", issue))
	if completionCalls.Load() != 2 || issueStateCalls.Load() != 1 || resolutionCalls.Load() != 1 || run.Status != scheduler.StatusResolvedExternally || run.WorkerLogOpen || len(got.Leases) != 0 {
		t.Fatalf("Drain late-closure state = %#v, Completion checks=%d issue-state checks=%d resolutions=%d", got, completionCalls.Load(), issueStateCalls.Load(), resolutionCalls.Load())
	}
	output.waitFor(t, "Drain complete: 0 Workers remaining; exiting successfully")
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

func TestRunnerDrainRemainsSuccessfulWhenCompletionRequiresAttention(t *testing.T) {
	for _, test := range []struct {
		name            string
		github          *fakeGitHub
		result          worker.Result
		groupExitProven bool
		wantStatus      scheduler.Status
	}{
		{
			name: "GitHub reconciliation",
			github: &fakeGitHub{
				candidates:     []scheduler.Candidate{{Number: 28, CreatedAt: time.Now()}},
				completionErrs: map[int]error{28: errors.New("GitHub unavailable")},
			},
			result:     worker.Result{ExitCode: 0},
			wantStatus: scheduler.StatusNeedsHuman,
		},
		{
			name:            "Pi RPC protocol",
			github:          &fakeGitHub{candidates: []scheduler.Candidate{{Number: 28, CreatedAt: time.Now()}}},
			result:          worker.Result{ExitCode: -1, StreamErr: errors.New("malformed Pi RPC JSON"), Err: errors.New("malformed Pi RPC JSON")},
			groupExitProven: true,
			wantStatus:      scheduler.StatusFailed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			workers := newFakeWorkers()
			workers.abortClosesProcessGroup = test.groupExitProven
			store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
			signals := make(chan os.Signal, 1)
			output := newSynchronizedOutput()
			runner := testRunner(test.github, workers, store, 1)
			runner.Signals = signals
			runner.Output = output
			summaries := 0
			runner.FinalSummary = func(state.State) error {
				summaries++
				return nil
			}

			done := make(chan error, 1)
			go func() { done <- runner.Run(context.Background()) }()
			workers.waitForStarts(t, 28)
			signals <- os.Interrupt
			output.waitFor(t, "Drain: admission stopped; 1 Worker remaining")
			workers.complete(28, test.result)

			if err := <-done; err != nil {
				t.Fatalf("Drain error = %v, want successful orderly shutdown", err)
			}
			if got := store.runStatus(28); got != test.wantStatus {
				t.Fatalf("Run status = %q, want %q", got, test.wantStatus)
			}
			if summaries != 0 {
				t.Fatalf("Drain printed %d natural-exhaustion summaries, want none", summaries)
			}
		})
	}
}

func TestRunnerDrainReportsProcessControlFailureAfterVerifiedExit(t *testing.T) {
	const issue = 32
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	workers.abortClosesProcessGroup = true
	workers.abortErr = errors.New("abort unavailable")
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining")
	streamErr := errors.New("malformed Pi RPC JSON")
	workers.complete(issue, worker.Result{ExitCode: -1, StreamErr: streamErr, Err: streamErr})

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "abort unavailable") {
		t.Fatalf("Drain error = %v, want process-control failure", err)
	}
	events := recorder.waitFor(t, func(events []OperationalEvent) bool {
		_, ok := findShutdownEvent(events, ShutdownStageDrainComplete, "exiting after an operational failure")
		return ok
	})
	terminal, _ := findShutdownEvent(events, ShutdownStageDrainComplete, "exiting after an operational failure")
	if terminal.Result != ShutdownResultFailure {
		t.Fatalf("operational-failure Drain result = %q, want %q", terminal.Result, ShutdownResultFailure)
	}
	got := store.LoadValue()
	run := findRun(got.Runs, fmt.Sprintf("run-%d", issue))
	if run.Status != scheduler.StatusFailed || run.PID != 0 || len(got.Leases) != 1 {
		t.Fatalf("state after verified process exit = %#v", got)
	}
}

func TestRunnerDrainReportsSettledCloseControlFailureAfterVerifiedExit(t *testing.T) {
	const issue = 33
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{issue: mergedOutcome(issue)},
	}
	workers := newFakeWorkers()
	workers.startupCloseResult = worker.Result{GroupExited: true, ControlErr: errors.New("close control failure"), Err: errors.New("close control failure")}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining")
	workers.complete(issue, worker.Result{ExitCode: 0})

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "close control failure") {
		t.Fatalf("Drain error = %v, want settled close control failure", err)
	}
}

func TestRunnerDrainFailsClosedWhenSettledWorkerExitIsUnverified(t *testing.T) {
	const issue = 31
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{issue: mergedOutcome(issue)},
	}
	workers := newFakeWorkers()
	workers.settledCloseLeavesGroup = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining")
	workers.complete(issue, worker.Result{ExitCode: 0})

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "settled Worker process-group exit was not verified") {
		t.Fatalf("Drain error = %v, want unverified settled Worker exit", err)
	}
	output.waitFor(t, "Drain incomplete: 0 supervised Workers remaining; 1 Worker retained with unverified liveness")
	events := recorder.waitFor(t, func(events []OperationalEvent) bool {
		_, ok := findShutdownEvent(events, ShutdownStageDrainIncomplete, "retaining Workers with unverified liveness")
		return ok
	})
	var incomplete *ShutdownEvent
	for _, event := range events {
		if shutdown, ok := event.(ShutdownEvent); ok && shutdown.Stage == ShutdownStageDrainIncomplete {
			incomplete = &shutdown
		}
	}
	if incomplete == nil || incomplete.Action != "retaining Workers with unverified liveness" || incomplete.RemainingWorkers != 0 || incomplete.NextInterrupt != NextInterruptNone {
		t.Fatalf("Drain incomplete event = %#v, want 0 remaining supervised Owned Workers and no next interrupt", incomplete)
	}
	got := store.LoadValue()
	run := findRun(got.Runs, fmt.Sprintf("run-%d", issue))
	if run.Status != scheduler.StatusNeedsHuman || run.PID != 1000+issue || run.ProcessIdentity == "" || len(got.Leases) != 1 {
		t.Fatalf("state after settled Worker exit failure = %#v", got)
	}
}

func TestRunnerDrainFailsClosedWhenMalformedWorkerExitIsUnverified(t *testing.T) {
	const issue = 30
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 1 Worker remaining")
	streamErr := errors.New("malformed Pi RPC JSON")
	workers.complete(issue, worker.Result{ExitCode: -1, StreamErr: streamErr, Err: streamErr})

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "process-group exit was not verified") {
		t.Fatalf("Drain error = %v, want unverified Worker exit", err)
	}
	output.waitFor(t, "Drain incomplete: 0 supervised Workers remaining; 1 Worker retained with unverified liveness")
	got := store.LoadValue()
	run := findRun(got.Runs, fmt.Sprintf("run-%d", issue))
	if run.Status != scheduler.StatusNeedsHuman || run.PID != 1000+issue || run.ProcessIdentity == "" || len(got.Leases) != 1 {
		t.Fatalf("state after malformed Worker exit = %#v", got)
	}
}

func TestRunnerDrainSettlesRemainingWorkersAfterCompletionStateFailure(t *testing.T) {
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{
			{Number: 28, CreatedAt: time.Now()},
			{Number: 29, CreatedAt: time.Now().Add(time.Second)},
		},
		completions: map[int]ghadapter.CompletionOutcome{
			28: mergedOutcome(28),
			29: mergedOutcome(29),
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	output := newSynchronizedOutput()
	runner := testRunner(github, workers, store, 2)
	runner.Signals = signals
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 28, 29)
	signals <- os.Interrupt
	output.waitFor(t, "Drain: admission stopped; 2 Workers remaining")
	store.failNext()
	workers.complete(28, worker.Result{ExitCode: 0})
	output.waitFor(t, "Drain: 1 Worker remaining")
	workers.complete(29, worker.Result{ExitCode: 0})

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "persist completion") || !strings.Contains(err.Error(), "process-group exit was not verified") {
		t.Fatalf("Drain error = %v, want completion state and process-group verification failures", err)
	}
	got := store.LoadValue()
	if findRun(got.Runs, "run-28").Status != scheduler.StatusNeedsHuman || findRun(got.Runs, "run-29").Status != scheduler.StatusMerged {
		t.Fatalf("state after draining through persistence failure = %#v", got)
	}
}

func TestRunnerDrainReportsSecondOrderCompletionRecoveryFailures(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*memoryStore)
		validate  func(*testing.T, error)
	}{
		{
			name: "reload",
			configure: func(store *memoryStore) {
				store.failAtLoad = 2
				store.failNext()
			},
			validate: func(t *testing.T, err error) {
				if !strings.Contains(err.Error(), "injected state load failure") {
					t.Fatalf("Drain error = %v, want recovery reload failure", err)
				}
			},
		},
		{
			name: "recovery save",
			configure: func(store *memoryStore) {
				store.failNextN(2)
			},
			validate: func(t *testing.T, err error) {
				if strings.Count(err.Error(), "injected state save failure") < 2 {
					t.Fatalf("Drain error = %v, want initial and recovery save failures", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			github := &fakeGitHub{
				candidates:  []scheduler.Candidate{{Number: 34, CreatedAt: time.Now()}, {Number: 35, CreatedAt: time.Now().Add(time.Second)}},
				completions: map[int]ghadapter.CompletionOutcome{34: mergedOutcome(34), 35: mergedOutcome(35)},
			}
			workers := newFakeWorkers()
			workers.abortClosesProcessGroup = true
			store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
			signals := make(chan os.Signal, 1)
			output := newSynchronizedOutput()
			runner := testRunner(github, workers, store, 2)
			runner.Signals = signals
			runner.Output = output

			done := make(chan error, 1)
			go func() { done <- runner.Run(context.Background()) }()
			workers.waitForStarts(t, 34, 35)
			waitForPersistedRun(t, store, 35, func(run scheduler.Run) bool {
				return run.Status == scheduler.StatusRunning && run.PID != 0 && run.ProcessIdentity != ""
			})
			signals <- os.Interrupt
			output.waitFor(t, "Drain: admission stopped; 2 Workers remaining")
			test.configure(store)
			workers.complete(34, worker.Result{ExitCode: 0})
			output.waitFor(t, "Drain: 1 Worker remaining")
			workers.complete(35, worker.Result{ExitCode: 0})

			err := <-done
			if err == nil {
				t.Fatal("Drain succeeded despite recovery failure")
			}
			test.validate(t, err)
			if got := store.LoadValue(); findRun(got.Runs, "run-35").Status != scheduler.StatusMerged {
				t.Fatalf("remaining Worker did not settle after recovery failure: %#v", got)
			}
		})
	}
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

func TestRunnerCancellationPreservesShutdownFailure(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 40, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	shutdownErr := errors.New("abort unavailable")
	workers.abortErr = shutdownErr
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	workers.waitForStarts(t, 40)
	cancel()

	if err := <-done; !errors.Is(err, shutdownErr) {
		t.Fatalf("canceled Runner shutdown error = %v, want %v", err, shutdownErr)
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
	assertInterventionRequired(t, <-done, 1)
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
	assertInterventionRequired(t, <-done, 1)
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
	assertInterventionRequired(t, runner.Run(context.Background()), 1)
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

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	if got := store.runStatus(1); got != scheduler.StatusNeedsHuman {
		t.Fatalf("stale PID status = %q, want needs-human", got)
	}
}

func TestRunnerOverAgeRecoveredWorkerRetainsCapacity(t *testing.T) {
	t.Parallel()

	live := scheduler.Run{
		Issue: 1, RunID: "stale-live", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 1234, ProcessIdentity: "identity-1234", Branch: "agent/issue-1-stale-live", Worktree: "/tmp/stale-live",
		SessionID: "backlog-stale-live", SessionDir: "/state/sessions/stale-live",
		StartedAt: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	}
	suspended := resumableRun(t, 2, "resume-2")
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{live, suspended}, Leases: []scheduler.Lease{
			{LeaseID: "lease-1", Issue: 1, RunID: live.RunID},
			{LeaseID: "lease-2", Issue: 2, RunID: suspended.RunID},
		},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(pid int) bool { return pid == 1234 }
	runner.Config.MaxWorkerAge = 24 * time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	waitForPersistedRun(t, store, 1, func(run scheduler.Run) bool {
		return run.Status == scheduler.StatusNeedsHuman && run.PID == 1234
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := store.LoadValue()
	if stale := findActiveRun(&got, 1); stale.Status != scheduler.StatusNeedsHuman || stale.PID != 1234 {
		t.Fatalf("over-age live Worker = %#v", stale)
	}
	if resumed := findActiveRun(&got, 2); resumed.Status != scheduler.StatusSuspended || workers.wasStarted(2) {
		t.Fatalf("capacity-blocked suspended Run = %#v, starts=%v", resumed, workers.startedSnapshot())
	}
}

func TestRunnerUncertainRecoveredWorkerIdentityRetainsCapacity(t *testing.T) {
	t.Parallel()

	live := scheduler.Run{
		Issue: 3, RunID: "uncertain-live", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 1235, ProcessIdentity: "identity-1235", Branch: "agent/issue-3-uncertain-live", Worktree: "/tmp/uncertain-live",
		SessionID: "backlog-uncertain-live", SessionDir: "/state/sessions/uncertain-live", StartedAt: time.Now(),
	}
	suspended := resumableRun(t, 4, "resume-4")
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{live, suspended}, Leases: []scheduler.Lease{
			{LeaseID: "lease-3", Issue: 3, RunID: live.RunID},
			{LeaseID: "lease-4", Issue: 4, RunID: suspended.RunID},
		},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(pid int) bool { return pid == 1235 }
	runner.PIDIdentity = func(context.Context, int) (string, error) { return "", errors.New("identity inspection unavailable") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	waitForPersistedRun(t, store, 3, func(run scheduler.Run) bool {
		return run.Status == scheduler.StatusNeedsHuman && run.PID == 1235
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := store.LoadValue()
	if uncertain := findActiveRun(&got, 3); uncertain.Status != scheduler.StatusNeedsHuman || uncertain.PID != 1235 || !strings.Contains(uncertain.Error, "identity is uncertain") {
		t.Fatalf("uncertain live Worker = %#v", uncertain)
	}
	if resumed := findActiveRun(&got, 4); resumed.Status != scheduler.StatusSuspended || workers.wasStarted(4) {
		t.Fatalf("capacity-blocked suspended Run = %#v, starts=%v", resumed, workers.startedSnapshot())
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

func TestRunnerStartupAutomaticallyResolvesBeforeAdmissionAndNaturalExhaustion(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 1, RunID: "partial", Status: scheduler.StatusResolvingExternally, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "partial", Issue: 1, RunID: "partial"}},
	}}
	github := &fakeGitHub{candidatesFunc: func(context.Context) ([]scheduler.Candidate, error) {
		current := store.LoadValue()
		if len(current.Leases) != 0 || current.Runs[0].Status != scheduler.StatusResolvedExternally {
			return nil, errors.New("Candidate Admission ran before automatic External Resolution persisted")
		}
		return nil, nil
	}}
	runner := testRunner(github, newFakeWorkers(), store, 1)
	runner.ExternalResolution = externalResolutionFunc(func(_ context.Context, run scheduler.Run) (bool, error) {
		current := store.LoadValue()
		resolved := findRun(current.Runs, run.RunID)
		resolved.Status = scheduler.StatusResolvedExternally
		resolved.Error = ""
		resolvedAt := time.Now().UTC()
		resolved.ResolvedExternallyAt = &resolvedAt
		resolved.ClosureReason = "completed"
		replaceRun(&current, resolved)
		removeLease(&current, run.RunID)
		return true, store.Save(current)
	})
	var summary state.State
	runner.FinalSummary = func(current state.State) error {
		summary = cloneState(current)
		return nil
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("automatic startup External Resolution: %v", err)
	}
	if len(summary.Leases) != 0 || summary.Runs[0].Status != scheduler.StatusResolvedExternally {
		t.Fatalf("final summary used stale pre-resolution state: %#v", summary)
	}
	if workers := runner.Workers.(*fakeWorkers).startedSnapshot(); len(workers) != 0 {
		t.Fatalf("External Resolution created a replacement Worker: %v", workers)
	}
}

func TestRunnerReloadsAutomaticResolutionProgressBeforeReturningCancellation(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 6, RunID: "failed", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "failed", Issue: 6, RunID: "failed"}},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		persisted := store.LoadValue()
		partial := findRun(persisted.Runs, "failed")
		partial.Status = scheduler.StatusResolvingExternally
		partial.Error = "durable partial External Resolution progress"
		replaceRun(&persisted, partial)
		if err := store.Save(persisted); err != nil {
			return true, err
		}
		cancel()
		return true, context.Canceled
	})
	current := store.LoadValue()

	err := runner.reconcileExternalResolutions(ctx, &current, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("automatic External Resolution cancellation = %v", err)
	}
	if got := findRun(current.Runs, "failed"); got.Status != scheduler.StatusResolvingExternally || got.Error != "durable partial External Resolution progress" {
		t.Fatalf("reloaded partial External Resolution = %#v", got)
	}
	if err := store.Save(current); err != nil {
		t.Fatal(err)
	}
	if got := findRun(store.LoadValue().Runs, "failed"); got.Status != scheduler.StatusResolvingExternally || got.Error != "durable partial External Resolution progress" {
		t.Fatalf("shutdown persistence overwrote partial External Resolution = %#v", got)
	}
}

func TestRunnerWatchAutomaticallyResolvesClosureDuringPoll(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 2, RunID: "failed", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "failed", Issue: 2, RunID: "failed"}},
	}}
	var calls atomic.Int32
	resolved := make(chan struct{})
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.Config.Watch = true
	runner.ExternalResolution = externalResolutionFunc(func(_ context.Context, run scheduler.Run) (bool, error) {
		if calls.Add(1) == 1 {
			return true, errors.New("issue reopened during verification")
		}
		current := store.LoadValue()
		retired := findRun(current.Runs, run.RunID)
		retired.Status = scheduler.StatusResolvedExternally
		at := time.Now().UTC()
		retired.ResolvedExternallyAt = &at
		retired.ClosureReason = "not-planned"
		replaceRun(&current, retired)
		removeLease(&current, run.RunID)
		if err := store.Save(current); err != nil {
			return true, err
		}
		close(resolved)
		return true, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-resolved:
		cancel()
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("watch reconciliation did not discover issue closure")
	}
	if err := <-done; err != nil {
		t.Fatalf("watch shutdown after External Resolution: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusResolvedExternally || len(got.Leases) != 0 {
		t.Fatalf("watch External Resolution state = %#v", got)
	}
}

func TestRunnerNeverAutomaticallyResolvesRunWithOwnedWorker(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 5, RunID: "running", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC, PID: 100, ProcessIdentity: "identity-100"}},
		Leases: []scheduler.Lease{{LeaseID: "running", Issue: 5, RunID: "running"}},
	}}
	var calls atomic.Int32
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		calls.Add(1)
		return true, errors.New("must not inspect an Owned Worker")
	})
	current := store.LoadValue()
	if err := runner.reconcile(context.Background(), &current, map[int]WorkerProcess{5: nil}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 || current.Runs[0].Status != scheduler.StatusRunning || len(current.Leases) != 1 {
		t.Fatalf("Owned Worker entered automatic External Resolution: calls=%d state=%#v", calls.Load(), current)
	}
}

func TestRunnerResolvesClosedIssueImmediatelyAfterNormalWorkerSettlement(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 5, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{5: {IssueClosed: true}},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(_ context.Context, run scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		persisted := store.LoadValue()
		settled := findRun(persisted.Runs, run.RunID)
		if workers.runningCount() != 0 || settled.WorkerLogOpen {
			return true, fmt.Errorf("resolution started before Worker exit and durable log closure: running=%d run=%#v", workers.runningCount(), settled)
		}
		settled.Status = scheduler.StatusResolvedExternally
		settledAt := runner.Now().UTC()
		settled.ResolvedExternallyAt = &settledAt
		settled.ClosureReason = "completed"
		replaceRun(&persisted, settled)
		removeLease(&persisted, run.RunID)
		github.setCandidates(nil)
		return true, store.Save(persisted)
	})
	var summary state.State
	runner.FinalSummary = func(current state.State) error {
		summary = cloneState(current)
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 5)
	// Several polling and reconciliation ticks while the issue is already closed
	// must not inspect or control the Owned Worker. Close remains part of normal
	// settlement only.
	time.Sleep(20 * time.Millisecond)
	if resolutionCalls.Load() != 0 || workers.runningCount() != 1 || workers.abortedCount() != 0 || workers.suspendedCount() != 0 || workers.closedCount() != 0 || workers.authorizedForceStopCount() != 0 {
		t.Fatalf("closure controlled Owned Worker: resolutions=%d running=%d aborts=%d suspends=%d closes=%d force-stops=%d", resolutionCalls.Load(), workers.runningCount(), workers.abortedCount(), workers.suspendedCount(), workers.closedCount(), workers.authorizedForceStopCount())
	}
	workers.complete(5, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("post-settlement External Resolution: %v", err)
	}
	got := store.LoadValue()
	if resolutionCalls.Load() != 1 || got.Runs[0].Status != scheduler.StatusResolvedExternally || got.Runs[0].WorkerLogOpen || len(got.Leases) != 0 {
		t.Fatalf("post-settlement External Resolution state = %#v, calls=%d", got, resolutionCalls.Load())
	}
	if workers.closedCount() != 1 || workers.abortedCount() != 0 || workers.suspendedCount() != 0 || workers.authorizedForceStopCount() != 0 {
		t.Fatalf("normal settlement controls: closes=%d aborts=%d suspends=%d force-stops=%d", workers.closedCount(), workers.abortedCount(), workers.suspendedCount(), workers.authorizedForceStopCount())
	}
	if len(summary.Leases) != 0 || summary.Runs[0].Status != scheduler.StatusResolvedExternally {
		t.Fatalf("final summary used pre-resolution state: %#v", summary)
	}
	if branches := github.completionBranchSnapshot(); len(branches) != 2 || branches[0] != branches[1] {
		t.Fatalf("Completion branches = %q, want initial and final checks of the expected branch", branches)
	}
}

func TestRunnerRefillsCapacityOnlyAfterPostSettlementExternalResolutionReleasesLease(t *testing.T) {
	first := scheduler.Candidate{Number: 5, CreatedAt: time.Now()}
	second := scheduler.Candidate{Number: 6, CreatedAt: first.CreatedAt.Add(time.Second)}
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{first, second},
		completions: map[int]ghadapter.CompletionOutcome{
			first.Number:  {IssueClosed: true},
			second.Number: mergedOutcome(second.Number),
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	resolutionStarted := make(chan struct{})
	finishResolution := make(chan struct{})
	admissionErr := make(chan error, 1)
	workers.onStart = func(issue int) {
		if issue != second.Number {
			return
		}
		persisted := store.LoadValue()
		resolved := findRun(persisted.Runs, fmt.Sprintf("run-%d", first.Number))
		if resolved.Status != scheduler.StatusResolvedExternally || findActiveRun(&persisted, first.Number).RunID != "" || len(persisted.Leases) != 1 || persisted.Leases[0].Issue != second.Number {
			admissionErr <- fmt.Errorf("second Admission preceded durable External Resolution: %#v", persisted)
		}
	}
	runner := testRunner(github, workers, store, 1)
	runner.ExternalResolution = externalResolutionFunc(func(_ context.Context, run scheduler.Run) (bool, error) {
		close(resolutionStarted)
		<-finishResolution
		persisted := store.LoadValue()
		resolved := findRun(persisted.Runs, run.RunID)
		resolved.Status = scheduler.StatusResolvedExternally
		resolvedAt := runner.Now().UTC()
		resolved.ResolvedExternallyAt = &resolvedAt
		resolved.ClosureReason = "completed"
		replaceRun(&persisted, resolved)
		removeLease(&persisted, run.RunID)
		github.setCandidates([]scheduler.Candidate{second})
		return true, store.Save(persisted)
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, first.Number)
	workers.complete(first.Number, worker.Result{ExitCode: 0})
	<-resolutionStarted
	if workers.wasStarted(second.Number) {
		t.Fatal("second Candidate started while the first Run still owned its Lease")
	}
	persisted := store.LoadValue()
	if active := findActiveRun(&persisted, first.Number); active.RunID == "" || len(persisted.Leases) != 1 || persisted.Leases[0].Issue != first.Number {
		t.Fatalf("ownership released before durable External Resolution: %#v", persisted)
	}
	close(finishResolution)
	workers.waitForStarts(t, second.Number)
	select {
	case err := <-admissionErr:
		t.Fatal(err)
	default:
	}
	workers.complete(second.Number, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("capacity refill after External Resolution: %v", err)
	}
	got := store.LoadValue()
	if findRun(got.Runs, "run-5").Status != scheduler.StatusResolvedExternally || findRun(got.Runs, "run-6").Status != scheduler.StatusMerged || len(got.Leases) != 0 {
		t.Fatalf("capacity refill state = %#v", got)
	}
}

func TestRunnerRecordsLateCompletionAfterWorkerExitBeforeExternalResolution(t *testing.T) {
	var completionCalls atomic.Int32
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 6, CreatedAt: time.Now()}}}
	github.completionFunc = func(_ context.Context, _ int, branch string) (ghadapter.CompletionOutcome, error) {
		github.mu.Lock()
		github.completionBranches = append(github.completionBranches, branch)
		github.mu.Unlock()
		if completionCalls.Add(1) == 1 {
			return ghadapter.CompletionOutcome{IssueClosed: true}, nil
		}
		github.setCandidates(nil)
		return mergedOutcome(6), nil
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		return true, errors.New("External Resolution must not replace late Completion")
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 6)
	workers.complete(6, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("late Completion: %v", err)
	}
	got := store.LoadValue()
	if completionCalls.Load() != 2 || resolutionCalls.Load() != 0 || got.Runs[0].Status != scheduler.StatusMerged || got.Runs[0].WorkerLogOpen || len(got.Leases) != 0 {
		t.Fatalf("late Completion state = %#v, completion calls=%d resolution calls=%d", got, completionCalls.Load(), resolutionCalls.Load())
	}
	if runner.Worktrees.(*fakeWorktrees).cleanupCount() != 1 {
		t.Fatal("late Completion did not clean up after Worker exit")
	}
}

func TestRunnerPostSettlementReopeningRetainsLease(t *testing.T) {
	var completionCalls atomic.Int32
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 7, CreatedAt: time.Now()}}}
	github.completionFunc = func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
		if completionCalls.Add(1) == 1 {
			return ghadapter.CompletionOutcome{IssueClosed: true}, nil
		}
		return ghadapter.CompletionOutcome{}, nil
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		return true, errors.New("External Resolution must not start after reopening")
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 7)
	workers.complete(7, worker.Result{ExitCode: 0})
	assertInterventionRequired(t, <-done, 1)
	got := store.LoadValue()
	if completionCalls.Load() != 2 || resolutionCalls.Load() != 0 || got.Runs[0].Status != scheduler.StatusFailed || got.Runs[0].WorkerLogOpen || len(got.Leases) != 1 {
		t.Fatalf("reopened post-settlement state = %#v, completion calls=%d resolution calls=%d", got, completionCalls.Load(), resolutionCalls.Load())
	}
}

func TestRunnerUncertainSettledProcessGroupRetainsLeaseWithoutResolution(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 8, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{8: {IssueClosed: true}},
	}
	workers := newFakeWorkers()
	workers.startupCloseResult = worker.Result{LogClosed: true}
	workers.settledCloseLeavesGroup = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		return true, errors.New("must not clean up with uncertain process-group liveness")
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 8)
	workers.complete(8, worker.Result{ExitCode: 0})
	assertInterventionRequired(t, <-done, 1)
	got := store.LoadValue()
	run := got.Runs[0]
	if resolutionCalls.Load() != 0 || run.Status != scheduler.StatusNeedsHuman || run.PID != 1008 || run.ProcessIdentity == "" || run.WorkerLogOpen || len(got.Leases) != 1 {
		t.Fatalf("closed-log, live-process-group state = %#v, calls=%d", got, resolutionCalls.Load())
	}
	if runner.Worktrees.(*fakeWorktrees).cleanupCount() != 0 {
		t.Fatal("uncertain process-group liveness started cleanup")
	}
}

func TestRunnerSignalAfterUnverifiedSettledExitSnapshotReportsIncompleteSuspension(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 15, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{15: {IssueClosed: true}},
	}
	workers := newFakeWorkers()
	workers.startupCloseResult = worker.Result{LogClosed: true}
	workers.settledCloseLeavesGroup = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, workers, store, 1)
	runner.Signals = signals
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		return true, errors.New("must not resolve an unverified process group")
	})
	var signalOnce sync.Once
	store.beforeSave = func(value state.State) {
		run := findRun(value.Runs, "run-15")
		if run.Status != scheduler.StatusNeedsHuman || run.PID != 1015 {
			return
		}
		signalOnce.Do(func() {
			signals <- syscall.SIGTERM
			<-runner.suspensionEventReady
		})
	}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 15)
	workers.complete(15, worker.Result{ExitCode: 0})
	err := <-done
	var signalExit *SignalExit
	if !errors.As(err, &signalExit) || signalExit.Code != 143 || signalExit.Cause == nil || !strings.Contains(signalExit.Cause.Error(), "continuation boundary") {
		t.Fatalf("raced suspension result = %v, want incomplete signal exit 143", err)
	}
	got := store.LoadValue()
	run := findRun(got.Runs, "run-15")
	if run.Status != scheduler.StatusNeedsHuman || run.PID != 1015 || run.ProcessIdentity != "identity-1015" || len(got.Leases) != 1 || resolutionCalls.Load() != 0 {
		t.Fatalf("raced unverified process-group state = %#v, resolutions=%d", got, resolutionCalls.Load())
	}
	if runner.Worktrees.(*fakeWorktrees).cleanupCount() != 0 {
		t.Fatal("raced unverified process group started cleanup")
	}
}

func TestRunnerReportsUnverifiedSettledExitPersistenceFailureWithoutWaitingTwice(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 12, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{12: {IssueClosed: true}},
	}
	workers := newFakeWorkers()
	workers.settledCloseLeavesGroup = true
	// Automatic-resolution startup and reconciled initialization are saves 1
	// and 2; admission, worktree, logs, identity, and the initial outcome are
	// saves 3 through 8. Retaining the unverified process group is save 9.
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 9}
	runner := testRunner(github, workers, store, 1)
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		return true, errors.New("must not inspect an unverified process group")
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 12)
	workers.complete(12, worker.Result{ExitCode: 0})
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "persist unverified Worker") {
			t.Fatalf("unverified-exit persistence error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Runner waited for an already-consumed Worker completion")
	}
}

func TestRunnerPostSettlementCompletionCheckFailurePreservesAttention(t *testing.T) {
	var completionCalls atomic.Int32
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 10, CreatedAt: time.Now()}}}
	github.completionFunc = func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
		if completionCalls.Add(1) == 1 {
			return ghadapter.CompletionOutcome{IssueClosed: true}, nil
		}
		return ghadapter.CompletionOutcome{}, errors.New("GitHub unavailable")
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	var resolutionCalls atomic.Int32
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		resolutionCalls.Add(1)
		return true, nil
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 10)
	workers.complete(10, worker.Result{ExitCode: 0})
	assertInterventionRequired(t, <-done, 1)
	got := store.LoadValue()
	if completionCalls.Load() != 2 || resolutionCalls.Load() != 0 || got.Runs[0].Status != scheduler.StatusNeedsHuman || len(got.Leases) != 1 || !strings.Contains(got.Runs[0].Error, "recheck GitHub Completion after Worker settlement") {
		t.Fatalf("failed final Completion check = %#v, completion calls=%d resolution calls=%d", got, completionCalls.Load(), resolutionCalls.Load())
	}
}

func TestRunnerPostSettlementPreconditionsAndPersistenceFailClosed(t *testing.T) {
	base := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 11, RunID: "run-11", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
			Branch: "agent/issue-11-run-11", Worktree: "/tmp/run-11",
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-11", Issue: 11, RunID: "run-11"}},
	}

	t.Run("open issue probe failure skips redundant Completion recheck", func(t *testing.T) {
		current := cloneState(base)
		current.Runs[0].Status = scheduler.StatusWaitingForMerge
		var issueStateCalls atomic.Int32
		var completionCalls atomic.Int32
		github := &fakeGitHub{
			issueStateFunc: func(int) (ghadapter.IssueState, error) {
				issueStateCalls.Add(1)
				return ghadapter.IssueState{}, errors.New("transient late-closure probe failure")
			},
			completionFunc: func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
				completionCalls.Add(1)
				return ghadapter.CompletionOutcome{}, errors.New("redundant Completion lookup")
			},
		}
		runner := testRunner(github, newFakeWorkers(), &memoryStore{value: cloneState(current)}, 1)
		var resolutionCalls atomic.Int32
		runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
			resolutionCalls.Add(1)
			return true, errors.New("must not inspect an unverified closure")
		})
		if err := runner.reconcileAfterWorkerSettlement(context.Background(), &current, "run-11", false); err != nil {
			t.Fatalf("open issue post-settlement reconciliation: %v", err)
		}
		if issueStateCalls.Load() != 1 || completionCalls.Load() != 0 || resolutionCalls.Load() != 0 || current.Runs[0].Status != scheduler.StatusWaitingForMerge || len(current.Leases) != 1 {
			t.Fatalf("open issue post-settlement state = %#v, issue-state checks=%d Completion checks=%d resolutions=%d", current, issueStateCalls.Load(), completionCalls.Load(), resolutionCalls.Load())
		}
	})

	t.Run("durable log closure", func(t *testing.T) {
		current := cloneState(base)
		current.Runs[0].WorkerLogOpen = true
		runner := testRunner(&fakeGitHub{}, newFakeWorkers(), &memoryStore{value: cloneState(current)}, 1)
		runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
			return true, errors.New("must not inspect before log closure")
		})
		if err := runner.reconcileAfterWorkerSettlement(context.Background(), &current, "run-11", true); err == nil || !strings.Contains(err.Error(), "before durable Worker log closure") {
			t.Fatalf("open-log post-settlement error = %v", err)
		}
	})

	t.Run("Completion persistence", func(t *testing.T) {
		current := cloneState(base)
		store := &memoryStore{value: cloneState(current)}
		store.failNext()
		runner := testRunner(&fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{11: mergedOutcome(11)}}, newFakeWorkers(), store, 1)
		runner.Output = io.Discard
		runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
			return true, errors.New("must not replace Completion")
		})
		if err := runner.reconcileAfterWorkerSettlement(context.Background(), &current, "run-11", true); err == nil || !strings.Contains(err.Error(), "persist post-settlement Completion") {
			t.Fatalf("Completion persistence error = %v", err)
		}
		if got := store.LoadValue(); len(got.Leases) != 1 || got.Runs[0].Status != scheduler.StatusFailed {
			t.Fatalf("failed Completion persistence released ownership: %#v", got)
		}
	})
}

func TestRunnerPostSettlementResolutionFailurePreservesAttention(t *testing.T) {
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: 9, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{9: {IssueClosed: true}},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	runner := testRunner(github, workers, store, 1)
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		return true, errors.New("owned worktree cleanup failed")
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 9)
	workers.complete(9, worker.Result{ExitCode: 0})
	assertInterventionRequired(t, <-done, 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusFailed || len(got.Leases) != 1 || !strings.Contains(got.Runs[0].Error, "automatic External Resolution refused: owned worktree cleanup failed") {
		t.Fatalf("failed post-settlement cleanup = %#v", got)
	}
}

func TestRunnerAcceptsFinalizedAutomaticResolutionAfterVerificationReadFailure(t *testing.T) {
	t.Parallel()

	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 7, RunID: "partial", Status: scheduler.StatusResolvingExternally, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "partial", Issue: 7, RunID: "partial"}},
	}}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
		persisted := store.LoadValue()
		resolved := findRun(persisted.Runs, "partial")
		resolved.Status = scheduler.StatusResolvedExternally
		resolvedAt := time.Now().UTC()
		resolved.ResolvedExternallyAt = &resolvedAt
		resolved.ClosureReason = "completed"
		replaceRun(&persisted, resolved)
		removeLease(&persisted, resolved.RunID)
		if err := store.Save(persisted); err != nil {
			return true, err
		}
		return true, errors.New("verify finalized External Resolution state: injected preview failure")
	})
	current := store.LoadValue()

	if err := runner.reconcileExternalResolutions(context.Background(), &current, nil); err != nil {
		t.Fatalf("accept persisted terminal External Resolution: %v", err)
	}
	got := findRun(current.Runs, "partial")
	if got.Status != scheduler.StatusResolvedExternally || got.ResolvedExternallyAt == nil || got.ClosureReason != "completed" || len(current.Leases) != 0 {
		t.Fatalf("persisted terminal External Resolution = %#v / %#v", got, current.Leases)
	}
}

func TestRunnerRetainsLeaseAndAdoptsPersistedProgressWhenAutomaticResolutionIsRefused(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		persist        bool
		wantStatus     scheduler.Status
		wantWorktree   string
		wantDiagnostic string
	}{
		{
			name: "active Run requires intervention", wantStatus: scheduler.StatusNeedsHuman, wantWorktree: "/tmp/run-3",
			wantDiagnostic: "earlier failure; automatic External Resolution refused: remote branch liveness is unknown",
		},
		{
			name: "durable partial progress is adopted", persist: true, wantStatus: scheduler.StatusResolvingExternally,
			wantDiagnostic: "durable partial cleanup; automatic External Resolution refused: remote branch liveness is unknown",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &memoryStore{value: state.State{
				Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
				Runs: []scheduler.Run{{
					Issue: 3, RunID: "partial", Status: scheduler.StatusWaitingForMerge, WorkerMode: scheduler.WorkerModePrint,
					Worktree: "/tmp/run-3", Error: "earlier failure",
				}},
				Leases: []scheduler.Lease{{LeaseID: "partial", Issue: 3, RunID: "partial"}},
			}}
			runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
			runner.Output = io.Discard
			runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
				if test.persist {
					persisted := store.LoadValue()
					persisted.Runs[0].Status = scheduler.StatusResolvingExternally
					persisted.Runs[0].Worktree = ""
					persisted.Runs[0].Error = "durable partial cleanup"
					if err := store.Save(persisted); err != nil {
						return true, err
					}
				}
				return true, errors.New("remote branch liveness is unknown")
			})
			current := store.LoadValue()

			if err := runner.reconcileExternalResolutions(context.Background(), &current, nil); err != nil {
				t.Fatalf("refused automatic External Resolution: %v", err)
			}
			got := store.LoadValue()
			if got.Runs[0].Status != test.wantStatus || len(got.Leases) != 1 || got.Runs[0].Worktree != test.wantWorktree || got.Runs[0].Error != test.wantDiagnostic {
				t.Fatalf("refused automatic External Resolution = %#v, want status %s, worktree %q, diagnostic %q", got, test.wantStatus, test.wantWorktree, test.wantDiagnostic)
			}
		})
	}
}

func TestRunnerReportsAutomaticResolutionStatePersistenceFailures(t *testing.T) {
	base := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 4, RunID: "failed", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "failed", Issue: 4, RunID: "failed"}},
	}
	tests := []struct {
		name      string
		configure func(*memoryStore, *Runner)
		want      string
	}{
		{
			name: "initial snapshot", want: "persist runner state before automatic External Resolution",
			configure: func(store *memoryStore, runner *Runner) {
				store.failAtSave = 1
				runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) { return false, nil })
			},
		},
		{
			name: "outcome reload", want: "reload state after automatic External Resolution",
			configure: func(store *memoryStore, runner *Runner) {
				store.failAtLoad = 2
				runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) { return true, nil })
			},
		},
		{
			name: "refusal diagnostic", want: "persist automatic External Resolution diagnostic",
			configure: func(store *memoryStore, runner *Runner) {
				store.failAtSave = 2
				runner.ExternalResolution = externalResolutionFunc(func(context.Context, scheduler.Run) (bool, error) {
					return true, errors.New("unsafe cleanup")
				})
			},
		},
		{
			name: "Lease changed", want: "failed after its Lease changed",
			configure: func(store *memoryStore, runner *Runner) {
				runner.ExternalResolution = externalResolutionFunc(func(_ context.Context, run scheduler.Run) (bool, error) {
					current := store.LoadValue()
					removeLease(&current, run.RunID)
					if err := store.Save(current); err != nil {
						return true, err
					}
					return true, errors.New("late refusal")
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &memoryStore{value: cloneState(base)}
			runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
			test.configure(store, runner)
			if err := runner.Run(context.Background()); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("automatic resolution persistence error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunnerStartupPreservesResolvingExternallyRunAndLease(t *testing.T) {
	t.Parallel()

	initial := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{{
			Issue: 1, RunID: "resolving", Status: scheduler.StatusResolvingExternally, WorkerMode: scheduler.WorkerModePrint,
			Branch: "agent/issue-1-resolving", Error: "retained diagnostic",
		}},
		Leases: []scheduler.Lease{{LeaseID: "resolving", Issue: 1, RunID: "resolving"}},
	}
	github := &fakeGitHub{}
	store := &memoryStore{value: cloneState(initial)}
	runner := testRunner(github, newFakeWorkers(), store, 1)

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if !reflect.DeepEqual(got.Runs, initial.Runs) || !reflect.DeepEqual(got.Leases, initial.Leases) {
		t.Fatalf("resolving Run or Lease changed during Runner startup: got %#v, want %#v", got, initial)
	}
	if branches := github.completionBranchSnapshot(); len(branches) != 0 {
		t.Fatalf("resolving Run triggered GitHub Completion lookup: %q", branches)
	}
}

func TestRunnerLeavesResettingRunForResetReconciliation(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{}
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs:   []scheduler.Run{{Issue: 1, RunID: "resetting", Status: scheduler.StatusResetting, WorkerMode: scheduler.WorkerModePrint}},
		Leases: []scheduler.Lease{{LeaseID: "resetting", Issue: 1, RunID: "resetting"}},
	}}
	runner := testRunner(github, newFakeWorkers(), store, 1)
	current := store.LoadValue()
	if err := runner.reconcile(context.Background(), &current, map[int]WorkerProcess{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusResetting || len(got.Leases) != 1 {
		t.Fatalf("resetting Run changed during runner reconciliation: %#v", got)
	}
	if branches := github.completionBranchSnapshot(); len(branches) != 0 {
		t.Fatalf("resetting Run triggered GitHub completion lookup: %q", branches)
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

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
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

func TestRunnerResumesSuspendedRunBeforeNewCandidateWithSameIdentity(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 61, "resume-61")
	originalStartedAt := run.StartedAt
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 62, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-61", Issue: 61, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	pendingObserved := make(chan error, 1)
	workers.onStart = func(issue int) {
		persisted := store.LoadValue()
		resuming := findActiveRun(&persisted, issue)
		if resuming.Status != scheduler.StatusSuspended || !resuming.ResumePending || resuming.PID != 0 {
			pendingObserved <- fmt.Errorf("replacement Worker started without a durable pending marker: %#v", resuming)
			return
		}
		pendingObserved <- nil
	}
	releaseObserved := make(chan error, 1)
	workers.onRelease = func(issue int) {
		persisted := store.LoadValue()
		resumed := findActiveRun(&persisted, issue)
		if resumed.Status != scheduler.StatusRunning || resumed.PID != 1000+issue || resumed.ProcessIdentity == "" {
			releaseObserved <- fmt.Errorf("replacement Worker released before identity persistence: %#v", resumed)
			return
		}
		releaseObserved <- nil
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()

	workers.waitForStarts(t, 61)
	if got := workers.startedSnapshot(); len(got) != 1 || got[0] != 61 {
		t.Fatalf("Worker start order = %v, want resumed issue first", got)
	}
	request := workers.requestFor(61)
	if !request.Resume || request.RunID != run.RunID || request.SessionID != run.SessionID || request.SessionDir != run.SessionDir || request.SessionFile != run.Continuation.SessionFile || request.Worktree != run.Worktree {
		t.Fatalf("replacement Worker request = %#v, want retained Run/session/worktree identity", request)
	}
	if err := <-pendingObserved; err != nil {
		t.Fatal(err)
	}
	if err := <-releaseObserved; err != nil {
		t.Fatal(err)
	}
	waitForPersistedRun(t, store, 61, func(run scheduler.Run) bool {
		return run.Status == scheduler.StatusRunning && run.PID == 1061 && run.ProcessIdentity != ""
	})
	persisted := store.LoadValue()
	resumed := findActiveRun(&persisted, 61)
	if resumed.Status != scheduler.StatusRunning || resumed.PID != 1061 || resumed.ProcessIdentity == "" || resumed.Branch != run.Branch ||
		!resumed.StartedAt.Equal(originalStartedAt) || resumed.WorkerStartedAt.IsZero() || len(persisted.Leases) != 1 || persisted.Leases[0].LeaseID != "lease-61" {
		t.Fatalf("resumed Run and Lease = %#v / %#v", resumed, persisted.Leases)
	}

	github.setCompletion(61, mergedOutcome(61))
	workers.complete(61, worker.Result{ExitCode: 0})
	workers.waitForStarts(t, 62)
	workers.complete(62, worker.Result{ExitCode: 1, Err: errors.New("stop fixture")})
	assertInterventionRequired(t, <-done, 1)
}

func TestRunnerFailsClosedAfterCrashDuringReplacementLaunch(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 64, "resume-crash-64")
	run.ResumePending = true
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-64", Issue: 64, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	resumed := findActiveRun(&got, 64)
	if resumed.Status != scheduler.StatusNeedsHuman || !resumed.ResumePending || !strings.Contains(resumed.Error, "launch was interrupted") || len(got.Leases) != 1 || workers.wasStarted(64) {
		t.Fatalf("interrupted replacement launch = %#v, starts=%v", got, workers.startedSnapshot())
	}
}

func TestRunnerRechecksGitHubCompletionImmediatelyBeforeResume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		outcome ghadapter.CompletionOutcome
		status  scheduler.Status
	}{
		{name: "merged", outcome: mergedOutcome(65), status: scheduler.StatusMerged},
		{name: "waiting for merge", outcome: ghadapter.CompletionOutcome{PRFound: true, PullRequest: "https://example.test/pull/165", AutoMergeArmed: true}, status: scheduler.StatusWaitingForMerge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := resumableRun(t, 65, "resume-65")
			calls := 0
			github := &fakeGitHub{}
			github.completionFunc = func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
				calls++
				if calls == 1 {
					return ghadapter.CompletionOutcome{}, nil
				}
				return test.outcome, nil
			}
			workers := newFakeWorkers()
			store := &memoryStore{value: state.State{
				Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
				Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-65", Issue: 65, RunID: run.RunID}},
			}}
			runner := testRunner(github, workers, store, 1)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- runner.Run(ctx) }()
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if got := store.LoadValue(); len(got.Runs) == 1 && got.Runs[0].Status == test.status {
					break
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			got := store.LoadValue()
			if calls < 2 || got.Runs[0].Status != test.status || workers.wasStarted(65) {
				t.Fatalf("Completion recheck result: calls=%d state=%#v starts=%v", calls, got, workers.startedSnapshot())
			}
		})
	}
}

func TestRunnerRefusesCompletionCleanupForReplacedResumedWorktree(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 66, "resume-66")
	if err := os.RemoveAll(run.Worktree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(run.Worktree, []byte("not a worktree"), 0o600); err != nil {
		t.Fatal(err)
	}
	github := &fakeGitHub{completionFunc: func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
		return mergedOutcome(66), nil
	}}
	workers := newFakeWorkers()
	worktrees := &fakeWorktrees{verifyErr: errors.New("worktree identity changed")}
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-66", Issue: 66, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.Worktrees = worktrees

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "worktree identity changed") || len(got.Leases) != 1 || worktrees.cleanupCount() != 0 || workers.wasStarted(66) {
		t.Fatalf("changed completed worktree = %#v, cleanup=%d, starts=%v", got, worktrees.cleanupCount(), workers.startedSnapshot())
	}
}

func TestRunnerRefusesCompletionCleanupWhenWorktreeInspectionIsUncertain(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 67, "resume-67")
	github := &fakeGitHub{completionFunc: func(context.Context, int, string) (ghadapter.CompletionOutcome, error) {
		return mergedOutcome(67), nil
	}}
	workers := newFakeWorkers()
	worktrees := &fakeWorktrees{}
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-67", Issue: 67, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.Worktrees = worktrees
	runner.Lstat = func(string) (os.FileInfo, error) { return nil, errors.New("worktree inspection unavailable") }

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "inspection unavailable") || len(got.Leases) != 1 || worktrees.cleanupCount() != 0 || workers.wasStarted(67) {
		t.Fatalf("uncertain completed worktree = %#v, cleanup=%d, starts=%v", got, worktrees.cleanupCount(), workers.startedSnapshot())
	}
}

func TestRunnerVerifiesResumedWorktreeBeforeSettledCleanup(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 68, "settled-68")
	run.Status = scheduler.StatusMerged
	now := time.Now()
	run.CompletedAt = &now
	store := &memoryStore{value: state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}}
	worktrees := &fakeWorktrees{verifyErr: errors.New("worktree changed after Resume")}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.Worktrees = worktrees
	runner.Output = io.Discard
	current := store.LoadValue()

	if err := runner.finalizeSettledWorker(context.Background(), &current, run.RunID, nil, true); err != nil {
		t.Fatal(err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "changed after Resume") || len(got.Leases) != 1 || worktrees.cleanupCount() != 0 {
		t.Fatalf("settled resumed cleanup = %#v, cleanup=%d", got, worktrees.cleanupCount())
	}
}

func TestRunnerVerifiesResumedWorktreeBeforeForceStoppedCleanup(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 69, "force-settled-69")
	run.Status = scheduler.StatusMerged
	store := &memoryStore{value: state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{run}}}
	worktrees := &fakeWorktrees{verifyErr: errors.New("worktree changed after Resume")}
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	runner.Worktrees = worktrees
	current := store.LoadValue()

	err := runner.finalizeForceStoppedSettledWorker(&current, run.RunID, nil)
	if err == nil || !strings.Contains(err.Error(), "changed after Resume") || worktrees.cleanupCount() != 0 {
		t.Fatalf("force-stopped resumed cleanup error = %v, cleanup=%d", err, worktrees.cleanupCount())
	}
}

func TestRunnerRecoversPersistedContinuationMarkerBeforeFinalSuspendedState(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 63, "resume-63")
	run.Status = scheduler.StatusRunning
	run.PID = 999999
	run.ProcessIdentity = "999999:old"
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-63", Issue: 63, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(int) bool { return false }
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 63)
	waitForPersistedRun(t, store, 63, func(run scheduler.Run) bool {
		return run.Status == scheduler.StatusRunning && run.PID == 1063 && run.ProcessIdentity != "999999:old"
	})
	workers.complete(63, worker.Result{ExitCode: 1, Err: errors.New("stop fixture")})
	assertInterventionRequired(t, <-done, 1)
}

func TestRunnerDoesNotResumeBeyondCapacityConsumedByRecoveredLiveWorker(t *testing.T) {
	t.Parallel()

	live := resumableRun(t, 69, "live-69")
	live.Status = scheduler.StatusRunning
	live.PID = 999
	live.ProcessIdentity = "identity-999"
	suspended := resumableRun(t, 70, "resume-70")
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{live, suspended}, Leases: []scheduler.Lease{
			{LeaseID: "lease-69", Issue: 69, RunID: live.RunID},
			{LeaseID: "lease-70", Issue: 70, RunID: suspended.RunID},
		},
	}}
	runner := testRunner(github, workers, store, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	waitForPersistedRun(t, store, 69, func(run scheduler.Run) bool {
		return run.Status == scheduler.StatusNeedsHuman && run.PID == 999
	})
	time.Sleep(20 * time.Millisecond)
	if workers.wasStarted(70) {
		t.Fatal("Suspended Run resumed despite recovered live Worker consuming all capacity")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := store.LoadValue()
	if findActiveRun(&got, 70).Status != scheduler.StatusSuspended || len(got.Leases) != 2 {
		t.Fatalf("capacity-blocked Suspended Run = %#v", got)
	}
}

func TestRunnerRejectsCrashRecoveryWhileOldWorkerProcessGroupRemains(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 64, "resume-64")
	run.Status = scheduler.StatusRunning
	run.PID = 999999
	run.ProcessIdentity = "999999:old"
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-64", Issue: 64, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(int) bool { return false }
	runner.ProcessGroupAlive = func(int) (bool, error) { return true, nil }
	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 999999 || got.Runs[0].ProcessIdentity != "999999:old" || len(got.Leases) != 1 || workers.wasStarted(64) {
		t.Fatalf("surviving old Worker process group = %#v", got)
	}
}

func TestRunnerRejectsCrashRecoveryWhenProcessGroupInspectionIsUncertain(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 67, "resume-67")
	run.Status = scheduler.StatusRunning
	run.PID = 999999
	run.ProcessIdentity = "999999:old"
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-67", Issue: 67, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDAlive = func(int) bool { return false }
	runner.ProcessGroupAlive = func(int) (bool, error) { return false, errors.New("inspection unavailable") }
	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 999999 || !strings.Contains(got.Runs[0].Error, "uncertain") || len(got.Leases) != 1 || workers.wasStarted(67) {
		t.Fatalf("uncertain old Worker process group = %#v", got)
	}
}

func TestRunnerClassifiesStructurallyMalformedPersistedRunningContinuationAsNeedsHuman(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	run := resumableRun(t, 66, "resume-66")
	run.Status = scheduler.StatusRunning
	run.PID = 999999
	run.ProcessIdentity = "999999:old"
	persisted := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-66", Issue: 66, RunID: run.RunID}},
	}
	encoded, err := json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	var malformed map[string]any
	if err := json.Unmarshal(encoded, &malformed); err != nil {
		t.Fatal(err)
	}
	malformed["runs"].([]any)[0].(map[string]any)["continuation"] = "malformed"
	encoded, err = json.Marshal(malformed)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "state.json")
	if err := os.WriteFile(statePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	runner := &Runner{
		Config: Config{Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1, PollInterval: 5 * time.Millisecond, SessionsDir: filepath.Join(root, "sessions")},
		GitHub: github, Store: state.FileStore{Path: statePath}, Worktrees: &fakeWorktrees{}, Workers: workers,
		PIDAlive: func(int) bool { return false }, ProcessGroupAlive: func(int) (bool, error) { return false, nil },
	}
	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "continuation") || len(got.Leases) != 1 || workers.wasStarted(66) {
		t.Fatalf("malformed persisted Running continuation = %#v", got)
	}
}

func TestRunnerClassifiesMissingContinuationVerificationTimeAsNeedsHuman(t *testing.T) {
	t.Parallel()

	for index, status := range []scheduler.Status{scheduler.StatusSuspended, scheduler.StatusRunning} {
		status := status
		t.Run(string(status), func(t *testing.T) {
			root := t.TempDir()
			issue := 72 + index
			run := resumableRun(t, issue, fmt.Sprintf("missing-time-%d", issue))
			run.Status = status
			run.Continuation.VerifiedAt = time.Time{}
			if status == scheduler.StatusRunning {
				run.PID = 999999
				run.ProcessIdentity = "999999:old"
			}
			persisted := state.State{
				Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
				Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: fmt.Sprintf("lease-%d", issue), Issue: issue, RunID: run.RunID}},
			}
			encoded, err := json.Marshal(persisted)
			if err != nil {
				t.Fatal(err)
			}
			statePath := filepath.Join(root, "state.json")
			if err := os.WriteFile(statePath, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			workers := newFakeWorkers()
			runner := &Runner{
				Config: Config{Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1, PollInterval: 5 * time.Millisecond, SessionsDir: filepath.Join(root, "sessions")},
				GitHub: &fakeGitHub{}, Store: state.FileStore{Path: statePath}, Worktrees: &fakeWorktrees{}, Workers: workers,
				PIDAlive: func(int) bool { return false }, ProcessGroupAlive: func(int) (bool, error) { return false, nil },
			}
			assertInterventionRequired(t, runner.Run(context.Background()), 1)
			got, err := (state.FileStore{Path: statePath}).Load()
			if err != nil {
				t.Fatal(err)
			}
			if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "timestamp") || len(got.Leases) != 1 || workers.wasStarted(issue) {
				t.Fatalf("missing verification timestamp = %#v", got)
			}
		})
	}
}

func TestRunnerClassifiesPersistedRunningRPCWithMissingSessionIdentityAsNeedsHuman(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	run := scheduler.Run{
		Issue: 71, RunID: "resume-71", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModeRPC,
		PID: 999999, ProcessIdentity: "999999:old", Branch: "agent/issue-71-resume-71", Worktree: t.TempDir(), StartedAt: time.Now(),
	}
	persisted := state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1,
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-71", Issue: 71, RunID: run.RunID}},
	}
	encoded, err := json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "state.json")
	if err := os.WriteFile(statePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	workers := newFakeWorkers()
	runner := &Runner{
		Config: Config{Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 1, PollInterval: 5 * time.Millisecond, SessionsDir: filepath.Join(root, "sessions")},
		GitHub: &fakeGitHub{}, Store: state.FileStore{Path: statePath}, Worktrees: &fakeWorktrees{}, Workers: workers,
		PIDAlive: func(int) bool { return false }, ProcessGroupAlive: func(int) (bool, error) { return false, nil },
	}
	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "missing durable session") || len(got.Leases) != 1 || workers.wasStarted(71) {
		t.Fatalf("missing persisted RPC session identity = %#v", got)
	}
}

func TestRunnerRejectsUnsafeSuspendedContinuationAndRetainsLease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mutate    func(*scheduler.Run, *fakeGitHub, *fakeWorktrees)
		wantError string
	}{
		{name: "missing continuation", mutate: func(run *scheduler.Run, _ *fakeGitHub, _ *fakeWorktrees) {
			run.Continuation = nil
		}, wantError: "incomplete"},
		{name: "legacy print mode", mutate: func(run *scheduler.Run, _ *fakeGitHub, _ *fakeWorktrees) {
			run.WorkerMode = scheduler.WorkerModePrint
			run.Continuation = nil
		}, wantError: "legacy print-mode"},
		{name: "changed session", mutate: func(run *scheduler.Run, _ *fakeGitHub, _ *fakeWorktrees) {
			run.Continuation.SHA256 = strings.Repeat("0", 64)
		}, wantError: "hash"},
		{name: "changed branch or worktree", mutate: func(_ *scheduler.Run, _ *fakeGitHub, worktrees *fakeWorktrees) {
			worktrees.verifyErr = errors.New("branch changed")
		}, wantError: "branch changed"},
		{name: "closed issue", mutate: func(_ *scheduler.Run, github *fakeGitHub, _ *fakeWorktrees) {
			github.issueStateFunc = func(int) (ghadapter.IssueState, error) {
				return ghadapter.IssueState{Open: false, Labels: []string{"in-progress"}}, nil
			}
		}, wantError: "not open"},
		{name: "missing in-progress", mutate: func(_ *scheduler.Run, github *fakeGitHub, _ *fakeWorktrees) {
			github.issueStateFunc = func(int) (ghadapter.IssueState, error) {
				return ghadapter.IssueState{Open: true, Labels: []string{"spec"}}, nil
			}
		}, wantError: "in-progress"},
		{name: "case-variant in-progress", mutate: func(_ *scheduler.Run, github *fakeGitHub, _ *fakeWorktrees) {
			github.issueStateFunc = func(int) (ghadapter.IssueState, error) {
				return ghadapter.IssueState{Open: true, Labels: []string{"IN-PROGRESS"}}, nil
			}
		}, wantError: "in-progress"},
		{name: "ready-for-agent conflict", mutate: func(_ *scheduler.Run, github *fakeGitHub, _ *fakeWorktrees) {
			github.issueStateFunc = func(int) (ghadapter.IssueState, error) {
				return ghadapter.IssueState{Open: true, Labels: []string{"in-progress", "ready-for-agent"}}, nil
			}
		}, wantError: "ready-for-agent"},
		{name: "needs-triage label", mutate: resumeIssueLabels("needs-triage"), wantError: "needs-triage"},
		{name: "needs-info label", mutate: resumeIssueLabels("needs-info"), wantError: "needs-info"},
		{name: "ready-for-human label", mutate: resumeIssueLabels("ready-for-human"), wantError: "ready-for-human"},
		{name: "wontfix label", mutate: resumeIssueLabels("wontfix"), wantError: "wontfix"},
		{name: "uncertain issue state", mutate: func(_ *scheduler.Run, github *fakeGitHub, _ *fakeWorktrees) {
			github.issueStateFunc = func(int) (ghadapter.IssueState, error) {
				return ghadapter.IssueState{}, errors.New("GitHub unavailable")
			}
		}, wantError: "GitHub unavailable"},
	}
	for index, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			run := resumableRun(t, 70+index, fmt.Sprintf("unsafe-%d", index))
			github := &fakeGitHub{}
			workers := newFakeWorkers()
			worktrees := &fakeWorktrees{}
			test.mutate(&run, github, worktrees)
			store := &memoryStore{value: state.State{
				Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
				Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
			}}
			runner := testRunner(github, workers, store, 1)
			runner.Worktrees = worktrees
			assertInterventionRequired(t, runner.Run(context.Background()), 1)
			got := store.LoadValue()
			if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, test.wantError) || len(got.Leases) != 1 || workers.wasStarted(run.Issue) {
				t.Fatalf("unsafe continuation result = %#v, starts = %v", got, workers.startedSnapshot())
			}
		})
	}
}

func resumeIssueLabels(label string) func(*scheduler.Run, *fakeGitHub, *fakeWorktrees) {
	return func(_ *scheduler.Run, github *fakeGitHub, _ *fakeWorktrees) {
		github.issueStateFunc = func(int) (ghadapter.IssueState, error) {
			return ghadapter.IssueState{Open: true, Labels: []string{"in-progress", label}}, nil
		}
	}
}

func waitForPersistedRun(t *testing.T, store *memoryStore, issue int, condition func(scheduler.Run) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		current := store.LoadValue()
		run := findActiveRun(&current, issue)
		if condition(run) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	current := store.LoadValue()
	t.Fatalf("persisted Run for issue #%d did not reach expected state: %#v", issue, findActiveRun(&current, issue))
}

func TestRunnerReverifiesContinuationAtReplacementReleaseGate(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 68, "resume-68")
	github := &fakeGitHub{}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-68", Issue: 68, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	runner.PIDIdentity = func(_ context.Context, pid int) (string, error) {
		if err := os.WriteFile(run.Continuation.SessionFile, []byte("changed after initial verification\n"), 0o600); err != nil {
			return "", err
		}
		workers.mu.Lock()
		workers.processes[68].closeResult.GroupExited = true
		workers.mu.Unlock()
		return fmt.Sprintf("identity-%d", pid), nil
	}
	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "reverify") || got.Runs[0].PID != 0 || len(got.Leases) != 1 || workers.releases != 0 || workers.abortedCount() == 0 {
		t.Fatalf("changed continuation at release gate = %#v, releases=%d aborts=%d", got, workers.releases, workers.abortedCount())
	}
}

func TestRunnerFailsClosedWhenReplacementWorkerCannotLaunchSafely(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*Runner, *fakeWorkers)
		wantError string
		wantPID   bool
	}{
		{name: "start", configure: func(_ *Runner, workers *fakeWorkers) { workers.startErr = errors.New("start failed") }, wantError: "start failed"},
		{name: "process identity", configure: func(runner *Runner, _ *fakeWorkers) {
			runner.PIDIdentity = func(context.Context, int) (string, error) { return "", errors.New("identity unavailable") }
		}, wantError: "identity unavailable", wantPID: true},
		{name: "release", configure: func(_ *Runner, workers *fakeWorkers) { workers.releaseErr = errors.New("release failed") }, wantError: "release failed", wantPID: true},
	}
	for index, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			run := resumableRun(t, 80+index, fmt.Sprintf("launch-%d", index))
			github := &fakeGitHub{}
			workers := newFakeWorkers()
			store := &memoryStore{value: state.State{
				Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
				Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}},
			}}
			runner := testRunner(github, workers, store, 1)
			test.configure(runner, workers)
			assertInterventionRequired(t, runner.Run(context.Background()), 1)
			got := store.LoadValue()
			if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, test.wantError) || (got.Runs[0].PID != 0) != test.wantPID || len(got.Leases) != 1 {
				t.Fatalf("unsafe replacement launch = %#v", got)
			}
		})
	}
}

func TestRunnerRetainsUnverifiedReplacementAfterIdentityPersistenceFailure(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 88, "persist-88")
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-88", Issue: 88, RunID: run.RunID}},
	}, failAtSave: 2}
	runner := testRunner(&fakeGitHub{}, workers, store, 1)
	runner.Output = io.Discard
	current := store.LoadValue()

	process, err := runner.resume(context.Background(), context.Background(), &current, run)
	if err == nil || !strings.Contains(err.Error(), "persist replacement Worker identity") || process != nil {
		t.Fatalf("replacement persistence result = %#v, %v", process, err)
	}
	got := store.LoadValue()
	resumed := findActiveRun(&got, 88)
	if resumed.Status != scheduler.StatusNeedsHuman || resumed.PID != 1088 || resumed.ProcessIdentity != "identity-1088" ||
		!strings.Contains(resumed.Error, "persist replacement Worker identity") || len(got.Leases) != 1 || workers.abortedCount() == 0 {
		t.Fatalf("unverified replacement persistence = %#v", got)
	}
}

func TestRunnerInterruptedReplacementIdentityMarksSuspensionIncomplete(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 89, "interrupted-89")
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-89", Issue: 89, RunID: run.RunID}},
	}}
	runner := testRunner(&fakeGitHub{}, workers, store, 1)
	runner.Output = io.Discard
	operationCtx, cancel := context.WithCancel(context.Background())
	cancel()
	runner.PIDIdentity = func(ctx context.Context, _ int) (string, error) { return "", ctx.Err() }
	current := store.LoadValue()

	process, err := runner.resume(context.Background(), operationCtx, &current, run)
	if err != nil || process != nil {
		t.Fatalf("interrupted Resume = %#v, %v", process, err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID == 0 || len(got.Leases) != 1 || !runner.suspensionFailed.Load() {
		t.Fatalf("interrupted replacement identity = %#v", got)
	}
	err = runner.suspendOwned(&current, map[int]WorkerProcess{}, 143)
	var signalExit *SignalExit
	if !errors.As(err, &signalExit) || signalExit.Cause == nil {
		t.Fatalf("suspension result = %v, want incomplete SignalExit", err)
	}
}

func resumableRun(t *testing.T, issue int, runID string) scheduler.Run {
	t.Helper()
	sessionDir := t.TempDir()
	worktreePath := t.TempDir()
	sessionFile := filepath.Join(sessionDir, "session.jsonl")
	content := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":\"session-%d\",\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"continue\"}}\n", issue, worktreePath)
	if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(content))
	return scheduler.Run{
		Issue: issue, RunID: runID, Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
		Branch: fmt.Sprintf("agent/issue-%d-%s", issue, runID), Worktree: worktreePath,
		SessionName: fmt.Sprintf("afk #%d", issue), SessionID: fmt.Sprintf("session-%d", issue), SessionDir: sessionDir,
		Continuation: &scheduler.ContinuationBoundary{
			SessionID: fmt.Sprintf("session-%d", issue), SessionFile: sessionFile, Worktree: worktreePath,
			LeafID: "leaf", EntryCount: 1, SHA256: hex.EncodeToString(hash[:]), VerifiedAt: time.Now(),
		},
		StartedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now(),
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
	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || !strings.Contains(got.Runs[0].Error, "issue remains open") || len(got.Leases) != 1 {
		t.Fatalf("reconciled merged-open suspended Run = %#v", got)
	}
}

func TestRunnerRefusesSuspendedCompletionWhileOldWorkerAbsenceIsUnproven(t *testing.T) {
	t.Parallel()

	run := resumableRun(t, 26, "unsafe-suspended-26")
	run.PID = 999999
	run.ProcessIdentity = "999999:old"
	github := &fakeGitHub{completions: map[int]ghadapter.CompletionOutcome{26: mergedOutcome(26)}}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-26", Issue: 26, RunID: run.RunID}},
	}}
	runner := testRunner(github, workers, store, 1)
	worktrees := runner.Worktrees.(*fakeWorktrees)

	assertInterventionRequired(t, runner.Run(context.Background()), 1)
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusNeedsHuman || got.Runs[0].PID != 999999 || !strings.Contains(got.Runs[0].Error, "absence is not proven") || len(got.Leases) != 1 || worktrees.cleanupCount() != 0 {
		t.Fatalf("unsafe suspended Completion = %#v, cleanup=%d", got, worktrees.cleanupCount())
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
	if run.Status != scheduler.StatusSuspended || run.PID != 0 || run.ProcessIdentity != "" || run.Continuation == nil ||
		run.SuspendingAt != nil || run.SuspendedAt == nil {
		t.Fatalf("suspended Run = %#v", run)
	}
	if len(got.Leases) != 1 || run.Branch == "" || run.Worktree == "" || run.SessionID == "" {
		t.Fatalf("retained Run artifacts/Lease = %#v/%#v", run, got.Leases)
	}
}

func TestRunnerBoundsCleanupWhenSuspensionStartsAfterCleanup(t *testing.T) {
	const issue = 84
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{issue: mergedOutcome(issue)},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 2)
	cleanupStarted := make(chan struct{})
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 40 * time.Millisecond
	runner.Signals = signals
	runner.Worktrees = &blockingCleanupWorktrees{cleanupStarted: cleanupStarted}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	workers.complete(issue, worker.Result{ExitCode: 0})
	<-cleanupStarted
	started := time.Now()
	signals <- os.Interrupt
	signals <- os.Interrupt
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("cleanup outlived shared suspension deadline: %s", elapsed)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusMerged || got.Runs[0].CompletedAt == nil || !got.Runs[0].CleanupPending ||
		!strings.Contains(got.Runs[0].Error, "cleanup remains pending") || len(got.Leases) != 0 {
		t.Fatalf("merged outcome after bounded cleanup = %#v", got)
	}
}

func TestRunnerOrderlySettledCleanupUsesSuspensionDeadline(t *testing.T) {
	const issue = 83
	github := &fakeGitHub{
		candidates:  []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}},
		completions: map[int]ghadapter.CompletionOutcome{issue: mergedOutcome(issue)},
	}
	workers := newFakeWorkers()
	workers.blockSettledClose = true
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 3)
	worktrees := &deadlineRecordingWorktrees{}
	runner := testRunner(github, workers, store, 1)
	runner.Config.SuspensionTimeout = 5 * time.Second
	runner.Signals = signals
	runner.Worktrees = worktrees

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	workers.complete(issue, worker.Result{ExitCode: 0})
	<-workers.settledCloseStarted
	signals <- os.Interrupt
	signals <- os.Interrupt
	signals <- os.Interrupt
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}

	deadlines := worktrees.deadlineSnapshot()
	if len(deadlines) != 1 {
		t.Fatalf("cleanup deadlines = %v, want one", deadlines)
	}
	runner.suspensionMu.Lock()
	wantDeadline := runner.suspensionDeadline
	runner.suspensionMu.Unlock()
	if !deadlines[0].Equal(wantDeadline) {
		t.Fatalf("cleanup deadline = %s, want shared deadline %s", deadlines[0], wantDeadline)
	}
}

func TestRunnerMergedCleanupUsesOneSharedSuspensionDeadline(t *testing.T) {
	github := &fakeGitHub{
		candidates: []scheduler.Candidate{
			{Number: 81, CreatedAt: time.Now()},
			{Number: 82, CreatedAt: time.Now().Add(time.Second)},
		},
		completions: map[int]ghadapter.CompletionOutcome{
			81: mergedOutcome(81),
			82: mergedOutcome(82),
		},
	}
	workers := newFakeWorkers()
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}}
	signals := make(chan os.Signal, 2)
	worktrees := &deadlineRecordingWorktrees{}
	runner := testRunner(github, workers, store, 2)
	runner.Config.SuspensionTimeout = 5 * time.Second
	runner.Signals = signals
	runner.Worktrees = worktrees

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 81, 82)
	signals <- os.Interrupt
	signals <- os.Interrupt
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}

	deadlines := worktrees.deadlineSnapshot()
	if len(deadlines) != 2 {
		t.Fatalf("cleanup deadlines = %v, want two", deadlines)
	}
	if !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("cleanup deadlines = %v, want one shared deadline", deadlines)
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
			run.SuspendingAt = nil
			if status == scheduler.StatusSuspended {
				now := time.Now()
				run.SuspendedAt = &now
			}
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
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 8}
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
	workers.suspendFunc = func(_ context.Context, _ int, request worker.ContinuationRequest) (worker.Continuation, error) {
		if err := os.MkdirAll(request.SessionDir, 0o700); err != nil {
			return worker.Continuation{}, err
		}
		sessionFile := filepath.Join(request.SessionDir, "session.jsonl")
		content := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":%q,\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"continue\"}}\n", request.SessionID, request.Worktree)
		if err := os.WriteFile(sessionFile, []byte(content), 0o600); err != nil {
			return worker.Continuation{}, err
		}
		hash := sha256.Sum256([]byte(content))
		return worker.Continuation{
			SessionID: request.SessionID, SessionFile: sessionFile, Worktree: request.Worktree,
			LeafID: "leaf", EntryCount: 1, SHA256: hex.EncodeToString(hash[:]),
		}, nil
	}
	store := &memoryStore{value: state.State{Version: state.CurrentVersion}, failAtSave: 9}
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

	restarted := testRunner(&fakeGitHub{}, newFakeWorkers(), store, 1)
	restarted.Output = io.Discard
	restarted.PIDAlive = func(int) bool { return false }
	restarted.ProcessGroupAlive = func(int) (bool, error) { return false, nil }
	current := store.LoadValue()
	if err := restarted.reconcile(context.Background(), &current, map[int]WorkerProcess{}); err != nil {
		t.Fatalf("reconcile restart: %v", err)
	}
	got := store.LoadValue()
	if got.Runs[0].Status != scheduler.StatusSuspended || got.Runs[0].PID != 0 || got.Runs[0].ProcessIdentity != "" || got.Runs[0].Continuation == nil ||
		got.Runs[0].SuspendedAt == nil || !got.Runs[0].SuspendedAt.Equal(restarted.Now()) || !got.Runs[0].UpdatedAt.Equal(*got.Runs[0].SuspendedAt) || len(got.Leases) != 1 {
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
	output := newSynchronizedOutput()
	runner.Output = output
	runner.Worktrees = &blockingWorktrees{prepareStarted: prepareStarted, finishPrepare: make(chan struct{})}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-prepareStarted
	started := time.Now()
	signals <- syscall.SIGTERM
	if err := <-done; !isSignalExit(err, 143) || !strings.Contains(err.Error(), "continuation boundary") {
		t.Fatalf("run: %v, want failed-closed SIGTERM exit 143 with continuation cause", err)
	}
	output.waitFor(t, "Suspension incomplete: suspension could not establish a continuation boundary")
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

func assertInterventionRequired(t *testing.T, err error, count int) {
	t.Helper()
	var intervention *InterventionRequired
	if !errors.As(err, &intervention) || intervention.Count != count {
		t.Fatalf("run error = %v, want InterventionRequired count %d", err, count)
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
	mu                 sync.Mutex
	value              state.State
	saveHistory        []state.State
	saveCount          int
	loadCount          int
	failAtSave         int
	failAtLoad         int
	failSavesRemaining int
	beforeSave         func(state.State)
}

func (s *memoryStore) Load() (state.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadCount++
	if s.failAtLoad > 0 && s.loadCount == s.failAtLoad {
		return state.State{}, errors.New("injected state load failure")
	}
	return cloneState(s.value), nil
}
func (s *memoryStore) Save(value state.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.beforeSave != nil {
		s.beforeSave(cloneState(value))
	}
	s.saveCount++
	if s.failSavesRemaining > 0 || s.failAtSave > 0 && s.saveCount == s.failAtSave {
		if s.failSavesRemaining > 0 {
			s.failSavesRemaining--
		}
		return errors.New("injected state save failure")
	}
	s.value = cloneState(value)
	s.saveHistory = append(s.saveHistory, cloneState(value))
	return nil
}
func (s *memoryStore) SaveHistory() []state.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	history := make([]state.State, len(s.saveHistory))
	for index, saved := range s.saveHistory {
		history[index] = cloneState(saved)
	}
	return history
}
func (s *memoryStore) failNext() {
	s.failNextN(1)
}
func (s *memoryStore) failNextN(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failSavesRemaining = count
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

type externalResolutionFunc func(context.Context, scheduler.Run) (bool, error)

func (f externalResolutionFunc) Reconcile(ctx context.Context, run scheduler.Run) (bool, error) {
	return f(ctx, run)
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
	issueStateFunc     func(int) (ghadapter.IssueState, error)
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
func (g *fakeGitHub) IssueState(_ context.Context, _ string, issue int) (ghadapter.IssueState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.issueStateFunc != nil {
		return g.issueStateFunc(issue)
	}
	return ghadapter.IssueState{Open: true, Labels: []string{"in-progress"}}, nil
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
	mu        sync.Mutex
	cleaned   []worktree.Assignment
	verifyErr error
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

type deadlineRecordingWorktrees struct {
	fakeWorktrees
	mu        sync.Mutex
	deadlines []time.Time
}

func (w *deadlineRecordingWorktrees) Cleanup(ctx context.Context, _ worktree.Assignment) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("cleanup context has no suspension deadline")
	}
	w.mu.Lock()
	w.deadlines = append(w.deadlines, deadline)
	w.mu.Unlock()
	return nil
}

func (w *deadlineRecordingWorktrees) deadlineSnapshot() []time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Time(nil), w.deadlines...)
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
func (w *fakeWorktrees) Verify(context.Context, worktree.Assignment) error {
	return w.verifyErr
}
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
func (p *fakeProcess) LogPaths() (string, string) {
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if p.owner.omitLogPaths {
		return "", ""
	}
	return fmt.Sprintf("/logs/run-%d.jsonl", p.issue), fmt.Sprintf("/logs/run-%d.stderr.log", p.issue)
}
func (p *fakeProcess) Release() error {
	p.owner.mu.Lock()
	releaseErr := p.owner.releaseErr
	p.owner.mu.Unlock()
	p.owner.released(p.issue)
	return releaseErr
}
func (p *fakeProcess) Abort() error {
	p.owner.mu.Lock()
	p.owner.abortCount++
	abortErr := p.owner.abortErr
	if p.owner.abortClosesProcessGroup {
		p.closeResult.LogClosed = true
		p.closeResult.GroupExited = true
	}
	p.owner.mu.Unlock()
	select {
	case p.done <- worker.Result{ExitCode: -1, Err: context.Canceled}:
	default:
	}
	return abortErr
}
func (p *fakeProcess) Suspend(ctx context.Context, request worker.ContinuationRequest) (worker.Continuation, error) {
	p.owner.mu.Lock()
	p.owner.suspendCount++
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
	p.closeOnce.Do(func() {
		p.owner.mu.Lock()
		p.owner.closeCount++
		p.owner.mu.Unlock()
		p.owner.finished(p.issue)
	})
	return p.closeResult
}
func (p *fakeProcess) CloseWithForceContext(ctx context.Context, authorizeKill func() error) worker.Result {
	p.owner.mu.Lock()
	blockSettledClose := p.owner.blockSettledClose
	settledCloseLeavesGroup := p.owner.settledCloseLeavesGroup
	settledCloseStarted := p.owner.settledCloseStarted
	onSettledClose := p.owner.onSettledClose
	authorizeClose := p.owner.authorizeClose
	p.owner.mu.Unlock()
	if !blockSettledClose {
		result := p.Close()
		if onSettledClose != nil {
			if err := onSettledClose(p.issue); err != nil {
				result.Err = err
				return result
			}
		}
		if !settledCloseLeavesGroup {
			result.GroupExited = true
		}
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
	mu                      sync.Mutex
	started                 []int
	requests                []worker.Request
	processes               map[int]*fakeProcess
	running                 int
	maximum                 int
	releases                int
	recoveredReleases       int
	onStart                 func(int)
	onRelease               func(int)
	onCloseContext          func(int) error
	onSettledClose          func(int) error
	authorizeClose          bool
	waitForForce            bool
	blockSettledClose       bool
	settledCloseLeavesGroup bool
	abortClosesProcessGroup bool
	authorizedForceStops    int
	abortCount              int
	suspendCount            int
	closeCount              int
	startErr                error
	omitLogPaths            bool
	releaseErr              error
	abortErr                error
	startupCloseResult      worker.Result
	suspendFunc             func(context.Context, int, worker.ContinuationRequest) (worker.Continuation, error)
	startChanged            chan struct{}
	closeContextStarted     chan int
	settledCloseStarted     chan int
}

func newFakeWorkers() *fakeWorkers {
	return &fakeWorkers{
		processes: make(map[int]*fakeProcess), startChanged: make(chan struct{}, 20),
		closeContextStarted: make(chan int, 20), settledCloseStarted: make(chan int, 20),
	}
}
func (w *fakeWorkers) Start(_ context.Context, request worker.Request) (WorkerProcess, error) {
	if w.onStart != nil {
		w.onStart(request.Issue)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.startErr != nil {
		return nil, w.startErr
	}
	process := &fakeProcess{issue: request.Issue, owner: w, done: make(chan worker.Result, 1), closeResult: w.startupCloseResult}
	w.processes[request.Issue] = process
	w.started = append(w.started, request.Issue)
	w.requests = append(w.requests, request)
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
func (w *fakeWorkers) suspendedCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.suspendCount
}
func (w *fakeWorkers) closedCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeCount
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
func (w *fakeWorkers) requestFor(issue int) worker.Request {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, request := range w.requests {
		if request.Issue == issue {
			return request
		}
	}
	return worker.Request{}
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
