package runner

import (
	"context"
	"errors"
	"fmt"
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

type fakeGitHub struct {
	mu                 sync.Mutex
	candidates         []scheduler.Candidate
	completions        map[int]ghadapter.CompletionOutcome
	completionErrs     map[int]error
	completionBranches []string
}

func (g *fakeGitHub) Candidates(context.Context, string) ([]scheduler.Candidate, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]scheduler.Candidate(nil), g.candidates...), nil
}
func (g *fakeGitHub) Completion(_ context.Context, _ string, issue int, branch string) (ghadapter.CompletionOutcome, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.completionBranches = append(g.completionBranches, branch)
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
func mergedOutcome(issue int) ghadapter.CompletionOutcome {
	return ghadapter.CompletionOutcome{PRFound: true, PullRequest: fmt.Sprintf("https://example.test/pr/%d", issue), Merged: true, IssueClosed: true}
}

type fakeWorktrees struct {
	mu      sync.Mutex
	cleaned []worktree.Assignment
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
	p.owner.finished(p.issue)
	return p.closeResult
}

type fakeWorkers struct {
	mu           sync.Mutex
	started      []int
	processes    map[int]*fakeProcess
	running      int
	maximum      int
	releases     int
	onRelease    func(int)
	startChanged chan struct{}
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
func (w *fakeWorkers) Release(string) error { return nil }
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
func (w *fakeWorkers) wasStarted(issue int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, started := range w.started {
		if started == issue {
			return true
		}
	}
	return false
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
