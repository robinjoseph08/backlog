package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

func TestURLOpenerExecutableUsesPlatformDefault(t *testing.T) {
	for _, test := range []struct {
		goos string
		want string
	}{
		{goos: "darwin", want: "open"},
		{goos: "linux", want: "xdg-open"},
		{goos: "freebsd", want: "xdg-open"},
	} {
		if got := urlOpenerExecutable(test.goos); got != test.want {
			t.Fatalf("URL opener for %s = %q, want %q", test.goos, got, test.want)
		}
	}
}

func TestRunURLOpenerReportsProcessExitFailureAndCancellation(t *testing.T) {
	opener := writeExecutable(t, "#!/bin/sh\nexit 23\n")
	err := runURLOpener(context.Background(), opener, "https://github.com/acme/widgets/issues/12")
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 23 {
		t.Fatalf("URL opener failure = %v, want exit code 23", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runURLOpener(ctx, opener, "https://github.com/acme/widgets/issues/12"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled URL opener = %v, want context cancellation", err)
	}
}

func TestRunnerHostOrdersExternalAndPresentationSignalsThroughOneIngress(t *testing.T) {
	external := make(chan os.Signal, 2)
	external <- os.Interrupt
	firstObserved := make(chan struct{})
	secondObserved := make(chan struct{})
	thirdObserved := make(chan struct{})
	got := make(chan []os.Signal, 1)

	host := runnerHost{terminal: TerminalDependencies{Signals: external}}
	presentation := func(ctx context.Context, control PresentationControl) error {
		<-firstObserved
		if err := control.Interrupt(ctx); err != nil {
			return err
		}
		<-secondObserved
		external <- syscall.SIGTERM
		<-thirdObserved
		if err := control.Interrupt(ctx); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	err := host.run(context.Background(), func(signals <-chan lifecycleSignal, _ func(runner.OperationalEvent)) error {
		observed := make([]os.Signal, 0, 4)
		for len(observed) < 4 {
			event := <-signals
			observed = append(observed, event.signal)
			switch len(observed) {
			case 1:
				close(firstObserved)
			case 2:
				close(secondObserved)
			case 3:
				close(thirdObserved)
			}
			event.accept()
		}
		got <- observed
		return nil
	}, presentation)
	if err != nil {
		t.Fatalf("hosted Runner: %v", err)
	}
	want := []os.Signal{os.Interrupt, os.Interrupt, syscall.SIGTERM, os.Interrupt}
	if observed := <-got; !reflect.DeepEqual(observed, want) {
		t.Fatalf("ordered signals = %v, want %v", observed, want)
	}
}

func TestPresentationEventQueueBoundsIgnoredConsumer(t *testing.T) {
	queue := newPresentationEventQueue()
	published := make(chan struct{})
	go func() {
		for failure := 1; failure <= presentationEventLimit*100; failure++ {
			queue.publish(runner.CandidateDiscoveryFailed{ConsecutiveFailures: failure})
		}
		close(published)
	}()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("publishing blocked on an ignored presentation event consumer")
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.events) != presentationAdmissionFailureLimit {
		t.Fatalf("ignored-consumer queue length = %d, want Admission failure limit %d", len(queue.events), presentationAdmissionFailureLimit)
	}
	latest, ok := queue.events[len(queue.events)-1].(runner.CandidateDiscoveryFailed)
	if !ok || latest.ConsecutiveFailures != presentationEventLimit*100 {
		t.Fatalf("latest retained Admission event = %#v", queue.events[len(queue.events)-1])
	}
	totalOccurrences := 0
	for _, event := range queue.events {
		failure, ok := event.(runner.CandidateDiscoveryFailed)
		if !ok {
			t.Fatalf("retained event = %T, want CandidateDiscoveryFailed", event)
		}
		totalOccurrences += presentationFailureOccurrences(failure)
	}
	if totalOccurrences != presentationEventLimit*100 {
		t.Fatalf("retained equivalent occurrences = %d, want %d", totalOccurrences, presentationEventLimit*100)
	}
}

func TestPresentationEventQueueRetainsExactCountsBeyondOneThousandIdentities(t *testing.T) {
	const distinctIdentities = 1024
	queue := newPresentationEventQueue()
	queue.publish(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Cause: "recurring cause", Occurrences: 1,
	})
	for identity := 1; identity <= distinctIdentities; identity++ {
		queue.publish(runner.CandidateDiscoveryFailed{
			Operation: runner.CandidateDiscoveryList, Cause: fmt.Sprintf("distinct cause %d", identity), Occurrences: 1,
		})
	}
	queue.publish(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Cause: "recurring cause", Occurrences: 1,
	})

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if identities := len(queue.evictedFailureOccurrences); identities != distinctIdentities-presentationAdmissionFailureLimit+1 {
		t.Fatalf("lightweight presentation identities = %d, want %d retained episode identities", identities, distinctIdentities-presentationAdmissionFailureLimit+1)
	}
	if failures := presentationAdmissionFailureCount(queue.events); failures != presentationAdmissionFailureLimit {
		t.Fatalf("queued failure records = %d, want %d", failures, presentationAdmissionFailureLimit)
	}
	latest := queue.events[len(queue.events)-1].(runner.CandidateDiscoveryFailed)
	if occurrences := presentationFailureOccurrences(latest); occurrences != 2 {
		t.Fatalf("recurring cause after %d identities = %d occurrences, want 2", distinctIdentities, occurrences)
	}
}

type composedBackpressureGitHub struct {
	failures          []error
	firstWave         int
	continueAfterWave <-chan struct{}
	firstWaveReady    chan<- struct{}
	freshQueued       chan<- struct{}
	deliveryProgress  *atomic.Int64
	deliveryBaseline  int64
	calls             atomic.Int64
	clockUnix         *atomic.Int64
}

func (g *composedBackpressureGitHub) Candidates(ctx context.Context, _ string) ([]scheduler.Candidate, error) {
	call := int(g.calls.Add(1)) - 1
	g.clockUnix.Store(int64(call + 1))
	if call == g.firstWave {
		close(g.firstWaveReady)
		select {
		case <-g.continueAfterWave:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if call > g.firstWave && call <= len(g.failures) {
		target := g.deliveryBaseline + int64(call-g.firstWave)
		for g.deliveryProgress.Load() < target {
			select {
			case <-time.After(50 * time.Microsecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if call < len(g.failures) {
		return nil, g.failures[call]
	}
	if call == len(g.failures) {
		return nil, nil
	}
	if call == len(g.failures)+1 {
		return nil, &ghadapter.CandidateDiscoveryError{
			Operation: ghadapter.CandidateDiscoveryList,
			Cause:     "recurring cause",
			Err:       errors.New("fresh recurring diagnostic"),
		}
	}
	close(g.freshQueued)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*composedBackpressureGitHub) Completion(context.Context, string, int, string) (ghadapter.CompletionOutcome, error) {
	return ghadapter.CompletionOutcome{}, nil
}

func (*composedBackpressureGitHub) IssueState(context.Context, string, int) (ghadapter.IssueState, error) {
	return ghadapter.IssueState{}, nil
}

type composedBackpressureStore struct{ current state.State }

func (s *composedBackpressureStore) Load() (state.State, error) { return s.current, nil }
func (s *composedBackpressureStore) Save(current state.State) error {
	s.current = current
	return nil
}

type composedBackpressureWorktrees struct{}

func (composedBackpressureWorktrees) Plan(int, string) (worktree.Assignment, error) {
	return worktree.Assignment{}, errors.New("unexpected worktree plan")
}
func (composedBackpressureWorktrees) Prepare(context.Context, worktree.Assignment) error {
	return errors.New("unexpected worktree preparation")
}
func (composedBackpressureWorktrees) Verify(context.Context, worktree.Assignment) error {
	return errors.New("unexpected worktree verification")
}
func (composedBackpressureWorktrees) Cleanup(context.Context, worktree.Assignment) error {
	return errors.New("unexpected worktree cleanup")
}
func (composedBackpressureWorktrees) Exists(worktree.Assignment) bool { return false }

type composedBackpressureWorkers struct{}

func (composedBackpressureWorkers) Start(context.Context, worker.Request) (runner.WorkerProcess, error) {
	return nil, errors.New("unexpected Worker start")
}
func (composedBackpressureWorkers) Release(string) error { return nil }

type composedOversizedGitHub struct {
	failures []error
	calls    atomic.Int64
	blocked  chan struct{}
}

func (g *composedOversizedGitHub) Candidates(ctx context.Context, _ string) ([]scheduler.Candidate, error) {
	call := int(g.calls.Add(1)) - 1
	if call < len(g.failures) {
		return nil, g.failures[call]
	}
	if call == len(g.failures) {
		return nil, nil
	}
	close(g.blocked)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*composedOversizedGitHub) Completion(context.Context, string, int, string) (ghadapter.CompletionOutcome, error) {
	return ghadapter.CompletionOutcome{}, nil
}

func (*composedOversizedGitHub) IssueState(context.Context, string, int) (ghadapter.IssueState, error) {
	return ghadapter.IssueState{}, nil
}

func TestOversizedAdmissionFailuresRemainCompleteInPlainOutputAndLatestTwentyDiagnostics(t *testing.T) {
	const failures = 25
	failureErrors := make([]error, 0, failures)
	for failure := 1; failure <= failures; failure++ {
		tail := fmt.Sprintf("complete oversized tail %02d", failure)
		evidence := strings.Repeat(fmt.Sprintf("oversized failure %02d evidence ", failure), 300) + tail
		failureErrors = append(failureErrors, &ghadapter.CandidateDiscoveryError{
			Operation: ghadapter.CandidateDiscoveryList, Cause: "oversized failure",
			Err: errors.New(evidence),
		})
	}
	github := &composedOversizedGitHub{failures: failureErrors, blocked: make(chan struct{})}
	queue := newPresentationEventQueue()
	var plain bytes.Buffer
	candidateRunner := &runner.Runner{
		Config: runner.Config{
			Repo: "acme/widgets", DefaultBranch: "master", MaxConcurrentIssues: 1,
			PollInterval: 10 * time.Microsecond, Watch: true, SessionsDir: t.TempDir(),
		},
		GitHub: github, Store: &composedBackpressureStore{current: state.State{Version: state.CurrentVersion}},
		Worktrees: composedBackpressureWorktrees{}, Workers: composedBackpressureWorkers{},
		Output: &plain, OnOperationalEvent: queue.publish,
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- candidateRunner.Run(ctx) }()
	select {
	case <-github.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("Runner did not emit the oversized failure sequence")
	}
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Runner oversized failure sequence: %v", err)
	}
	candidateRunner.WaitForOperationalEventDelivery()

	output := plain.String()
	if count := strings.Count(output, "candidate discovery failed; admission paused"); count != failures {
		t.Fatalf("plain oversized failure rows = %d, want %d", count, failures)
	}
	for failure := 1; failure <= failures; failure++ {
		if tail := fmt.Sprintf("complete oversized tail %02d", failure); !strings.Contains(output, tail) {
			t.Fatalf("plain output omitted complete diagnostic %q", tail)
		}
	}
	if strings.Contains(output, runner.ErrCandidateDiscoveryDiagnosticExpired.Error()) || strings.Contains(output, "truncated") {
		t.Fatal("plain output replaced complete evidence with an expiry or truncation marker")
	}

	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
	drainCtx, stopDrain := context.WithCancel(context.Background())
	stopDrain()
	for {
		event, err := queue.next(drainCtx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		dashboard.operationalEvent(event)
		queue.complete()
	}
	if got := len(dashboard.admission.failures); got != dashboardDiagnosticLimit {
		t.Fatalf("dashboard oversized Diagnostics = %d, want latest %d", got, dashboardDiagnosticLimit)
	}
	first, latest := dashboard.admission.failures[0], dashboard.admission.failures[dashboardDiagnosticLimit-1]
	if first.unavailable || latest.unavailable || !strings.Contains(first.evidence, "complete oversized tail 06") || !strings.Contains(latest.evidence, "complete oversized tail 25") {
		t.Fatalf("dashboard did not retain complete latest-twenty oversized evidence")
	}
}

func TestAdmissionBackpressureComposesRunnerPresentationAndDashboardBounds(t *testing.T) {
	const (
		firstDistinct  = 600
		secondDistinct = 600
	)
	failures := make([]error, 0, firstDistinct+secondDistinct+3)
	failure := func(cause string) error {
		return &ghadapter.CandidateDiscoveryError{
			Operation: ghadapter.CandidateDiscoveryList,
			Cause:     cause,
			Err:       fmt.Errorf("full diagnostic for %s", cause),
		}
	}
	failures = append(failures, failure("recurring cause"))
	for identity := 1; identity <= firstDistinct; identity++ {
		failures = append(failures, failure(fmt.Sprintf("first distinct cause %d", identity)))
	}
	failures = append(failures, failure("recurring cause"))
	firstWave := len(failures)
	for identity := 1; identity <= secondDistinct; identity++ {
		failures = append(failures, failure(fmt.Sprintf("second distinct cause %d", identity)))
	}
	failures = append(failures, failure("recurring cause"))

	base := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	var clockUnix atomic.Int64
	firstWaveReady := make(chan struct{})
	continueAfterWave := make(chan struct{})
	freshQueued := make(chan struct{})
	var delivered atomic.Int64
	deliveryBaseline := int64(1 + presentationAdmissionFailureLimit)
	github := &composedBackpressureGitHub{
		failures: failures, firstWave: firstWave, continueAfterWave: continueAfterWave,
		firstWaveReady: firstWaveReady, freshQueued: freshQueued, deliveryProgress: &delivered,
		deliveryBaseline: deliveryBaseline, clockUnix: &clockUnix,
	}
	queue := newPresentationEventQueue()
	firstDeliveryStarted := make(chan struct{})
	releaseFirstDelivery := make(chan struct{})
	recoveryDeliveryStarted := make(chan struct{})
	releaseRecovery := make(chan struct{})
	var first atomic.Bool
	candidateRunner := &runner.Runner{
		Config: runner.Config{
			Repo: "acme/widgets", DefaultBranch: "master", MaxConcurrentIssues: 1,
			PollInterval: 10 * time.Microsecond, Watch: true, SessionsDir: t.TempDir(),
		},
		GitHub: github, Store: &composedBackpressureStore{current: state.State{Version: state.CurrentVersion}},
		Worktrees: composedBackpressureWorktrees{}, Workers: composedBackpressureWorkers{},
		Output: io.Discard, SuppressOperationalEventOutput: true,
		Now: func() time.Time { return base.Add(time.Duration(clockUnix.Load()) * time.Second) },
		OnOperationalEvent: func(event runner.OperationalEvent) {
			if first.CompareAndSwap(false, true) {
				close(firstDeliveryStarted)
				<-releaseFirstDelivery
			}
			if _, recovered := event.(runner.CandidateDiscoveryRecovered); recovered {
				close(recoveryDeliveryStarted)
				<-releaseRecovery
			}
			queue.publish(event)
			delivered.Add(1)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- candidateRunner.Run(ctx) }()
	<-firstDeliveryStarted
	<-firstWaveReady
	close(releaseFirstDelivery)

	waitFor := func(description string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !condition() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", description)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitFor("bounded first Runner wave delivery", func() bool { return delivered.Load() >= deliveryBaseline })
	if got := delivered.Load(); got != deliveryBaseline {
		t.Fatalf("Runner deliveries from blocked first wave = %d, want one in-flight plus twenty bounded records", got)
	}
	close(continueAfterWave)
	<-recoveryDeliveryStarted
	if got := delivered.Load(); got <= firstDistinct {
		t.Fatalf("presentation deliveries before recovery = %d, want pressure beyond %d identities", got, firstDistinct)
	}

	queue.mu.Lock()
	queuedFailures := presentationAdmissionFailureCount(queue.events)
	presentationIdentities := len(queue.evictedFailureOccurrences)
	queueRecurring := 0
	var queueRecurringFirst time.Time
	for _, event := range queue.events {
		failure, ok := event.(runner.CandidateDiscoveryFailed)
		if !ok || failure.Cause != "recurring cause" {
			continue
		}
		queueRecurring += presentationFailureOccurrences(failure)
		if queueRecurringFirst.IsZero() || failure.FirstFailureAt.Before(queueRecurringFirst) {
			queueRecurringFirst = failure.FirstFailureAt
		}
	}
	queue.mu.Unlock()
	if queuedFailures != presentationAdmissionFailureLimit {
		t.Fatalf("presentation failure records = %d, want bounded %d", queuedFailures, presentationAdmissionFailureLimit)
	}
	if presentationIdentities <= 256 {
		t.Fatalf("presentation lightweight identities = %d, want exact episode state well beyond 256 under pressure", presentationIdentities)
	}
	if queueRecurring != 3 || !queueRecurringFirst.Equal(base.Add(time.Second)) {
		t.Fatalf("presentation recurring failures = %d from %s, want 3 from %s", queueRecurring, queueRecurringFirst, base.Add(time.Second))
	}

	close(releaseRecovery)
	<-freshQueued
	waitFor("fresh episode presentation delivery", func() bool {
		queue.mu.Lock()
		defer queue.mu.Unlock()
		for _, event := range queue.events {
			failure, ok := event.(runner.CandidateDiscoveryFailed)
			if ok && failure.ConsecutiveFailures == 1 && failure.OccurredAt.Equal(base.Add(time.Duration(len(failures)+2)*time.Second)) {
				return true
			}
		}
		return false
	})
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Runner under composed backpressure: %v", err)
	}
	candidateRunner.WaitForOperationalEventDelivery()

	queue.mu.Lock()
	if identities := len(queue.evictedFailureOccurrences); identities != 0 {
		queue.mu.Unlock()
		t.Fatalf("presentation recovery retained %d old episode identities", identities)
	}
	queue.mu.Unlock()

	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, func() time.Time { return base })
	drainCtx, stopDrain := context.WithCancel(context.Background())
	stopDrain()
	sawRecovery := false
	for {
		event, err := queue.next(drainCtx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, recovered := event.(runner.CandidateDiscoveryRecovered); recovered {
			dashboard.mu.Lock()
			key := string(runner.CandidateDiscoveryList) + "\x00recurring cause"
			if dashboard.admission.consecutiveFailures != len(failures) ||
				dashboard.admission.equivalentFailures[key] != 3 ||
				!dashboard.admission.firstFailure.Equal(base.Add(time.Second)) ||
				!dashboard.admission.latestFailure.Equal(base.Add(time.Duration(len(failures))*time.Second)) {
				dashboard.mu.Unlock()
				t.Fatalf("dashboard old episode lost or doubled count/time: %#v", dashboard.admission)
			}
			dashboard.mu.Unlock()
			sawRecovery = true
		}
		dashboard.operationalEvent(event)
		queue.complete()
		if sawRecovery {
			dashboard.mu.Lock()
			if _, recovered := event.(runner.CandidateDiscoveryRecovered); recovered &&
				(dashboard.admission.degraded || !dashboard.admission.snapshotComplete || dashboard.admission.equivalentFailures != nil) {
				dashboard.mu.Unlock()
				t.Fatalf("dashboard recovery did not reset episode state: %#v", dashboard.admission)
			}
			dashboard.mu.Unlock()
		}
	}
	if !sawRecovery {
		t.Fatal("bounded composed delivery lost recovery transition")
	}

	dashboard.mu.Lock()
	key := string(runner.CandidateDiscoveryList) + "\x00recurring cause"
	freshAt := base.Add(time.Duration(len(failures)+2) * time.Second)
	if !dashboard.admission.degraded || dashboard.admission.consecutiveFailures != 1 ||
		dashboard.admission.equivalentFailures[key] != 1 ||
		!dashboard.admission.firstFailure.Equal(freshAt) || !dashboard.admission.latestFailure.Equal(freshAt) {
		dashboard.mu.Unlock()
		t.Fatalf("fresh dashboard episode inherited old count/time: %#v", dashboard.admission)
	}
	if diagnostics := len(dashboard.admission.failures); diagnostics != dashboardDiagnosticLimit {
		dashboard.mu.Unlock()
		t.Fatalf("dashboard diagnostics = %d, want bounded latest %d", diagnostics, dashboardDiagnosticLimit)
	}
	for _, diagnostic := range dashboard.admission.failures {
		if diagnostic.unavailable {
			dashboard.mu.Unlock()
			t.Fatal("bounded composed delivery retained an expired latest diagnostic")
		}
	}
	if identities := len(dashboard.admission.equivalentFailures); identities != 1 {
		dashboard.mu.Unlock()
		t.Fatalf("fresh dashboard identities = %d, want one fresh episode identity", identities)
	}
	dashboard.mu.Unlock()
}

func TestPresentationEventQueuePreservesOrderedShutdownAndTerminalDeliveryForSlowConsumer(t *testing.T) {
	queue := newPresentationEventQueue()
	for failure := 1; failure <= presentationEventLimit*4; failure++ {
		queue.publish(runner.CandidateDiscoveryFailed{ConsecutiveFailures: failure})
	}
	for _, stage := range []runner.ShutdownStage{
		runner.ShutdownStageDraining,
		runner.ShutdownStageSuspending,
		runner.ShutdownStageForceStopping,
		runner.ShutdownStageSuspensionIncomplete,
	} {
		queue.publish(runner.ShutdownEvent{Stage: stage})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var events []runner.OperationalEvent
	for {
		event, err := queue.next(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("drain bounded event queue: %v", err)
			}
			break
		}
		events = append(events, event)
	}
	wantEvents := presentationAdmissionFailureLimit + 4
	if len(events) != wantEvents {
		t.Fatalf("slow-consumer delivery count = %d, want bounded %d", len(events), wantEvents)
	}
	previousFailure := 0
	for _, event := range events[:len(events)-4] {
		failure, ok := event.(runner.CandidateDiscoveryFailed)
		if !ok || failure.ConsecutiveFailures <= previousFailure {
			t.Fatalf("retained Admission delivery is not ordered: %#v", events)
		}
		previousFailure = failure.ConsecutiveFailures
	}
	for index, stage := range []runner.ShutdownStage{
		runner.ShutdownStageDraining,
		runner.ShutdownStageSuspending,
		runner.ShutdownStageForceStopping,
		runner.ShutdownStageSuspensionIncomplete,
	} {
		shutdown, ok := events[len(events)-4+index].(runner.ShutdownEvent)
		if !ok || shutdown.Stage != stage {
			t.Fatalf("shutdown delivery %d = %#v, want stage %s", index, events[len(events)-4+index], stage)
		}
	}
}

func TestPresentationEventQueuePreservesLatestAdmissionTransitionUnderLifecyclePressure(t *testing.T) {
	firstFailure := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	queue := newPresentationEventQueue()
	dashboard := newLiveDashboard(io.Discard, nil, state.State{Version: state.CurrentVersion}, time.Now)
	queue.publish(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Cause: "recurring cause", Err: errors.New("old full diagnostic"),
		FirstFailureAt: firstFailure, OccurredAt: firstFailure, ConsecutiveFailures: 1, Occurrences: 1,
	})
	event, err := queue.next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dashboard.operationalEvent(event)
	queue.complete()

	recoveredAt := firstFailure.Add(time.Minute)
	queue.publish(runner.CandidateDiscoveryRecovered{OccurredAt: recoveredAt, Failures: 1})
	for lifecycle := 0; lifecycle < presentationEventLimit+8; lifecycle++ {
		queue.publish(runner.RunLifecycleEvent{Stage: runner.RunLifecycleClaimed, Message: fmt.Sprintf("Run %d claimed", lifecycle)})
	}
	queue.mu.Lock()
	recoveryRetained := false
	for _, queued := range queue.events {
		if _, ok := queued.(runner.CandidateDiscoveryRecovered); ok {
			recoveryRetained = true
			break
		}
	}
	queue.mu.Unlock()
	if !recoveryRetained {
		t.Fatal("latest Admission recovery was evicted by lifecycle pressure")
	}

	freshFailure := recoveredAt.Add(time.Minute)
	queue.publish(runner.CandidateDiscoveryFailed{
		Operation: runner.CandidateDiscoveryList, Cause: "recurring cause", Err: errors.New("fresh full diagnostic"),
		FirstFailureAt: freshFailure, OccurredAt: freshFailure, RetryAt: freshFailure.Add(time.Minute),
		ConsecutiveFailures: 1, Occurrences: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		event, err := queue.next(ctx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		dashboard.operationalEvent(event)
		queue.complete()
	}

	_, body, _ := dashboard.renderParts(freshFailure)
	for _, want := range []string{
		"Admission: DEGRADED | 1 consecutive failure",
		"First failure: 2026-07-28T12:02:00Z",
		"Latest failure: 2026-07-28T12:02:00Z",
		"Cause: recurring cause",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("fresh Admission episode missing %q after queue pressure:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Equivalent failures:") {
		t.Fatalf("fresh Admission episode joined stale equivalent counts:\n%s", body)
	}
}

func TestPresentationInterruptWaitsForLifecycleAcceptance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingress := newOrderedSignalIngress(ctx, nil)
	returned := make(chan error, 1)
	go func() { returned <- ingress.submit(context.Background(), os.Interrupt) }()

	event := <-ingress.events
	select {
	case err := <-returned:
		t.Fatalf("interrupt returned before lifecycle acceptance: %v", err)
	default:
	}
	event.accept()
	if err := <-returned; err != nil {
		t.Fatalf("accepted interrupt: %v", err)
	}
}

func TestRunnerHostPresentationFailureRequestsSuspensionAndWaitsForRunner(t *testing.T) {
	for _, test := range []struct {
		name         string
		presentation Presentation
		wantFailure  string
	}{
		{
			name: "returned error",
			presentation: func(context.Context, PresentationControl) error {
				return errors.New("screen unavailable")
			},
			wantFailure: "screen unavailable",
		},
		{
			name: "panic",
			presentation: func(context.Context, PresentationControl) error {
				panic("renderer panic")
			},
			wantFailure: "panic: renderer panic",
		},
		{
			name: "unexpected clean return",
			presentation: func(context.Context, PresentationControl) error {
				return nil
			},
			wantFailure: "presentation stopped while Runner was active",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handling := make(chan struct{})
			finishHandling := make(chan struct{})
			done := make(chan error, 1)
			host := runnerHost{terminal: TerminalDependencies{}}
			go func() {
				done <- host.run(context.Background(), func(signals <-chan lifecycleSignal, _ func(runner.OperationalEvent)) error {
					event := <-signals
					if event.signal != syscall.SIGTERM {
						t.Errorf("presentation failure signal = %v, want SIGTERM", event.signal)
					}
					event.accept()
					close(handling)
					<-finishHandling
					return &runner.SignalExit{Code: 143}
				}, test.presentation)
			}()

			select {
			case <-handling:
			case <-time.After(time.Second):
				t.Fatal("Runner did not receive presentation-failure suspension")
			}
			select {
			case err := <-done:
				t.Fatalf("host returned before Runner handled Owned Workers: %v", err)
			default:
			}
			close(finishHandling)
			err := <-done
			var failure *PresentationFailure
			var signalExit *runner.SignalExit
			if !errors.As(err, &failure) || !errors.As(err, &signalExit) || signalExit.Code != 143 || !strings.Contains(err.Error(), test.wantFailure) {
				t.Fatalf("host error = %v, want presentation failure joined with signal exit 143", err)
			}
		})
	}
}

func TestRunnerHostAcceptsCleanPresentationReturnAfterParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runnerStarted := make(chan struct{})
	presentationCanceled := make(chan struct{})
	receivedSignal := make(chan os.Signal, 1)
	runnerErr := errors.New("Runner stopped after parent cancellation")
	host := runnerHost{terminal: TerminalDependencies{}}
	done := make(chan error, 1)
	go func() {
		done <- host.run(ctx, func(signals <-chan lifecycleSignal, _ func(runner.OperationalEvent)) error {
			close(runnerStarted)
			<-ctx.Done()
			select {
			case event := <-signals:
				receivedSignal <- event.signal
				event.accept()
			case <-time.After(250 * time.Millisecond):
			}
			return runnerErr
		}, func(presentationCtx context.Context, _ PresentationControl) error {
			<-presentationCtx.Done()
			close(presentationCanceled)
			return nil
		})
	}()

	<-runnerStarted
	cancel()
	<-presentationCanceled
	if err := <-done; err != runnerErr {
		t.Fatalf("host error = %v, want Runner completion after clean canceled presentation return", err)
	}
	select {
	case signal := <-receivedSignal:
		t.Fatalf("clean canceled presentation return submitted signal %v", signal)
	default:
	}
}

func TestRunnerFirstCompletionAcceptsMatchingPresentationDeadline(t *testing.T) {
	parentCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	presentationCtx, stopPresentation := context.WithCancel(parentCtx)
	presentationDone := make(chan error, 1)
	presentationDone <- errors.Join(errors.New("terminal restore stopped"), context.DeadlineExceeded)
	runnerErr := errors.New("Runner stopped after deadline")

	err := finishPresentationAfterRunner(presentationCtx, runnerErr, stopPresentation, presentationDone)
	if err != runnerErr {
		t.Fatalf("host error = %v, want Runner completion after matching presentation deadline", err)
	}
}

func TestRunnerHostReportsRunnerFirstCompletionAsPresentationFailure(t *testing.T) {
	presentationStarted := make(chan struct{})
	runnerErr := errors.New("Runner completed unsuccessfully")
	host := runnerHost{terminal: TerminalDependencies{}}
	err := host.run(context.Background(), func(<-chan lifecycleSignal, func(runner.OperationalEvent)) error {
		<-presentationStarted
		return runnerErr
	}, func(ctx context.Context, _ PresentationControl) error {
		close(presentationStarted)
		<-ctx.Done()
		return errors.New("terminal restore failed")
	})

	var failure *PresentationFailure
	if !errors.As(err, &failure) || failure.RunnerErr != runnerErr {
		t.Fatalf("host error = %v, want presentation failure with Runner completion", err)
	}
	if !strings.Contains(err.Error(), "Runner completion: Runner completed unsuccessfully") {
		t.Fatalf("host error = %q, want accurate Runner completion label", err)
	}
}

func TestMainWithTerminalSuppliesCompleteSeamAndRoutesPresentationCtrlC(t *testing.T) {
	for _, test := range []struct {
		name       string
		interrupts int
		wantExit   int
		wantOutput string
	}{
		{name: "first Ctrl-C drains", interrupts: 1, wantExit: 0, wantOutput: "Drain: admission stopped during setup"},
		{name: "second Ctrl-C suspends", interrupts: 2, wantExit: 130, wantOutput: "Suspension: repeated SIGINT accepted during setup"},
		{name: "third Ctrl-C force stops", interrupts: 3, wantExit: 130, wantOutput: "Force stop: additional signal accepted during setup"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			started := filepath.Join(root, "git-started")
			git := writeExecutable(t, `#!/bin/sh
set -eu
touch `+quote(started)+`
exec sleep 30
`)
			input := strings.NewReader("terminal input")
			var stdout, stderr bytes.Buffer
			fixedNow := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
			opened := ""
			presentationChecked := make(chan struct{})
			dependencies := TerminalDependencies{
				Input: input, Output: &stdout, ErrorOutput: &stderr,
				IsTerminal: func() bool { return true },
				Dimensions: func() (TerminalDimensions, error) {
					return TerminalDimensions{Width: 91, Height: 23}, nil
				},
				ColorProfile: func() TerminalColorProfile { return TerminalColorANSI256 },
				Now:          func() time.Time { return fixedNow },
				OpenURL: func(_ context.Context, url string) error {
					opened = url
					return nil
				},
			}
			dependencies.Presentation = func(ctx context.Context, control PresentationControl) error {
				if control.Terminal.Input != input || control.Terminal.Output != &stdout || control.Terminal.ErrorOutput != &stderr {
					return errors.New("presentation received different terminal streams")
				}
				if control.Terminal.IsTerminal == nil || !control.Terminal.IsTerminal() {
					return errors.New("presentation received different terminal capability")
				}
				dimensions, err := control.Terminal.Dimensions()
				if err != nil || dimensions != (TerminalDimensions{Width: 91, Height: 23}) {
					return errors.New("presentation received different dimensions")
				}
				if control.Terminal.ColorProfile() != TerminalColorANSI256 || !control.Terminal.Now().Equal(fixedNow) {
					return errors.New("presentation received different color profile or clock")
				}
				if err := control.Terminal.OpenURL(ctx, "https://example.test/issue/65"); err != nil || opened == "" {
					return errors.New("presentation received different URL opener")
				}
				if err := waitForPresentationPath(ctx, started); err != nil {
					return err
				}
				for count := 0; count < test.interrupts; count++ {
					if err := control.Interrupt(ctx); err != nil {
						return err
					}
				}
				close(presentationChecked)
				<-ctx.Done()
				return ctx.Err()
			}

			exit := MainWithTerminal(context.Background(), []string{"run", "--git", git}, dependencies)
			if exit != test.wantExit {
				t.Fatalf("exit = %d, want %d, stdout = %q, stderr = %q", exit, test.wantExit, stdout.String(), stderr.String())
			}
			select {
			case <-presentationChecked:
			default:
				t.Fatal("presentation did not exercise terminal dependencies")
			}
			if !strings.Contains(stdout.String(), test.wantOutput) {
				t.Fatalf("stdout = %q, want %q", stdout.String(), test.wantOutput)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q", stderr.String())
			}
		})
	}
}

func TestMainWithTerminalRoutesTypedOperationalEventsToSelectedPresentation(t *testing.T) {
	repository := initializeFollowRepository(t)
	root := t.TempDir()
	failedOnce := filepath.Join(root, "candidate-failed")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if ! test -f `+quote(failedOnce)+`; then touch `+quote(failedOnce)+`; echo "GitHub temporarily unavailable" >&2; exit 1; fi
    printf '%s\n' '[]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	var stdout, stderr bytes.Buffer
	var events []runner.OperationalEvent
	dependencies := TerminalDependencies{
		Output: &stdout, ErrorOutput: &stderr, IsTerminal: func() bool { return true },
	}
	dependencies.Presentation = func(ctx context.Context, control PresentationControl) error {
		interrupted := false
		for {
			event, err := control.NextOperationalEvent(ctx)
			if err != nil {
				return err
			}
			events = append(events, event)
			if _, recovered := event.(runner.CandidateDiscoveryRecovered); recovered && !interrupted {
				interrupted = true
				if err := control.Interrupt(ctx); err != nil {
					return err
				}
			}
			if shutdown, ok := event.(runner.ShutdownEvent); ok && shutdown.Stage == runner.ShutdownStageDrainComplete {
				<-ctx.Done()
				return ctx.Err()
			}
		}
	}

	exit := MainWithTerminal(context.Background(), []string{
		"run", "--watch", "--repo-dir", repository, "--state-dir", filepath.Join(root, "state"), "--poll", "5ms", "--gh", gh,
	}, dependencies)
	if exit != 0 {
		t.Fatalf("exit = %d, stdout = %q, stderr = %q", exit, stdout.String(), stderr.String())
	}
	if len(events) != 4 {
		t.Fatalf("presentation events = %#v, want failure, recovery, Drain, and Drain completion", events)
	}
	if _, ok := events[0].(runner.CandidateDiscoveryFailed); !ok {
		t.Fatalf("first presentation event = %T, want CandidateDiscoveryFailed", events[0])
	}
	if _, ok := events[1].(runner.CandidateDiscoveryRecovered); !ok {
		t.Fatalf("second presentation event = %T, want CandidateDiscoveryRecovered", events[1])
	}
	for index, stage := range []runner.ShutdownStage{runner.ShutdownStageDraining, runner.ShutdownStageDrainComplete} {
		shutdown, ok := events[index+2].(runner.ShutdownEvent)
		if !ok || shutdown.Stage != stage {
			t.Fatalf("presentation event %d = %#v, want shutdown stage %s", index+2, events[index+2], stage)
		}
	}
	for _, message := range []string{
		"candidate discovery failed; admission paused",
		"candidate discovery recovered; admission resumed after 1 failure",
		"Drain: admission stopped; 0 Workers remaining",
		"Drain complete: 0 Workers remaining; exiting successfully",
	} {
		if !strings.Contains(stdout.String(), message) {
			t.Fatalf("compatible plain output omitted %q: %q", message, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "Backlog Run Dashboard") || strings.Contains(stdout.String(), "\x1b[") || stderr.Len() != 0 {
		t.Fatalf("selected presentation output compatibility changed: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	persisted, err := os.ReadFile(filepath.Join(root, "state", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, transient := range []string{"GitHub temporarily unavailable", "candidate discovery", "Admission", "Diagnostics"} {
		if strings.Contains(string(persisted), transient) {
			t.Fatalf("transient presentation state %q was persisted: %s", transient, persisted)
		}
	}
}

func TestMainWithTerminalRoutesPresentationCtrlCDuringOwnedWorker(t *testing.T) {
	for _, test := range []struct {
		name         string
		interrupts   int
		workerScript func(root, started string) string
		wantExit     int
		wantStatus   scheduler.Status
		wantOutput   string
		wantBoundary bool
		wantLease    bool
	}{
		{
			name: "first Ctrl-C drains active Worker", interrupts: 1, wantExit: 0,
			wantStatus: scheduler.StatusMerged, wantOutput: "Drain: admission stopped; 1 Worker remaining",
			workerScript: func(root, started string) string {
				return `#!/bin/sh
set -eu
IFS= read -r prompt
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
touch ` + quote(started) + `
while ! test -f ` + quote(filepath.Join(root, "release-worker")) + `; do sleep 0.01; done
touch ` + quote(filepath.Join(root, "worker-completed")) + `
printf '%s\n' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
`
			},
		},
		{
			name: "second Ctrl-C suspends active Worker", interrupts: 2, wantExit: 130,
			wantStatus: scheduler.StatusSuspended, wantOutput: "Drain: additional interrupt recorded as a suspension request; 1 Worker remaining",
			wantBoundary: true, wantLease: true,
			workerScript: presentationSuspendingWorkerScript,
		},
		{
			name: "third Ctrl-C force stops active Worker", interrupts: 3, wantExit: 130,
			wantStatus: scheduler.StatusNeedsHuman, wantOutput: "Force stop: additional signal accepted",
			wantLease: true,
			workerScript: func(_ string, started string) string {
				return `#!/bin/sh
set -eu
IFS= read -r prompt
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
touch ` + quote(started) + `
IFS= read -r abort
trap '' TERM
while :; do sleep 1; done
`
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPresentationWorkerFixture(t, test.workerScript)
			var stdout synchronizedBuffer
			var stderr bytes.Buffer
			dependencies := TerminalDependencies{
				Input: strings.NewReader(""), Output: &stdout, ErrorOutput: &stderr,
				IsTerminal: func() bool { return true },
			}
			dependencies.Presentation = func(ctx context.Context, control PresentationControl) error {
				if err := waitForPresentationPath(ctx, fixture.workerStarted); err != nil {
					return err
				}
				for count := 0; count < test.interrupts; count++ {
					if err := control.Interrupt(ctx); err != nil {
						return err
					}
				}
				if test.interrupts < 3 {
					if err := os.WriteFile(filepath.Join(fixture.root, "release-worker"), nil, 0o600); err != nil {
						return err
					}
				}
				<-ctx.Done()
				return ctx.Err()
			}

			done := make(chan int, 1)
			go func() { done <- MainWithTerminal(context.Background(), fixture.args, dependencies) }()
			var exit int
			select {
			case exit = <-done:
			case <-time.After(15 * time.Second):
				t.Fatal("hosted Runner did not finish after presentation Ctrl-C")
			}
			if exit != test.wantExit {
				t.Fatalf("exit = %d, want %d, stdout = %q, stderr = %q", exit, test.wantExit, stdout.String(), stderr.String())
			}
			current, err := (state.FileStore{Path: fixture.statePath}).Load()
			if err != nil {
				t.Fatal(err)
			}
			if len(current.Runs) != 1 || current.Runs[0].Status != test.wantStatus || (current.Runs[0].Continuation != nil) != test.wantBoundary || (len(current.Leases) == 1) != test.wantLease {
				t.Fatalf("final state after presentation Ctrl-C = %#v", current)
			}
			if !strings.Contains(stdout.String(), test.wantOutput) {
				t.Fatalf("stdout = %q, want %q", stdout.String(), test.wantOutput)
			}
			if strings.Contains(stdout.String(), "Backlog Run Dashboard") || strings.Contains(stdout.String(), "\x1b[") {
				t.Fatalf("injected presentation shared output with legacy dashboard: %q", stdout.String())
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q", stderr.String())
			}
		})
	}
}

type armedPresentationFailureWriter struct {
	output synchronizedBuffer
	armed  atomic.Bool
	failed atomic.Bool
}

func (w *armedPresentationFailureWriter) Write(content []byte) (int, error) {
	written, err := w.output.Write(content)
	if err == nil && len(content) > 0 && w.armed.Load() && w.failed.CompareAndSwap(false, true) {
		return written, errors.New("terminal output lost")
	}
	return written, err
}

func (w *armedPresentationFailureWriter) String() string {
	return w.output.String()
}

func TestDefaultDashboardPresentationFailurePrintsSuspendedOwnedWorkerInFinalSummary(t *testing.T) {
	fixture := newPresentationWorkerFixture(t, presentationSuspendingWorkerScript)
	suspensionStarted := filepath.Join(fixture.root, "suspension-started")
	var stdout armedPresentationFailureWriter
	var stderr bytes.Buffer
	dependencies := TerminalDependencies{
		Input: strings.NewReader(""), Output: &stdout, ErrorOutput: &stderr,
		IsTerminal: func() bool { return true },
		Dimensions: func() (TerminalDimensions, error) {
			return TerminalDimensions{Width: 80, Height: 24}, nil
		},
		ColorProfile: func() TerminalColorProfile { return TerminalColorNone },
	}
	done := make(chan int, 1)
	go func() { done <- MainWithTerminal(context.Background(), fixture.args, dependencies) }()
	waitForFile(t, fixture.workerStarted)
	stdout.armed.Store(true)
	waitForFile(t, suspensionStarted)
	if err := os.WriteFile(filepath.Join(fixture.root, "release-worker"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case exit := <-done:
		if exit != 1 {
			t.Fatalf("exit = %d, want presentation failure 1", exit)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("default dashboard did not return after presentation-failure suspension")
	}

	current, err := (state.FileStore{Path: fixture.statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusSuspended || current.Runs[0].Continuation == nil || len(current.Leases) != 1 {
		t.Fatalf("final state after default-dashboard presentation failure = %#v", current)
	}
	raw := stdout.String()
	summaryAt := strings.LastIndex(raw, "Final aggregate summary")
	if summaryAt < 0 {
		t.Fatalf("default dashboard omitted final static summary: %q", raw)
	}
	summary := raw[summaryAt:]
	for _, want := range []string{"Active (1)", "#65  Terminal host  suspended", "Attention Required (0)"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("final static summary omitted %q after suspension:\n%s", want, summary)
		}
	}
	if strings.Contains(summary, "Active (0)") {
		t.Fatalf("final static summary used stale pre-suspension state:\n%s", summary)
	}
	if !stdout.failed.Load() || !strings.Contains(stderr.String(), "error: presentation failed: write terminal presentation: terminal output lost") {
		t.Fatalf("presentation failure was not exercised: failed=%t stderr=%q", stdout.failed.Load(), stderr.String())
	}
}

func TestMainWithTerminalPresentationFailureSuspendsOwnedWorkerBeforeReturning(t *testing.T) {
	fixture := newPresentationWorkerFixture(t, presentationSuspendingWorkerScript)
	suspensionStarted := filepath.Join(fixture.root, "suspension-started")
	var stdout synchronizedBuffer
	var stderr bytes.Buffer
	dependencies := TerminalDependencies{
		Input: strings.NewReader(""), Output: &stdout, ErrorOutput: &stderr,
		IsTerminal: func() bool { return true },
		Presentation: func(ctx context.Context, _ PresentationControl) error {
			if err := waitForPresentationPath(ctx, fixture.workerStarted); err != nil {
				return err
			}
			return errors.New("render output closed")
		},
	}
	done := make(chan int, 1)
	go func() { done <- MainWithTerminal(context.Background(), fixture.args, dependencies) }()
	waitForFile(t, suspensionStarted)
	select {
	case exit := <-done:
		t.Fatalf("host returned before active Worker suspension completed: %d", exit)
	default:
	}
	if err := os.WriteFile(filepath.Join(fixture.root, "release-worker"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case exit := <-done:
		if exit != 1 {
			t.Fatalf("exit = %d, want operational failure 1", exit)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("host did not return after active Worker suspension completed")
	}
	current, err := (state.FileStore{Path: fixture.statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusSuspended || current.Runs[0].Continuation == nil || current.Runs[0].PID != 0 || len(current.Leases) != 1 {
		t.Fatalf("final state after presentation failure = %#v", current)
	}
	if strings.Contains(stdout.String(), "Drain:") || !strings.Contains(stdout.String(), "Suspension: SIGTERM accepted; 1 Worker share one 1m0s deadline") || !strings.Contains(stdout.String(), "Suspension complete: 0 Workers remaining") {
		t.Fatalf("presentation failure did not use direct completed suspension: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "error: presentation failed: render output closed") {
		t.Fatalf("presentation failure stderr = %q", stderr.String())
	}
}

type presentationWorkerFixture struct {
	root          string
	args          []string
	statePath     string
	workerStarted string
}

func newPresentationWorkerFixture(t *testing.T, workerScript func(root, started string) string) presentationWorkerFixture {
	t.Helper()
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := filepath.Join(root, "state")
	workerStarted := filepath.Join(root, "worker-started")
	completedMarker := filepath.Join(root, "worker-completed")
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if test -f `+quote(completedMarker)+`; then printf '%s\n' '[]'; else printf '%s\n' '[{"number":65,"title":"Terminal host","createdAt":"2026-01-01T00:00:00Z","url":"https://example.test/issues/65"}]'; fi ;;
  "issue view 65 --repo acme/widgets --json number,title,body,state,url,createdAt") printf '%s\n' '{"number":65,"title":"Terminal host","body":"","state":"OPEN","url":"https://example.test/issues/65","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/65/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/65/dependencies/blocked_by?per_page=100 --paginate --slurp") printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-65-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    if test -f `+quote(completedMarker)+`; then head=$8; printf '[{"number":165,"url":"https://github.com/acme/widgets/pull/165","state":"MERGED","mergedAt":"2026-01-02T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"%s","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$head"; else printf '%s\n' '[]'; fi ;;
  "issue view 65 --repo acme/widgets --json number,state,title,url")
    if test -f `+quote(completedMarker)+`; then printf '%s\n' '{"number":65,"state":"CLOSED","title":"Terminal host","url":"https://github.com/acme/widgets/issues/65"}'; else printf '%s\n' '{"number":65,"state":"OPEN","title":"Terminal host","url":"https://github.com/acme/widgets/issues/65"}'; fi ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
if [ "$3" = "rev-parse" ] && [ "$4" = "--show-toplevel" ]; then printf '%s\n' `+quote(repository)+`; exit 0; fi
if [ "$3" = "rev-parse" ] && [ "$4" = "--git-common-dir" ]; then printf '%s\n' `+quote(filepath.Join(repository, ".git"))+`; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "add" ]; then mkdir -p "$7"; exit 0; fi
if [ "$3" = "worktree" ] && [ "$4" = "remove" ]; then rm -rf "$6"; exit 0; fi
exit 0
`)
	pi := writeExecutable(t, workerScript(root, workerStarted))
	return presentationWorkerFixture{
		root: root, statePath: filepath.Join(stateDir, "state.json"), workerStarted: workerStarted,
		args: []string{"run", "--repo-dir", repository, "--state-dir", stateDir, "--max-workers", "1", "--poll", "5ms", "--gh", gh, "--git", git, "--pi", pi},
	}
}

func presentationSuspendingWorkerScript(root, started string) string {
	return `#!/bin/sh
set -eu
session_dir= session_id=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir) session_dir=$2; shift 2 ;;
    --session-id) session_id=$2; shift 2 ;;
    *) shift ;;
  esac
done
worktree=$(pwd)
IFS= read -r prompt
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
touch ` + quote(started) + `
IFS= read -r abort
touch ` + quote(filepath.Join(root, "suspension-started")) + `
while ! test -f ` + quote(filepath.Join(root, "release-worker")) + `; do sleep 0.01; done
session_file="$session_dir/session.jsonl"
printf '{"type":"session","version":3,"id":"%s","cwd":"%s"}\n' "$session_id" "$worktree" > "$session_file"
printf '%s\n' '{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}' >> "$session_file"
printf '%s\n' '{"id":"backlog-suspend-abort","type":"response","command":"abort","success":true}' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
IFS= read -r state
printf '{"id":"backlog-suspend-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":"%s","sessionId":"%s"}}\n' "$session_file" "$session_id"
IFS= read -r entries
printf '%s\n' '{"id":"backlog-suspend-entries","type":"response","command":"get_entries","success":true,"data":{"entries":[{"type":"message","id":"leaf","parentId":null,"message":{"role":"user","content":"work"}}],"leafId":"leaf"}}'
IFS= read -r final_state
printf '{"id":"backlog-suspend-final-state","type":"response","command":"get_state","success":true,"data":{"isStreaming":false,"isCompacting":false,"pendingMessageCount":0,"sessionFile":"%s","sessionId":"%s"}}\n' "$session_file" "$session_id"
while IFS= read -r ignored; do :; done
`
}

func TestMainWithTerminalParsesRunHelpBeforeStartingPresentation(t *testing.T) {
	for _, test := range []struct {
		name      string
		terminal  bool
		arguments []string
	}{
		{name: "interactive help", terminal: true, arguments: []string{"run", "--help"}},
		{name: "plain override", terminal: true, arguments: []string{"run", "--plain", "--help"}},
		{name: "plain boolean override", terminal: true, arguments: []string{"run", "--plain=1", "--help"}},
		{name: "redirected output", arguments: []string{"run", "--help"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			var stdout, stderr bytes.Buffer
			exit := MainWithTerminal(context.Background(), test.arguments, TerminalDependencies{
				Output: &stdout, ErrorOutput: &stderr,
				IsTerminal: func() bool { return test.terminal },
				Presentation: func(context.Context, PresentationControl) error {
					called = true
					return nil
				},
			})
			if exit != 0 || called || !strings.Contains(stderr.String(), "max-workers") {
				t.Fatalf("exit = %d, presentation called = %t, help = %q", exit, called, stderr.String())
			}
		})
	}
}

func TestMainWithTerminalPresentationFailureUsesOperationalExitAfterSetupSuspension(t *testing.T) {
	root := t.TempDir()
	started := filepath.Join(root, "git-started")
	git := writeExecutable(t, `#!/bin/sh
set -eu
touch `+quote(started)+`
exec sleep 30
`)
	var stdout, stderr bytes.Buffer
	exit := MainWithTerminal(context.Background(), []string{"run", "--git", git}, TerminalDependencies{
		Input: strings.NewReader(""), Output: &stdout, ErrorOutput: &stderr,
		IsTerminal: func() bool { return true },
		Presentation: func(ctx context.Context, _ PresentationControl) error {
			if err := waitForPresentationPath(ctx, started); err != nil {
				return err
			}
			return errors.New("render output closed")
		},
	})
	if exit != 1 {
		t.Fatalf("exit = %d, want operational failure 1", exit)
	}
	if !strings.Contains(stdout.String(), "Suspension: SIGTERM accepted during setup; 0 Workers remaining") {
		t.Fatalf("presentation failure did not complete setup suspension: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "error: presentation failed: render output closed") {
		t.Fatalf("presentation failure stderr = %q", stderr.String())
	}
}

func waitForPresentationPath(ctx context.Context, path string) error {
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("timed out waiting for presentation setup path")
		case <-ticker.C:
		}
	}
}
