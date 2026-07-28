package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
)

type operationalEventRecorder struct {
	mu     sync.Mutex
	events []OperationalEvent
}

type blockingOperationalOutput struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingOperationalOutput) Write(content []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(content), nil
}

func (r *operationalEventRecorder) record(event OperationalEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *operationalEventRecorder) snapshot() []OperationalEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]OperationalEvent(nil), r.events...)
}

func (r *operationalEventRecorder) waitFor(t *testing.T, match func([]OperationalEvent) bool) []OperationalEvent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		events := r.snapshot()
		if match(events) {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("operational events = %#v, want matching event sequence", events)
		}
		time.Sleep(time.Millisecond)
	}
}

func findShutdownEvent(events []OperationalEvent, stage ShutdownStage, action string) (ShutdownEvent, bool) {
	for _, event := range events {
		shutdown, ok := event.(ShutdownEvent)
		if ok && shutdown.Stage == stage && shutdown.Action == action {
			return shutdown, true
		}
	}
	return ShutdownEvent{}, false
}

func findRunLifecycleEvent(events []OperationalEvent, stage RunLifecycleStage) (RunLifecycleEvent, bool) {
	for _, event := range events {
		lifecycle, ok := event.(RunLifecycleEvent)
		if ok && lifecycle.Stage == stage {
			return lifecycle, true
		}
	}
	return RunLifecycleEvent{}, false
}

func TestRunnerReportsClaimStartAndMergeLifecycleEvents(t *testing.T) {
	t.Parallel()

	const issue = 42
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	runner := testRunner(github, workers, &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	var output bytes.Buffer
	runner.Output = &output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, issue)
	github.setCompletion(issue, mergedOutcome(issue))
	workers.complete(issue, worker.Result{ExitCode: 0})
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	runner.WaitForOperationalEventDelivery()

	events := recorder.snapshot()
	for _, test := range []struct {
		stage RunLifecycleStage
		want  string
	}{
		{stage: RunLifecycleClaimed, want: "claimed issue #42"},
		{stage: RunLifecycleStarted, want: "started issue #42"},
		{stage: RunLifecycleMerged, want: "verified merged completion for issue #42"},
	} {
		event, ok := findRunLifecycleEvent(events, test.stage)
		if !ok || !strings.Contains(event.Message, test.want) {
			t.Fatalf("%s lifecycle event = %#v, %t, want message containing %q", test.stage, event, ok, test.want)
		}
		if !strings.Contains(output.String(), event.Message+"\n") {
			t.Fatalf("plain output omitted lifecycle event %q: %q", event.Message, output.String())
		}
	}
}

func TestRunnerReportsCandidateDiscoveryFailuresAndResetsCountAfterRecovery(t *testing.T) {
	t.Parallel()

	listErr := errors.New("i/o timeout")
	inspectErr := errors.New("TLS handshake timeout")
	unknownErr := errors.New("unexpected adapter operation")
	laterErr := errors.New("connection reset")
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{err: &ghadapter.CandidateDiscoveryError{Operation: ghadapter.CandidateDiscoveryList, Err: listErr}},
			{err: &ghadapter.CandidateDiscoveryError{Operation: ghadapter.CandidateDiscoveryInspect, Issue: 17, Err: inspectErr}},
			{err: &ghadapter.CandidateDiscoveryError{Operation: ghadapter.CandidateDiscoveryOperation("unexpected"), Issue: 99, Err: unknownErr}},
			{},
			{err: &ghadapter.CandidateDiscoveryError{Operation: ghadapter.CandidateDiscoveryList, Err: laterErr}},
			{},
		},
		candidateChanged: make(chan struct{}, 6),
	}
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = 20 * time.Millisecond
	runner.Config.Watch = true
	var output bytes.Buffer
	runner.Output = &output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	events := recorder.waitFor(t, func(events []OperationalEvent) bool { return len(events) >= 6 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(events) != 6 {
		t.Fatalf("operational events = %#v, want two failure episodes and recoveries", events)
	}
	listing, ok := events[0].(CandidateDiscoveryFailed)
	if !ok {
		t.Fatalf("first event = %T, want CandidateDiscoveryFailed", events[0])
	}
	occurredAt := time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC)
	if listing.Operation != CandidateDiscoveryList || listing.Issue != nil || !errors.Is(listing.Err, listErr) || listing.Cause != "i/o timeout" || !listing.FirstFailureAt.Equal(occurredAt) || !listing.OccurredAt.Equal(occurredAt) || !listing.RetryAt.Equal(occurredAt.Add(20*time.Millisecond)) || listing.ConsecutiveFailures != 1 {
		t.Fatalf("Candidate listing failure = %#v", listing)
	}
	inspection, ok := events[1].(CandidateDiscoveryFailed)
	if !ok || inspection.Operation != CandidateDiscoveryInspect || inspection.Issue == nil || *inspection.Issue != 17 || !errors.Is(inspection.Err, inspectErr) || !inspection.FirstFailureAt.Equal(occurredAt) || inspection.ConsecutiveFailures != 2 {
		t.Fatalf("Candidate inspection failure = %#v", events[1])
	}
	fallback, ok := events[2].(CandidateDiscoveryFailed)
	if !ok || fallback.Operation != CandidateDiscoverySnapshot || fallback.Issue != nil || !errors.Is(fallback.Err, unknownErr) {
		t.Fatalf("unknown Candidate discovery operation = %#v", events[2])
	}
	recovery, ok := events[3].(CandidateDiscoveryRecovered)
	if !ok || recovery.Failures != 3 || !recovery.OccurredAt.Equal(occurredAt) {
		t.Fatalf("recovery = %#v", events[3])
	}
	laterFailure, ok := events[4].(CandidateDiscoveryFailed)
	if !ok || laterFailure.Operation != CandidateDiscoveryList || !errors.Is(laterFailure.Err, laterErr) || laterFailure.ConsecutiveFailures != 1 {
		t.Fatalf("later Candidate discovery failure = %#v, want a reset consecutive count", events[4])
	}
	laterRecovery, ok := events[5].(CandidateDiscoveryRecovered)
	if !ok || laterRecovery.Failures != 1 || !laterRecovery.OccurredAt.Equal(occurredAt) {
		t.Fatalf("later recovery = %#v, want one failure", events[5])
	}
	for _, want := range []string{
		"candidate discovery failed; admission paused; retry due in 20ms: list candidates: i/o timeout\n",
		"candidate discovery failed; admission paused; retry due in 20ms: inspect candidate #17: TLS handshake timeout\n",
		"candidate discovery failed; admission paused; retry due in 20ms: unexpected #99: unexpected adapter operation\n",
		"candidate discovery recovered; admission resumed after 3 failures\n",
		"candidate discovery failed; admission paused; retry due in 20ms: list candidates: connection reset\n",
		"candidate discovery recovered; admission resumed after 1 failure\n",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("plain output omitted %q: %q", want, output.String())
		}
	}
}

func TestRunnerStructuredPresentationCanSuppressCompatibleAdmissionOutput(t *testing.T) {
	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: errors.New("temporary failure")}, {}},
		candidateChanged: make(chan struct{}, 2),
	}
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = time.Millisecond
	runner.SuppressOperationalEventOutput = true
	var output bytes.Buffer
	runner.Output = &output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	runner.WaitForOperationalEventDelivery()
	if events := recorder.snapshot(); len(events) != 2 {
		t.Fatalf("structured events = %#v, want failure and recovery", events)
	}
	if strings.Contains(output.String(), "candidate discovery") {
		t.Fatalf("structured presentation received compatible Admission rows: %q", output.String())
	}
}

func TestRunnerRetainsAtMostTwentyFullAdmissionDiagnostics(t *testing.T) {
	runner := &Runner{}
	events := make([]CandidateDiscoveryFailed, 0, candidateDiscoveryDiagnosticLimit+5)
	for failure := 1; failure <= candidateDiscoveryDiagnosticLimit+5; failure++ {
		event := runner.retainCandidateDiagnostic(CandidateDiscoveryFailed{
			ConsecutiveFailures: failure,
			Err:                 fmt.Errorf("full failure %d", failure),
		}).(CandidateDiscoveryFailed)
		events = append(events, event)
	}
	if count := runner.candidateDiagnostics.count(); count != candidateDiscoveryDiagnosticLimit {
		t.Fatalf("retained full diagnostics = %d, want %d", count, candidateDiscoveryDiagnosticLimit)
	}
	if got := events[0].Err.Error(); !strings.Contains(got, "no longer retained") {
		t.Fatalf("old diagnostic = %q, want bounded-retention marker", got)
	}
	if got := events[len(events)-1].Err.Error(); got != "full failure 25" {
		t.Fatalf("latest diagnostic = %q, want full failure 25", got)
	}
}

func TestRunnerBoundsLightweightAdmissionIdentitiesWhileRetainingRecentRecurrence(t *testing.T) {
	var counts map[string]int
	var order []string
	counts = retainOperationalFailureOccurrences(counts, &order, "recurring cause", 1)
	for identity := 1; identity <= 200; identity++ {
		counts = retainOperationalFailureOccurrences(counts, &order, fmt.Sprintf("distinct cause %d", identity), 1)
	}
	if occurrences := takeOperationalFailureOccurrences(counts, &order, "recurring cause"); occurrences != 1 {
		t.Fatalf("recurring cause after 200 identities = %d occurrences, want 1 retained occurrence", occurrences)
	}

	for identity := 201; identity <= operationalAggregationIdentityLimit+400; identity++ {
		counts = retainOperationalFailureOccurrences(counts, &order, fmt.Sprintf("distinct cause %d", identity), 1)
	}
	counts = retainOperationalFailureOccurrences(counts, &order, "current recurring cause", 1)
	counts = retainOperationalFailureOccurrences(counts, &order, "current recurring cause", 1)
	if identities := len(counts); identities > operationalAggregationIdentityLimit {
		t.Fatalf("lightweight Runner identities = %d, want at most %d", identities, operationalAggregationIdentityLimit)
	}
	if occurrences := counts["current recurring cause"]; occurrences != 2 {
		t.Fatalf("current recurring cause = %d occurrences, want 2", occurrences)
	}
}

func TestRunnerPreservesAdmissionOccurrencesAcrossSlowDeliveryAndClearsOnRecovery(t *testing.T) {
	runner := &Runner{}
	deliveryStarted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseDelivery) })
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = func(event OperationalEvent) {
		startOnce.Do(func() { close(deliveryStarted) })
		<-releaseDelivery
		recorder.record(event)
	}

	runner.enqueueOperationalEvent(ShutdownEvent{Stage: ShutdownStageDraining})
	<-deliveryStarted

	episodeStarted := time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC)
	enqueueFailure := func(cause string, consecutive int, firstFailureAt time.Time) {
		runner.enqueueOperationalEvent(CandidateDiscoveryFailed{
			Operation: CandidateDiscoveryList, Err: fmt.Errorf("full diagnostic for %s", cause), Cause: cause,
			FirstFailureAt: firstFailureAt, OccurredAt: episodeStarted.Add(time.Duration(consecutive) * time.Second),
			ConsecutiveFailures: consecutive, Occurrences: 1,
		})
	}

	enqueueFailure("recurring cause", 1, episodeStarted)
	for failure := 2; failure <= operationalAdmissionFailureLimit+2; failure++ {
		enqueueFailure(fmt.Sprintf("distinct cause %d", failure), failure, episodeStarted)
	}
	enqueueFailure("recurring cause", operationalAdmissionFailureLimit+3, episodeStarted)

	runner.operationalEventMu.Lock()
	if len(runner.operationalEvictedFailureCounts) == 0 {
		runner.operationalEventMu.Unlock()
		t.Fatal("slow delivery did not retain lightweight counts for evicted failure identities")
	}
	runner.operationalEventMu.Unlock()

	runner.enqueueOperationalEvent(CandidateDiscoveryRecovered{
		OccurredAt: episodeStarted.Add(time.Minute), Failures: operationalAdmissionFailureLimit + 3,
	})
	enqueueFailure("recurring cause", 1, episodeStarted.Add(2*time.Minute))

	runner.operationalEventMu.Lock()
	queuedFailures := operationalAdmissionFailureCount(runner.operationalEvents)
	retainedCounts := len(runner.operationalEvictedFailureCounts)
	runner.operationalEventMu.Unlock()
	if queuedFailures != operationalAdmissionFailureLimit {
		t.Fatalf("queued full failure records = %d, want %d", queuedFailures, operationalAdmissionFailureLimit)
	}
	if retainedCounts != 0 {
		t.Fatalf("recovery retained episode-wide failure counts: %d", retainedCounts)
	}
	if diagnostics := runner.candidateDiagnostics.count(); diagnostics != candidateDiscoveryDiagnosticLimit {
		t.Fatalf("retained full diagnostics = %d, want %d", diagnostics, candidateDiscoveryDiagnosticLimit)
	}

	runner.stopOperationalEventDelivery()
	releaseOnce.Do(func() { close(releaseDelivery) })
	runner.WaitForOperationalEventDelivery()

	recovered := false
	beforeRecoveryOccurrences := 0
	afterRecoveryOccurrences := 0
	for _, event := range recorder.snapshot() {
		switch event := event.(type) {
		case CandidateDiscoveryRecovered:
			recovered = true
		case CandidateDiscoveryFailed:
			if event.Cause != "recurring cause" {
				continue
			}
			if recovered {
				afterRecoveryOccurrences += candidateDiscoveryFailureOccurrences(event)
			} else {
				beforeRecoveryOccurrences += candidateDiscoveryFailureOccurrences(event)
			}
		}
	}
	if beforeRecoveryOccurrences != 2 {
		t.Fatalf("recurring failure occurrences before recovery = %d, want 2", beforeRecoveryOccurrences)
	}
	if afterRecoveryOccurrences != 1 {
		t.Fatalf("recurring failure occurrences after recovery = %d, want 1", afterRecoveryOccurrences)
	}
}

func TestRunnerDoesNotReportAdmissionRecoveryAfterDrainIsAccepted(t *testing.T) {
	calls := 0
	retryStarted := make(chan struct{})
	github := &fakeGitHub{candidatesFunc: func(ctx context.Context) ([]scheduler.Candidate, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("temporary failure")
		}
		close(retryStarted)
		<-ctx.Done()
		// Simulate an adapter that completed successfully despite concurrent
		// cancellation after the signal observer accepted Drain.
		return nil, nil
	}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = time.Millisecond
	runner.Config.Watch = true
	runner.Signals = signals
	var output bytes.Buffer
	runner.Output = &output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-retryStarted
	signals <- os.Interrupt
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	runner.WaitForOperationalEventDelivery()

	events := recorder.snapshot()
	if _, ok := findShutdownEvent(events, ShutdownStageDraining, "admission stopped"); !ok {
		t.Fatalf("operational events = %#v, want Drain acceptance", events)
	}
	for _, event := range events {
		if _, ok := event.(CandidateDiscoveryRecovered); ok {
			t.Fatalf("operational events = %#v, reported recovery after Drain acceptance", events)
		}
	}
	if strings.Contains(output.String(), "admission resumed") {
		t.Fatalf("plain output reported recovery after Drain acceptance: %q", output.String())
	}
}

func TestRunnerDoesNotRecoverOrExhaustAfterCanceledCandidateSuccess(t *testing.T) {
	calls := 0
	retryStarted := make(chan struct{})
	github := &fakeGitHub{candidatesFunc: func(ctx context.Context) ([]scheduler.Candidate, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("temporary failure")
		}
		close(retryStarted)
		<-ctx.Done()
		// Simulate an adapter that reports success after its parent context was
		// canceled instead of returning the cancellation error.
		return nil, nil
	}}
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = time.Millisecond
	var output bytes.Buffer
	runner.Output = &output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record
	summaries := 0
	runner.FinalSummary = func(state.State) error {
		summaries++
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-retryStarted
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	runner.WaitForOperationalEventDelivery()

	events := recorder.snapshot()
	if len(events) != 1 {
		t.Fatalf("operational events = %#v, want only the pre-cancellation failure", events)
	}
	if _, ok := events[0].(CandidateDiscoveryFailed); !ok {
		t.Fatalf("operational event = %T, want CandidateDiscoveryFailed", events[0])
	}
	if summaries != 0 || strings.Contains(output.String(), "admission resumed") {
		t.Fatalf("canceled Candidate success reported recovery or natural exhaustion: summaries=%d output=%q", summaries, output.String())
	}
}

func TestRunnerDoesNotReportCandidateDiscoveryFailureAfterDrainIsAccepted(t *testing.T) {
	candidateContext := make(chan context.Context, 1)
	nowStarted := make(chan struct{})
	releaseNow := make(chan struct{})
	var nowOnce sync.Once
	github := &fakeGitHub{candidatesFunc: func(ctx context.Context) ([]scheduler.Candidate, error) {
		candidateContext <- ctx
		return nil, errors.New("late discovery failure")
	}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.Watch = true
	runner.Signals = signals
	runner.Now = func() time.Time {
		nowOnce.Do(func() {
			close(nowStarted)
			<-releaseNow
		})
		return time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC)
	}
	var output bytes.Buffer
	runner.Output = &output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	ctx := <-candidateContext
	<-nowStarted
	signals <- os.Interrupt
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Drain was not accepted while failure reporting was paused")
	}
	close(releaseNow)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	runner.WaitForOperationalEventDelivery()

	events := recorder.snapshot()
	for _, event := range events {
		if _, ok := event.(CandidateDiscoveryFailed); ok {
			t.Fatalf("operational events = %#v, reported failure after Drain acceptance", events)
		}
	}
	if strings.Contains(output.String(), "candidate discovery failed") {
		t.Fatalf("plain output reported failure after Drain acceptance: %q", output.String())
	}
}

func TestAdmissionOutputBackpressureDoesNotDelayDrainAcceptance(t *testing.T) {
	candidateContext := make(chan context.Context, 1)
	github := &fakeGitHub{candidatesFunc: func(ctx context.Context) ([]scheduler.Candidate, error) {
		candidateContext <- ctx
		return nil, errors.New("temporary failure")
	}}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.Watch = true
	runner.Signals = signals
	output := &blockingOperationalOutput{started: make(chan struct{}), release: make(chan struct{})}
	runner.Output = output

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	ctx := <-candidateContext
	<-output.started
	signals <- os.Interrupt
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		close(output.release)
		t.Fatal("plain-output backpressure delayed Drain acceptance")
	}
	close(output.release)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerControlAndOutputDoNotWaitForOperationalEventDelivery(t *testing.T) {
	pollInterval := 50 * time.Millisecond
	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: errors.New("temporary failure")}, {}},
		candidateChanged: make(chan struct{}, 2),
	}
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = pollInterval
	var output bytes.Buffer
	runner.Output = &output
	eventStarted := make(chan struct{})
	releaseEvent := make(chan struct{})
	recoveryStarted := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseEvent) })
	runner.OnOperationalEvent = func(event OperationalEvent) {
		switch event.(type) {
		case CandidateDiscoveryFailed:
			close(eventStarted)
			<-releaseEvent
		case CandidateDiscoveryRecovered:
			close(recoveryStarted)
		}
	}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-eventStarted
	github.waitForCandidateCalls(t, 2)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Runner waited for blocked operational event delivery")
	}
	if !strings.Contains(output.String(), "candidate discovery failed; admission paused") {
		t.Fatalf("plain output waited for operational event delivery: %q", output.String())
	}
	select {
	case <-recoveryStarted:
		t.Fatal("operational callbacks ran concurrently while failure delivery was blocked")
	default:
	}
	deliveryDone := make(chan struct{})
	go func() {
		runner.WaitForOperationalEventDelivery()
		close(deliveryDone)
	}()
	select {
	case <-deliveryDone:
		t.Fatal("delivery boundary returned while queued callbacks were blocked")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(releaseEvent) })
	select {
	case <-recoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("ordered recovery event was not delivered after the callback resumed")
	}
	select {
	case <-deliveryDone:
	case <-time.After(time.Second):
		t.Fatal("delivery boundary did not finish after queued callbacks were delivered")
	}
}

func TestRunnerIsolatesOperationalEventCallbackPanics(t *testing.T) {
	github := &fakeGitHub{
		candidateResults: []candidateResult{{err: errors.New("temporary failure")}, {}},
		candidateChanged: make(chan struct{}, 2),
	}
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = time.Millisecond
	callbackStarted := make(chan struct{}, 2)
	runner.OnOperationalEvent = func(OperationalEvent) {
		callbackStarted <- struct{}{}
		panic("presentation failed")
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	for range 2 {
		select {
		case <-callbackStarted:
		case <-time.After(time.Second):
			t.Fatal("operational event delivery did not continue after a callback panic")
		}
	}
}

func TestRunnerReportsStructuredDrainStages(t *testing.T) {
	t.Parallel()

	github := &fakeGitHub{candidateChanged: make(chan struct{}, 1)}
	signals := make(chan os.Signal, 1)
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.Watch = true
	runner.Signals = signals
	events := make(chan ShutdownEvent, 2)
	runner.OnOperationalEvent = func(event OperationalEvent) {
		if shutdown, ok := event.(ShutdownEvent); ok {
			events <- shutdown
		}
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	github.waitForCandidateCalls(t, 1)
	signals <- os.Interrupt
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	draining := <-events
	if draining.Stage != ShutdownStageDraining || draining.Action != "admission stopped" || draining.RemainingWorkers != 0 || draining.NextInterrupt != NextInterruptSuspends {
		t.Fatalf("Drain event = %#v", draining)
	}
	complete := <-events
	if complete.Stage != ShutdownStageDrainComplete || complete.Result != ShutdownResultSuccess || complete.Action != "exiting successfully" || complete.RemainingWorkers != 0 || complete.NextInterrupt != NextInterruptNone {
		t.Fatalf("Drain completion event = %#v", complete)
	}
}

func TestRunnerReportsStructuredSuspensionStages(t *testing.T) {
	github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: 40, CreatedAt: time.Now()}}}
	workers := newFakeWorkers()
	signals := make(chan os.Signal, 2)
	runner := testRunner(github, workers, &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Signals = signals
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	workers.waitForStarts(t, 40)
	signals <- os.Interrupt
	signals <- os.Interrupt
	if err := <-done; !isSignalExit(err, 130) {
		t.Fatalf("run: %v, want signal exit 130", err)
	}
	events := recorder.waitFor(t, func(events []OperationalEvent) bool {
		_, ok := findShutdownEvent(events, ShutdownStageSuspensionComplete, "exiting after suspension")
		return ok
	})
	suspending, ok := findShutdownEvent(events, ShutdownStageSuspending, "establishing continuation boundaries")
	if !ok || suspending.RemainingWorkers != 1 || suspending.NextInterrupt != NextInterruptForceStops {
		t.Fatalf("Suspending event = %#v, found=%t", suspending, ok)
	}
	complete, _ := findShutdownEvent(events, ShutdownStageSuspensionComplete, "exiting after suspension")
	if complete.RemainingWorkers != 0 || complete.NextInterrupt != NextInterruptNone {
		t.Fatalf("Suspension completion event = %#v", complete)
	}
}

func TestRunnerReportsStructuredForceStopStages(t *testing.T) {
	tests := []struct {
		name       string
		exitCode   int
		timeout    time.Duration
		trigger    func(chan os.Signal, <-chan int)
		wantAction string
	}{
		{
			name: "third SIGINT", exitCode: 130, timeout: 5 * time.Second,
			trigger: func(signals chan os.Signal, closeStarted <-chan int) {
				signals <- os.Interrupt
				signals <- os.Interrupt
				<-closeStarted
				signals <- os.Interrupt
			},
			wantAction: "requesting force stop",
		},
		{
			name: "suspension timeout", exitCode: 143, timeout: 20 * time.Millisecond,
			trigger: func(signals chan os.Signal, _ <-chan int) {
				signals <- syscall.SIGTERM
			},
			wantAction: "requesting force stop after suspension deadline",
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issue := 50 + index
			github := &fakeGitHub{candidates: []scheduler.Candidate{{Number: issue, CreatedAt: time.Now()}}}
			workers := newFakeWorkers()
			workers.authorizeClose = true
			workers.waitForForce = true
			signals := make(chan os.Signal, 3)
			runner := testRunner(github, workers, &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
			runner.Config.SuspensionTimeout = test.timeout
			runner.Signals = signals
			recorder := &operationalEventRecorder{}
			runner.OnOperationalEvent = recorder.record

			done := make(chan error, 1)
			go func() { done <- runner.Run(context.Background()) }()
			workers.waitForStarts(t, issue)
			test.trigger(signals, workers.closeContextStarted)
			if err := <-done; !isSignalExit(err, test.exitCode) {
				t.Fatalf("run: %v, want signal exit %d", err, test.exitCode)
			}
			events := recorder.waitFor(t, func(events []OperationalEvent) bool {
				_, force := findShutdownEvent(events, ShutdownStageForceStopping, test.wantAction)
				_, complete := findShutdownEvent(events, ShutdownStageSuspensionComplete, "exiting after suspension")
				return force && complete
			})
			force, _ := findShutdownEvent(events, ShutdownStageForceStopping, test.wantAction)
			if force.RemainingWorkers != 1 || force.NextInterrupt != NextInterruptRepeatsForceStop {
				t.Fatalf("Force-stop event = %#v", force)
			}
			complete, _ := findShutdownEvent(events, ShutdownStageSuspensionComplete, "exiting after suspension")
			if complete.RemainingWorkers != 0 || complete.NextInterrupt != NextInterruptNone {
				t.Fatalf("post-force suspension completion event = %#v", complete)
			}
		})
	}
}

func TestRunnerReportsStructuredIncompleteSuspension(t *testing.T) {
	runner := testRunner(&fakeGitHub{}, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	var output bytes.Buffer
	runner.Output = &output
	runner.suspensionFailed.Store(true)
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record
	current := state.State{Version: state.CurrentVersion}

	err := runner.suspendOwned(&current, map[int]WorkerProcess{}, 143)
	if !isSignalExit(err, 143) {
		t.Fatalf("run: %v, want signal exit 143", err)
	}
	events := recorder.waitFor(t, func(events []OperationalEvent) bool {
		_, ok := findShutdownEvent(events, ShutdownStageSuspensionIncomplete, "exiting with incomplete suspension")
		return ok
	})
	incomplete, _ := findShutdownEvent(events, ShutdownStageSuspensionIncomplete, "exiting with incomplete suspension")
	if incomplete.Result != ShutdownResultFailure || incomplete.RemainingWorkers != 0 || incomplete.NextInterrupt != NextInterruptNone {
		t.Fatalf("Suspension incomplete event = %#v", incomplete)
	}
}
