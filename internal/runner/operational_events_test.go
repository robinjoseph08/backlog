package runner

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/state"
)

type operationalEventRecorder struct {
	mu     sync.Mutex
	events []OperationalEvent
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

func TestRunnerReportsCandidateDiscoveryFailureAndAdmissionRecovery(t *testing.T) {
	t.Parallel()

	listErr := errors.New("i/o timeout")
	inspectErr := errors.New("TLS handshake timeout")
	unknownErr := errors.New("unexpected adapter operation")
	github := &fakeGitHub{
		candidateResults: []candidateResult{
			{err: &ghadapter.CandidateDiscoveryError{Operation: ghadapter.CandidateDiscoveryList, Err: listErr}},
			{err: &ghadapter.CandidateDiscoveryError{Operation: ghadapter.CandidateDiscoveryInspect, Issue: 17, Err: inspectErr}},
			{err: &ghadapter.CandidateDiscoveryError{Operation: ghadapter.CandidateDiscoveryOperation("unexpected"), Issue: 99, Err: unknownErr}},
			{},
		},
		candidateChanged: make(chan struct{}, 4),
	}
	runner := testRunner(github, newFakeWorkers(), &memoryStore{value: state.State{Version: state.CurrentVersion}}, 1)
	runner.Config.PollInterval = 20 * time.Millisecond
	var output bytes.Buffer
	runner.Output = &output
	recorder := &operationalEventRecorder{}
	runner.OnOperationalEvent = recorder.record

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	events := recorder.snapshot()
	if len(events) != 4 {
		t.Fatalf("operational events = %#v, want three discovery failures and recovery", events)
	}
	listing, ok := events[0].(CandidateDiscoveryFailed)
	if !ok {
		t.Fatalf("first event = %T, want CandidateDiscoveryFailed", events[0])
	}
	occurredAt := time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC)
	if listing.Operation != CandidateDiscoveryList || listing.Issue != nil || !errors.Is(listing.Err, listErr) || !listing.OccurredAt.Equal(occurredAt) || !listing.RetryAt.Equal(occurredAt.Add(20*time.Millisecond)) || listing.ConsecutiveFailures != 1 {
		t.Fatalf("Candidate listing failure = %#v", listing)
	}
	inspection, ok := events[1].(CandidateDiscoveryFailed)
	if !ok || inspection.Operation != CandidateDiscoveryInspect || inspection.Issue == nil || *inspection.Issue != 17 || !errors.Is(inspection.Err, inspectErr) || inspection.ConsecutiveFailures != 2 {
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
	for _, want := range []string{
		"candidate discovery failed; admission paused; retry due in 20ms: list candidates: i/o timeout\n",
		"candidate discovery failed; admission paused; retry due in 20ms: inspect candidate #17: TLS handshake timeout\n",
		"candidate discovery failed; admission paused; retry due in 20ms: unexpected #99: unexpected adapter operation\n",
		"candidate discovery recovered; admission resumed after 3 failures\n",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("plain output omitted %q: %q", want, output.String())
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
	if complete.Stage != ShutdownStageDrainComplete || complete.Action != "exiting successfully" || complete.RemainingWorkers != 0 || complete.NextInterrupt != NextInterruptNone {
		t.Fatalf("Drain completion event = %#v", complete)
	}
}
