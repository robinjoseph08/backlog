package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/runner"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

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
	if len(queue.events) != presentationEventLimit {
		t.Fatalf("ignored-consumer queue length = %d, want hard limit %d", len(queue.events), presentationEventLimit)
	}
	latest, ok := queue.events[len(queue.events)-1].(runner.CandidateDiscoveryFailed)
	if !ok || latest.ConsecutiveFailures != presentationEventLimit*100 {
		t.Fatalf("latest retained Admission event = %#v", queue.events[len(queue.events)-1])
	}
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
	if len(events) != presentationEventLimit {
		t.Fatalf("slow-consumer delivery count = %d, want bounded %d", len(events), presentationEventLimit)
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

func TestMainWithTerminalDoesNotStartPresentationForPlainOrRedirectedRun(t *testing.T) {
	for _, test := range []struct {
		name      string
		terminal  bool
		arguments []string
	}{
		{name: "plain override", terminal: true, arguments: []string{"run", "--plain", "--help"}},
		{name: "plain boolean override", terminal: true, arguments: []string{"run", "--plain=1", "--help"}},
		{name: "redirected output", arguments: []string{"run", "--help"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			exit := MainWithTerminal(context.Background(), test.arguments, TerminalDependencies{
				Output: io.Discard, ErrorOutput: io.Discard,
				IsTerminal: func() bool { return test.terminal },
				Presentation: func(context.Context, PresentationControl) error {
					called = true
					return nil
				},
			})
			if exit != 0 || called {
				t.Fatalf("exit = %d, presentation called = %t", exit, called)
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
