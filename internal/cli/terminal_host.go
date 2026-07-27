package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/robinjoseph08/backlog/internal/runner"
	"golang.org/x/term"
)

// TerminalColorProfile describes the color capability available to a terminal
// presentation without coupling the CLI boundary to a rendering library.
type TerminalColorProfile uint8

const (
	TerminalColorNone TerminalColorProfile = iota
	TerminalColorANSI
	TerminalColorANSI256
	TerminalColorTrueColor
)

// TerminalDimensions is the current terminal viewport in cells.
type TerminalDimensions struct {
	Width  int
	Height int
}

// TerminalDependencies is the deterministic boundary between a command and
// its terminal. The host retains Signals so a presentation cannot consume
// external lifecycle events outside the ordered ingress.
type TerminalDependencies struct {
	Input       io.Reader
	Output      io.Writer
	ErrorOutput io.Writer

	IsTerminal   func() bool
	Dimensions   func() (TerminalDimensions, error)
	ColorProfile func() TerminalColorProfile
	Now          func() time.Time
	OpenURL      func(context.Context, string) error
	Signals      <-chan os.Signal

	// Presentation runs beside backlog run when output is interactive and
	// --plain is not selected. It must remain active until its context ends.
	Presentation Presentation
}

// Presentation is a full-screen command presentation hosted beside the
// Runner. Returning before its context ends while the Runner is active is a
// presentation failure.
type Presentation func(context.Context, PresentationControl) error

// PresentationTerminal contains the terminal services available to a hosted
// presentation. Signal ownership remains with the Runner host.
type PresentationTerminal struct {
	Input        io.Reader
	Output       io.Writer
	ErrorOutput  io.Writer
	IsTerminal   func() bool
	Dimensions   func() (TerminalDimensions, error)
	ColorProfile func() TerminalColorProfile
	Now          func() time.Time
	OpenURL      func(context.Context, string) error
}

// PresentationControl gives presentation input one path into Runner shutdown.
// Interrupt represents a raw-mode Ctrl-C key event.
type PresentationControl struct {
	Terminal          PresentationTerminal
	ingress           *orderedSignalIngress
	operationalEvents *presentationEventQueue
}

// NextOperationalEvent waits for the next typed Admission or shutdown event
// from the hosted Runner. Events are returned in Runner delivery order without
// requiring the presentation to parse compatible plain output.
func (c PresentationControl) NextOperationalEvent(ctx context.Context) (runner.OperationalEvent, error) {
	return c.operationalEvents.next(ctx)
}

// Interrupt submits a SIGINT-equivalent event to the Runner's ordered signal
// ingress and waits until Runner-side lifecycle handling accepts it. If ctx or
// the host lifecycle ends before acceptance, Interrupt returns the completed
// context's error.
func (c PresentationControl) Interrupt(ctx context.Context) error {
	return c.ingress.submit(ctx, os.Interrupt)
}

// PresentationFailure reports a presentation error together with the hosted
// Runner's completion result. When the presentation fails while the Runner is
// active, the host requests SIGTERM-equivalent bounded shutdown and waits for
// completion. If the Runner completes first, the host makes no shutdown
// request and stops the presentation.
type PresentationFailure struct {
	Err       error
	RunnerErr error
}

func (e *PresentationFailure) Error() string {
	if e.RunnerErr != nil {
		return fmt.Sprintf("presentation failed: %v; Runner completion: %v", e.Err, e.RunnerErr)
	}
	return fmt.Sprintf("presentation failed: %v", e.Err)
}

func (e *PresentationFailure) Unwrap() []error {
	if e.RunnerErr == nil {
		return []error{e.Err}
	}
	return []error{e.Err, e.RunnerErr}
}

const presentationEventLimit = 32

type presentationEventQueue struct {
	mu       sync.Mutex
	events   []runner.OperationalEvent
	inFlight int
	wake     chan struct{}
}

func newPresentationEventQueue() *presentationEventQueue {
	return &presentationEventQueue{wake: make(chan struct{}, 1)}
}

func (q *presentationEventQueue) publish(event runner.OperationalEvent) {
	q.mu.Lock()
	q.events = append(q.events, event)
	for len(q.events) > presentationEventLimit {
		q.remove(presentationEventEvictionIndex(q.events))
	}
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// presentationEventEvictionIndex prefers obsolete progress, then the oldest
// nonterminal state. This retains a recent ordered lifecycle window and never
// trades a terminal shutdown result for optional progress.
func presentationEventEvictionIndex(events []runner.OperationalEvent) int {
	for index, event := range events {
		if presentationEventIsTerminal(event) {
			continue
		}
		if presentationEventIsSuperseded(events, index) {
			return index
		}
	}
	for index, event := range events {
		if !presentationEventIsTerminal(event) {
			return index
		}
	}
	return 0
}

func presentationEventIsSuperseded(events []runner.OperationalEvent, index int) bool {
	switch event := events[index].(type) {
	case runner.CandidateDiscoveryFailed:
		for _, later := range events[index+1:] {
			switch later.(type) {
			case runner.CandidateDiscoveryFailed:
				return true
			case runner.CandidateDiscoveryRecovered:
				return false
			}
		}
	case runner.ShutdownEvent:
		if presentationEventIsTerminal(event) {
			return false
		}
		for _, later := range events[index+1:] {
			shutdown, ok := later.(runner.ShutdownEvent)
			if ok && shutdown.Stage == event.Stage {
				return true
			}
		}
	}
	return false
}

func presentationEventIsTerminal(event runner.OperationalEvent) bool {
	shutdown, ok := event.(runner.ShutdownEvent)
	if !ok {
		return false
	}
	switch shutdown.Stage {
	case runner.ShutdownStageDrainComplete, runner.ShutdownStageDrainIncomplete,
		runner.ShutdownStageSuspensionComplete, runner.ShutdownStageSuspensionIncomplete:
		return true
	default:
		return false
	}
}

func (q *presentationEventQueue) remove(index int) {
	copy(q.events[index:], q.events[index+1:])
	q.events[len(q.events)-1] = nil
	q.events = q.events[:len(q.events)-1]
}

func (q *presentationEventQueue) pop() runner.OperationalEvent {
	event := q.events[0]
	q.events[0] = nil
	q.events = q.events[1:]
	if len(q.events) == 0 {
		q.events = nil
	}
	q.inFlight++
	return event
}

func (q *presentationEventQueue) complete() {
	q.mu.Lock()
	if q.inFlight > 0 {
		q.inFlight--
	}
	q.mu.Unlock()
}

func (q *presentationEventQueue) idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.events) == 0 && q.inFlight == 0
}

func (q *presentationEventQueue) next(ctx context.Context) (runner.OperationalEvent, error) {
	for {
		q.mu.Lock()
		if len(q.events) > 0 {
			event := q.pop()
			q.mu.Unlock()
			return event, nil
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-ctx.Done():
			// Prefer queued events over cancellation so a slow presentation can
			// still observe the Runner's ordered terminal lifecycle stage before
			// host teardown completes.
			q.mu.Lock()
			if len(q.events) > 0 {
				event := q.pop()
				q.mu.Unlock()
				return event, nil
			}
			q.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

type lifecycleSignal struct {
	signal   os.Signal
	accepted chan struct{}
}

func (s lifecycleSignal) accept() {
	if s.accepted != nil {
		close(s.accepted)
	}
}

// orderedSignalIngress is the only path from external signals and
// presentation-generated lifecycle requests into a hosted Runner.
type orderedSignalIngress struct {
	ctx         context.Context
	external    <-chan os.Signal
	submissions chan lifecycleSignal
	events      chan lifecycleSignal
}

func newOrderedSignalIngress(ctx context.Context, external <-chan os.Signal) *orderedSignalIngress {
	ingress := &orderedSignalIngress{
		ctx:         ctx,
		external:    external,
		submissions: make(chan lifecycleSignal),
		events:      make(chan lifecycleSignal, 16),
	}
	go ingress.forward()
	return ingress
}

func (i *orderedSignalIngress) forward() {
	for {
		select {
		case signal, ok := <-i.external:
			if !ok {
				i.external = nil
				continue
			}
			if !i.publish(lifecycleSignal{signal: signal}) {
				return
			}
		case submission := <-i.submissions:
			if !i.publish(submission) {
				return
			}
		case <-i.ctx.Done():
			return
		}
	}
}

func (i *orderedSignalIngress) publish(signal lifecycleSignal) bool {
	select {
	case i.events <- signal:
		return true
	case <-i.ctx.Done():
		return false
	}
}

func (i *orderedSignalIngress) submit(ctx context.Context, signal os.Signal) error {
	accepted := make(chan struct{})
	request := lifecycleSignal{signal: signal, accepted: accepted}
	select {
	case i.submissions <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-i.ctx.Done():
		return i.ctx.Err()
	}
	select {
	case <-accepted:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-i.ctx.Done():
		return i.ctx.Err()
	}
}

type runnerHost struct {
	terminal TerminalDependencies
}

func (h runnerHost) run(ctx context.Context, run func(<-chan lifecycleSignal, func(runner.OperationalEvent)) error, presentation Presentation) error {
	hostCtx, stopHost := context.WithCancel(context.Background())
	defer stopHost()
	ingress := newOrderedSignalIngress(hostCtx, h.terminal.Signals)
	var operationalEvents *presentationEventQueue
	var publishOperationalEvent func(runner.OperationalEvent)
	if presentation != nil {
		operationalEvents = newPresentationEventQueue()
		publishOperationalEvent = operationalEvents.publish
	}

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- run(ingress.events, publishOperationalEvent) }()
	if presentation == nil {
		return <-runnerDone
	}

	presentationCtx, stopPresentation := context.WithCancel(ctx)
	defer stopPresentation()
	presentationDone := make(chan error, 1)
	control := PresentationControl{Terminal: PresentationTerminal{
		Input: h.terminal.Input, Output: h.terminal.Output, ErrorOutput: h.terminal.ErrorOutput,
		IsTerminal: h.terminal.IsTerminal, Dimensions: h.terminal.Dimensions,
		ColorProfile: h.terminal.ColorProfile, Now: h.terminal.Now, OpenURL: h.terminal.OpenURL,
	}, ingress: ingress, operationalEvents: operationalEvents}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				presentationDone <- fmt.Errorf("panic: %v", recovered)
			}
		}()
		presentationDone <- presentation(presentationCtx, control)
	}()

	select {
	case runnerErr := <-runnerDone:
		return finishPresentationAfterRunner(presentationCtx, runnerErr, stopPresentation, presentationDone)
	case presentationErr := <-presentationDone:
		if presentationReturnMatchesContext(presentationCtx, presentationErr) {
			return <-runnerDone
		}
		if presentationErr == nil {
			presentationErr = errors.New("presentation stopped while Runner was active")
		}
		// The submission is acknowledged only after the Runner side accepts
		// SIGTERM from the same ordered stream used by external signals and
		// Ctrl-C input. The Runner may finish concurrently, so do not make its
		// completion depend on an acknowledgement it can no longer provide.
		submissionDone := make(chan error, 1)
		go func() { submissionDone <- ingress.submit(hostCtx, syscall.SIGTERM) }()
		var runnerErr error
		select {
		case <-submissionDone:
			runnerErr = <-runnerDone
		case runnerErr = <-runnerDone:
		}
		stopPresentation()
		return &PresentationFailure{Err: presentationErr, RunnerErr: runnerErr}
	}
}

func finishPresentationAfterRunner(
	presentationCtx context.Context,
	runnerErr error,
	stopPresentation context.CancelFunc,
	presentationDone <-chan error,
) error {
	stopPresentation()
	presentationErr := <-presentationDone
	if presentationErr != nil && !presentationReturnMatchesContext(presentationCtx, presentationErr) {
		return &PresentationFailure{Err: presentationErr, RunnerErr: runnerErr}
	}
	return runnerErr
}

func presentationReturnMatchesContext(ctx context.Context, err error) bool {
	ctxErr := ctx.Err()
	return ctxErr != nil && (err == nil || errors.Is(err, ctxErr))
}

func normalizeTerminalDependencies(dependencies TerminalDependencies) TerminalDependencies {
	if dependencies.Input == nil {
		dependencies.Input = os.Stdin
	}
	if dependencies.Output == nil {
		dependencies.Output = os.Stdout
	}
	if dependencies.ErrorOutput == nil {
		dependencies.ErrorOutput = os.Stderr
	}
	if dependencies.IsTerminal == nil {
		output := dependencies.Output
		dependencies.IsTerminal = func() bool { return outputIsTerminal(output) }
	}
	if dependencies.Dimensions == nil {
		output := dependencies.Output
		dependencies.Dimensions = func() (TerminalDimensions, error) {
			file, ok := output.(interface{ Fd() uintptr })
			if !ok {
				return TerminalDimensions{}, errors.New("terminal output has no file descriptor")
			}
			width, height, err := term.GetSize(int(file.Fd()))
			if err != nil {
				return TerminalDimensions{}, err
			}
			return TerminalDimensions{Width: width, Height: height}, nil
		}
	}
	if dependencies.ColorProfile == nil {
		dependencies.ColorProfile = environmentColorProfile
	}
	if dependencies.Now == nil {
		dependencies.Now = time.Now
	}
	if dependencies.OpenURL == nil {
		dependencies.OpenURL = openURL
	}
	return dependencies
}

func environmentColorProfile() TerminalColorProfile {
	if os.Getenv("NO_COLOR") != "" || strings.EqualFold(os.Getenv("TERM"), "dumb") {
		return TerminalColorNone
	}
	if colorTerm := strings.ToLower(os.Getenv("COLORTERM")); strings.Contains(colorTerm, "truecolor") || strings.Contains(colorTerm, "24bit") {
		return TerminalColorTrueColor
	}
	if strings.Contains(strings.ToLower(os.Getenv("TERM")), "256color") {
		return TerminalColorANSI256
	}
	return TerminalColorANSI
}

func openURL(ctx context.Context, url string) error {
	executable := "xdg-open"
	if runtime.GOOS == "darwin" {
		executable = "open"
	}
	command := exec.CommandContext(ctx, executable, url)
	if err := command.Start(); err != nil {
		return err
	}
	return command.Process.Release()
}
