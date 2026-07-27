package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

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
	Terminal PresentationTerminal
	ingress  *orderedSignalIngress
}

// Interrupt submits a SIGINT-equivalent event to the Runner's ordered signal
// ingress.
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

func (h runnerHost) run(ctx context.Context, run func(<-chan lifecycleSignal) error, presentation Presentation) error {
	hostCtx, stopHost := context.WithCancel(context.Background())
	defer stopHost()
	ingress := newOrderedSignalIngress(hostCtx, h.terminal.Signals)

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- run(ingress.events) }()
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
	}, ingress: ingress}
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
		stopPresentation()
		presentationErr := <-presentationDone
		if presentationErr != nil && !errors.Is(presentationErr, context.Canceled) {
			return &PresentationFailure{Err: presentationErr, RunnerErr: runnerErr}
		}
		return runnerErr
	case presentationErr := <-presentationDone:
		presentationCtxErr := presentationCtx.Err()
		if presentationCtxErr != nil && (presentationErr == nil || errors.Is(presentationErr, presentationCtxErr)) {
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

func plainRunRequested(args []string) bool {
	for _, argument := range args {
		if argument == "--plain" || argument == "-plain" {
			return true
		}
		for _, prefix := range []string{"--plain=", "-plain="} {
			if !strings.HasPrefix(argument, prefix) {
				continue
			}
			plain, err := strconv.ParseBool(strings.TrimPrefix(argument, prefix))
			if err != nil || plain {
				return true
			}
		}
	}
	return false
}
