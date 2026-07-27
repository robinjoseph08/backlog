package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/runner"
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
	err := host.run(context.Background(), func(signals <-chan lifecycleSignal) error {
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
				done <- host.run(context.Background(), func(signals <-chan lifecycleSignal) error {
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
	deadline := time.NewTimer(2 * time.Second)
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
