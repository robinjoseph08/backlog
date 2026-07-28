package cli

import (
	"context"
	"image/color"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

func TestDashboardStylingPairsSemanticColorsWithLabels(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	plain := "Active Runs (4)\n" +
		"  #1  Active work\n" +
		"    State: running | Elapsed: 1m0s | Worker liveness: alive (PID 1 and process-start identity verified)\n" +
		"  #6  Degraded work\n" +
		"    State: running | Elapsed: 1m0s | Worker liveness: unknown (PID liveness could not be verified)\n" +
		"  #2  Waiting work\n" +
		"    State: waiting-for-merge | Elapsed: 2m0s | Worker liveness: absent\n" +
		"  #3  Suspended work\n" +
		"    State: suspended | Elapsed: 3m0s | Worker liveness: absent\n" +
		"Attention Required (1)\n" +
		"  #4  Fatal work\n" +
		"    State: needs-human | Elapsed: 4m0s | Worker liveness: dead (recorded PID 4 is absent)\n" +
		"    Diagnostic: Worker exited without a verified outcome\n" +
		"Recent Completions (1)\n" +
		"  #5  Completed work | PR: https://example.test/5 | Elapsed: 5m0s | Completed: 1m0s ago\n" +
		"Operational messages\n" +
		"  candidate discovery failed; admission paused\n" +
		"  Force stop: additional signal accepted"

	got := styler.body(plain)
	if stripped := ansi.Strip(got); stripped != plain {
		t.Fatalf("styling changed semantic labels:\n%s", stripped)
	}
	for _, want := range []string{
		styler.active.Render("  #1  Active work"),
		styler.active.Render("    State: running"),
		styler.warning.Render("  #6  Degraded work"),
		styler.warning.Render("Worker liveness: unknown (PID liveness could not be verified)"),
		styler.warning.Render("  #2  Waiting work"),
		styler.warning.Render("    State: waiting-for-merge"),
		styler.warning.Render("  #3  Suspended work"),
		styler.attention.Render("Attention Required (1)"),
		styler.attention.Render("    Diagnostic: Worker exited without a verified outcome"),
		styler.completion.Render("  #5  Completed work"),
		styler.metadata.Render("Elapsed: 1m0s"),
		styler.warning.Render("  candidate discovery failed; admission paused"),
		styler.attention.Render("  Force stop: additional signal accepted"),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("styled dashboard omitted semantic span %q:\n%q", ansi.Strip(want), got)
		}
	}
}

func TestDashboardPaletteAdaptsToBackgroundAndReducedProfiles(t *testing.T) {
	for _, profile := range []TerminalColorProfile{TerminalColorANSI, TerminalColorANSI256, TerminalColorTrueColor} {
		dark := newDashboardStyler(profile, true)
		light := newDashboardStyler(profile, false)
		if sameColor(dark.active.GetForeground(), light.active.GetForeground()) ||
			sameColor(dark.completion.GetForeground(), light.completion.GetForeground()) ||
			sameColor(dark.warning.GetForeground(), light.warning.GetForeground()) ||
			sameColor(dark.attention.GetForeground(), light.attention.GetForeground()) {
			t.Fatalf("profile %d did not adapt every semantic color to terminal background", profile)
		}
		for _, semantic := range []color.Color{dark.active.GetForeground(), dark.completion.GetForeground(), dark.warning.GetForeground(), dark.attention.GetForeground()} {
			if sameColor(dark.metadata.GetForeground(), semantic) {
				t.Fatalf("profile %d metadata foreground was not visually muted from semantic state", profile)
			}
		}
		got := dark.body("Active Runs (0)\n  none")
		if ansi.Strip(got) != "Active Runs (0)\n  none" || !strings.Contains(got, "\x1b[") {
			t.Fatalf("profile %d did not preserve readable text while styling: %q", profile, got)
		}
		if profile == TerminalColorANSI && (strings.Contains(got, "38;2;") || strings.Contains(got, "38;5;")) {
			t.Fatalf("ANSI profile used a higher color profile: %q", got)
		}
		if profile == TerminalColorANSI256 && strings.Contains(got, "38;2;") {
			t.Fatalf("ANSI256 profile used true color: %q", got)
		}
	}
}

func TestDashboardChromeUsesSemanticStylesWithoutRemovingNavigation(t *testing.T) {
	styler := newDashboardStyler(TerminalColorTrueColor, true)
	for _, test := range []struct {
		line  string
		style lipgloss.Style
	}{
		{line: "Runner stage: Running | Next Ctrl-C: start Drain", style: styler.active},
		{line: "Runner stage: Draining | Next Ctrl-C: suspend", style: styler.warning},
		{line: "Runner stage: Drain complete | Next Ctrl-C: no effect", style: styler.completion},
		{line: "Runner stage: Force stopping | Next Ctrl-C: repeat", style: styler.attention},
		{line: "Repository: acme/widgets | Worker capacity: 1 used", style: styler.metadata},
	} {
		got := styler.chrome(test.line)
		if ansi.Strip(got) != test.line || got != test.style.Render(test.line) {
			t.Fatalf("styled chrome = %q, want labeled %q", got, test.line)
		}
	}
}

func TestDashboardStylingDisablesColorsForNoColorProfile(t *testing.T) {
	styler := newDashboardStyler(TerminalColorNone, true)
	plainBody := "Active Runs (1)\n  #1  Active work\n    State: running | Elapsed: 1s | Worker liveness: alive"
	plainChrome := "Runner stage: Running | Next Ctrl-C: start Drain and stop Admission"
	if got := styler.body(plainBody); got != plainBody || strings.Contains(got, "\x1b[") {
		t.Fatalf("colorless body changed: %q", got)
	}
	if got := styler.chrome(plainChrome); got != plainChrome || strings.Contains(got, "\x1b[") {
		t.Fatalf("colorless navigation changed: %q", got)
	}
}

func TestBubbleDashboardAdaptsPaletteFromTerminalBackgroundResponse(t *testing.T) {
	model := newBubbleDashboardModel(context.Background(), PresentationControl{Terminal: PresentationTerminal{
		Now: time.Now, ColorProfile: func() TerminalColorProfile { return TerminalColorTrueColor },
	}}, newBubbleDashboardSession(time.Now), TerminalDimensions{Width: 80, Height: 12})
	darkActive := model.styler.active.GetForeground()
	updated, _ := model.Update(tea.BackgroundColorMsg{Color: color.White})
	model = updated.(bubbleDashboardModel)
	if sameColor(darkActive, model.styler.active.GetForeground()) {
		t.Fatal("light terminal background response did not adapt the dashboard palette")
	}
	updated, _ = model.Update(tea.BackgroundColorMsg{Color: color.Black})
	model = updated.(bubbleDashboardModel)
	if !sameColor(darkActive, model.styler.active.GetForeground()) {
		t.Fatal("dark terminal background response did not restore the dark palette")
	}
}

func TestEnvironmentColorProfileHonorsNoColorAndReducedTerminals(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLORTERM", "truecolor")
	if got := environmentColorProfile(); got != TerminalColorNone {
		t.Fatalf("NO_COLOR profile = %d, want none", got)
	}

	t.Setenv("NO_COLOR", "")
	t.Setenv("COLORTERM", "")
	if got := environmentColorProfile(); got != TerminalColorANSI256 {
		t.Fatalf("reduced TERM profile = %d, want ANSI256", got)
	}
}

func sameColor(left, right color.Color) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftR, leftG, leftB, leftA := left.RGBA()
	rightR, rightG, rightB, rightA := right.RGBA()
	return leftR == rightR && leftG == rightG && leftB == rightB && leftA == rightA
}
