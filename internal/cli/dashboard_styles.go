package cli

import (
	"image/color"

	"charm.land/lipgloss/v2"
	"github.com/robinjoseph08/backlog/internal/scheduler"
)

type dashboardStyler struct {
	enabled    bool
	active     lipgloss.Style
	completion lipgloss.Style
	warning    lipgloss.Style
	attention  lipgloss.Style
	metadata   lipgloss.Style
}

func newDashboardFallbackStyler(profile TerminalColorProfile) dashboardStyler {
	if profile == TerminalColorNone {
		return dashboardStyler{}
	}
	return dashboardStyler{
		enabled:    true,
		active:     lipgloss.NewStyle().Bold(true),
		completion: lipgloss.NewStyle().Italic(true),
		warning:    lipgloss.NewStyle().Underline(true),
		attention:  lipgloss.NewStyle().Bold(true).Underline(true),
		metadata:   lipgloss.NewStyle().Faint(true),
	}
}

func newDashboardStyler(profile TerminalColorProfile, darkBackground bool) dashboardStyler {
	if profile == TerminalColorNone {
		return dashboardStyler{}
	}

	var active, completion, warning, attention, metadata color.Color
	switch profile {
	case TerminalColorANSI:
		if darkBackground {
			active, completion, warning, attention, metadata = lipgloss.BrightCyan, lipgloss.BrightGreen, lipgloss.BrightYellow, lipgloss.BrightRed, lipgloss.BrightBlack
		} else {
			active, completion, warning, attention, metadata = lipgloss.Blue, lipgloss.Green, lipgloss.Yellow, lipgloss.Red, lipgloss.BrightBlack
		}
	case TerminalColorANSI256:
		if darkBackground {
			active, completion, warning, attention, metadata = lipgloss.ANSIColor(81), lipgloss.ANSIColor(78), lipgloss.ANSIColor(221), lipgloss.ANSIColor(203), lipgloss.ANSIColor(245)
		} else {
			active, completion, warning, attention, metadata = lipgloss.ANSIColor(25), lipgloss.ANSIColor(28), lipgloss.ANSIColor(94), lipgloss.ANSIColor(124), lipgloss.ANSIColor(242)
		}
	default:
		if darkBackground {
			active, completion, warning, attention, metadata = lipgloss.Color("#5FD7FF"), lipgloss.Color("#5FD787"), lipgloss.Color("#FFD75F"), lipgloss.Color("#FF6B6B"), lipgloss.Color("#A0A0A0")
		} else {
			active, completion, warning, attention, metadata = lipgloss.Color("#005FAF"), lipgloss.Color("#1A7F37"), lipgloss.Color("#8A4B00"), lipgloss.Color("#B42318"), lipgloss.Color("#5F6368")
		}
	}

	return dashboardStyler{
		enabled:    true,
		active:     lipgloss.NewStyle().Foreground(active),
		completion: lipgloss.NewStyle().Foreground(completion),
		warning:    lipgloss.NewStyle().Foreground(warning),
		attention:  lipgloss.NewStyle().Foreground(attention),
		metadata:   lipgloss.NewStyle().Foreground(metadata),
	}
}

type dashboardSemantic uint8

const (
	dashboardSemanticNone dashboardSemantic = iota
	dashboardSemanticMetadata
	dashboardSemanticActive
	dashboardSemanticCompletion
	dashboardSemanticWarning
	dashboardSemanticAttention
)

func (s dashboardStyler) render(semantic dashboardSemantic, text string) string {
	if !s.enabled || text == "" {
		return text
	}
	switch semantic {
	case dashboardSemanticMetadata:
		return s.metadata.Render(text)
	case dashboardSemanticActive:
		return s.active.Render(text)
	case dashboardSemanticCompletion:
		return s.completion.Render(text)
	case dashboardSemanticWarning:
		return s.warning.Render(text)
	case dashboardSemanticAttention:
		return s.attention.Render(text)
	default:
		return text
	}
}

func dashboardSectionSemantic(section statusSection) dashboardSemantic {
	switch section {
	case statusActive:
		return dashboardSemanticActive
	case statusAttention, statusOutcomes:
		return dashboardSemanticAttention
	case statusCompletions:
		return dashboardSemanticCompletion
	default:
		return dashboardSemanticNone
	}
}

// dashboardRunSemantic is the single typed classification used for both a
// Run's identity and its displayed lifecycle state.
func dashboardRunSemantic(observed statusRun, section statusSection) dashboardSemantic {
	if semantic := dashboardSectionSemantic(section); semantic != dashboardSemanticActive {
		return semantic
	}
	run := observed.run
	if displayedRunIsSuspending(run, observed.observation.process) {
		return dashboardSemanticWarning
	}
	switch run.Status {
	case scheduler.StatusWaitingForMerge, scheduler.StatusSuspended, scheduler.StatusResetting:
		return dashboardSemanticWarning
	case scheduler.StatusRunning:
		if observed.observation.process.workerLivenessState != workerLivenessAlive {
			return dashboardSemanticWarning
		}
	}
	return dashboardSemanticActive
}

func dashboardLivenessSemantic(observed statusRun) dashboardSemantic {
	switch observed.observation.process.workerLivenessState {
	case workerLivenessDead, workerLivenessUnknown:
		return dashboardSemanticWarning
	case workerLivenessAbsent:
		if observed.run.Status == scheduler.StatusRunning {
			return dashboardSemanticWarning
		}
	}
	return dashboardSemanticMetadata
}
