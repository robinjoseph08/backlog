package cli

import (
	"image/color"
	"strings"

	"charm.land/lipgloss/v2"
)

type dashboardStyler struct {
	enabled    bool
	active     lipgloss.Style
	completion lipgloss.Style
	warning    lipgloss.Style
	attention  lipgloss.Style
	metadata   lipgloss.Style
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

func (s dashboardStyler) body(content string) string {
	if !s.enabled || content == "" {
		return content
	}
	lines := strings.Split(content, "\n")
	section := ""
	for index, line := range lines {
		switch {
		case strings.HasPrefix(line, "Active Runs ("):
			section = "active"
			lines[index] = s.active.Render(line)
		case strings.HasPrefix(line, "Attention Required ("):
			section = "attention"
			lines[index] = s.attention.Render(line)
		case strings.HasPrefix(line, "Outcomes to Acknowledge ("):
			section = "attention"
			lines[index] = s.attention.Render(line)
		case strings.HasPrefix(line, "Recent Completions ("):
			section = "completion"
			lines[index] = s.completion.Render(line)
		case line == "Operational messages":
			section = "messages"
			lines[index] = s.metadata.Render(line)
		case strings.HasPrefix(line, "  #"):
			lines[index] = s.styleRunIdentity(lines, index, section)
		case strings.HasPrefix(line, "    State: "):
			lines[index] = s.styleStateLine(line, section)
		case strings.HasPrefix(line, "    Diagnostic: "):
			lines[index] = s.attention.Render(line)
		case strings.HasPrefix(line, "  ") && section == "messages":
			lines[index] = s.styleOperationalMessage(line)
		case strings.HasPrefix(line, "    "), strings.HasPrefix(line, "  none"):
			lines[index] = s.metadata.Render(line)
		}
	}
	return strings.Join(lines, "\n")
}

func (s dashboardStyler) styleRunIdentity(lines []string, index int, section string) string {
	line := lines[index]
	switch section {
	case "attention":
		return s.attention.Render(line)
	case "completion":
		identity, metadata, found := strings.Cut(line, " | ")
		if !found {
			return s.completion.Render(line)
		}
		return s.completion.Render(identity) + s.metadata.Render(" | "+metadata)
	case "active":
		style := s.active
		for next := index + 1; next < len(lines) && !strings.HasPrefix(lines[next], "  #") && !dashboardSectionHeading(lines[next]); next++ {
			if activeRunIsWarning(lines[next]) {
				style = s.warning
				break
			}
		}
		return style.Render(line)
	default:
		return line
	}
}

func dashboardSectionHeading(line string) bool {
	return strings.HasPrefix(line, "Active Runs (") || strings.HasPrefix(line, "Attention Required (") ||
		strings.HasPrefix(line, "Outcomes to Acknowledge (") || strings.HasPrefix(line, "Recent Completions (") ||
		line == "Operational messages"
}

func activeRunIsWarning(line string) bool {
	if strings.HasPrefix(line, "    State: ") {
		state := strings.TrimPrefix(strings.SplitN(line, " | ", 2)[0], "    State: ")
		switch state {
		case "waiting-for-merge", "suspended", "suspending", "resetting":
			return true
		case "running":
			return strings.Contains(line, "Worker liveness: absent") || strings.Contains(line, "Worker liveness: dead") || strings.Contains(line, "Worker liveness: unknown")
		}
	}
	return false
}

func (s dashboardStyler) styleOperationalMessage(line string) string {
	switch {
	case strings.Contains(line, "Force stop:"):
		return s.attention.Render(line)
	case strings.Contains(line, "Drain complete:"):
		return s.completion.Render(line)
	case strings.Contains(line, "candidate discovery recovered; admission resumed"):
		return s.active.Render(line)
	case strings.Contains(line, "candidate discovery failed; admission paused"), strings.Contains(line, "Drain:"), strings.Contains(line, "Suspension:"):
		return s.warning.Render(line)
	default:
		return s.metadata.Render(line)
	}
}

func (s dashboardStyler) styleStateLine(line, section string) string {
	parts := strings.Split(line, " | ")
	state := strings.TrimPrefix(parts[0], "    State: ")
	stateStyle := s.active
	if section == "attention" {
		stateStyle = s.attention
	} else {
		switch state {
		case "waiting-for-merge", "suspended", "suspending", "resetting":
			stateStyle = s.warning
		case "merged":
			stateStyle = s.completion
		case "failed", "needs-human":
			stateStyle = s.attention
		}
	}
	parts[0] = stateStyle.Render(parts[0])
	for index := 1; index < len(parts); index++ {
		style := s.metadata
		if strings.HasPrefix(parts[index], "Worker liveness: dead") || strings.HasPrefix(parts[index], "Worker liveness: unknown") ||
			(state == "running" && strings.HasPrefix(parts[index], "Worker liveness: absent")) {
			style = s.warning
		}
		parts[index] = style.Render(parts[index])
	}
	return strings.Join(parts, s.metadata.Render(" | "))
}

func (s dashboardStyler) chrome(line string) string {
	if !s.enabled || line == "" {
		return line
	}
	switch {
	case strings.Contains(line, "Force stopping"), strings.Contains(line, "Drain incomplete"), strings.Contains(line, "S:Force stopping"), strings.Contains(line, "S:Drain incomplete"):
		return s.attention.Render(line)
	case strings.Contains(line, "Suspending"), strings.Contains(line, "Draining"), strings.Contains(line, "Suspension finished"), strings.Contains(line, "Stopped;"), strings.Contains(line, "S:Suspending"), strings.Contains(line, "S:Draining"), strings.Contains(line, "S:Suspension finished"), strings.Contains(line, "S:Stopped;"):
		return s.warning.Render(line)
	case strings.Contains(line, "Drain complete"), strings.Contains(line, "Complete;"), strings.Contains(line, "S:Drain complete"), strings.Contains(line, "S:Complete;"):
		return s.completion.Render(line)
	case strings.Contains(line, "Runner stage: Running"), strings.Contains(line, "S:Running"):
		return s.active.Render(line)
	case strings.HasPrefix(line, "Repository: "), strings.HasPrefix(line, "Worker capacity: "), strings.HasPrefix(line, "R:"), strings.HasPrefix(line, "W:"):
		return s.metadata.Render(line)
	default:
		return line
	}
}
