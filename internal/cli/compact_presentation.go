package cli

import (
	"strings"

	"github.com/charmbracelet/x/ansi"
)

const defaultCompactPresentationWidth = 80

// compactPresentation describes the append-only terminal treatment shared by
// status and Follow. Redirected output keeps the existing plain contracts.
type compactPresentation struct {
	enabled bool
	width   int
	styler  dashboardStyler
}

func compactPresentationFor(terminal TerminalDependencies) compactPresentation {
	if !terminal.IsTerminal() {
		return compactPresentation{}
	}
	width := defaultCompactPresentationWidth
	if dimensions, err := terminal.Dimensions(); err == nil && dimensions.Width > 0 {
		width = dimensions.Width
	}
	return compactPresentation{
		enabled: true,
		width:   width,
		styler:  newDashboardStyler(terminal.ColorProfile(), true),
	}
}

func (p compactPresentation) render(semantic dashboardSemantic, text string) string {
	if !p.enabled {
		return text
	}
	return p.styler.render(semantic, truncateDashboardContent(text, p.width))
}

func (p compactPresentation) fieldLines(indent string, fields ...string) []string {
	if len(fields) == 0 {
		return nil
	}
	width := max(1, p.width)
	prefix := ansi.Truncate(indent, width, "")
	available := max(0, width-ansi.StringWidth(prefix))
	lines := make([]string, 0, len(fields))
	current := prefix
	currentWidth := ansi.StringWidth(prefix)
	for _, field := range fields {
		field = ansi.Truncate(strings.TrimSpace(field), available, "")
		separator := ""
		if currentWidth > ansi.StringWidth(prefix) {
			separator = " | "
		}
		fieldWidth := ansi.StringWidth(separator + field)
		if separator != "" && currentWidth+fieldWidth > width {
			lines = append(lines, current)
			current = prefix + field
			currentWidth = ansi.StringWidth(current)
			continue
		}
		current += separator + field
		currentWidth += fieldWidth
	}
	lines = append(lines, current)
	return lines
}
