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

func (p compactPresentation) renderSpans(spans ...compactSpan) string {
	if !p.enabled {
		var plain strings.Builder
		for _, span := range spans {
			plain.WriteString(span.text)
		}
		return plain.String()
	}
	var rendered strings.Builder
	remaining := p.width
	for _, span := range spans {
		if remaining <= 0 {
			break
		}
		text := ansi.Truncate(span.text, remaining, "")
		rendered.WriteString(p.styler.render(span.semantic, text))
		remaining -= ansi.StringWidth(text)
	}
	return rendered.String()
}

type compactSpan struct {
	semantic dashboardSemantic
	text     string
}
