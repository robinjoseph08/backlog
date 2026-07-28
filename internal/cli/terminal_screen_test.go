package cli

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func TestTerminalScreenTextAppliesCursorAddressedDashboardUpdates(t *testing.T) {
	output := "Runner stage: Running\nNext Ctrl-C: start Drain" +
		"\x1b[1;15HDraining\x1b[2;14Hsuspend unfinished Runs" +
		"\x1b[1;20H complete\x1b[2;14Hno effect\x1b[K"
	visible := terminalScreenText(output, 48, 2)
	for _, want := range []string{"Runner stage: Drain complete", "Next Ctrl-C: no effect"} {
		if !strings.Contains(visible, want) {
			t.Fatalf("terminal screen missing %q:\n%s", want, visible)
		}
	}
}

type testTerminalScreen struct {
	cells       [][]rune
	row, column int
}

// terminalScreenText applies the cursor and erase operations used by Bubble Tea
// while retaining alternate-screen contents after the terminal is restored.
func terminalScreenText(output string, width, height int) string {
	screen := newTestTerminalScreen(width, height)
	parser := ansi.NewParser()
	parser.SetHandler(ansi.Handler{
		Print:     screen.print,
		Execute:   screen.execute,
		HandleCsi: screen.handleCSI,
	})
	parser.Parse([]byte(output))
	return screen.text()
}

func newTestTerminalScreen(width, height int) *testTerminalScreen {
	cells := make([][]rune, height)
	for row := range cells {
		cells[row] = make([]rune, width)
		for column := range cells[row] {
			cells[row][column] = ' '
		}
	}
	return &testTerminalScreen{cells: cells}
}

func (s *testTerminalScreen) print(value rune) {
	if len(s.cells) == 0 || len(s.cells[0]) == 0 {
		return
	}
	if s.column >= len(s.cells[0]) {
		s.column = 0
		s.row++
	}
	if s.row < 0 || s.row >= len(s.cells) {
		return
	}
	s.cells[s.row][s.column] = value
	s.column++
}

func (s *testTerminalScreen) execute(control byte) {
	switch control {
	case '\n', '\v', '\f':
		s.row++
		s.column = 0
	case '\r':
		s.column = 0
	case '\b':
		if s.column > 0 {
			s.column--
		}
	case '\t':
		s.column = min((s.column/8+1)*8, s.width()-1)
	}
	s.clampCursor()
}

func (s *testTerminalScreen) handleCSI(command ansi.Cmd, params ansi.Params) {
	parameter := func(index, defaultValue int) int {
		value, _, _ := params.Param(index, defaultValue)
		if value == 0 {
			return defaultValue
		}
		return value
	}

	switch command.Final() {
	case 'H', 'f':
		s.row = parameter(0, 1) - 1
		s.column = parameter(1, 1) - 1
	case 'd':
		s.row = parameter(0, 1) - 1
	case 'G', '`':
		s.column = parameter(0, 1) - 1
	case 'A':
		s.row -= parameter(0, 1)
	case 'B':
		s.row += parameter(0, 1)
	case 'C':
		s.column += parameter(0, 1)
	case 'D':
		s.column -= parameter(0, 1)
	case 'E':
		s.row += parameter(0, 1)
		s.column = 0
	case 'F':
		s.row -= parameter(0, 1)
		s.column = 0
	case 'J':
		s.eraseDisplay(parameter(0, 0))
	case 'K':
		s.eraseLine(parameter(0, 0))
	}
	s.clampCursor()
}

func (s *testTerminalScreen) eraseDisplay(mode int) {
	if mode != 2 && mode != 3 {
		return
	}
	for row := range s.cells {
		for column := range s.cells[row] {
			s.cells[row][column] = ' '
		}
	}
}

func (s *testTerminalScreen) eraseLine(mode int) {
	if s.row < 0 || s.row >= len(s.cells) {
		return
	}
	start, end := s.column, s.width()
	switch mode {
	case 1:
		start, end = 0, min(s.column+1, s.width())
	case 2:
		start, end = 0, s.width()
	}
	for column := max(start, 0); column < end; column++ {
		s.cells[s.row][column] = ' '
	}
}

func (s *testTerminalScreen) width() int {
	if len(s.cells) == 0 {
		return 0
	}
	return len(s.cells[0])
}

func (s *testTerminalScreen) clampCursor() {
	if len(s.cells) == 0 || s.width() == 0 {
		s.row, s.column = 0, 0
		return
	}
	s.row = max(0, min(s.row, len(s.cells)-1))
	s.column = max(0, min(s.column, s.width()-1))
}

func (s *testTerminalScreen) text() string {
	lines := make([]string, len(s.cells))
	for row := range s.cells {
		lines[row] = strings.TrimRight(string(s.cells[row]), " ")
	}
	return strings.TrimRight(strings.Join(lines, "\n"), "\n")
}
