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

func TestTerminalScreenTextAppliesBubbleTeaInsertModeDelta(t *testing.T) {
	output := "Runner stage: Running\r\nNext Ctrl-C: start Drain and stop Admission" +
		"\x1b[1;15HDrai\x1b[4hn\x1b[4l\n\x1b[5Duspend unfinished Runs within the shared deadline"
	want := "Runner stage: Draining\nNext Ctrl-C: suspend unfinished Runs within the shared deadline"
	if got := terminalScreenText(output, 64, 2); got != want {
		t.Fatalf("terminal screen:\n%s\nwant:\n%s", got, want)
	}
}

func TestTerminalScreenTextScrollsConfiguredRegion(t *testing.T) {
	const initial = "top\none\ntwo\nthree\nbottom"
	tests := []struct {
		name, operation, want string
	}{
		{name: "scroll up", operation: "\x1b[2;4r\x1b[2S", want: "top\nthree\n\n\nbottom"},
		{name: "scroll down", operation: "\x1b[2;4r\x1b[2T", want: "top\n\n\none\nbottom"},
		{name: "line feed at lower margin", operation: "\x1b[2;4r\x1b[4;1H\n", want: "top\ntwo\nthree\n\nbottom"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := terminalScreenText(initial+test.operation, 8, 5); got != test.want {
				t.Fatalf("terminal screen:\n%s\nwant:\n%s", got, test.want)
			}
		})
	}
}

func TestTerminalScreenTextAppliesReverseIndexAtUpperMargin(t *testing.T) {
	output := "top\none\ntwo\nbottom\x1b[2;3r\x1b[2;1H\x1bM"
	want := "top\n\none\nbottom"
	if got := terminalScreenText(output, 8, 4); got != want {
		t.Fatalf("terminal screen:\n%s\nwant:\n%s", got, want)
	}
}

type testTerminalScreen struct {
	cells                   [][]rune
	row, column             int
	scrollTop, scrollBottom int
	insertMode              bool
	lineFeedColumn          int
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
		HandleEsc: screen.handleESC,
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
	return &testTerminalScreen{cells: cells, scrollBottom: height - 1, lineFeedColumn: -1}
}

func (s *testTerminalScreen) print(value rune) {
	s.lineFeedColumn = -1
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
	if s.insertMode {
		copy(s.cells[s.row][s.column+1:], s.cells[s.row][s.column:])
	}
	s.cells[s.row][s.column] = value
	s.column++
}

func (s *testTerminalScreen) execute(control byte) {
	switch control {
	case '\n', '\v', '\f':
		s.index()
		s.lineFeedColumn = s.column
		s.column = 0
	case '\r':
		s.lineFeedColumn = -1
		s.column = 0
	case '\b':
		s.lineFeedColumn = -1
		if s.column > 0 {
			s.column--
		}
	case '\t':
		s.lineFeedColumn = -1
		s.column = min((s.column/8+1)*8, s.width()-1)
	}
	s.clampCursor()
}

func (s *testTerminalScreen) handleESC(command ansi.Cmd) {
	s.lineFeedColumn = -1
	if command.Final() != 'M' {
		return
	}
	if s.row == s.scrollTop {
		s.scrollDown(1)
	} else {
		s.row--
	}
	s.clampCursor()
}

func (s *testTerminalScreen) handleCSI(command ansi.Cmd, params ansi.Params) {
	// The captured writer can contain either a terminal-mapped newline or the
	// renderer's raw line feed. A following relative move disambiguates the
	// latter because Bubble Tea computes it from the preserved column.
	if command.Final() == 'D' && s.lineFeedColumn >= 0 {
		s.column = s.lineFeedColumn
	}
	s.lineFeedColumn = -1
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
	case 'S':
		s.scrollUp(parameter(0, 1))
	case 'T':
		s.scrollDown(parameter(0, 1))
	case 'r':
		top := parameter(0, 1) - 1
		bottom := parameter(1, len(s.cells)) - 1
		if top >= 0 && top < bottom && bottom < len(s.cells) {
			s.scrollTop, s.scrollBottom = top, bottom
			s.row, s.column = 0, 0
		}
	case 'h', 'l':
		if command.Prefix() == 0 {
			params.ForEach(0, func(_ int, value int, _ bool) {
				if value == 4 {
					s.insertMode = command.Final() == 'h'
				}
			})
		}
	}
	s.clampCursor()
}

func (s *testTerminalScreen) index() {
	if s.row == s.scrollBottom {
		s.scrollUp(1)
		return
	}
	s.row++
}

func (s *testTerminalScreen) scrollUp(count int) {
	s.scroll(count, true)
}

func (s *testTerminalScreen) scrollDown(count int) {
	s.scroll(count, false)
}

func (s *testTerminalScreen) scroll(count int, up bool) {
	if len(s.cells) == 0 || s.scrollTop < 0 || s.scrollBottom >= len(s.cells) || s.scrollTop > s.scrollBottom {
		return
	}
	count = min(max(count, 0), s.scrollBottom-s.scrollTop+1)
	for step := 0; step < count; step++ {
		if up {
			for row := s.scrollTop; row < s.scrollBottom; row++ {
				copy(s.cells[row], s.cells[row+1])
			}
			s.clearRow(s.scrollBottom)
		} else {
			for row := s.scrollBottom; row > s.scrollTop; row-- {
				copy(s.cells[row], s.cells[row-1])
			}
			s.clearRow(s.scrollTop)
		}
	}
}

func (s *testTerminalScreen) clearRow(row int) {
	for column := range s.cells[row] {
		s.cells[row][column] = ' '
	}
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
