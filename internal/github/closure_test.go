package github

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestClientIssueClosureReasonValidatesGitHubSnapshot(t *testing.T) {
	tests := []struct {
		name, action, want, wantError string
	}{
		{
			name:   "completed",
			action: `printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"COMPLETED"}'`,
			want:   "completed",
		},
		{
			name:   "not planned",
			action: `printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"NOT_PLANNED"}'`,
			want:   "not-planned",
		},
		{
			name:      "mismatched number",
			action:    `printf '%s\n' '{"number":41,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"COMPLETED"}'`,
			wantError: "incomplete or mismatched issue identity/state",
		},
		{
			name:      "mismatched URL",
			action:    `printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/41","state":"CLOSED","stateReason":"COMPLETED"}'`,
			wantError: "incomplete or mismatched issue identity/state",
		},
		{
			name:      "missing state",
			action:    `printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","stateReason":"COMPLETED"}'`,
			wantError: "incomplete or mismatched issue identity/state",
		},
		{
			name:      "malformed closure reason",
			action:    `printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":{"name":"COMPLETED"}}'`,
			wantError: "unknown closure reason",
		},
		{
			name:      "unsupported closure reason",
			action:    `printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"FUTURE"}'`,
			wantError: "unsupported closure reason",
		},
		{
			name:      "command failure",
			action:    `echo 'GitHub API unavailable' >&2; exit 1`,
			wantError: "GitHub API unavailable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason") `+test.action+` ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

			got, err := (Client{Executable: gh}).IssueClosureReason(context.Background(), "acme/widgets", 42)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) || got != "" {
					t.Fatalf("IssueClosureReason() = %q, %v, want empty reason and error containing %q", got, err, test.wantError)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("IssueClosureReason() = %q, %v, want %q", got, err, test.want)
			}
		})
	}
}

func TestClientIssueClosureSeparatesVerifiedStateFromReasonSupport(t *testing.T) {
	for _, test := range []struct {
		name, response, wantReason, wantError string
		wantOpen                              bool
	}{
		{name: "missing reason", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":null}`},
		{name: "future reason", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"FUTURE"}`, wantReason: "FUTURE"},
		{name: "open", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","stateReason":null}`, wantOpen: true},
		{name: "mismatched identity", response: `{"number":41,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":null}`, wantError: "incomplete or mismatched issue identity/state"},
	} {
		t.Run(test.name, func(t *testing.T) {
			gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason") printf '%s\n' '`+test.response+`' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

			got, err := (Client{Executable: gh}).IssueClosure(context.Background(), "acme/widgets", 42)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("IssueClosure() = %#v, %v, want error containing %q", got, err, test.wantError)
				}
				return
			}
			if err != nil || got.Open != test.wantOpen || got.Reason != test.wantReason {
				t.Fatalf("IssueClosure() = %#v, %v, want open=%t reason=%q", got, err, test.wantOpen, test.wantReason)
			}
		})
	}
}

func TestInspectedClosureReasonSupportsGitHubReasons(t *testing.T) {
	for _, test := range []struct {
		state, raw, want string
		wantError        bool
	}{
		{"open", "null", "", false},
		{"closed", `"COMPLETED"`, "completed", false},
		{"closed", `"NOT_PLANNED"`, "not-planned", false},
		{"closed", "null", "", true},
		{"closed", `"FUTURE"`, "", true},
		{"open", `"COMPLETED"`, "", true},
	} {
		got, err := inspectedClosureReason(test.state, json.RawMessage(test.raw))
		if (err != nil) != test.wantError || got != test.want {
			t.Fatalf("state=%s raw=%s: got=%q err=%v", test.state, test.raw, got, err)
		}
	}
}
