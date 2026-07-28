package github

import (
	"encoding/json"
	"testing"
)

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
