package initialprompt

import (
	"strings"
	"testing"
)

func TestTemplateRendersBuiltinsOnceWithoutChangingPromptBytes(t *testing.T) {
	template, err := Compile("α {{issue_number}} {{issue_title}} {{issue_url}} {{repository}} {{default_branch}} {{run_id}} {{run_id}} {{branch}} {{worktree}}\n第二行")
	if err != nil {
		t.Fatal(err)
	}
	values := Values{
		IssueNumber: 42, IssueTitle: "literal {{run_id}}", IssueURL: "https://example.test/issues/42",
		Repository: "acme/widgets", DefaultBranch: "main", RunID: "run-42", Branch: "agent/run-42", Worktree: "/tmp/工作区",
	}
	want := strings.Join([]string{"α", "42", "literal {{run_id}}", "https://example.test/issues/42", "acme/widgets", "main", "run-42", values.RunID, "agent/run-42", "/tmp/工作区"}, " ") + "\n第二行"
	if got := template.Render(values); got != want {
		t.Fatalf("rendered prompt = %q, want %q", got, want)
	}
}

func TestDigestOwnsSHA256ValidationAndExactContentMatching(t *testing.T) {
	digest := Sum("prompt\n世界")
	if !digest.Valid() || !digest.Matches("prompt\n世界") || digest.Matches("prompt\n世界 ") {
		t.Fatalf("digest validation or matching failed for %q", digest)
	}
	if Digest("short").Valid() || Digest(strings.Repeat("z", 64)).Valid() {
		t.Fatal("invalid SHA-256 encoding was accepted")
	}
}

func TestTemplateRejectsEmptyUnknownAndMalformedPlaceholders(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{name: "empty template", source: "", want: "empty"},
		{name: "empty placeholder", source: "{{}}", want: "empty placeholder"},
		{name: "unknown placeholder", source: "{{issue}}", want: "unknown placeholder"},
		{name: "whitespace", source: "{{ issue_number }}", want: "malformed placeholder"},
		{name: "expression", source: "{{issue_number.x}}", want: "malformed placeholder"},
		{name: "unclosed", source: "{{issue_number}", want: "unclosed"},
		{name: "unmatched close", source: "issue_number}}", want: "unmatched closing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Compile(test.source)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile(%q) error = %v, want %q", test.source, err, test.want)
			}
		})
	}
}
