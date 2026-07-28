package github

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestClientDiscoversRepository(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"trunk"}}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	client := Client{Executable: gh, Dir: t.TempDir()}

	got, err := client.Repository(context.Background())
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	if got.Slug != "acme/widgets" || got.DefaultBranch != "trunk" {
		t.Fatalf("got %#v", got)
	}
}

func TestClientFindsNativeAndExplicitOpenBlockers(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":1,"title":"old","createdAt":"2026-01-01T00:00:00Z","url":"https://github.com/acme/widgets/issues/1"},{"number":2,"title":"new","createdAt":"2026-01-02T00:00:00Z","url":"https://github.com/acme/widgets/issues/2"}]' ;;
  "issue view 1 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":1,"title":"old","body":"Blocked by #9. Related: #99","state":"OPEN","url":"https://github.com/acme/widgets/issues/1","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "issue view 2 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":2,"title":"new","body":"Depends on acme/api#8","state":"OPEN","url":"https://github.com/acme/widgets/issues/2","createdAt":"2026-01-02T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/1/comments?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/2/comments?per_page=100 --paginate --slurp")
    printf '%s\n' '[[{"body":"The dependency on acme/api#8 was removed.","created_at":"2026-01-03T00:00:00Z"}]]' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/1/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[{"number":7,"title":"native","html_url":"https://github.com/acme/widgets/issues/7","state":"open"}]]' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/2/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "issue view 9 --repo acme/widgets --json state,title,url")
    printf '%s\n' '{"state":"OPEN","title":"text","url":"https://github.com/acme/widgets/issues/9"}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	client := Client{Executable: gh}

	got, err := client.Candidates(context.Background(), "acme/widgets")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d candidates, want 2", len(got))
	}
	if len(got[0].Blockers) != 2 || got[0].Blockers[0].Number != 7 || got[0].Blockers[1].Number != 9 {
		t.Fatalf("issue 1 blockers = %#v, want native #7 and text #9", got[0].Blockers)
	}
	if len(got[1].Blockers) != 0 {
		t.Fatalf("issue 2 blockers = %#v, want removal to supersede text blocker", got[1].Blockers)
	}
}

func TestClientFailsSafelyOnMalformedGitHubOutput(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue list "*) printf '%s\n' 'not-json' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	_, err := (Client{Executable: gh}).Candidates(context.Background(), "acme/widgets")
	if err == nil || !strings.Contains(err.Error(), "decode gh") {
		t.Fatalf("error = %v, want malformed GitHub output failure", err)
	}
	var discovery *CandidateDiscoveryError
	if !errors.As(err, &discovery) || discovery.Operation != CandidateDiscoveryList || discovery.Issue != 0 {
		t.Fatalf("Candidate discovery context = %#v", discovery)
	}
	if discovery.Cause != "invalid character 'o' in literal null (expecting 'u')" {
		t.Fatalf("Candidate discovery concise cause = %q", discovery.Cause)
	}
}

func TestClientSeparatesCandidateDiscoveryCauseFromFullCommand(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue list "*) echo "TLS handshake timeout" >&2; exit 1 ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	_, err := (Client{Executable: gh}).Candidates(context.Background(), "acme/widgets")
	var discovery *CandidateDiscoveryError
	if !errors.As(err, &discovery) {
		t.Fatalf("Candidate discovery error = %v", err)
	}
	if discovery.Cause != "TLS handshake timeout" {
		t.Fatalf("concise cause = %q", discovery.Cause)
	}
	if !strings.Contains(discovery.Error(), "gh issue list --repo acme/widgets") || !strings.Contains(discovery.Error(), discovery.Cause) {
		t.Fatalf("full error lost command evidence: %q", discovery.Error())
	}
}

func TestClientRejectsIncompleteCandidateSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		issue string
		want  string
	}{
		{
			name:  "mismatched issue number",
			issue: `{"number":2,"title":"title","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/2","createdAt":"2026-01-01T00:00:00Z"}`,
			want:  "identity mismatch",
		},
		{
			name:  "missing title",
			issue: `{"number":1,"title":" ","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/1","createdAt":"2026-01-01T00:00:00Z"}`,
			want:  "omitted required title",
		},
		{
			name:  "missing URL",
			issue: `{"number":1,"title":"title","body":"","state":"OPEN","url":"","createdAt":"2026-01-01T00:00:00Z"}`,
			want:  "omitted required title",
		},
		{
			name:  "missing creation time",
			issue: `{"number":1,"title":"title","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/1"}`,
			want:  "omitted required title",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gh := fakeGH(t, `
case "$*" in
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":1,"title":"title","createdAt":"2026-01-01T00:00:00Z","url":"https://github.com/acme/widgets/issues/1"}]' ;;
  "issue view 1 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '`+test.issue+`' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
			_, err := (Client{Executable: gh}).Candidates(context.Background(), "acme/widgets")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			var discovery *CandidateDiscoveryError
			if !errors.As(err, &discovery) || discovery.Operation != CandidateDiscoveryInspect || discovery.Issue != 1 {
				t.Fatalf("Candidate discovery context = %#v", discovery)
			}
		})
	}
}

func TestClientFailsClosedWhenNativeDependencyLookupFails(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    printf '%s\n' '[{"number":1,"title":"old","createdAt":"2026-01-01T00:00:00Z","url":"u"}]' ;;
  "issue view 1 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":1,"title":"old","body":"","state":"OPEN","url":"u","createdAt":"2026-01-01T00:00:00Z"}' ;;
  "api "*) echo "dependency API unavailable" >&2; exit 1 ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	client := Client{Executable: gh}

	_, err := client.Candidates(context.Background(), "acme/widgets")
	if err == nil || !strings.Contains(err.Error(), "native blockers") {
		t.Fatalf("got error %v, want native blocker lookup failure", err)
	}
}

func TestClientReadsIssueStateAndLabelsForResume(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	got, err := (Client{Executable: gh}).IssueState(context.Background(), "acme/widgets", 42)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Open || len(got.Labels) != 2 || got.Labels[0] != "in-progress" || got.Labels[1] != "spec" {
		t.Fatalf("issue state = %#v", got)
	}
}

func TestClientIssueInspectionRefusesAmbiguousManagedLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		labels string
		want   string
	}{
		{name: "duplicate", labels: `[{"name":"in-progress"},{"name":"in-progress"}]`, want: "duplicate label"},
		{name: "case alias", labels: `[{"name":"IN-PROGRESS"}]`, want: "non-canonical managed label"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":`+test.labels+`}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
			got, err := (Client{Executable: gh}).IssueState(context.Background(), "acme/widgets", 42)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if got.Open || got.Labels != nil {
				t.Fatalf("issue state = %#v, want fail-closed empty metadata", got)
			}
		})
	}
}

func TestClientInspectsResetIssueLabelsAndOwnedPullRequests(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-42-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":{"mergeMethod":"SQUASH"},"isDraft":false,"headRefName":"agent/issue-42-run","headRefOid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/100/comments?per_page=100 --paginate --slurp")
    printf '%s\n' '[[],[{"body":"existing comment"}]]' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	issue, pulls, err := (Client{Executable: gh}).ResetResources(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
	if err != nil {
		t.Fatal(err)
	}
	if issue.Number != 42 || issue.State != "open" || strings.Join(issue.Labels, ",") != "in-progress,spec" {
		t.Fatalf("issue = %#v", issue)
	}
	if len(pulls) != 1 || pulls[0].Number != 100 || pulls[0].State != "open" || !pulls[0].AutoMergeArmed ||
		pulls[0].Branch != "agent/issue-42-run" || pulls[0].Commit != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		strings.Join(pulls[0].Comments, ",") != "existing comment" {
		t.Fatalf("pulls = %#v", pulls)
	}
}

func TestClientResetInspectionRefusesMismatchedPullRequestOwner(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}' ;;
  "pr list "*)
    printf '%s\n' '[{"number":100,"url":"https://github.com/other/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRepositoryOwner":{"login":"other"},"headRepository":{"nameWithOwner":"other/widgets"}}]' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	_, _, err := (Client{Executable: gh}).ResetResources(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
	if err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("error = %v", err)
	}
}

func TestClientResetInspectionRefusesSameOwnerForkAndUnknownFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		issue string
		pull  string
		want  string
	}{
		{
			name:  "same owner fork",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRefOid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/fork"}}]`,
			want:  "mismatched",
		},
		{
			name:  "null head commit",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRefOid":"0000000000000000000000000000000000000000","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]`,
			want:  "mismatched",
		},
		{
			name:  "missing labels",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN"}`,
			pull:  `[]`,
			want:  "unknown labels",
		},
		{
			name:  "mismatched issue number",
			issue: `{"number":41,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[]`,
			want:  "identity/state",
		},
		{
			name:  "issue URL on wrong host",
			issue: `{"number":42,"url":"https://example.test/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[]`,
			want:  "identity/state",
		},
		{
			name:  "unknown issue state",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"UNKNOWN","labels":[]}`,
			pull:  `[]`,
			want:  "identity/state",
		},
		{
			name:  "null pull request list",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `null`,
			want:  "unknown pull request list",
		},
		{
			name:  "missing auto merge",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRefOid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]`,
			want:  "unknown auto-merge state",
		},
		{
			name:  "draft with auto merge request",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":{"mergeMethod":"SQUASH"},"isDraft":true,"headRefName":"agent/issue-42-run","headRefOid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]`,
			want:  "unknown auto-merge state",
		},
		{
			name:  "missing merged state",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRefOid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]`,
			want:  "unknown merged state",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '`+test.issue+`' ;;
  "pr list "*) printf '%s\n' '`+test.pull+`' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
			_, _, err := (Client{Executable: gh}).ResetResources(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClientResetInspectionRefusesUnknownCommentState(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		comments string
	}{
		{name: "null list", comments: `null`},
		{name: "missing pages", comments: `[]`},
		{name: "null page", comments: `[null]`},
		{name: "missing body", comments: `[[{}]]`},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}' ;;
  "pr list "*)
    printf '%s\n' '[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRefOid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "api "*) printf '%s\n' '`+test.comments+`' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
			_, _, err := (Client{Executable: gh}).ResetResources(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
			if err == nil || !strings.Contains(err.Error(), "comment") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestClientResetInspectionRefusesTruncatedPullRequestList(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}' ;;
  "pr list "*)
    printf '['
    i=1
    while [ "$i" -le 1000 ]; do
      if [ "$i" -gt 1 ]; then printf ','; fi
      printf '{}'
      i=$((i + 1))
    done
    printf ']\n' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	_, _, err := (Client{Executable: gh}).ResetResources(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
	if err == nil || !strings.Contains(err.Error(), "inspection limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestClientPerformsNarrowPullRequestResetMutations(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "calls")
	gh := fakeGH(t, `
printf '%s\n' "$*" >> `+shellQuote(logPath)+`
case "$*" in
  "pr merge 100 --repo acme/widgets --disable-auto"|\
  "pr comment 100 --repo acme/widgets --body explanation"|\
  "pr close 100 --repo acme/widgets") ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	client := Client{Executable: gh}
	if err := client.DisablePullRequestAutoMerge(context.Background(), "acme/widgets", 100); err != nil {
		t.Fatal(err)
	}
	if err := client.CommentOnPullRequest(context.Background(), "acme/widgets", 100, "explanation"); err != nil {
		t.Fatal(err)
	}
	if err := client.ClosePullRequest(context.Background(), "acme/widgets", 100); err != nil {
		t.Fatal(err)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "pr merge 100 --repo acme/widgets --disable-auto\n" +
		"pr comment 100 --repo acme/widgets --body explanation\n" +
		"pr close 100 --repo acme/widgets\n"
	if string(calls) != want {
		t.Fatalf("calls = %q, want %q", calls, want)
	}
}

func TestClientPullRequestResetMutationReportsGitHubFailure(t *testing.T) {
	t.Parallel()
	gh := fakeGH(t, `echo "pull request denied" >&2; exit 1`)
	if err := (Client{Executable: gh}).ClosePullRequest(context.Background(), "acme/widgets", 100); err == nil || !strings.Contains(err.Error(), "pull request denied") {
		t.Fatalf("error = %v", err)
	}
}

func TestClientMutatesOnlyOneIssueLabel(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "calls")
	gh := fakeGH(t, `
printf '%s\n' "$*" >> `+shellQuote(logPath)+`
case "$*" in
  "issue edit 42 --repo acme/widgets --remove-label in-progress"|\
  "issue edit 42 --repo acme/widgets --add-label ready-for-agent") ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	client := Client{Executable: gh}
	if err := client.RemoveIssueLabel(context.Background(), "acme/widgets", 42, "in-progress"); err != nil {
		t.Fatal(err)
	}
	if err := client.AddIssueLabel(context.Background(), "acme/widgets", 42, "ready-for-agent"); err != nil {
		t.Fatal(err)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "issue edit 42 --repo acme/widgets --remove-label in-progress\n" +
		"issue edit 42 --repo acme/widgets --add-label ready-for-agent\n"
	if string(calls) != want {
		t.Fatalf("calls = %q, want %q", calls, want)
	}
}

func TestClientLabelMutationReportsGitHubFailure(t *testing.T) {
	t.Parallel()
	gh := fakeGH(t, `echo "label denied" >&2; exit 1`)
	err := (Client{Executable: gh}).AddIssueLabel(context.Background(), "acme/widgets", 42, "ready-for-agent")
	if err == nil || !strings.Contains(err.Error(), "label denied") {
		t.Fatalf("error = %v", err)
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func TestClientVerifiesCompletionFromPullRequestAndIssue(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "pr list --repo acme/widgets --state all --head agent/issue-42-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"MERGED","mergedAt":"2026-01-03T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "issue view 42 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":42,"state":"CLOSED","title":"done","url":"https://github.com/acme/widgets/issues/42"}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	client := Client{Executable: gh}

	got, err := client.Completion(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
	if err != nil {
		t.Fatalf("completion: %v", err)
	}
	if !got.Merged || !got.IssueClosed || got.PullRequest != "https://github.com/acme/widgets/pull/100" {
		t.Fatalf("got %#v, want verified completion", got)
	}
}

func TestClientRecognizesArmedAutoMergeAsUnderstoodWait(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "pr list --repo acme/widgets --state all --head agent/issue-7-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":101,"url":"https://github.com/acme/widgets/pull/101","state":"OPEN","mergedAt":null,"autoMergeRequest":{"mergeMethod":"SQUASH"},"isDraft":false,"headRefName":"agent/issue-7-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "issue view 7 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":7,"state":"OPEN","title":"waiting","url":"https://github.com/acme/widgets/issues/7"}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

	got, err := (Client{Executable: gh}).Completion(context.Background(), "acme/widgets", 7, "agent/issue-7-run")
	if err != nil {
		t.Fatalf("completion: %v", err)
	}
	if !got.PRFound || got.Merged || !got.AutoMergeArmed {
		t.Fatalf("got %#v, want understood auto-merge wait", got)
	}
}

func TestClientIssueInspectionRefusesMismatchedIdentity(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":41,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"}]}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

	got, err := (Client{Executable: gh}).IssueState(context.Background(), "acme/widgets", 42)
	if err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("error = %v, want mismatched issue refusal", err)
	}
	if got.Open || got.Labels != nil {
		t.Fatalf("issue state = %#v, want fail-closed empty metadata", got)
	}
}

func TestClientIssueInspectionRefusesUnicodeRepositoryAlias(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgetſ/issues/42","state":"OPEN","labels":[{"name":"in-progress"}]}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

	got, err := (Client{Executable: gh}).IssueState(context.Background(), "acme/widgets", 42)
	if err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("error = %v, want Unicode repository alias refusal", err)
	}
	if got.Open || got.Labels != nil {
		t.Fatalf("issue state = %#v, want fail-closed empty metadata", got)
	}
}

func TestClientIssueInspectionRefusesDuplicateIdentityFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response string
	}{
		{name: "number", response: `{"number":41,"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"}]}`},
		{name: "number case alias", response: `{"number":41,"Number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"}]}`},
		{name: "lone number case alias", response: `{"Number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"}]}`},
		{name: "URL", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/41","url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"}]}`},
		{name: "state", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","state":"OPEN","labels":[{"name":"in-progress"}]}`},
		{name: "Unicode state alias", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","\u017Ftate":"OPEN","labels":[{"name":"in-progress"}]}`},
		{name: "lone Unicode state alias", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","\u017Ftate":"OPEN","labels":[{"name":"in-progress"}]}`},
		{name: "nested label case alias", response: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"ready-for-human","Name":"in-progress"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '`+test.response+`' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

			got, err := (Client{Executable: gh}).IssueState(context.Background(), "acme/widgets", 42)
			if err == nil || !strings.Contains(err.Error(), "JSON field") {
				t.Fatalf("error = %v, want ambiguous identity refusal", err)
			}
			if got.Open || got.Labels != nil {
				t.Fatalf("issue state = %#v, want fail-closed empty metadata", got)
			}
		})
	}
}

func TestClientCompletionRefusesMismatchedIssueIdentity(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "pr list --repo acme/widgets --state all --head agent/issue-42-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[]' ;;
  "issue view 42 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '{"number":41,"state":"OPEN","title":"wrong issue","url":"https://github.com/acme/widgets/issues/42"}' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

	got, err := (Client{Executable: gh}).Completion(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
	if err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("error = %v, want mismatched issue refusal", err)
	}
	if got != (CompletionOutcome{}) {
		t.Fatalf("completion = %#v, want fail-closed empty outcome", got)
	}
}

func TestClientCompletionRefusesDuplicateIssueIdentityFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response string
	}{
		{name: "number", response: `{"number":41,"number":42,"state":"OPEN","url":"https://github.com/acme/widgets/issues/42"}`},
		{name: "number case alias", response: `{"number":41,"Number":42,"state":"OPEN","url":"https://github.com/acme/widgets/issues/42"}`},
		{name: "lone number case alias", response: `{"Number":42,"state":"OPEN","url":"https://github.com/acme/widgets/issues/42"}`},
		{name: "URL", response: `{"number":42,"state":"OPEN","url":"https://github.com/acme/widgets/issues/41","url":"https://github.com/acme/widgets/issues/42"}`},
		{name: "state", response: `{"number":42,"state":"CLOSED","state":"OPEN","url":"https://github.com/acme/widgets/issues/42"}`},
		{name: "Unicode state alias", response: `{"number":42,"state":"CLOSED","\u017Ftate":"OPEN","url":"https://github.com/acme/widgets/issues/42"}`},
		{name: "lone Unicode state alias", response: `{"number":42,"\u017Ftate":"OPEN","url":"https://github.com/acme/widgets/issues/42"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gh := fakeGH(t, `
case "$*" in
  "pr list --repo acme/widgets --state all --head agent/issue-42-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[]' ;;
  "issue view 42 --repo acme/widgets --json number,state,title,url")
    printf '%s\n' '`+test.response+`' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

			got, err := (Client{Executable: gh}).Completion(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
			if err == nil || !strings.Contains(err.Error(), "JSON field") {
				t.Fatalf("error = %v, want ambiguous identity refusal", err)
			}
			if got != (CompletionOutcome{}) {
				t.Fatalf("completion = %#v, want fail-closed empty outcome", got)
			}
		})
	}
}

func TestClientCompletionRefusesCaseVariantPullRequestIdentity(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "pr list --repo acme/widgets --state all --head agent/issue-9-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":109,"url":"https://github.com/acme/widgets/pull/109","state":"MERGED","mergedAt":"2026-01-03T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-9-run","headRepositoryOwner":{"login":"other","Login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

	got, err := (Client{Executable: gh}).Completion(context.Background(), "acme/widgets", 9, "agent/issue-9-run")
	if err == nil || !strings.Contains(err.Error(), "duplicate JSON field") {
		t.Fatalf("error = %v, want case-variant pull request identity refusal", err)
	}
	if got != (CompletionOutcome{}) {
		t.Fatalf("completion = %#v, want fail-closed empty outcome", got)
	}
}

func TestClientCompletionRefusesSameBranchFromFork(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "pr list --repo acme/widgets --state all --head agent/issue-9-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":109,"url":"https://github.com/acme/widgets/pull/109","state":"MERGED","mergedAt":"2026-01-03T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-9-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/fork"}}]' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)

	_, err := (Client{Executable: gh}).Completion(context.Background(), "acme/widgets", 9, "agent/issue-9-run")
	if err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("error = %v, want mismatched pull request refusal", err)
	}
}

func TestMain(m *testing.M) {
	if filepath.Base(os.Args[0]) == "gh" {
		os.Exit(runFakeGH())
	}
	os.Exit(m.Run())
}

func runFakeGH() int {
	scriptPath := filepath.Join(filepath.Dir(os.Args[0]), "gh-script")
	args := append([]string{scriptPath}, os.Args[1:]...)
	cmd := exec.Command("/bin/sh", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 127
	}
	return 0
}

func fakeGH(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "gh-script")
	script := "#!/bin/sh\nset -eu\n" + body + "\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "gh")
	if err := os.Symlink(executable, path); err != nil {
		t.Fatal(err)
	}
	return path
}
