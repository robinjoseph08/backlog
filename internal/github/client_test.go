package github

import (
	"context"
	"os"
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
  "issue view 42 --repo acme/widgets --json state,labels")
    printf '%s\n' '{"state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
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

func TestClientInspectsResetIssueLabelsAndOwnedPullRequests(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-42-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":{"mergeMethod":"SQUASH"},"isDraft":false,"headRefName":"agent/issue-42-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) echo "unexpected: $*" >&2; exit 9 ;;
esac`)
	issue, pulls, err := (Client{Executable: gh}).ResetResources(context.Background(), "acme/widgets", 42, "agent/issue-42-run")
	if err != nil {
		t.Fatal(err)
	}
	if issue.Number != 42 || issue.State != "open" || strings.Join(issue.Labels, ",") != "in-progress,spec" {
		t.Fatalf("issue = %#v", issue)
	}
	if len(pulls) != 1 || pulls[0].Number != 100 || pulls[0].State != "open" || !pulls[0].AutoMergeArmed {
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
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/fork"}}]`,
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
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","mergedAt":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]`,
			want:  "unknown auto-merge state",
		},
		{
			name:  "missing merged state",
			issue: `{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[]}`,
			pull:  `[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"OPEN","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]`,
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

func TestClientVerifiesCompletionFromPullRequestAndIssue(t *testing.T) {
	t.Parallel()

	gh := fakeGH(t, `
case "$*" in
  "pr list --repo acme/widgets --state all --head agent/issue-42-run --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":100,"url":"https://github.com/acme/widgets/pull/100","state":"MERGED","mergedAt":"2026-01-03T00:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"agent/issue-42-run","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "issue view 42 --repo acme/widgets --json state,title,url")
    printf '%s\n' '{"state":"CLOSED","title":"done","url":"https://github.com/acme/widgets/issues/42"}' ;;
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
  "issue view 7 --repo acme/widgets --json state,title,url")
    printf '%s\n' '{"state":"OPEN","title":"waiting","url":"https://github.com/acme/widgets/issues/7"}' ;;
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

func fakeGH(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "gh")
	script := "#!/bin/sh\nset -eu\n" + body + "\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
