package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/resolution"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestResolveHelpDescribesCompleteArtifactRetirement(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"resolve", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	for _, want := range []string{"Safely retire owned unmerged pull requests", "remote", "local branches", "worktrees", "active Pi sessions", "preserve diagnostics", "release the Lease"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("help omitted %q: %q", want, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), "only when") || strings.Contains(stderr.String(), "are absent") {
		t.Fatalf("help still claims artifacts must already be absent: %q", stderr.String())
	}
}

type resolveFixture struct {
	repository, stateDir, git, gh, githubState string
	store                                      state.FileStore
}

func newResolveFixture(t *testing.T, labels []string, reason string) resolveFixture {
	t.Helper()
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	if output, err := exec.Command("git", "init", repository).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, output)
	}
	stateDir := filepath.Join(root, "state")
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	now := time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC)
	if err := store.Save(state.State{
		Version: state.CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main",
		Runs: []scheduler.Run{
			{Issue: 42, RunID: "older", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "older history", StartedAt: now.Add(-time.Hour)},
			{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "retained diagnostic", StartedAt: now},
		},
		Leases: []scheduler.Lease{{LeaseID: "lease-42", Issue: 42, RunID: "run-42"}},
	}); err != nil {
		t.Fatal(err)
	}
	encodedLabels, _ := json.Marshal(labels)
	encodedReason, _ := json.Marshal(reason)
	githubState := filepath.Join(root, "github.json")
	if err := os.WriteFile(githubState, []byte(`{"labels":`+string(encodedLabels)+`,"reason":`+string(encodedReason)+`,"state":"CLOSED","reopenAfterFirstMutation":false,"changeReasonAfterFirstMutation":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' `+quote(githubState)+`)
    state=$(jq -r '.state' `+quote(githubState)+`)
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"%s","labels":%s}\n' "$state" "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    jq -c '{number:42,url:"https://github.com/acme/widgets/issues/42",state:.state,stateReason:.reason}' `+quote(githubState)+` ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    tmp=`+quote(githubState)+`.tmp
    jq '(.labels |= map(select(ascii_downcase != "in-progress"))) | if .reopenAfterFirstMutation then .state = "OPEN" else . end | if .changeReasonAfterFirstMutation then .reason = "NOT_PLANNED" else . end' `+quote(githubState)+` > "$tmp"
    mv "$tmp" `+quote(githubState)+` ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    tmp=`+quote(githubState)+`.tmp; jq '.labels |= map(select(ascii_downcase != "ready-for-agent"))' `+quote(githubState)+` > "$tmp"; mv "$tmp" `+quote(githubState)+` ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	return resolveFixture{repository: repository, stateDir: stateDir, git: githubGit(t), gh: gh, githubState: githubState, store: store}
}

func (f resolveFixture) args(selector string, extra ...string) []string {
	args := []string{selector, "--repo-dir", f.repository, "--state-dir", f.stateDir, "--git", f.git, "--gh", f.gh}
	return append(args, extra...)
}

func assertResolveStateBindingsAbsent(t *testing.T, repository string) {
	t.Helper()
	for _, name := range []string{stateDirectoryBindingFile, legacyStateDirectoryBindingFile} {
		path := filepath.Join(repository, ".git", name)
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read-only Resolve created repository state binding %s: %v", path, err)
		}
	}
}

func TestResolveDryRunAndCancellationDoNotMigrateBindOrMutate(t *testing.T) {
	for _, test := range []struct {
		name, input string
		dry         bool
	}{{"dry run", "", true}, {"Enter", "\n", false}, {"EOF", "", false}, {"no", "no\n", false}} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newResolveFixture(t, []string{"in-progress", "ready-for-agent", "spec"}, "COMPLETED")
			encoded, err := os.ReadFile(fixture.store.Path)
			if err != nil {
				t.Fatal(err)
			}
			encoded = bytes.Replace(encoded, []byte(`"version": 4`), []byte(`"version": 3`), 1)
			if err := os.WriteFile(fixture.store.Path, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(fixture.store.Path)
			args := fixture.args("42")
			if test.dry {
				args = append(args, "--dry-run")
			}
			var stdout, stderr bytes.Buffer
			err = resolveCommandWithInput(context.Background(), args, strings.NewReader(test.input), !test.dry, &stdout, &stderr)
			if err != nil {
				t.Fatalf("resolve: %v, stderr=%q", err, stderr.String())
			}
			after, _ := os.ReadFile(fixture.store.Path)
			if !bytes.Equal(before, after) {
				t.Fatal("read-only operation persisted migration or mutation")
			}
			assertResolveStateBindingsAbsent(t, fixture.repository)
			if test.dry && !strings.Contains(stdout.String(), "Dry-run") || !test.dry && !strings.Contains(stdout.String(), "cancelled") {
				t.Fatalf("output = %q", stdout.String())
			}
		})
	}
}

func TestResolveClosureInspectionFailureDoesNotMutateStateOrLabels(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "ready-for-agent", "spec"}, "FUTURE")
	beforeState := fileDigest(t, fixture.store.Path)
	beforeGitHub := fileDigest(t, fixture.githubState)

	var stdout, stderr bytes.Buffer
	err := resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), `unsupported or unavailable GitHub closure reason "FUTURE"`) {
		t.Fatalf("Resolve closure inspection error = %v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
	}
	if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
		t.Fatal("closure inspection failure changed Run state or GitHub labels")
	}
}

func TestCompiledResolveDryRunAndInteractiveCancellation(t *testing.T) {
	binary := buildExecutable(t, t.TempDir())
	t.Run("dry run", func(t *testing.T) {
		fixture := newResolveFixture(t, []string{"in-progress", "spec"}, "COMPLETED")
		before := fileDigest(t, fixture.store.Path)
		command := exec.Command(binary, append([]string{"resolve"}, fixture.args("42", "--dry-run")...)...)
		output, err := command.CombinedOutput()
		if err != nil || !strings.Contains(string(output), "Dry-run: no changes made") {
			t.Fatalf("dry-run: %v\n%s", err, output)
		}
		if fileDigest(t, fixture.store.Path) != before {
			t.Fatal("compiled dry-run changed state")
		}
		assertResolveStateBindingsAbsent(t, fixture.repository)
	})
	for _, test := range []struct{ name, input string }{{"Enter", "\n"}, {"EOF", ""}, {"non-affirmative", "no\n"}} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newResolveFixture(t, []string{"in-progress", "spec"}, "COMPLETED")
			before := fileDigest(t, fixture.store.Path)
			command := compiledInteractiveCommand(binary, append([]string{"resolve"}, fixture.args("42")...)...)
			command.Stdin = strings.NewReader(test.input)
			output, err := command.CombinedOutput()
			if err != nil || !strings.Contains(string(output), "External Resolution cancelled") {
				t.Fatalf("cancellation: %v\n%s", err, output)
			}
			if fileDigest(t, fixture.store.Path) != before {
				t.Fatal("compiled cancellation changed state")
			}
			assertResolveStateBindingsAbsent(t, fixture.repository)
		})
	}
}

func TestResolveInteractiveYesFinalizesExternalResolution(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "ready-for-agent", "spec"}, "COMPLETED")

	var stdout, stderr bytes.Buffer
	if err := resolveCommandWithInput(context.Background(), fixture.args("run-42"), strings.NewReader("yes\n"), true, &stdout, &stderr); err != nil {
		t.Fatalf("interactive Resolve: %v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "Proceed with External Resolution? [y/N]") || !strings.Contains(stdout.String(), "External Resolution complete for Run run-42") {
		t.Fatalf("interactive Resolve output = %q", stdout.String())
	}
	persisted, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	run := persisted.Runs[1]
	if run.Status != scheduler.StatusResolvedExternally || run.ResolvedExternallyAt == nil || run.ClosureReason != "completed" || run.CompletedAt != nil || len(persisted.Leases) != 0 {
		t.Fatalf("interactive Resolve state = %#v", persisted)
	}
	var github struct {
		Labels []string `json:"labels"`
	}
	data, err := os.ReadFile(fixture.githubState)
	if err != nil || json.Unmarshal(data, &github) != nil {
		t.Fatalf("read interactive GitHub state: %v", err)
	}
	if strings.Join(github.Labels, ",") != "spec" {
		t.Fatalf("interactive Resolve labels = %v", github.Labels)
	}
}

func TestResolveConfirmationStopsWaitingWhenContextIsCancelled(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	prompted := make(chan struct{})
	stdout := writerFunc(func(data []byte) (int, error) {
		select {
		case <-prompted:
		default:
			close(prompted)
		}
		return len(data), nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := confirmResolve(ctx, bufio.NewReader(reader), stdout)
		done <- err
	}()
	<-prompted
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("confirmation error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Resolve confirmation did not stop waiting after cancellation")
	}
}

func TestResolveRequiresYesNonInteractivelyAndCompiledExecutableRefusesRunnerLock(t *testing.T) {
	fixture := newResolveFixture(t, []string{"spec"}, "COMPLETED")
	var stdout, stderr bytes.Buffer
	if err := resolveCommandWithInput(context.Background(), fixture.args("42"), strings.NewReader(""), false, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "requires --yes") {
		t.Fatalf("non-interactive error = %v", err)
	}
	binary := buildExecutable(t, t.TempDir())
	beforeState := fileDigest(t, fixture.store.Path)
	beforeGitHub := fileDigest(t, fixture.githubState)
	common, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := acquireRepositoryLock(common)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	command := exec.Command(binary, append([]string{"resolve"}, fixture.args("42", "--yes")...)...)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Runner owns repository coordination") {
		t.Fatalf("compiled lock error = %v\n%s", err, output)
	}
	if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
		t.Fatal("compiled lock refusal changed state")
	}
}

func TestCompiledResolveFinalizesAndRerunsIdempotently(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "ready-for-agent", "needs-info", "spec"}, "NOT_PLANNED")
	binary := buildExecutable(t, t.TempDir())
	var firstResolution time.Time
	for attempt := 0; attempt < 2; attempt++ {
		command := exec.Command(binary, append([]string{"resolve"}, fixture.args("42", "--yes")...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("attempt %d: %v\n%s", attempt+1, err, output)
		}
		if attempt == 0 {
			first, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			firstResolution = *first.Runs[1].ResolvedExternallyAt
		}
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	run := current.Runs[1]
	if run.Status != scheduler.StatusResolvedExternally || run.ResolvedExternallyAt == nil || run.ClosureReason != "not-planned" || run.CompletedAt != nil || run.Error != "retained diagnostic" || len(current.Leases) != 0 {
		t.Fatalf("resolved state = %#v", current)
	}
	if !run.ResolvedExternallyAt.Equal(firstResolution) || !run.UpdatedAt.Equal(firstResolution) {
		t.Fatalf("idempotent rerun changed resolution timestamps: %#v", run)
	}
	var github struct {
		Labels []string `json:"labels"`
	}
	data, _ := os.ReadFile(fixture.githubState)
	_ = json.Unmarshal(data, &github)
	if strings.Join(github.Labels, ",") != "needs-info,spec" {
		t.Fatalf("preserved labels = %v", github.Labels)
	}
}

func TestResolvedExternallyRerunIsVerificationOnly(t *testing.T) {
	for _, test := range []struct {
		name    string
		labels  []string
		wantErr string
	}{
		{name: "verified terminal outcome", labels: []string{"needs-info", "spec"}},
		{name: "managed label drift", labels: []string{"ready-for-agent", "spec"}, wantErr: "verification-only rerun will not mutate without a Lease"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newResolveFixture(t, test.labels, "NOT_PLANNED")
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			resolvedAt := time.Date(2026, 7, 28, 2, 3, 4, 0, time.UTC)
			current.Runs[1].Status = scheduler.StatusResolvedExternally
			current.Runs[1].ResolvedExternallyAt = &resolvedAt
			current.Runs[1].ClosureReason = "not-planned"
			current.Runs[1].UpdatedAt = resolvedAt
			current.Runs[1].CompletedAt = nil
			current.Runs[1].WorkerLogOpen = false
			current.Leases = nil
			if err := fixture.store.Save(current); err != nil {
				t.Fatal(err)
			}
			if test.wantErr != "" {
				data, err := os.ReadFile(fixture.githubState)
				if err != nil {
					t.Fatal(err)
				}
				data = bytes.Replace(data, []byte(`"reopenAfterFirstMutation":false`), []byte(`"reopenAfterFirstMutation":true`), 1)
				if err := os.WriteFile(fixture.githubState, data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			beforeState := fileDigest(t, fixture.store.Path)
			beforeGitHub := fileDigest(t, fixture.githubState)

			var stdout, stderr bytes.Buffer
			err = resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr)
			if test.wantErr == "" {
				if err != nil || !strings.Contains(stdout.String(), "External Resolution complete for Run run-42") {
					t.Fatalf("verified rerun: error=%v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("drift rerun: error=%v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
			}
			if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
				t.Fatal("Historical rerun changed original Run metadata or GitHub state")
			}
			assertResolveStateBindingsAbsent(t, fixture.repository)
		})
	}
}

func TestCompiledResolveRetiresCompleteOwnedArtifactSetAndRerunsIdempotently(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	runGit(t, fixture.repository, "push", "origin", fixture.branch)
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	current.Runs[0].IssueTitle = "Artifact-rich External Resolution"
	current.Runs[0].IssueURL = "https://github.com/acme/widgets/issues/42"
	current.Runs[0].Error = "preserved diagnostic"
	current.Runs[0].LogPath = filepath.Join(fixture.stateDir, "missing.jsonl")
	current.Runs[0].StderrPath = filepath.Join(fixture.stateDir, "missing.stderr")
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.githubState, []byte(`{"labels":["in-progress","ready-for-agent","needs-info","spec"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(fixture.githubState)+`
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"NOT_PLANNED"}' ;;
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository") printf '%s\n' '[]' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "in-progress"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "ready-for-agent"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	binary := buildExecutable(t, t.TempDir())
	args := []string{"resolve", "run-local", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git, "--gh", gh}

	dryRun := exec.Command(binary, append(args, "--dry-run")...)
	output, err := dryRun.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled artifact-rich dry-run: %v\n%s", err, output)
	}
	plan := string(output)
	ordered := []string{
		"mark Run run-local resolving-externally while retaining Lease lease-local",
		"delete remote branch " + fixture.branch,
		"remove local worktree " + fixture.worktree,
		"delete local branch " + fixture.branch,
		"archive Pi session backlog-run-local",
		"remove issue label in-progress",
		"remove issue label ready-for-agent",
		"mark Run run-local resolved-externally and release Lease lease-local",
	}
	position := -1
	for _, action := range ordered {
		next := strings.Index(plan, action)
		if next <= position {
			t.Fatalf("Resolution Plan action %q missing or out of order:\n%s", action, plan)
		}
		position = next
	}
	if strings.Contains(plan, "needs-info from") || strings.Contains(plan, "spec from") {
		t.Fatalf("dry-run mutates preserved labels:\n%s", plan)
	}
	if _, err := os.Stat(fixture.worktree); err != nil {
		t.Fatalf("dry-run removed worktree: %v", err)
	}

	mutation := exec.Command(binary, append(args, "--yes")...)
	output, err = mutation.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "External Resolution complete for Run run-local") {
		t.Fatalf("compiled artifact-rich resolution: %v\n%s", err, output)
	}
	current, err = fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	run := current.Runs[0]
	if run.Status != scheduler.StatusResolvedExternally || run.ResolvedExternallyAt == nil || run.ClosureReason != "not-planned" || len(current.Leases) != 0 {
		t.Fatalf("resolved artifact-rich state = %#v", current)
	}
	if run.IssueTitle != "Artifact-rich External Resolution" || run.IssueURL != "https://github.com/acme/widgets/issues/42" || run.Error != "preserved diagnostic" {
		t.Fatalf("resolution changed historical metadata: %#v", run)
	}
	if run.DiagnosticWarning == "" {
		t.Fatalf("missing logs did not produce a retained diagnostic warning: %#v", run)
	}
	if _, err := os.Stat(fixture.worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree survived resolution: %v", err)
	}
	if _, err := os.Stat(fixture.sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active session survived resolution: %v", err)
	}
	archive := filepath.Join(fixture.archiveDir, "session.jsonl")
	if _, err := os.Stat(archive); err != nil {
		t.Fatalf("session archive missing: %v", err)
	}
	if output, err := exec.Command("git", "-C", fixture.repository, "show-ref", "--verify", "--quiet", "refs/heads/"+fixture.branch).CombinedOutput(); err == nil {
		t.Fatalf("local branch survived resolution: %s", output)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("remote branch after resolution = %#v, %v", branch, err)
	}
	var github struct {
		Labels []string `json:"labels"`
	}
	data, _ := os.ReadFile(fixture.githubState)
	_ = json.Unmarshal(data, &github)
	if strings.Join(github.Labels, ",") != "needs-info,spec" {
		t.Fatalf("preserved GitHub labels = %v", github.Labels)
	}

	rerun := exec.Command(binary, append(args, "--yes")...)
	output, err = rerun.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "Required actions:\n  None.") {
		t.Fatalf("idempotent artifact-rich rerun: %v\n%s", err, output)
	}
}

func TestCompiledResolveDisarmsExplainsAndClosesOwnedPullRequest(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
	fixture.updateGitHubState(t, `.labels=["in-progress","ready-for-agent","needs-info","spec"]`)
	gh := writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(fixture.githubState)+`
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"COMPLETED"}' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "in-progress"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "ready-for-agent"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) exec `+quote(fixture.gh)+` "$@" ;;
esac
`)
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, "resolve", "run-github", "--yes", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git, "--gh", gh)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "External Resolution complete for Run run-github") {
		t.Fatalf("compiled pull request retirement: %v\n%s", err, output)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvedExternally || len(current.Leases) != 0 {
		t.Fatalf("pull request retirement state = %#v", current)
	}
	github := fixture.githubStateValue(t)
	if github.PR != "CLOSED" || github.Merged || github.Auto || len(github.Comments) != 1 || !strings.Contains(github.Comments[0], resolution.CommentMarker("run-github")) {
		t.Fatalf("retired pull request = %#v", github)
	}
	calls, err := os.ReadFile(fixture.githubCalls)
	if err != nil {
		t.Fatal(err)
	}
	if string(calls) != "disable\ncomment\nclose\n" {
		t.Fatalf("pull request mutation order = %q", calls)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("remote branch after pull request retirement = %#v, %v", branch, err)
	}
	var labels struct {
		Labels []string `json:"labels"`
	}
	data, _ := os.ReadFile(fixture.githubState)
	_ = json.Unmarshal(data, &labels)
	if strings.Join(labels.Labels, ",") != "needs-info,spec" {
		t.Fatalf("preserved labels = %v", labels.Labels)
	}
}

func TestCompiledResolveRecordsCompletionWhenExpectedPullRequestMergesDuringDisarm(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, true, false)
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":[{"name":"in-progress"}]}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"COMPLETED"}' ;;
  "pr merge 99 --repo acme/widgets --disable-auto")
    jq -e '.runs[] | select(.runId == "run-github" and .status == "resolving-externally")' `+quote(fixture.store.Path)+` >/dev/null
    exec `+quote(fixture.gh)+` "$@" ;;
  *) exec `+quote(fixture.gh)+` "$@" ;;
esac
`)
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, "resolve", "run-github", "--yes", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git, "--gh", gh)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "Completion recorded for Run run-github") {
		t.Fatalf("compiled merge race: %v\n%s", err, output)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusMerged || current.Runs[0].CompletedAt == nil || len(current.Leases) != 0 {
		t.Fatalf("merge-race Completion state = %#v", current)
	}
	calls, err := os.ReadFile(fixture.githubCalls)
	if err != nil {
		t.Fatal(err)
	}
	if string(calls) != "disable\n" {
		t.Fatalf("merge race performed actions after disarm: %q", calls)
	}
	github := fixture.githubStateValue(t)
	if !github.Merged || github.PR != "MERGED" {
		t.Fatalf("merge race pull request state = %#v", github)
	}
}

func TestResolveRecordsAndReportsCompletionFromMergedExpectedPullRequest(t *testing.T) {
	for _, test := range []struct{ name, reasonJSON string }{
		{name: "missing closure reason", reasonJSON: "null"},
		{name: "future closure reason", reasonJSON: `"FUTURE"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newResolveFixture(t, []string{"in-progress", "spec"}, "COMPLETED")
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			branch := "agent/issue-42-run-42"
			pullRequest := "https://github.com/acme/widgets/pull/9"
			logPath := filepath.Join(fixture.stateDir, "run-42.jsonl")
			if err := os.WriteFile(logPath, []byte("worker history\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			current.Runs[1].Status = scheduler.StatusWaitingForMerge
			current.Runs[1].Branch = branch
			current.Runs[1].Worktree = filepath.Join(fixture.stateDir, "worktrees", "issue-42-run-42")
			current.Runs[1].PullRequest = pullRequest
			current.Runs[1].LogPath = logPath
			current.Runs[1].WorkerLogOpen = true
			if err := fixture.store.Save(current); err != nil {
				t.Fatal(err)
			}
			commit := strings.Repeat("a", 40)
			fixture.git = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" remote get-url origin") printf '%s\n' 'git@github.com:acme/widgets.git' ;;
  *" ls-remote --exit-code --heads origin refs/heads/`+branch+`") exit 2 ;;
  *" for-each-ref --format=%(objectname) refs/heads/`+branch+`") exit 0 ;;
  *" worktree list --porcelain -z") exit 0 ;;
  *) exec git "$@" ;;
esac
`)
			fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":[{"name":"in-progress"},{"name":"spec"}]}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":`+test.reasonJSON+`}' ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":9,"url":"`+pullRequest+`","state":"MERGED","mergedAt":"2026-07-28T01:01:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)

			var stdout, stderr bytes.Buffer
			if err := resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr); err != nil {
				t.Fatalf("resolve Completion fallback: %v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
			}
			if !strings.Contains(stdout.String(), "Completion recorded for Run run-42 from merged expected pull request "+pullRequest) || strings.Contains(stdout.String(), "External Resolution complete") {
				t.Fatalf("Completion fallback output = %q", stdout.String())
			}
			persisted, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			run := persisted.Runs[1]
			if run.Status != scheduler.StatusMerged || run.PullRequest != pullRequest || run.CompletedAt == nil || run.WorkerLogOpen || run.ResolvedExternallyAt != nil || run.ClosureReason != "" || len(persisted.Leases) != 0 {
				t.Fatalf("Completion fallback state = %#v", persisted)
			}
			if !run.UpdatedAt.Equal(*run.CompletedAt) {
				t.Fatalf("Completion timestamps = updated %s, completed %s", run.UpdatedAt, run.CompletedAt)
			}
		})
	}
}

func TestResolveRefusesFinalizationWhenIssueReopensAfterFirstLabelMutation(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "ready-for-agent", "spec"}, "COMPLETED")
	data, err := os.ReadFile(fixture.githubState)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"reopenAfterFirstMutation":false`), []byte(`"reopenAfterFirstMutation":true`), 1)
	if err := os.WriteFile(fixture.githubState, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	err = resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "open") {
		t.Fatalf("reopen race error = %v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
	}
	if strings.Contains(stdout.String(), "External Resolution complete") || strings.Contains(stdout.String(), "Completion recorded") {
		t.Fatalf("reopen race reported success: %q", stdout.String())
	}
	persisted, loadErr := fixture.store.Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	run := persisted.Runs[1]
	if run.Status != scheduler.StatusResolvingExternally || run.ResolvedExternallyAt != nil || len(persisted.Leases) != 1 || persisted.Leases[0].RunID != run.RunID {
		t.Fatalf("reopen race state = %#v", persisted)
	}
	var github struct {
		Labels []string `json:"labels"`
		State  string   `json:"state"`
	}
	data, err = os.ReadFile(fixture.githubState)
	if err != nil || json.Unmarshal(data, &github) != nil {
		t.Fatalf("read GitHub race state: %v", err)
	}
	if github.State != "OPEN" || strings.Join(github.Labels, ",") != "ready-for-agent,spec" {
		t.Fatalf("GitHub race state = %#v", github)
	}
}

func TestResolveRefusesFinalizationWhenClosureReasonChangesAfterLabelMutation(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "ready-for-agent", "spec"}, "COMPLETED")
	data, err := os.ReadFile(fixture.githubState)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"changeReasonAfterFirstMutation":false`), []byte(`"changeReasonAfterFirstMutation":true`), 1)
	if err := os.WriteFile(fixture.githubState, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	err = resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "closure reason changed") {
		t.Fatalf("closure-reason race error = %v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
	}
	if strings.Contains(stdout.String(), "External Resolution complete") || strings.Contains(stdout.String(), "Completion recorded") {
		t.Fatalf("closure-reason race reported success: %q", stdout.String())
	}
	persisted, loadErr := fixture.store.Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	run := persisted.Runs[1]
	if run.Status != scheduler.StatusResolvingExternally || run.ResolvedExternallyAt != nil || run.ClosureReason != "" || len(persisted.Leases) != 1 || persisted.Leases[0].RunID != run.RunID {
		t.Fatalf("closure-reason race state = %#v", persisted)
	}
	var github struct {
		Labels []string `json:"labels"`
		Reason string   `json:"reason"`
	}
	data, err = os.ReadFile(fixture.githubState)
	if err != nil || json.Unmarshal(data, &github) != nil {
		t.Fatalf("read GitHub closure-reason race state: %v", err)
	}
	if github.Reason != "NOT_PLANNED" || strings.Join(github.Labels, ",") != "ready-for-agent,spec" {
		t.Fatalf("GitHub closure-reason race state = %#v", github)
	}
}

func TestResolveExactRunIDPrecedesNumericIssueAndIssueSelectsLease(t *testing.T) {
	current := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 7, RunID: "42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 42, RunID: "active", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
	}, Leases: []scheduler.Lease{{LeaseID: "numeric", Issue: 7, RunID: "42"}, {LeaseID: "active", Issue: 42, RunID: "active"}}}
	selected, lease, err := resolution.Policy("42").SelectRun(current)
	if err != nil || selected.RunID != "42" || lease.RunID != "42" {
		t.Fatalf("exact selection = %#v %#v %v", selected, lease, err)
	}
}

func TestCompiledResolveMigratesV3AndStatusAndFollowExposeResolution(t *testing.T) {
	fixture := newResolveFixture(t, []string{"spec"}, "COMPLETED")
	current, _, _ := fixture.store.Preview()
	current.Runs[1].LogPath = filepath.Join(fixture.stateDir, "missing.jsonl")
	current.Runs[1].StderrPath = filepath.Join(fixture.stateDir, "missing.stderr")
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(fixture.store.Path)
	if err != nil {
		t.Fatal(err)
	}
	encoded = bytes.Replace(encoded, []byte(`"version": 4`), []byte(`"version": 3`), 1)
	if err := os.WriteFile(fixture.store.Path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	binary := buildExecutable(t, t.TempDir())
	resolve := exec.Command(binary, append([]string{"resolve"}, fixture.args("run-42", "--yes")...)...)
	if output, err := resolve.CombinedOutput(); err != nil {
		t.Fatalf("compiled resolve: %v\n%s", err, output)
	}
	persisted, err := os.ReadFile(fixture.store.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(persisted, []byte(`"version": 4`)) {
		t.Fatalf("compiled resolve did not persist v3 migration:\n%s", persisted)
	}
	migrated, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if migrated.Version != state.CurrentVersion || len(migrated.Runs) != 2 || migrated.Runs[0].Error != "older history" ||
		migrated.Runs[1].Status != scheduler.StatusResolvedExternally || migrated.Runs[1].ResolvedExternallyAt == nil {
		t.Fatalf("compiled v3 migration changed existing state: %#v", migrated)
	}

	status := exec.Command(binary, "status", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git)
	statusOutput, err := status.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled default status: %v\n%s", err, statusOutput)
	}
	if strings.Contains(string(statusOutput), "run-42") || strings.Contains(string(statusOutput), "External Resolution") {
		t.Fatalf("default status exposed handled External Resolution: %s", statusOutput)
	}

	status = exec.Command(binary, "status", "--all", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git)
	statusOutput, err = status.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled full status: %v\n%s", err, statusOutput)
	}
	for _, want := range []string{"resolved-externally", "GitHub closure reason: completed", "retained diagnostic", "Diagnostic warning:"} {
		if !strings.Contains(string(statusOutput), want) {
			t.Fatalf("compiled full status missing %q: %s", want, statusOutput)
		}
	}

	follow := exec.Command(binary, "follow", "run-42", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git)
	followOutput, err := follow.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled Follow: %v\n%s", err, followOutput)
	}
	resolvedAt := migrated.Runs[1].ResolvedExternallyAt.UTC().Format(time.RFC3339)
	for _, want := range []string{
		"External Resolution: " + resolvedAt + " | GitHub closure reason: completed",
		"Retained diagnostic: retained diagnostic",
		"Diagnostic warning:",
	} {
		if !strings.Contains(string(followOutput), want) {
			t.Fatalf("compiled Follow missing %q: %s", want, followOutput)
		}
	}
}
