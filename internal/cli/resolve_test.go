package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/resolution"
	"github.com/robinjoseph08/backlog/internal/retirement"
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

func resolveGitHubWithLabelChangeAfterFirstInspection(t *testing.T, fixture resolveFixture) string {
	t.Helper()
	root := t.TempDir()
	viewed := filepath.Join(root, "viewed")
	changed := filepath.Join(root, "changed")
	return writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    if [ ! -f `+quote(viewed)+` ]; then
      touch `+quote(viewed)+`
    elif [ ! -f `+quote(changed)+` ]; then
      temporary=`+quote(fixture.githubState)+`.tmp
      jq '.labels += ["ready-for-agent"]' `+quote(fixture.githubState)+` > "$temporary"
      mv "$temporary" `+quote(fixture.githubState)+`
      touch `+quote(changed)+`
    fi ;;
esac
exec `+quote(fixture.gh)+` "$@"
`)
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

func localArtifactResolveGitHub(t *testing.T, fixture localArtifactResetFixture) string {
	t.Helper()
	if err := os.WriteFile(fixture.githubState, []byte(`{"labels":["in-progress","ready-for-agent","spec"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(fixture.githubState)+`
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"COMPLETED"}' ;;
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository") printf '%s\n' '[]' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "in-progress"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "ready-for-agent"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
}

func githubArtifactResolveGitHub(t *testing.T, fixture githubArtifactResetFixture) string {
	t.Helper()
	fixture.updateGitHubState(t, `.labels=["in-progress","ready-for-agent","spec"]`)
	return writeExecutable(t, `#!/bin/sh
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
}

func localArtifactResolveArgs(fixture localArtifactResetFixture, git, gh string, extra ...string) []string {
	arguments := []string{"resolve", "run-local", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", git, "--gh", gh}
	return append(arguments, extra...)
}

func githubArtifactResolveArgs(fixture githubArtifactResetFixture, git, gh string, extra ...string) []string {
	arguments := []string{"resolve", "run-github", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", git, "--gh", gh}
	return append(arguments, extra...)
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

func TestResolveChangedInteractivePlanRequiresConfirmationAgain(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "spec"}, "COMPLETED")
	fixture.gh = resolveGitHubWithLabelChangeAfterFirstInspection(t, fixture)
	before := fileDigest(t, fixture.store.Path)

	var stdout, stderr bytes.Buffer
	if err := resolveCommandWithInput(context.Background(), fixture.args("run-42"), strings.NewReader("yes\nno\n"), true, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if strings.Count(stdout.String(), "External Resolution Plan for issue #42") != 2 || !strings.Contains(stdout.String(), "confirm the current plan again") || !strings.Contains(stdout.String(), "remove issue label ready-for-agent") {
		t.Fatalf("changed-plan output = %q", stdout.String())
	}
	if fileDigest(t, fixture.store.Path) != before {
		t.Fatal("second confirmation refusal changed Run state")
	}
}

func TestResolveYesPrintsChangedCurrentPlanAndContinues(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "spec"}, "COMPLETED")
	fixture.gh = resolveGitHubWithLabelChangeAfterFirstInspection(t, fixture)

	var stdout, stderr bytes.Buffer
	if err := resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr); err != nil {
		t.Fatalf("Resolve with changed plan: %v, stderr=%q", err, stderr.String())
	}
	if strings.Count(stdout.String(), "External Resolution Plan for issue #42") != 2 || !strings.Contains(stdout.String(), "using the current plan") || !strings.Contains(stdout.String(), "remove issue label ready-for-agent") {
		t.Fatalf("changed-plan output = %q", stdout.String())
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[1].Status != scheduler.StatusResolvedExternally || len(current.Leases) != 0 {
		t.Fatalf("changed-plan Resolution state = %#v", current)
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

func TestResolveRerunsOnlyRemainingLocalActionsAfterEveryMutationBoundary(t *testing.T) {
	for _, test := range []struct {
		name            string
		wrapGit         func(*testing.T, localArtifactResetFixture) string
		wrapGitHub      func(*testing.T, localArtifactResetFixture, string) string
		completedAbsent []string
		remaining       string
	}{
		{
			name: "durable progress transition", remaining: "delete remote branch",
			wrapGitHub: func(t *testing.T, _ localArtifactResetFixture, underlying string) string {
				calls := filepath.Join(t.TempDir(), "repository-inspections")
				return writeExecutable(t, `#!/bin/sh
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' inspection >> `+quote(calls)+`
    if [ "$(wc -l < `+quote(calls)+`)" -eq 3 ]; then
      echo 'failure after durable progress transition' >&2
      exit 1
    fi ;;
esac
exec `+quote(underlying)+` "$@"
`)
			},
		},
		{
			name: "remote branch deletion", completedAbsent: []string{"delete remote branch"}, remaining: "remove local worktree",
			wrapGit: func(t *testing.T, fixture localArtifactResetFixture) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
case "$*" in
  *" push origin --force-with-lease="*)
    if [ ! -e `+quote(failed)+` ]; then touch `+quote(failed)+`; `+quote(fixture.git)+` "$@"; echo 'failure after remote branch deletion' >&2; exit 1; fi ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
			},
		},
		{
			name: "worktree removal", completedAbsent: []string{"delete remote branch", "remove local worktree"}, remaining: "delete local branch",
			wrapGit: func(t *testing.T, fixture localArtifactResetFixture) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
case "$*" in
  *" worktree remove --force "*)
    if [ ! -e `+quote(failed)+` ]; then touch `+quote(failed)+`; `+quote(fixture.git)+` "$@"; echo 'failure after worktree removal' >&2; exit 1; fi ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
			},
		},
		{
			name: "local branch deletion", completedAbsent: []string{"delete remote branch", "remove local worktree", "delete local branch"}, remaining: "archive Pi session",
			wrapGit: func(t *testing.T, fixture localArtifactResetFixture) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
case "$*" in
  *" update-ref -d refs/heads/`+fixture.branch+` "*)
    if [ ! -e `+quote(failed)+` ]; then touch `+quote(failed)+`; `+quote(fixture.git)+` "$@"; echo 'failure after local branch deletion' >&2; exit 1; fi ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
			},
		},
		{
			name: "session archival", completedAbsent: []string{"delete remote branch", "remove local worktree", "delete local branch", "archive Pi session"}, remaining: "remove issue label in-progress",
			wrapGitHub: func(t *testing.T, _ localArtifactResetFixture, underlying string) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
if [ "$*" = "issue edit 42 --repo acme/widgets --remove-label in-progress" ] && [ ! -e `+quote(failed)+` ]; then
  touch `+quote(failed)+`
  echo 'failure after session archival' >&2
  exit 1
fi
exec `+quote(underlying)+` "$@"
`)
			},
		},
		{
			name: "in-progress label removal", completedAbsent: []string{"delete remote branch", "remove local worktree", "delete local branch", "archive Pi session", "remove issue label in-progress"}, remaining: "remove issue label ready-for-agent",
			wrapGitHub: func(t *testing.T, _ localArtifactResetFixture, underlying string) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
if [ "$*" = "issue edit 42 --repo acme/widgets --remove-label in-progress" ] && [ ! -e `+quote(failed)+` ]; then
  touch `+quote(failed)+`
  `+quote(underlying)+` "$@"
  echo 'failure after in-progress label removal' >&2
  exit 1
fi
exec `+quote(underlying)+` "$@"
`)
			},
		},
		{
			name: "ready label removal", completedAbsent: []string{"delete remote branch", "remove local worktree", "delete local branch", "archive Pi session", "remove issue label in-progress", "remove issue label ready-for-agent"}, remaining: "mark Run run-local resolved-externally",
			wrapGitHub: func(t *testing.T, _ localArtifactResetFixture, underlying string) string {
				failed := filepath.Join(t.TempDir(), "failed")
				return writeExecutable(t, `#!/bin/sh
if [ "$*" = "issue edit 42 --repo acme/widgets --remove-label ready-for-agent" ] && [ ! -e `+quote(failed)+` ]; then
  touch `+quote(failed)+`
  `+quote(underlying)+` "$@"
  echo 'failure after ready label removal' >&2
  exit 1
fi
exec `+quote(underlying)+` "$@"
`)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLocalArtifactResetFixture(t, false)
			runGit(t, fixture.repository, "push", "origin", fixture.branch)
			gh := localArtifactResolveGitHub(t, fixture)
			git := fixture.git
			if test.wrapGit != nil {
				git = test.wrapGit(t, fixture)
			}
			failingGitHub := gh
			if test.wrapGitHub != nil {
				failingGitHub = test.wrapGitHub(t, fixture, gh)
			}

			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), localArtifactResolveArgs(fixture, git, failingGitHub, "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "failure after") {
				t.Fatalf("first exit = %d, stderr = %q", exit, stderr.String())
			}
			partial, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if partial.Runs[0].Status != scheduler.StatusResolvingExternally || len(partial.Leases) != 1 || partial.Leases[0].RunID != "run-local" {
				t.Fatalf("partial External Resolution released ownership: %#v", partial)
			}

			stdout.Reset()
			stderr.Reset()
			if exit := Main(context.Background(), localArtifactResolveArgs(fixture, fixture.git, gh, "--dry-run"), &stdout, &stderr); exit != 0 {
				t.Fatalf("partial plan exit = %d, stderr = %q", exit, stderr.String())
			}
			plan := stdout.String()
			if strings.Contains(plan, "mark Run run-local resolving-externally") {
				t.Fatalf("partial plan repeated durable progress transition:\n%s", plan)
			}
			for _, completed := range test.completedAbsent {
				if strings.Contains(plan, completed) {
					t.Fatalf("partial plan repeated completed %s action:\n%s", completed, plan)
				}
			}
			if !strings.Contains(plan, test.remaining) {
				t.Fatalf("partial plan omitted remaining %s action:\n%s", test.remaining, plan)
			}

			stdout.Reset()
			stderr.Reset()
			if exit := Main(context.Background(), localArtifactResolveArgs(fixture, fixture.git, gh, "--yes"), &stdout, &stderr); exit != 0 {
				t.Fatalf("rerun exit = %d, stderr = %q", exit, stderr.String())
			}
			final, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if final.Runs[0].Status != scheduler.StatusResolvedExternally || len(final.Leases) != 0 {
				t.Fatalf("rerun final state = %#v", final)
			}
		})
	}
}

func TestResolveRejectsSuccessfulLocalArtifactCommandsWithoutPostconditions(t *testing.T) {
	for _, test := range []struct {
		name, commandPattern, wantError string
	}{
		{name: "worktree removal", commandPattern: `*" worktree remove --force "*`, wantError: "still present after removal"},
		{name: "local branch deletion", commandPattern: `*" update-ref -d refs/heads/BRANCH "*`, wantError: "still present after deletion"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLocalArtifactResetFixture(t, false)
			gh := localArtifactResolveGitHub(t, fixture)
			pattern := strings.ReplaceAll(test.commandPattern, "BRANCH", fixture.branch)
			git := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  `+pattern+`) exit 0 ;;
esac
exec `+quote(fixture.git)+` "$@"
`)

			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), localArtifactResolveArgs(fixture, git, gh, "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), test.wantError) {
				t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if current.Runs[0].Status != scheduler.StatusResolvingExternally || len(current.Leases) != 1 || current.Leases[0].RunID != "run-local" {
				t.Fatalf("missing local postcondition released ownership: %#v", current)
			}
		})
	}
}

func TestResolveFinalVerificationRequiresDurablyClosedWorkerLogMarker(t *testing.T) {
	resolvedAt := time.Date(2026, 7, 28, 2, 3, 4, 0, time.UTC)
	expected := scheduler.Run{
		Issue: 42, RunID: "run-42", Status: scheduler.StatusResolvedExternally, WorkerMode: scheduler.WorkerModePrint,
		ResolvedExternallyAt: &resolvedAt, ClosureReason: "completed", UpdatedAt: resolvedAt,
	}
	persisted := expected
	persisted.WorkerLogOpen = true

	err := retirement.VerifyFinalState(state.State{Runs: []scheduler.Run{persisted}}, expected, resolution.Policy(expected.RunID))
	if err == nil || !strings.Contains(err.Error(), "Worker-log-open marker remains open") {
		t.Fatalf("open Worker-log marker verification error = %v", err)
	}
}

func TestResolveRerunsOnlyFinalizationAfterStatePersistenceFailure(t *testing.T) {
	fixture := newResolveFixture(t, []string{"spec"}, "COMPLETED")
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	current.Runs = current.Runs[1:]
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	commonDirectory, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	store := &failingResetStore{
		resetStateStore: fixture.store,
		failStatus:      scheduler.StatusResolvedExternally,
		failure:         errors.New("injected External Resolution finalization failure"),
	}
	module, err := retirement.New(retirement.Config{
		Store: store, GitHub: ghadapter.Client{Executable: fixture.gh, Dir: fixture.repository},
		RepositoryRoot: fixture.repository, CommonDirectory: commonDirectory,
		StateDirectory: fixture.stateDir, GitExecutable: fixture.git,
	}, resolution.Policy("run-42"))
	if err != nil {
		t.Fatal(err)
	}
	approved, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Retire(context.Background(), approved); err == nil || !strings.Contains(err.Error(), "injected External Resolution finalization failure") {
		t.Fatalf("finalization failure = %v", err)
	}
	partial, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if partial.Runs[0].Status != scheduler.StatusResolvingExternally || len(partial.Leases) != 1 || partial.Leases[0].RunID != "run-42" {
		t.Fatalf("failed finalization released ownership: %#v", partial)
	}

	module, err = retirement.New(retirement.Config{
		Store: fixture.store, GitHub: ghadapter.Client{Executable: fixture.gh, Dir: fixture.repository},
		RepositoryRoot: fixture.repository, CommonDirectory: commonDirectory,
		StateDirectory: fixture.stateDir, GitExecutable: fixture.git,
	}, resolution.Policy("run-42"))
	if err != nil {
		t.Fatal(err)
	}
	rerun, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rerun.Actions) != 1 || !strings.Contains(rerun.Actions[0].String(), "resolved-externally") {
		t.Fatalf("rerun actions = %v, want only finalization", rerun.Actions)
	}
	if err := module.Retire(context.Background(), rerun); err != nil {
		t.Fatalf("rerun finalization: %v", err)
	}
	final, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if final.Runs[0].Status != scheduler.StatusResolvedExternally || len(final.Leases) != 0 {
		t.Fatalf("rerun final state = %#v", final)
	}
}

func TestCompiledResolveAcceptsAlreadyArchivedSession(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	if err := os.MkdirAll(filepath.Dir(fixture.archiveDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(fixture.sessionDir, fixture.archiveDir); err != nil {
		t.Fatal(err)
	}
	gh := localArtifactResolveGitHub(t, fixture)
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, localArtifactResolveArgs(fixture, fixture.git, gh, "--yes")...)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "External Resolution complete for Run run-local") || strings.Contains(string(output), "archive Pi session") {
		t.Fatalf("compiled already-archived resolution: %v\n%s", err, output)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvedExternally || len(current.Leases) != 0 {
		t.Fatalf("already-archived final state = %#v", current)
	}
	if _, err := os.Stat(filepath.Join(fixture.archiveDir, "session.jsonl")); err != nil {
		t.Fatalf("matching historical session archive was not preserved: %v", err)
	}
}

func TestResolveRetriesAlreadyArchivedSessionDurabilityBeforeFinalization(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	if err := os.MkdirAll(filepath.Dir(fixture.archiveDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(fixture.sessionDir, fixture.archiveDir); err != nil {
		t.Fatal(err)
	}
	gh := localArtifactResolveGitHub(t, fixture)
	commonDirectory, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	syncFailures, syncAttempts := 1, 0
	module, err := retirement.New(retirement.Config{
		Store: fixture.store, GitHub: ghadapter.Client{Executable: gh, Dir: fixture.repository},
		RepositoryRoot: fixture.repository, CommonDirectory: commonDirectory,
		StateDirectory: fixture.stateDir, GitExecutable: fixture.git,
		SyncPath: func(path string) error {
			syncAttempts++
			if syncFailures > 0 {
				syncFailures--
				return errors.New("injected historical session sync failure")
			}
			return syncFilesystemPath(path)
		},
	}, resolution.Policy("run-local"))
	if err != nil {
		t.Fatal(err)
	}
	approved, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Retire(context.Background(), approved); err == nil || !strings.Contains(err.Error(), "injected historical session sync failure") {
		t.Fatalf("already-archived durability error = %v", err)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvingExternally || len(current.Leases) != 1 || current.Leases[0].RunID != "run-local" {
		t.Fatalf("already-archived sync failure released ownership: %#v", current)
	}
	rerun, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rerun.Actions) != 1 || !strings.Contains(rerun.Actions[0].String(), "resolved-externally") {
		t.Fatalf("already-archived sync rerun actions = %#v", rerun.Actions)
	}
	if err := module.Retire(context.Background(), rerun); err != nil {
		t.Fatalf("retry already-archived durability synchronization: %v", err)
	}
	current, err = fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvedExternally || len(current.Leases) != 0 {
		t.Fatalf("already-archived sync retry final state = %#v", current)
	}
	if syncAttempts <= 1 {
		t.Fatalf("already-archived durability synchronization attempts = %d, want a retry", syncAttempts)
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

func TestCompiledResolveRetiresEveryOwnedUnmergedPullRequest(t *testing.T) {
	fixture := newTwoPullRequestResetFixture(t)
	fixture.updateGitHubState(t, `.failClose=0`)
	gh := githubArtifactResolveGitHub(t, fixture)
	binary := buildExecutable(t, t.TempDir())

	command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, gh, "--yes")...)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "External Resolution complete for Run run-github") {
		t.Fatalf("compiled multiple pull request retirement: %v\n%s", err, output)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvedExternally || len(current.Leases) != 0 {
		t.Fatalf("multiple pull request retirement state = %#v", current)
	}
	var github struct {
		Pulls []struct {
			Number   int      `json:"number"`
			State    string   `json:"state"`
			Comments []string `json:"comments"`
		} `json:"pulls"`
	}
	data, err := os.ReadFile(fixture.githubState)
	if err != nil || json.Unmarshal(data, &github) != nil {
		t.Fatalf("read multiple pull request state: %v", err)
	}
	if len(github.Pulls) != 2 {
		t.Fatalf("retired pull requests = %#v", github.Pulls)
	}
	for _, pull := range github.Pulls {
		if pull.State != "CLOSED" || len(pull.Comments) != 1 || !strings.Contains(pull.Comments[0], resolution.CommentMarker("run-github")) {
			t.Fatalf("pull request #%d was not completely retired: %#v", pull.Number, pull)
		}
	}
	calls, err := os.ReadFile(fixture.githubCalls)
	if err != nil {
		t.Fatal(err)
	}
	if string(calls) != "disable 99\ncomment 99\nclose 99\ncomment 100\nclose 100\n" {
		t.Fatalf("multiple pull request mutation order = %q", calls)
	}
}

func TestCompiledResolveExplainsClosedUnmergedPullRequestWithoutReclosing(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
	fixture.updateGitHubState(t, `.pr="CLOSED" | .auto=false | .comments=[]`)
	gh := githubArtifactResolveGitHub(t, fixture)
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, gh, "--yes")...)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "External Resolution complete for Run run-github") {
		t.Fatalf("compiled closed pull request retirement: %v\n%s", err, output)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvedExternally || len(current.Leases) != 0 {
		t.Fatalf("closed pull request retirement state = %#v", current)
	}
	github := fixture.githubStateValue(t)
	if github.PR != "CLOSED" || github.Merged || github.Auto || len(github.Comments) != 1 || !strings.Contains(github.Comments[0], resolution.CommentMarker("run-github")) {
		t.Fatalf("explained closed pull request = %#v", github)
	}
	calls, err := os.ReadFile(fixture.githubCalls)
	if err != nil {
		t.Fatal(err)
	}
	if string(calls) != "comment\n" {
		t.Fatalf("closed pull request mutations = %q, want one explanation and no close", calls)
	}
}

func TestResolveRerunsOnlyRemainingPullRequestActionsAfterEveryMutationBoundary(t *testing.T) {
	for _, test := range []struct {
		name, pattern, completedAbsent, remaining string
	}{
		{name: "disable auto-merge", pattern: `"pr merge 99 --repo acme/widgets --disable-auto"`, completedAbsent: "disable auto-merge", remaining: "explain External Resolution"},
		{name: "pull request explanation", pattern: `pr\ comment\ 99\ --repo\ acme/widgets\ --body\ *`, completedAbsent: "explain External Resolution", remaining: "close unmerged pull request"},
		{name: "pull request closure", pattern: `"pr close 99 --repo acme/widgets"`, completedAbsent: "close unmerged pull request", remaining: "delete remote branch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
			gh := githubArtifactResolveGitHub(t, fixture)
			failed := filepath.Join(t.TempDir(), "failed")
			failingGitHub := writeExecutable(t, `#!/bin/sh
case "$*" in
  `+test.pattern+`)
    if [ ! -e `+quote(failed)+` ]; then
      touch `+quote(failed)+`
      `+quote(gh)+` "$@"
      echo 'failure after `+test.name+`' >&2
      exit 1
    fi ;;
esac
exec `+quote(gh)+` "$@"
`)

			var stdout, stderr bytes.Buffer
			if exit := Main(context.Background(), githubArtifactResolveArgs(fixture, fixture.git, failingGitHub, "--yes"), &stdout, &stderr); exit == 0 || !strings.Contains(stderr.String(), "failure after") {
				t.Fatalf("first exit = %d, stderr = %q", exit, stderr.String())
			}
			partial, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if partial.Runs[0].Status != scheduler.StatusResolvingExternally || len(partial.Leases) != 1 || partial.Leases[0].RunID != "run-github" {
				t.Fatalf("partial External Resolution released ownership: %#v", partial)
			}

			stdout.Reset()
			stderr.Reset()
			if exit := Main(context.Background(), githubArtifactResolveArgs(fixture, fixture.git, gh, "--dry-run"), &stdout, &stderr); exit != 0 {
				t.Fatalf("partial plan exit = %d, stderr = %q", exit, stderr.String())
			}
			plan := stdout.String()
			if strings.Contains(plan, "mark Run run-github resolving-externally") || strings.Contains(plan, test.completedAbsent) {
				t.Fatalf("partial plan repeated completed action:\n%s", plan)
			}
			if !strings.Contains(plan, test.remaining) {
				t.Fatalf("partial plan omitted remaining %s action:\n%s", test.remaining, plan)
			}

			stdout.Reset()
			stderr.Reset()
			if exit := Main(context.Background(), githubArtifactResolveArgs(fixture, fixture.git, gh, "--yes"), &stdout, &stderr); exit != 0 {
				t.Fatalf("rerun exit = %d, stderr = %q", exit, stderr.String())
			}
			final, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if final.Runs[0].Status != scheduler.StatusResolvedExternally || len(final.Leases) != 0 {
				t.Fatalf("rerun final state = %#v", final)
			}
			calls, err := os.ReadFile(fixture.githubCalls)
			if err != nil {
				t.Fatal(err)
			}
			if string(calls) != "disable\ncomment\nclose\n" {
				t.Fatalf("rerun repeated a completed pull request mutation: %q", calls)
			}
		})
	}
}

func TestCompiledResolveRecordsCompletionWhenExpectedPullRequestMergesDuringRevalidation(t *testing.T) {
	binary := buildExecutable(t, t.TempDir())
	for _, test := range []struct {
		name    string
		mergeAt int
	}{
		{name: "initial full plan", mergeAt: 3},
		{name: "pre-action plan", mergeAt: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, false, false)
			gh := githubArtifactResolveGitHub(t, fixture)
			counter := filepath.Join(t.TempDir(), "pull-request-inspections")
			racingGitHub := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    count=0
    if [ -f `+quote(counter)+` ]; then count=$(cat `+quote(counter)+`); fi
    count=$((count + 1))
    printf '%s\n' "$count" > `+quote(counter)+`
    if [ "$count" -eq `+strconv.Itoa(test.mergeAt)+` ]; then
      temporary=`+quote(fixture.githubState)+`.tmp
      jq '.pr="MERGED" | .merged=true | .auto=false' `+quote(fixture.githubState)+` > "$temporary"
      mv "$temporary" `+quote(fixture.githubState)+`
    fi ;;
esac
exec `+quote(gh)+` "$@"
`)

			command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, racingGitHub, "--yes")...)
			output, err := command.CombinedOutput()
			if err != nil || !strings.Contains(string(output), "Completion recorded for Run run-github") {
				t.Fatalf("compiled %s merge revalidation: %v\n%s", test.name, err, output)
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if current.Runs[0].Status != scheduler.StatusMerged || current.Runs[0].CompletedAt == nil || len(current.Leases) != 0 {
				t.Fatalf("%s merge revalidation state = %#v", test.name, current)
			}
			if calls, err := os.ReadFile(fixture.githubCalls); err == nil && len(calls) != 0 || err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s merge revalidation performed a retirement mutation: %q, %v", test.name, calls, err)
			}
			github := fixture.githubStateValue(t)
			if !github.Merged || github.PR != "MERGED" {
				t.Fatalf("%s merge revalidation pull request = %#v", test.name, github)
			}
		})
	}
}

func TestCompiledResolveRefusesCompletionAfterExpectedPullRequestCommitChanges(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, false, false)
	gh := githubArtifactResolveGitHub(t, fixture)
	counter := filepath.Join(t.TempDir(), "pull-request-inspections")
	replacement := strings.Repeat("b", 40)
	racingGitHub := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    count=0
    if [ -f `+quote(counter)+` ]; then count=$(cat `+quote(counter)+`); fi
    count=$((count + 1))
    printf '%s\n' "$count" > `+quote(counter)+`
    if [ "$count" -eq 3 ]; then
      temporary=`+quote(fixture.githubState)+`.tmp
      jq --arg replacement `+quote(replacement)+` '.pr="MERGED" | .merged=true | .auto=false | .head=$replacement' `+quote(fixture.githubState)+` > "$temporary"
      mv "$temporary" `+quote(fixture.githubState)+`
    fi ;;
esac
exec `+quote(gh)+` "$@"
`)

	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, racingGitHub, "--yes")...)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "expected commit identity changed") {
		t.Fatalf("force-pushed merge error = %v\n%s", err, output)
	}
	current, loadErr := fixture.store.Load()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if current.Runs[0].Status != scheduler.StatusResolvingExternally || len(current.Leases) != 1 {
		t.Fatalf("force-pushed merge released unapproved ownership: %#v", current)
	}
	if strings.Contains(string(output), "Completion recorded") {
		t.Fatalf("force-pushed merge reported Completion: %s", output)
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

func TestCompiledResolveRecordsCompletionWhenExpectedPullRequestMergesDuringFailedGitHubMutation(t *testing.T) {
	binary := buildExecutable(t, t.TempDir())
	tests := []struct {
		name, pattern, call, initialState string
	}{
		{name: "disable auto-merge", pattern: `"pr merge 99 --repo acme/widgets --disable-auto"`, call: "disable"},
		{name: "pull request explanation", pattern: `pr\ comment\ 99\ --repo\ acme/widgets\ --body\ *`, call: "comment", initialState: `.auto=false`},
		{name: "pull request closure", pattern: `"pr close 99 --repo acme/widgets"`, call: "close", initialState: fmt.Sprintf(`.auto=false | .comments=[%q]`, resolution.Explanation(scheduler.Run{Issue: 42, RunID: "run-github"}))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, false, false)
			if test.initialState != "" {
				fixture.updateGitHubState(t, test.initialState)
			}
			gh := githubArtifactResolveGitHub(t, fixture)
			failingGitHub := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  `+test.pattern+`)
    printf '%s\n' `+quote(test.call)+` >> `+quote(fixture.githubCalls)+`
    temporary=`+quote(fixture.githubState)+`.tmp
    jq '.pr="MERGED" | .merged=true | .auto=false' `+quote(fixture.githubState)+` > "$temporary"
    mv "$temporary" `+quote(fixture.githubState)+`
    echo 'mutation failed after pull request merged' >&2
    exit 1 ;;
esac
exec `+quote(gh)+` "$@"
`)

			command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, failingGitHub, "--yes")...)
			output, err := command.CombinedOutput()
			if err != nil || !strings.Contains(string(output), "Completion recorded for Run run-github") {
				t.Fatalf("compiled failed-mutation merge race: %v\n%s", err, output)
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if current.Runs[0].Status != scheduler.StatusMerged || current.Runs[0].CompletedAt == nil || len(current.Leases) != 0 {
				t.Fatalf("failed-mutation Completion state = %#v", current)
			}
			calls, err := os.ReadFile(fixture.githubCalls)
			if err != nil {
				t.Fatal(err)
			}
			if string(calls) != test.call+"\n" {
				t.Fatalf("merge race performed mutations after failed %s: %q", test.call, calls)
			}
			github := fixture.githubStateValue(t)
			if !github.Merged || github.PR != "MERGED" {
				t.Fatalf("failed-mutation merge race pull request = %#v", github)
			}
		})
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
			if run.Status != scheduler.StatusMerged || run.PullRequest != pullRequest || run.CompletedAt == nil || run.WorkerLogOpen || run.ResolvedExternallyAt != nil || run.ClosureReason != "" || run.Error != "retained diagnostic" || len(persisted.Leases) != 0 {
				t.Fatalf("Completion fallback state = %#v", persisted)
			}
			if !run.UpdatedAt.Equal(*run.CompletedAt) {
				t.Fatalf("Completion timestamps = updated %s, completed %s", run.UpdatedAt, run.CompletedAt)
			}
		})
	}
}

func TestCompiledResolveRefusesFinalizationWhenIssueReopensAfterFirstLabelMutation(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "ready-for-agent", "spec"}, "COMPLETED")
	data, err := os.ReadFile(fixture.githubState)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"reopenAfterFirstMutation":false`), []byte(`"reopenAfterFirstMutation":true`), 1)
	if err := os.WriteFile(fixture.githubState, data, 0o600); err != nil {
		t.Fatal(err)
	}

	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, append([]string{"resolve"}, fixture.args("run-42", "--yes")...)...)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(strings.ToLower(string(output)), "open") {
		t.Fatalf("compiled reopen race error = %v, output=%q", err, output)
	}
	if strings.Contains(string(output), "External Resolution complete") || strings.Contains(string(output), "Completion recorded") {
		t.Fatalf("reopen race reported success: %q", output)
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
