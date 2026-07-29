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
	"reflect"
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

func TestResolveHelpDescribesCompletionSafetyAndCompleteArtifactRetirement(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exit := Main(context.Background(), []string{"resolve", "--help"}, &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	for _, want := range []string{
		"Completion is preferred",
		"Verified Completion",
		"retires owned branches, worktrees, active Pi sessions, and managed labels",
		"`in-progress` and `ready-for-agent` before recording the merged outcome",
		"merged outcome and releasing the Lease",
		"no recorded pull request",
		"exactly one merged pull request discovered from its expected branch",
		"Recovered Runs require the stricter Recovered Completion path",
		"commits must match the merged pull request head",
		"Failed validation retains the Lease",
		"Multiple unrecorded merged pull requests",
		"are ambiguous and are refused with the Lease retained",
		"Safely retire owned unmerged pull requests",
		"remote",
		"local branches",
		"worktrees",
		"active Pi sessions",
		"preserve diagnostics",
		"release the Lease",
	} {
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
			encoded = bytes.Replace(encoded, []byte(`"version": 5`), []byte(`"version": 3`), 1)
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
		_, err := confirmResolve(ctx, bufio.NewReader(reader), stdout, "External Resolution")
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
	for _, mode := range []struct {
		name string
		args []string
	}{
		{name: "confirmed", args: fixture.args("42", "--yes")},
		{name: "dry-run", args: fixture.args("42", "--dry-run")},
		{name: "interactive", args: fixture.args("42")},
	} {
		t.Run(mode.name, func(t *testing.T) {
			command := exec.Command(binary, append([]string{"resolve"}, mode.args...)...)
			output, commandErr := command.CombinedOutput()
			if commandErr == nil || !strings.Contains(string(output), "Runner owns repository coordination") || !strings.Contains(string(output), "supervising Runner handles automatic reconciliation at startup, during watch polling, and after normal Worker settlement") {
				t.Fatalf("compiled lock error = %v\n%s", commandErr, output)
			}
		})
	}
	if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
		t.Fatal("compiled lock refusal changed state")
	}
}

type automaticArtifactResolveFixture struct {
	githubArtifactResetFixture
	worktree, sessionDir, archiveDir string
}

func newAutomaticArtifactResolveFixture(t *testing.T, issueState string) automaticArtifactResolveFixture {
	t.Helper()
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
	worktree := filepath.Join(fixture.stateDir, "worktrees", "issue-42-run-github")
	if err := os.MkdirAll(filepath.Dir(worktree), 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, fixture.repository, "worktree", "add", "-b", fixture.branch, worktree, "origin/"+fixture.branch)
	sessionDir := filepath.Join(fixture.stateDir, "sessions", "run-github")
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "session.jsonl"), []byte(fmt.Sprintf("{\"type\":\"session\",\"id\":%q,\"cwd\":%q}\n", "backlog-run-github", worktree)), 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	current.Runs[0].WorkerMode = scheduler.WorkerModeRPC
	current.Runs[0].Worktree = worktree
	current.Runs[0].SessionID = "backlog-run-github"
	current.Runs[0].SessionDir = sessionDir
	current.Runs[0].Error = "retained automatic-resolution diagnostic"
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	fixture.updateGitHubState(t, fmt.Sprintf(`.issue=%q | .labels=["in-progress","ready-for-agent","spec"]`, issueState))
	return automaticArtifactResolveFixture{
		githubArtifactResetFixture: fixture,
		worktree:                   worktree, sessionDir: sessionDir,
		archiveDir: filepath.Join(fixture.stateDir, "history", "sessions", "run-github"),
	}
}

func automaticArtifactResolveGitHub(t *testing.T, fixture automaticArtifactResolveFixture, candidateChecked string) string {
	t.Helper()
	return writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(fixture.githubState)+`
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    issue_state=$(jq -r .issue "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"%s","labels":%s}\n' "$issue_state" "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    jq -c '{number:42,url:"https://github.com/acme/widgets/issues/42",state:.issue,stateReason:(if .issue == "CLOSED" then "COMPLETED" else null end)}' "$state" ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    touch `+quote(candidateChecked)+`; printf '%s\n' '[]' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "in-progress"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "ready-for-agent"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) exec `+quote(fixture.gh)+` "$@" ;;
esac
`)
}

func TestCompiledRunStartupResumesPartialExternalResolutionBeforeCandidateAdmission(t *testing.T) {
	fixture := newAutomaticArtifactResolveFixture(t, "CLOSED")
	partial, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	partial.Runs[0].Status = scheduler.StatusResolvingExternally
	if err := fixture.store.Save(partial); err != nil {
		t.Fatal(err)
	}
	fixture.updateGitHubState(t, fmt.Sprintf(`.auto=false | .comments=[%q]`, resolution.Explanation(partial.Runs[0])))

	candidateChecked := filepath.Join(t.TempDir(), "candidate-checked")
	gh := automaticArtifactResolveGitHub(t, fixture, candidateChecked)
	workerStarted := filepath.Join(t.TempDir(), "worker-started")
	pi := writeExecutable(t, "#!/bin/sh\ntouch "+quote(workerStarted)+"\nexit 9\n")
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, "run", "--plain", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir,
		"--max-workers", "1", "--git", fixture.git, "--gh", gh, "--pi", pi)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compiled startup partial External Resolution: %v\n%s", err, output)
	}
	if _, err := os.Stat(candidateChecked); err != nil {
		t.Fatalf("Candidate Admission did not run after External Resolution: %v", err)
	}
	if _, err := os.Stat(workerStarted); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("External Resolution created a replacement Worker: %v", err)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Leases) != 0 || current.Runs[0].Status != scheduler.StatusResolvedExternally || current.Runs[0].Error != "retained automatic-resolution diagnostic" {
		t.Fatalf("startup partial External Resolution state = %#v", current)
	}
	if calls, err := os.ReadFile(fixture.githubCalls); err != nil || string(calls) != "close\n" {
		t.Fatalf("partial startup repeated completed pull request actions: calls=%q err=%v", calls, err)
	}
	if _, err := os.Stat(fixture.worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup partial External Resolution retained worktree: %v", err)
	}
	if _, err := os.Stat(fixture.sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup partial External Resolution retained active session: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.archiveDir, "session.jsonl")); err != nil {
		t.Fatalf("startup partial External Resolution did not archive session: %v", err)
	}
	if output, err := exec.Command("git", "-C", fixture.repository, "show-ref", "--verify", "--quiet", "refs/heads/"+fixture.branch).CombinedOutput(); err == nil {
		t.Fatalf("startup partial External Resolution retained local branch: %s", output)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("startup partial External Resolution remote branch = %#v, %v", branch, err)
	}
	labelsData, err := os.ReadFile(fixture.githubState)
	if err != nil {
		t.Fatal(err)
	}
	var githubState struct {
		PullRequest string   `json:"pr"`
		Labels      []string `json:"labels"`
	}
	if err := json.Unmarshal(labelsData, &githubState); err != nil {
		t.Fatal(err)
	}
	if githubState.PullRequest != "CLOSED" || strings.Join(githubState.Labels, ",") != "spec" {
		t.Fatalf("startup partial External Resolution GitHub state = %#v", githubState)
	}
}

func TestCompiledRunWatchUsesCompleteExternalResolutionDuringPolling(t *testing.T) {
	fixture := newAutomaticArtifactResolveFixture(t, "OPEN")
	beforeState, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	beforeGitHub := fileDigest(t, fixture.githubState)
	beforeLocalBranch := strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", "refs/heads/"+fixture.branch))
	beforeRemoteBranch := strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", "refs/remotes/origin/"+fixture.branch))
	beforeWorktree := fileDigest(t, filepath.Join(fixture.worktree, "owned"))
	beforeSession := fileDigest(t, filepath.Join(fixture.sessionDir, "session.jsonl"))

	polled := filepath.Join(t.TempDir(), "candidate-polled")
	gh := automaticArtifactResolveGitHub(t, fixture, polled)
	workerStarted := filepath.Join(t.TempDir(), "worker-started")
	pi := writeExecutable(t, "#!/bin/sh\ntouch "+quote(workerStarted)+"\nexit 9\n")
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, "run", "--watch", "--plain", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir,
		"--max-workers", "1", "--poll", "10ms", "--git", fixture.git, "--gh", gh, "--pi", pi)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, polled)

	openState, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(openState, beforeState) {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("open issue polling changed Run, Lease, or diagnostics: got %#v, want %#v", openState, beforeState)
	}
	if fileDigest(t, fixture.githubState) != beforeGitHub ||
		strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", "refs/heads/"+fixture.branch)) != beforeLocalBranch ||
		strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", "refs/remotes/origin/"+fixture.branch)) != beforeRemoteBranch ||
		fileDigest(t, filepath.Join(fixture.worktree, "owned")) != beforeWorktree ||
		fileDigest(t, filepath.Join(fixture.sessionDir, "session.jsonl")) != beforeSession {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal("open issue polling changed labels or owned artifacts")
	}
	if _, err := os.Stat(fixture.archiveDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open issue polling archived the active session: %v", err)
	}
	if calls, err := os.ReadFile(fixture.githubCalls); err == nil && len(calls) != 0 {
		t.Fatalf("open issue polling mutated the pull request: %q", calls)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := os.Stat(workerStarted); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open issue polling launched a Worker: %v", err)
	}

	fixture.updateGitHubState(t, `.issue="CLOSED"`)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		current, loadErr := fixture.store.Load()
		if loadErr == nil && len(current.Leases) == 0 && current.Runs[0].Status == scheduler.StatusResolvedExternally {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	current, err := fixture.store.Load()
	if err != nil || len(current.Leases) != 0 || current.Runs[0].Status != scheduler.StatusResolvedExternally {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("compiled watch did not complete External Resolution: state=%#v err=%v output=%s", current, err, output.String())
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("compiled watch Drain after External Resolution: %v\n%s", err, output.String())
	}
	labelsData, err := os.ReadFile(fixture.githubState)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(labelsData), "in-progress") || strings.Contains(string(labelsData), "ready-for-agent") || !strings.Contains(string(labelsData), "spec") {
		t.Fatalf("compiled watch managed labels = %s", labelsData)
	}
	if _, err := os.Stat(workerStarted); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("watch External Resolution launched a Worker: %v", err)
	}
}

func TestCompiledRunResolvesIssueClosedWhileOwnedWorkerIsActiveAfterSettlement(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	runGit(t, root, "init", "-b", "main", repository)
	runGit(t, repository, "config", "user.email", "test@example.com")
	runGit(t, repository, "config", "user.name", "Test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "README.md")
	runGit(t, repository, "commit", "-m", "fixture")
	runGit(t, repository, "remote", "add", "origin", "git@github.com:acme/widgets.git")
	base := strings.TrimSpace(gitOutput(t, repository, "rev-parse", "HEAD"))
	runGit(t, repository, "update-ref", "refs/remotes/origin/main", base)

	stateDir := filepath.Join(root, "state")
	statePath := filepath.Join(stateDir, "state.json")
	workerStarted := filepath.Join(root, "worker-started")
	issueClosed := filepath.Join(root, "issue-closed")
	closedIssuePolled := filepath.Join(root, "closed-issue-polled")
	settleWorker := filepath.Join(root, "settle-worker")
	workerExited := filepath.Join(root, "worker-exited")
	removedReady := filepath.Join(root, "removed-ready")
	removedProgress := filepath.Join(root, "removed-progress")

	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue list --repo acme/widgets --state open --label ready-for-agent --limit 1000 --json number,title,createdAt,url")
    if test -f `+quote(issueClosed)+`; then touch `+quote(closedIssuePolled)+`; printf '%s\n' '[]'; else printf '%s\n' '[{"number":84,"title":"Settlement closure","createdAt":"2026-07-28T00:00:00Z","url":"https://github.com/acme/widgets/issues/84"}]'; fi ;;
  "issue view 84 --repo acme/widgets --json number,title,body,state,url,createdAt")
    printf '%s\n' '{"number":84,"title":"Settlement closure","body":"","state":"OPEN","url":"https://github.com/acme/widgets/issues/84","createdAt":"2026-07-28T00:00:00Z"}' ;;
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/84/comments?per_page=100 --paginate --slurp"|\
  "api -H Accept: application/vnd.github+json -H X-GitHub-Api-Version: 2026-03-10 repos/acme/widgets/issues/84/dependencies/blocked_by?per_page=100 --paginate --slurp")
    printf '%s\n' '[[]]' ;;
  "pr list --repo acme/widgets --state all --head agent/issue-84-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository"|\
  "pr list --repo acme/widgets --state all --head agent/issue-84-"*" --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[]' ;;
  "issue view 84 --repo acme/widgets --json number,state,title,url")
    if test -f `+quote(issueClosed)+`; then state=CLOSED; else state=OPEN; fi
    printf '{"number":84,"state":"%s","title":"Settlement closure","url":"https://github.com/acme/widgets/issues/84"}\n' "$state" ;;
  "issue view 84 --repo acme/widgets --json number,url,state,stateReason")
    if test -f `+quote(issueClosed)+`; then printf '%s\n' '{"number":84,"url":"https://github.com/acme/widgets/issues/84","state":"CLOSED","stateReason":"NOT_PLANNED"}'; else printf '%s\n' '{"number":84,"url":"https://github.com/acme/widgets/issues/84","state":"OPEN","stateReason":null}'; fi ;;
  "issue view 84 --repo acme/widgets --json number,url,state,labels")
    labels=""
    if ! test -f `+quote(removedProgress)+`; then labels='{"name":"in-progress"}'; fi
    if ! test -f `+quote(removedReady)+`; then if test -n "$labels"; then labels="$labels,"; fi; labels="${labels}{\"name\":\"ready-for-agent\"}"; fi
    printf '{"number":84,"url":"https://github.com/acme/widgets/issues/84","state":"CLOSED","labels":[%s]}\n' "$labels" ;;
  "issue edit 84 --repo acme/widgets --remove-label in-progress") touch `+quote(removedProgress)+` ;;
  "issue edit 84 --repo acme/widgets --remove-label ready-for-agent") touch `+quote(removedReady)+` ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	git := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" fetch origin main") exit 0 ;;
  *" ls-remote --exit-code --heads origin refs/heads/agent/issue-84-"*) exit 2 ;;
  *) exec git "$@" ;;
esac
`)
	pi := writeExecutable(t, `#!/bin/sh
set -eu
session_dir=""
session_id=""
while test "$#" -gt 0; do
  if test "$1" = "--session-dir"; then session_dir=$2; shift 2; continue; fi
  if test "$1" = "--session-id"; then session_id=$2; shift 2; continue; fi
  shift
done
printf '{"type":"session","id":"%s","cwd":"%s"}\n' "$session_id" "$PWD" >"$session_dir/session.jsonl"
IFS= read -r prompt
printf '%s\n' '{"id":"backlog-afk-prompt","type":"response","command":"prompt","success":true}' '{"type":"agent_start"}' '{"type":"turn_start"}'
touch `+quote(workerStarted)+`
while ! test -f `+quote(settleWorker)+`; do sleep 0.01; done
printf '%s\n' '{"type":"turn_end"}' '{"type":"agent_end"}' '{"type":"agent_settled"}'
while IFS= read -r ignored; do :; done
touch `+quote(workerExited)+`
`)

	binary := buildExecutable(t, root)
	command := exec.Command(binary, "run", "--plain", "--repo-dir", repository, "--state-dir", stateDir,
		"--max-workers", "1", "--poll", "5ms", "--git", git, "--gh", gh, "--pi", pi)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, workerStarted)
	if err := os.WriteFile(issueClosed, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, closedIssuePolled)
	active, err := (state.FileStore{Path: statePath}).Load()
	if err != nil || len(active.Runs) != 1 || active.Runs[0].Status != scheduler.StatusRunning || len(active.Leases) != 1 {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("issue closure controlled Owned Worker: state=%#v err=%v output=%s", active, err, output.String())
	}
	if _, err := os.Stat(workerExited); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Worker exited before normal settlement: %v", err)
	}
	if err := os.WriteFile(settleWorker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("compiled post-settlement External Resolution: %v\n%s", err, output.String())
	}
	waitForFile(t, workerExited)
	current, err := (state.FileStore{Path: statePath}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Runs) != 1 || current.Runs[0].Status != scheduler.StatusResolvedExternally || current.Runs[0].ClosureReason != "not-planned" || current.Runs[0].WorkerLogOpen || len(current.Leases) != 0 {
		t.Fatalf("compiled post-settlement state = %#v\n%s", current, output.String())
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

func TestCompiledResolveRetiresOwnedArtifactsBeforeRecordingCompletion(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	runGit(t, fixture.repository, "push", "origin", fixture.branch)
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	pullRequest := "https://github.com/acme/widgets/pull/99"
	current.Runs[0].PullRequest = pullRequest
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.githubState, []byte(`{"labels":["in-progress","ready-for-agent","spec"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	commit := strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", fixture.branch))
	git := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" push origin --force-with-lease=refs/heads/`+fixture.branch+`:"*)
    jq -e '.runs[] | select(.runId == "run-local" and .status == "resolving-externally")' `+quote(fixture.store.Path)+` >/dev/null ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
	gh := writeExecutable(t, `#!/bin/sh
set -eu
state=`+quote(fixture.githubState)+`
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef") printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' "$state")
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"COMPLETED"}' ;;
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":99,"url":"`+pullRequest+`","state":"MERGED","mergedAt":"2026-07-29T14:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+fixture.branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "in-progress"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    temporary="$state.tmp"; jq '.labels |= map(select(. != "ready-for-agent"))' "$state" > "$temporary"; mv "$temporary" "$state" ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	binary := buildExecutable(t, t.TempDir())
	args := []string{"resolve", "run-local", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", git, "--gh", gh}

	dryRun := exec.Command(binary, append(args, "--dry-run")...)
	output, err := dryRun.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled Completion dry-run: %v\n%s", err, output)
	}
	plan := string(output)
	if !strings.Contains(plan, "Completion Plan for issue #42") || strings.Contains(plan, "External Resolution Plan for issue #42") {
		t.Fatalf("merged outcome used misleading plan terminology:\n%s", plan)
	}
	ordered := []string{
		"mark Run run-local resolving-externally while retaining Lease lease-local",
		"delete remote branch " + fixture.branch,
		"remove local worktree " + fixture.worktree,
		"delete local branch " + fixture.branch,
		"archive Pi session backlog-run-local",
		"remove issue label in-progress",
		"remove issue label ready-for-agent",
		"record Completion from merged expected pull request #99",
	}
	position := -1
	for _, action := range ordered {
		next := strings.Index(plan, action)
		if next <= position {
			t.Fatalf("Completion Plan action %q missing or out of order:\n%s", action, plan)
		}
		position = next
	}

	var stdout, stderr bytes.Buffer
	err = resolveCommandWithInput(context.Background(), args[1:], strings.NewReader("yes\n"), true, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Proceed with Completion? [y/N]") ||
		strings.Contains(stdout.String(), "Proceed with External Resolution? [y/N]") ||
		!strings.Contains(stdout.String(), "Completion recorded for Run run-local") {
		t.Fatalf("interactive Completion retirement: %v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
	}
	persisted, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	run := persisted.Runs[0]
	if run.Status != scheduler.StatusMerged || run.CompletedAt == nil || run.CleanupPending || len(persisted.Leases) != 0 {
		t.Fatalf("Completion state = %#v", persisted)
	}
	if _, err := os.Stat(fixture.worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree survived Completion: %v", err)
	}
	if output, err := exec.Command("git", "-C", fixture.repository, "show-ref", "--verify", "--quiet", "refs/heads/"+fixture.branch).CombinedOutput(); err == nil {
		t.Fatalf("local branch survived Completion: %s", output)
	}
	if branch, err := inspectRemoteBranch(context.Background(), git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("remote branch after Completion = %#v, %v", branch, err)
	}
	if _, err := os.Stat(fixture.sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active session survived Completion: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.archiveDir, "session.jsonl")); err != nil {
		t.Fatalf("Completion session archive missing: %v", err)
	}
	var github struct {
		Labels []string `json:"labels"`
	}
	data, _ := os.ReadFile(fixture.githubState)
	_ = json.Unmarshal(data, &github)
	if strings.Join(github.Labels, ",") != "spec" {
		t.Fatalf("preserved GitHub labels = %v", github.Labels)
	}
}

func TestCompiledResolveFinishesHistoricalCompletionCleanupAndRerunsWithoutMutation(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	runGit(t, fixture.repository, "push", "origin", fixture.branch)
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	completedAt := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)
	updatedAt := completedAt.Add(time.Minute)
	pullRequest := "https://github.com/acme/widgets/pull/99"
	run := &current.Runs[0]
	run.Status = scheduler.StatusMerged
	run.PullRequest = pullRequest
	run.CompletedAt = &completedAt
	run.UpdatedAt = updatedAt
	run.CleanupPending = true
	run.Error = "preserved Historical Completion diagnostic"
	current.Leases = nil
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	commit := strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", fixture.branch))
	baseGH := localArtifactResolveGitHub(t, fixture)
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":99,"url":"`+pullRequest+`","state":"MERGED","mergedAt":"2026-07-29T14:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+fixture.branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) exec `+quote(baseGH)+` "$@" ;;
esac
`)
	binary := buildExecutable(t, t.TempDir())
	args := func(selector string, extra ...string) []string {
		values := []string{"resolve", selector, "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git, "--gh", gh}
		return append(values, extra...)
	}

	tracked := filepath.Join(fixture.worktree, "tracked")
	trackedInfo, err := os.Stat(tracked)
	if err != nil {
		t.Fatal(err)
	}
	staleTime := trackedInfo.ModTime().Add(2 * time.Second)
	if err := os.Chtimes(tracked, staleTime, staleTime); err != nil {
		t.Fatal(err)
	}
	worktreeIndex := strings.TrimSpace(gitOutput(t, fixture.worktree, "rev-parse", "--path-format=absolute", "--git-path", "index"))
	beforeDryRunState := fileDigest(t, fixture.store.Path)
	beforeDryRunIndex := fileDigest(t, worktreeIndex)
	output, err := exec.Command(binary, args("run-local", "--dry-run")...).CombinedOutput()
	if err != nil {
		t.Fatalf("compiled Historical Completion dry-run: %v\n%s", err, output)
	}
	plan := string(output)
	for _, want := range []string{"Completion Cleanup Plan for issue #42", "Lease: absent", "delete remote branch " + fixture.branch, "remove local worktree " + fixture.worktree, "delete local branch " + fixture.branch, "archive Pi session backlog-run-local", "remove issue label in-progress", "remove issue label ready-for-agent", "clear pending Completion cleanup"} {
		if !strings.Contains(plan, want) {
			t.Fatalf("Historical Completion dry-run omitted %q:\n%s", want, plan)
		}
	}
	if fileDigest(t, fixture.store.Path) != beforeDryRunState {
		t.Fatal("Historical Completion dry-run changed state")
	}
	if fileDigest(t, worktreeIndex) != beforeDryRunIndex {
		t.Fatal("Historical Completion dry-run refreshed the worktree index")
	}

	output, err = exec.Command(binary, args("42", "--yes")...).CombinedOutput()
	if err != nil {
		t.Fatalf("compiled Historical Completion cleanup by issue number: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Completion cleanup verified for Historical Run run-local") {
		t.Fatalf("Historical Completion cleanup outcome = %s", output)
	}
	persisted, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	got := persisted.Runs[0]
	expected := *run
	expected.CleanupPending = false
	if !reflect.DeepEqual(got, expected) || len(persisted.Leases) != 0 {
		t.Fatalf("Historical Completion metadata changed:\ngot  %#v\nwant %#v", got, expected)
	}
	if _, err := os.Stat(fixture.worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Historical Completion worktree survived: %v", err)
	}
	if _, err := os.Stat(fixture.sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Historical Completion active session survived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.archiveDir, "session.jsonl")); err != nil {
		t.Fatalf("Historical Completion session was not archived: %v", err)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("Historical Completion remote branch = %#v, %v", branch, err)
	}
	if output, err := exec.Command("git", "-C", fixture.repository, "show-ref", "--verify", "--quiet", "refs/heads/"+fixture.branch).CombinedOutput(); err == nil {
		t.Fatalf("Historical Completion local branch survived: %s", output)
	}

	for _, name := range []string{stateDirectoryBindingFile, legacyStateDirectoryBindingFile} {
		if err := os.Remove(filepath.Join(fixture.repository, ".git", name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	assertResolveStateBindingsAbsent(t, fixture.repository)
	beforeState := fileDigest(t, fixture.store.Path)
	beforeGitHub := fileDigest(t, fixture.githubState)
	beforeRefs := gitSnapshot(t, fixture.repository)
	beforeArchive := filesystemSnapshot(t, fixture.archiveDir)
	output, err = exec.Command(binary, args("run-local", "--yes")...).CombinedOutput()
	if err != nil || !strings.Contains(string(output), "Required actions:\n  None.") {
		t.Fatalf("Historical Completion no-op rerun: %v\n%s", err, output)
	}
	if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub || gitSnapshot(t, fixture.repository) != beforeRefs || filesystemSnapshot(t, fixture.archiveDir) != beforeArchive {
		t.Fatal("Historical Completion no-op rerun performed a mutation")
	}
	assertResolveStateBindingsAbsent(t, fixture.repository)
}

func TestHistoricalCompletionCleanupRefusesChangedArtifactWithoutChangingCompletion(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	completedAt := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)
	pullRequest := "https://github.com/acme/widgets/pull/99"
	current.Runs[0].Status = scheduler.StatusMerged
	current.Runs[0].PullRequest = pullRequest
	current.Runs[0].CompletedAt = &completedAt
	current.Runs[0].CleanupPending = true
	current.Leases = nil
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	mergedCommit := strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", fixture.branch))
	if err := os.WriteFile(filepath.Join(fixture.worktree, "changed-after-completion"), []byte("do not remove\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	baseGH := localArtifactResolveGitHub(t, fixture)
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":99,"url":"`+pullRequest+`","state":"MERGED","mergedAt":"2026-07-29T14:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+fixture.branch+`","headRefOid":"`+mergedCommit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) exec `+quote(baseGH)+` "$@" ;;
esac
`)
	beforeState := fileDigest(t, fixture.store.Path)
	beforeWorktree := filesystemSnapshot(t, fixture.worktree)
	args := []string{"run-local", "--yes", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git, "--gh", gh}
	var stdout, stderr bytes.Buffer
	err = resolveCommandWithInput(context.Background(), args, strings.NewReader(""), false, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "uncommitted changes") {
		t.Fatalf("changed Historical Completion artifact error = %v, stdout=%q, stderr=%q", err, stdout.String(), stderr.String())
	}
	if fileDigest(t, fixture.store.Path) != beforeState || filesystemSnapshot(t, fixture.worktree) != beforeWorktree {
		t.Fatal("changed Historical Completion artifact altered Completion metadata or worktree")
	}
}

func TestCompletionRetriesArchivedSessionSynchronizationBeforeReleasingLease(t *testing.T) {
	fixture := newResolveFixture(t, []string{"spec"}, "COMPLETED")
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	run := &current.Runs[1]
	run.WorkerMode = scheduler.WorkerModeRPC
	run.Branch = "agent/issue-42-run-42"
	run.Worktree = filepath.Join(fixture.stateDir, "worktrees", "issue-42-run-42")
	run.SessionID = "backlog-run-42"
	run.SessionDir = filepath.Join(fixture.stateDir, "sessions", "run-42")
	run.PullRequest = "https://github.com/acme/widgets/pull/9"
	archiveDir := filepath.Join(fixture.stateDir, "history", "sessions", "run-42")
	if err := os.MkdirAll(filepath.Dir(run.SessionDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, "session.jsonl"), []byte(fmt.Sprintf("{\"type\":\"session\",\"id\":%q,\"cwd\":%q}\n", run.SessionID, run.Worktree)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	commit := strings.Repeat("a", 40)
	fixture.git = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  *" remote get-url origin") printf '%s\n' 'git@github.com:acme/widgets.git' ;;
  *" ls-remote --exit-code --heads origin refs/heads/`+run.Branch+`") exit 2 ;;
  *" for-each-ref --format=%(objectname) refs/heads/`+run.Branch+`") exit 0 ;;
  *) exec git "$@" ;;
esac
`)
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "pr list --repo acme/widgets --state all --head `+run.Branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":9,"url":"`+run.PullRequest+`","state":"MERGED","mergedAt":"2026-07-29T14:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+run.Branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) exec `+quote(fixture.gh)+` "$@" ;;
esac
`)
	commonDirectory, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	newModule := func(syncPath func(string) error) retirement.Module {
		module, moduleErr := retirement.New(retirement.Config{
			Store: fixture.store, GitHub: ghadapter.Client{Executable: gh, Dir: fixture.repository},
			RepositoryRoot: fixture.repository, CommonDirectory: commonDirectory,
			StateDirectory: fixture.stateDir, GitExecutable: fixture.git, SyncPath: syncPath,
		}, resolution.Policy("run-42"))
		if moduleErr != nil {
			t.Fatal(moduleErr)
		}
		return module
	}

	module := newModule(func(string) error { return errors.New("injected archive sync retry failure") })
	approved, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Retire(context.Background(), approved); err == nil || !strings.Contains(err.Error(), "injected archive sync retry failure") {
		t.Fatalf("Completion archive sync retry error = %v", err)
	}
	current, err = fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[1].Status != scheduler.StatusFailed || len(current.Leases) != 1 {
		t.Fatalf("archive sync retry failure released Completion ownership: %#v", current)
	}

	syncCalls := 0
	module = newModule(func(path string) error {
		syncCalls++
		return syncFilesystemPath(path)
	})
	approved, err = module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Retire(context.Background(), approved); err != nil {
		t.Fatalf("rerun Completion after archive sync failure: %v", err)
	}
	current, err = fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if syncCalls == 0 || current.Runs[1].Status != scheduler.StatusMerged || len(current.Leases) != 0 {
		t.Fatalf("rerun Completion durability state = %#v, sync calls = %d", current, syncCalls)
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

func TestCompiledResolveTreatsAbsentRemoteBranchWithStderrAsRetired(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	gh := localArtifactResolveGitHub(t, fixture)
	called := filepath.Join(t.TempDir(), "ls-remote-called")
	git := writeExecutable(t, `#!/bin/sh
case "$*" in
  *" ls-remote --exit-code --heads origin refs/heads/`+fixture.branch+`")
    touch `+quote(called)+`
    echo 'Warning: Permanently added github.com to the list of known hosts.' >&2
    exit 2 ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, localArtifactResolveArgs(fixture, git, gh, "--dry-run")...)
	output, err := command.CombinedOutput()
	if _, statErr := os.Stat(called); statErr != nil {
		t.Fatalf("compiled dry-run bypassed warning shim: %v\n%s", statErr, output)
	}
	if err != nil {
		t.Fatalf("compiled dry-run with absent remote branch warning: %v\n%s", err, output)
	}
	plan := string(output)
	if strings.Contains(plan, "delete remote branch") {
		t.Fatalf("dry-run did not treat absent remote branch as retired:\n%s", plan)
	}
	if !strings.Contains(plan, "remove local worktree") {
		t.Fatalf("dry-run did not continue after absent remote branch inspection:\n%s", plan)
	}
}

func TestCompiledResolveFailsClosedWhenRemoteBranchInspectionIsUnknown(t *testing.T) {
	fixture := newLocalArtifactResetFixture(t, false)
	gh := localArtifactResolveGitHub(t, fixture)
	git := writeExecutable(t, `#!/bin/sh
case "$*" in
  *" ls-remote --exit-code --heads origin refs/heads/`+fixture.branch+`")
    echo 'remote inspection unavailable' >&2
    exit 1 ;;
esac
exec `+quote(fixture.git)+` "$@"
`)
	beforeState := fileDigest(t, fixture.store.Path)
	beforeGitHub := fileDigest(t, fixture.githubState)
	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, localArtifactResolveArgs(fixture, git, gh, "--yes")...)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "git exited 1; state is unknown") {
		t.Fatalf("compiled Resolve accepted unknown remote branch state: %v\n%s", err, output)
	}
	if fileDigest(t, fixture.store.Path) != beforeState || fileDigest(t, fixture.githubState) != beforeGitHub {
		t.Fatal("unknown remote branch inspection changed Run state or GitHub labels")
	}
	for _, path := range []string{fixture.worktree, fixture.sessionDir} {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Fatalf("unknown remote branch inspection removed %s: %v", path, statErr)
		}
	}
	if _, statErr := os.Stat(fixture.archiveDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unknown remote branch inspection created session archive: %v", statErr)
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

func TestCompiledResolveAllowsClosureReasonChangeWhenExpectedPullRequestMerges(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, false, false)
	gh := githubArtifactResolveGitHub(t, fixture)
	counter := filepath.Join(t.TempDir(), "pull-request-inspections")
	racingGitHub := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    if jq -e '.merged' `+quote(fixture.githubState)+` >/dev/null; then
      printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"FUTURE"}'
      exit 0
    fi ;;
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    count=0
    if [ -f `+quote(counter)+` ]; then count=$(cat `+quote(counter)+`); fi
    count=$((count + 1))
    printf '%s\n' "$count" > `+quote(counter)+`
    if [ "$count" -eq 3 ]; then
      temporary=`+quote(fixture.githubState)+`.tmp
      jq '.pr="MERGED" | .merged=true | .auto=false' `+quote(fixture.githubState)+` > "$temporary"
      mv "$temporary" `+quote(fixture.githubState)+`
    fi ;;
esac
exec `+quote(gh)+` "$@"
`)

	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, racingGitHub, "--yes")...)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "Completion recorded for Run run-github") {
		t.Fatalf("closure-reason Completion race: %v\n%s", err, output)
	}
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusMerged || current.Runs[0].CompletedAt == nil || len(current.Leases) != 0 {
		t.Fatalf("closure-reason Completion race state = %#v", current)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("closure-reason Completion race remote branch = %#v, %v", branch, err)
	}
	var github struct {
		Labels []string `json:"labels"`
	}
	data, err := os.ReadFile(fixture.githubState)
	if err != nil || json.Unmarshal(data, &github) != nil {
		t.Fatalf("read closure-reason Completion race labels: %v", err)
	}
	if strings.Join(github.Labels, ",") != "spec" {
		t.Fatalf("closure-reason Completion race retained labels: %v", github.Labels)
	}
}

func TestCompiledResolveAllowsClosureReasonChangeAfterCompletionCleanupStarts(t *testing.T) {
	binary := buildExecutable(t, t.TempDir())
	for _, test := range []struct {
		name       string
		activation func(githubArtifactResetFixture) string
	}{
		{
			name: "during cleanup revalidation",
			activation: func(fixture githubArtifactResetFixture) string {
				return `git --git-dir=` + quote(fixture.remote) + ` show-ref --verify --quiet refs/heads/` + fixture.branch + ` || active=true`
			},
		},
		{
			name: "immediately before finalization",
			activation: func(fixture githubArtifactResetFixture) string {
				return `jq -e '.labels == ["spec"]' ` + quote(fixture.githubState) + ` >/dev/null && active=true || :`
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGitHubArtifactResetFixture(t, scheduler.StatusWaitingForMerge, false, false, false)
			gh := githubArtifactResolveGitHub(t, fixture)
			pullCounter := filepath.Join(t.TempDir(), "pull-request-inspections")
			driftCounter := filepath.Join(t.TempDir(), "post-cleanup-closure-inspections")
			for _, counter := range []string{pullCounter, driftCounter} {
				if err := os.WriteFile(counter, []byte("0\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			racingGitHub := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    active=false
    `+test.activation(fixture)+`
    [ "$active" != true ] || {
      count=$(cat `+quote(driftCounter)+`)
      count=$((count + 1))
      printf '%s\n' "$count" > `+quote(driftCounter)+`
      case "$count" in
        1|2) ;;
        *)
          printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"FUTURE"}'
          exit 0 ;;
      esac
    } ;;
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    count=$(cat `+quote(pullCounter)+`)
    count=$((count + 1))
    printf '%s\n' "$count" > `+quote(pullCounter)+`
    [ "$count" -ne 3 ] || {
      temporary=`+quote(fixture.githubState)+`.tmp
      jq '.pr="MERGED" | .merged=true | .auto=false' `+quote(fixture.githubState)+` > "$temporary"
      mv "$temporary" `+quote(fixture.githubState)+`
    } ;;
esac
exec `+quote(gh)+` "$@"
`)

			command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, racingGitHub, "--yes")...)
			output, err := command.CombinedOutput()
			if err != nil || !strings.Contains(string(output), "Completion recorded for Run run-github") {
				t.Fatalf("later closure-reason Completion race: %v\n%s", err, output)
			}
			current, err := fixture.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if current.Runs[0].Status != scheduler.StatusMerged || current.Runs[0].CompletedAt == nil || len(current.Leases) != 0 {
				t.Fatalf("later closure-reason Completion state = %#v", current)
			}
			if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
				t.Fatalf("later closure-reason Completion remote branch = %#v, %v", branch, err)
			}
			var github struct {
				Labels []string `json:"labels"`
			}
			githubData, err := os.ReadFile(fixture.githubState)
			if err != nil || json.Unmarshal(githubData, &github) != nil {
				t.Fatalf("read later closure-reason Completion labels: %v", err)
			}
			if strings.Join(github.Labels, ",") != "spec" {
				t.Fatalf("later closure-reason Completion retained labels: %v", github.Labels)
			}
			data, err := os.ReadFile(driftCounter)
			count, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil || parseErr != nil || count < 3 {
				t.Fatalf("later closure-reason drift was not observed after cleanup began: count=%q, err=%v, parse=%v", data, err, parseErr)
			}
		})
	}
}

func TestCompiledResolvePreservesRetiredArtifactCommitIdentityAcrossLateMerge(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	current.Runs[0].PullRequest = ""
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	gh := githubArtifactResolveGitHub(t, fixture)
	replacement := strings.Repeat("b", 40)
	racingGitHub := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    if git --git-dir=`+quote(fixture.remote)+` show-ref --verify --quiet refs/heads/`+fixture.branch+`; then
      printf '%s\n' '[]'
    else
      printf '%s\n' '[{"number":99,"url":"https://github.com/acme/widgets/pull/99","state":"MERGED","mergedAt":"2026-07-29T14:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+fixture.branch+`","headRefOid":"`+replacement+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]'
    fi
    exit 0 ;;
esac
exec `+quote(gh)+` "$@"
`)

	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, racingGitHub, "--yes")...)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "approved remote branch commit identity does not match the merged expected pull request head") {
		t.Fatalf("retired-artifact late merge error = %v\n%s", err, output)
	}
	current, err = fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvingExternally || current.Runs[0].CompletedAt != nil || len(current.Leases) != 1 {
		t.Fatalf("retired-artifact late merge released ownership: %#v", current)
	}
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("approved remote branch was not retired before late merge = %#v, %v", branch, err)
	}
	if strings.Contains(string(output), "Completion recorded") {
		t.Fatalf("retired-artifact late merge reported Completion: %s", output)
	}
}

func TestCompiledResolveRefusesUnapprovedCleanupIntroducedByLateMerge(t *testing.T) {
	fixture := newGitHubArtifactResetFixture(t, scheduler.StatusFailed, false, false, false)
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	current.Runs[0].PullRequest = ""
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	gh := githubArtifactResolveGitHub(t, fixture)
	fixture.updateGitHubState(t, `.labels=["spec"]`)
	commit := strings.TrimSpace(gitOutput(t, fixture.repository, "rev-parse", "origin/"+fixture.branch))
	mutation := filepath.Join(t.TempDir(), "unapproved-label-mutation")
	racingGitHub := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    if git --git-dir=`+quote(fixture.remote)+` show-ref --verify --quiet refs/heads/`+fixture.branch+`; then
      labels='[{"name":"spec"}]'
    else
      labels='[{"name":"in-progress"},{"name":"spec"}]'
    fi
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels"
    exit 0 ;;
  "pr list --repo acme/widgets --state all --head `+fixture.branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    if git --git-dir=`+quote(fixture.remote)+` show-ref --verify --quiet refs/heads/`+fixture.branch+`; then
      printf '%s\n' '[]'
    else
      printf '%s\n' '[{"number":99,"url":"https://github.com/acme/widgets/pull/99","state":"MERGED","mergedAt":"2026-07-29T14:00:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+fixture.branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]'
    fi
    exit 0 ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    touch `+quote(mutation)+`
    exec `+quote(gh)+` "$@" ;;
esac
exec `+quote(gh)+` "$@"
`)

	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, githubArtifactResolveArgs(fixture, fixture.git, racingGitHub, "--yes")...)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "merged Completion requires unapproved retirement action") {
		t.Fatalf("unapproved late-merge cleanup error = %v\n%s", err, output)
	}
	current, err = fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if current.Runs[0].Status != scheduler.StatusResolvingExternally || current.Runs[0].CompletedAt != nil || len(current.Leases) != 1 {
		t.Fatalf("unapproved late-merge cleanup released ownership: %#v", current)
	}
	if _, err := os.Stat(mutation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unapproved late-merge cleanup mutated a label: %v", err)
	}
	if strings.Contains(string(output), "Completion recorded") {
		t.Fatalf("unapproved late-merge cleanup reported Completion: %s", output)
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
	if err == nil || !strings.Contains(string(output), "artifact commit identity does not match the merged expected pull request head") {
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
	fixture.updateGitHubState(t, `.labels=["in-progress"]`)
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' `+quote(fixture.githubState)+`)
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"COMPLETED"}' ;;
  "pr merge 99 --repo acme/widgets --disable-auto")
    jq -e '.runs[] | select(.runId == "run-github" and .status == "resolving-externally")' `+quote(fixture.store.Path)+` >/dev/null
    exec `+quote(fixture.gh)+` "$@" ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary=`+quote(fixture.githubState)+`.tmp
    jq '.labels |= map(select(. != "in-progress"))' `+quote(fixture.githubState)+` > "$temporary"
    mv "$temporary" `+quote(fixture.githubState)+` ;;
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
	if branch, err := inspectRemoteBranch(context.Background(), fixture.git, fixture.repository, fixture.branch); err != nil || branch.Present {
		t.Fatalf("merge race remote branch = %#v, %v", branch, err)
	}
	var labels struct {
		Labels []string `json:"labels"`
	}
	data, err := os.ReadFile(fixture.githubState)
	if err != nil || json.Unmarshal(data, &labels) != nil {
		t.Fatalf("read merge race labels: %v", err)
	}
	if len(labels.Labels) != 0 {
		t.Fatalf("merge race retained managed labels: %v", labels.Labels)
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

func TestCompiledResolveRecordsCompletionWhenPullRequestIsCreatedAndMergedAfterConfirmation(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "spec"}, "COMPLETED")
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	branch := "agent/issue-42-run-42"
	pullRequest := "https://github.com/acme/widgets/pull/9"
	current.Runs[1].Branch = branch
	current.Runs[1].Worktree = filepath.Join(fixture.stateDir, "worktrees", "issue-42-run-42")
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
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
	counter := filepath.Join(t.TempDir(), "pull-request-inspections")
	commit := strings.Repeat("a", 40)
	fixture.gh = writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    count=0
    if [ -f `+quote(counter)+` ]; then count=$(cat `+quote(counter)+`); fi
    count=$((count + 1))
    printf '%s\n' "$count" > `+quote(counter)+`
    if [ "$count" -lt 3 ]; then
      printf '%s\n' '[]'
    else
      printf '%s\n' '[{"number":9,"url":"`+pullRequest+`","state":"MERGED","mergedAt":"2026-07-28T01:01:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]'
    fi ;;
  *) exec `+quote(fixture.gh)+` "$@" ;;
esac
`)

	binary := buildExecutable(t, t.TempDir())
	command := exec.Command(binary, append([]string{"resolve"}, fixture.args("run-42", "--yes")...)...)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "Completion recorded for Run run-42 from merged expected pull request "+pullRequest) {
		t.Fatalf("created-and-merged race: %v\n%s", err, output)
	}
	persisted, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Runs[1].Status != scheduler.StatusMerged || persisted.Runs[1].PullRequest != pullRequest || persisted.Runs[1].CompletedAt == nil || len(persisted.Leases) != 0 {
		t.Fatalf("created-and-merged race state = %#v", persisted)
	}
}

func TestResolveKeepsLeaseForAmbiguousUnrecordedMergedExpectedBranchPullRequests(t *testing.T) {
	fixture := newResolveFixture(t, []string{"in-progress", "spec"}, "COMPLETED")
	current, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	branch := "agent/issue-42-run-42"
	current.Runs[1].Branch = branch
	current.Runs[1].Worktree = filepath.Join(fixture.stateDir, "worktrees", "issue-42-run-42")
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
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
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":9,"url":"https://github.com/acme/widgets/pull/9","state":"MERGED","mergedAt":"2026-07-28T01:01:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+branch+`","headRefOid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}},{"number":10,"url":"https://github.com/acme/widgets/pull/10","state":"MERGED","mergedAt":"2026-07-28T01:02:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+branch+`","headRefOid":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  *) exec `+quote(fixture.gh)+` "$@" ;;
esac
`)

	var stdout, stderr bytes.Buffer
	err = resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "multiple merged pull requests") {
		t.Fatalf("ambiguous merged pull requests error = %v, stderr=%q, stdout=%q", err, stderr.String(), stdout.String())
	}
	persisted, err := fixture.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Runs[1].Status != scheduler.StatusFailed || persisted.Runs[1].CompletedAt != nil || len(persisted.Leases) != 1 || persisted.Leases[0].RunID != "run-42" {
		t.Fatalf("ambiguous merged pull requests state = %#v", persisted)
	}
	if strings.Contains(stdout.String(), "Completion recorded") || strings.Contains(stdout.String(), "External Resolution complete") {
		t.Fatalf("ambiguous merged pull requests reported success: %q", stdout.String())
	}
}

func TestResolveRecordsAndReportsCompletionFromMergedExpectedBranchPullRequest(t *testing.T) {
	for _, test := range []struct {
		name, reasonJSON string
		status           scheduler.Status
		recorded         bool
	}{
		{name: "missing closure reason for recorded pull request", reasonJSON: "null", status: scheduler.StatusWaitingForMerge, recorded: true},
		{name: "missing closure reason for discovered pull request", reasonJSON: "null", status: scheduler.StatusFailed},
		{name: "future closure reason for recorded pull request", reasonJSON: `"FUTURE"`, status: scheduler.StatusWaitingForMerge, recorded: true},
		{name: "future closure reason for discovered pull request", reasonJSON: `"FUTURE"`, status: scheduler.StatusFailed},
		{name: "discovered after Run failure", reasonJSON: `"COMPLETED"`, status: scheduler.StatusFailed},
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
			current.Runs[1].Status = test.status
			current.Runs[1].Branch = branch
			current.Runs[1].Worktree = filepath.Join(fixture.stateDir, "worktrees", "issue-42-run-42")
			if test.recorded {
				current.Runs[1].PullRequest = pullRequest
			}
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
    labels=$(jq -c '[.labels[] | {name:.}]' `+quote(fixture.githubState)+`)
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":`+test.reasonJSON+`}' ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    printf '%s\n' '[{"number":9,"url":"`+pullRequest+`","state":"MERGED","mergedAt":"2026-07-28T01:01:00Z","autoMergeRequest":null,"isDraft":false,"headRefName":"`+branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]' ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    temporary=`+quote(fixture.githubState)+`.tmp
    jq '.labels |= map(select(. != "in-progress"))' `+quote(fixture.githubState)+` > "$temporary"
    mv "$temporary" `+quote(fixture.githubState)+` ;;
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
	encoded = bytes.Replace(encoded, []byte(`"version": 5`), []byte(`"version": 3`), 1)
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
	if !bytes.Contains(persisted, []byte(`"version": 5`)) {
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
