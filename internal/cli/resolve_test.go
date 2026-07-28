package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
	if err := os.WriteFile(githubState, []byte(`{"labels":`+string(encodedLabels)+`,"reason":`+string(encodedReason)+`}`), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := writeExecutable(t, `#!/bin/sh
set -eu
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels=$(jq -c '[.labels[] | {name:.}]' `+quote(githubState)+`)
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":%s}\n' "$labels" ;;
  "issue view 42 --repo acme/widgets --json number,url,state,stateReason")
    reason=$(jq -r '.reason' `+quote(githubState)+`)
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","stateReason":"%s"}\n' "$reason" ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    tmp=`+quote(githubState)+`.tmp; jq '.labels |= map(select(ascii_downcase != "in-progress"))' `+quote(githubState)+` > "$tmp"; mv "$tmp" `+quote(githubState)+` ;;
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
			if test.dry && !strings.Contains(stdout.String(), "Dry-run") || !test.dry && !strings.Contains(stdout.String(), "cancelled") {
				t.Fatalf("output = %q", stdout.String())
			}
		})
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
		})
	}
}

func TestResolveRequiresYesNonInteractivelyAndRefusesRunnerLock(t *testing.T) {
	fixture := newResolveFixture(t, []string{"spec"}, "COMPLETED")
	var stdout, stderr bytes.Buffer
	if err := resolveCommandWithInput(context.Background(), fixture.args("42"), strings.NewReader(""), false, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "requires --yes") {
		t.Fatalf("non-interactive error = %v", err)
	}
	common, err := gitCommonDirectory(context.Background(), fixture.git, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := acquireRepositoryLock(common)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	if err := resolveCommandWithInput(context.Background(), fixture.args("42", "--yes"), strings.NewReader(""), false, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "Runner owns repository coordination") {
		t.Fatalf("lock error = %v", err)
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

func TestResolvedRunStatusAndFollowExposeMetadataAndMissingLogWarning(t *testing.T) {
	fixture := newResolveFixture(t, []string{"spec"}, "COMPLETED")
	current, _, _ := fixture.store.Preview()
	current.Runs[1].LogPath = filepath.Join(fixture.stateDir, "missing.jsonl")
	current.Runs[1].StderrPath = filepath.Join(fixture.stateDir, "missing.stderr")
	if err := fixture.store.Save(current); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := resolveCommandWithInput(context.Background(), fixture.args("run-42", "--yes"), strings.NewReader(""), false, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), []string{"status", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git}, &stdout, &stderr); exit != 0 {
		t.Fatal(stderr.String())
	}
	if strings.Contains(stdout.String(), "run-42") || strings.Contains(stdout.String(), "External Resolution") {
		t.Fatalf("default status exposed handled External Resolution: %s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), []string{"status", "--all", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git}, &stdout, &stderr); exit != 0 {
		t.Fatal(stderr.String())
	}
	for _, want := range []string{"resolved-externally", "GitHub closure reason: completed", "retained diagnostic", "Diagnostic warning:"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("full status missing %q: %s", want, stdout.String())
		}
	}
	stdout.Reset()
	stderr.Reset()
	if exit := Main(context.Background(), []string{"follow", "run-42", "--repo-dir", fixture.repository, "--state-dir", fixture.stateDir, "--git", fixture.git}, &stdout, &stderr); exit != 0 {
		t.Fatal(stderr.String())
	}
	if !strings.Contains(stdout.String(), "External Resolution:") || !strings.Contains(stdout.String(), "Diagnostic warning:") {
		t.Fatalf("follow = %s", stdout.String())
	}
}
