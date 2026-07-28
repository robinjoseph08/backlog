package retirement

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestBuildAppliesLifecyclePolicyWithoutOwningLifecycleDecisions(t *testing.T) {
	policy := testPolicy()
	snapshot := Snapshot{
		Run:   scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed},
		Lease: scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"},
		Issue: Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", Open: true, Labels: []string{"owned", "unrelated"}},
		PullRequests: []PullRequest{{
			Number: 7, URL: "https://github.com/acme/widgets/pull/7", Branch: "", Commit: "abc", State: PullRequestOpen,
		}},
	}

	plan, err := Build(policy, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"mark Run run-42 resetting while retaining Lease lease-42",
		"explain retirement on pull request #7 (https://github.com/acme/widgets/pull/7)",
		"close unmerged pull request #7 (https://github.com/acme/widgets/pull/7)",
		"remove issue label owned from https://github.com/acme/widgets/issues/42",
		"add issue label available to https://github.com/acme/widgets/issues/42",
		"mark Run run-42 reset and release Lease lease-42",
	}
	if strings.Join(actionDescriptions(plan), "\n") != strings.Join(want, "\n") {
		t.Fatalf("actions = %q, want %q", actionDescriptions(plan), want)
	}
	var output bytes.Buffer
	WritePlan(&output, plan)
	if !strings.Contains(output.String(), "Reset Plan for issue #42") {
		t.Fatalf("policy operation missing from plan output: %q", output.String())
	}
}

func TestModuleRetiresWithDistinctExternalResolutionPolicy(t *testing.T) {
	const (
		statusResolvingExternally scheduler.Status = "resolving-externally"
		statusResolvedExternally  scheduler.Status = "resolved-externally"
		explanation                                = "externally resolved run-42"
	)
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	readyLabel := filepath.Join(root, "ready-label")
	progressLabel := filepath.Join(root, "progress-label")
	openPullRequest := filepath.Join(root, "open-pull-request")
	commentedPullRequest := filepath.Join(root, "commented-pull-request")
	mutationLog := filepath.Join(root, "mutations")
	for _, path := range []string{readyLabel, progressLabel, openPullRequest} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	branch := "agent/issue-42-run-42"
	commit := strings.Repeat("a", 40)
	git := writeRetirementExecutable(t, `#!/bin/sh
case "$*" in
  *" remote get-url origin") printf '%s\n' 'https://github.com/acme/widgets.git' ;;
  *" ls-remote --exit-code --heads origin refs/heads/`+branch+`") exit 2 ;;
  *" for-each-ref --format=%(objectname) refs/heads/`+branch+`") exit 0 ;;
  *" worktree list --porcelain -z") exit 0 ;;
  *) echo "unexpected git: $*" >&2; exit 9 ;;
esac
`)
	gh := writeRetirementExecutable(t, `#!/bin/sh
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    labels='{"name":"unrelated"}'
    if [ -f `+shellQuote(readyLabel)+` ]; then labels="$labels,{\"name\":\"ready-for-agent\"}"; fi
    if [ -f `+shellQuote(progressLabel)+` ]; then labels="$labels,{\"name\":\"in-progress\"}"; fi
    printf '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":[%s]}\n' "$labels" ;;
  "pr list --repo acme/widgets --state all --head `+branch+` --limit 1000 --json number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository")
    state=CLOSED
    if [ -f `+shellQuote(openPullRequest)+` ]; then state=OPEN; fi
    printf '[{"number":7,"url":"https://github.com/acme/widgets/pull/7","state":"%s","mergedAt":null,"autoMergeRequest":null,"isDraft":false,"headRefName":"`+branch+`","headRefOid":"`+commit+`","headRepositoryOwner":{"login":"acme"},"headRepository":{"nameWithOwner":"acme/widgets"}}]\n' "$state" ;;
  api\ *)
    if [ -f `+shellQuote(commentedPullRequest)+` ]; then
      printf '%s\n' '[[{"body":"`+explanation+`"}]]'
    else
      printf '%s\n' '[[]]'
    fi ;;
  pr\ comment\ 7\ *)
    printf 'explanation:%s\n' "$7" >> `+shellQuote(mutationLog)+`
    : > `+shellQuote(commentedPullRequest)+` ;;
  "pr close 7 --repo acme/widgets")
    printf '%s\n' 'close:7' >> `+shellQuote(mutationLog)+`
    rm -f `+shellQuote(openPullRequest)+` ;;
  "issue edit 42 --repo acme/widgets --remove-label ready-for-agent")
    printf '%s\n' 'remove:ready-for-agent' >> `+shellQuote(mutationLog)+`
    rm -f `+shellQuote(readyLabel)+` ;;
  "issue edit 42 --repo acme/widgets --remove-label in-progress")
    printf '%s\n' 'remove:in-progress' >> `+shellQuote(mutationLog)+`
    rm -f `+shellQuote(progressLabel)+` ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)

	run := scheduler.Run{
		Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint,
		Branch: branch, Worktree: filepath.Join(stateDir, "worktrees", "issue-42-run-42"), WorkerLogOpen: true,
	}
	lease := scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: run.RunID}
	store := &policyStateStore{current: state.State{
		Repo: "acme/widgets", DefaultBranch: "main", Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease},
	}}
	policy := testPolicy()
	policy.Operation = "External Resolution"
	policy.SelectRun = func(current state.State) (scheduler.Run, scheduler.Lease, error) {
		selected := current.Runs[0]
		for _, candidate := range current.Leases {
			if candidate.RunID == selected.RunID {
				return selected, candidate, nil
			}
		}
		return selected, scheduler.Lease{}, nil
	}
	policy.ValidateSnapshot = func(snapshot Snapshot) error {
		if snapshot.Issue.Open {
			return errors.New("issue is not externally resolved")
		}
		return nil
	}
	policy.EligibleStatuses = []scheduler.Status{
		scheduler.StatusFailed, statusResolvingExternally, statusResolvedExternally,
	}
	policy.CanTransition = func(from, to scheduler.Status) bool {
		return from == scheduler.StatusFailed && to == statusResolvingExternally ||
			from == statusResolvingExternally && to == statusResolvedExternally
	}
	policy.Explanation = func(scheduler.Run) string { return explanation }
	policy.ExplanationAction = "explain External Resolution"
	policy.Labels = LabelOutcome{Remove: []string{"ready-for-agent", "in-progress"}}
	policy.ProgressStatus = statusResolvingExternally
	policy.TerminalStatus = statusResolvedExternally

	module, err := New(Config{
		Store: store, GitHub: ghadapter.Client{Executable: gh, Dir: root}, RepositoryRoot: root,
		CommonDirectory: root, StateDirectory: stateDir, GitExecutable: git,
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"mark Run run-42 resolving-externally while retaining Lease lease-42",
		"explain External Resolution on pull request #7 (https://github.com/acme/widgets/pull/7)",
		"close unmerged pull request #7 (https://github.com/acme/widgets/pull/7)",
		"remove issue label ready-for-agent from https://github.com/acme/widgets/issues/42",
		"remove issue label in-progress from https://github.com/acme/widgets/issues/42",
		"mark Run run-42 resolved-externally and release Lease lease-42",
	}
	if strings.Join(actionDescriptions(approved), "\n") != strings.Join(want, "\n") {
		t.Fatalf("External Resolution actions = %q, want %q", actionDescriptions(approved), want)
	}
	var output bytes.Buffer
	WritePlan(&output, approved)
	if !strings.Contains(output.String(), "External Resolution Plan for issue #42") ||
		!strings.Contains(output.String(), "Issue: https://github.com/acme/widgets/issues/42 (closed; labels: in-progress, ready-for-agent, unrelated)") {
		t.Fatalf("External Resolution plan output = %q", output.String())
	}
	if err := module.Retire(context.Background(), approved); err != nil {
		t.Fatalf("External Resolution retirement: %v", err)
	}
	if len(store.saved) != 2 || store.saved[0].Runs[0].Status != statusResolvingExternally || len(store.saved[0].Leases) != 1 {
		t.Fatalf("External Resolution progress state = %#v", store.saved)
	}
	terminal := store.current
	if terminal.Runs[0].Status != statusResolvedExternally || terminal.Runs[0].WorkerLogOpen || terminal.Runs[0].CompletedAt == nil || len(terminal.Leases) != 0 {
		t.Fatalf("External Resolution terminal state = %#v", terminal)
	}
	mutations, err := os.ReadFile(mutationLog)
	if err != nil {
		t.Fatal(err)
	}
	wantMutations := "explanation:" + explanation + "\nclose:7\nremove:ready-for-agent\nremove:in-progress\n"
	if string(mutations) != wantMutations {
		t.Fatalf("External Resolution mutations = %q, want %q", mutations, wantMutations)
	}

	rerun, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rerun.Actions) != 0 {
		t.Fatalf("idempotent External Resolution actions = %q", actionDescriptions(rerun))
	}
	if err := module.Retire(context.Background(), rerun); err != nil {
		t.Fatalf("idempotent External Resolution retirement: %v", err)
	}
	if len(store.saved) != 2 {
		t.Fatalf("idempotent External Resolution persisted %d states, want 2", len(store.saved))
	}
}

func TestModulePerformsRequiredProgressTransitionWhenArtifactsAreAlreadyRetired(t *testing.T) {
	const (
		statusResolvingExternally scheduler.Status = "resolving-externally"
		statusResolvedExternally  scheduler.Status = "resolved-externally"
	)
	root := t.TempDir()
	git := writeRetirementExecutable(t, `#!/bin/sh
case "$*" in
  *" remote get-url origin") printf '%s\n' 'https://github.com/acme/widgets.git' ;;
  *) echo "unexpected git: $*" >&2; exit 9 ;;
esac
`)
	gh := writeRetirementExecutable(t, `#!/bin/sh
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"CLOSED","labels":[]}' ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
	run := scheduler.Run{
		Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint,
		WorkerLogOpen: true,
	}
	lease := scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: run.RunID}
	store := &policyStateStore{current: state.State{
		Repo: "acme/widgets", DefaultBranch: "main", Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease},
	}}
	policy := testPolicy()
	policy.Operation = "External Resolution"
	policy.SelectRun = func(current state.State) (scheduler.Run, scheduler.Lease, error) {
		selected := current.Runs[0]
		for _, candidate := range current.Leases {
			if candidate.RunID == selected.RunID {
				return selected, candidate, nil
			}
		}
		return selected, scheduler.Lease{}, nil
	}
	policy.ValidateSnapshot = func(snapshot Snapshot) error {
		if snapshot.Issue.Open {
			return errors.New("issue is not externally resolved")
		}
		return nil
	}
	policy.EligibleStatuses = []scheduler.Status{
		scheduler.StatusFailed, statusResolvingExternally, statusResolvedExternally,
	}
	policy.CanTransition = func(from, to scheduler.Status) bool {
		return from == scheduler.StatusFailed && to == statusResolvingExternally ||
			from == statusResolvingExternally && to == statusResolvedExternally
	}
	policy.Labels = LabelOutcome{Remove: []string{"ready-for-agent"}}
	policy.ProgressStatus = statusResolvingExternally
	policy.TerminalStatus = statusResolvedExternally

	module, err := New(Config{
		Store: store, GitHub: ghadapter.Client{Executable: gh, Dir: root}, RepositoryRoot: root,
		CommonDirectory: root, StateDirectory: root, GitExecutable: git,
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"mark Run run-42 resolving-externally while retaining Lease lease-42",
		"mark Run run-42 resolved-externally and release Lease lease-42",
	}
	if strings.Join(actionDescriptions(approved), "\n") != strings.Join(want, "\n") {
		t.Fatalf("already-retired actions = %q, want %q", actionDescriptions(approved), want)
	}
	if err := module.Retire(context.Background(), approved); err != nil {
		t.Fatalf("already-retired External Resolution: %v", err)
	}
	if len(store.saved) != 2 || store.saved[0].Runs[0].Status != statusResolvingExternally || len(store.saved[0].Leases) != 1 {
		t.Fatalf("required progress state = %#v", store.saved)
	}
	terminal := store.current
	if terminal.Runs[0].Status != statusResolvedExternally || terminal.Runs[0].WorkerLogOpen || terminal.Runs[0].CompletedAt == nil || len(terminal.Leases) != 0 {
		t.Fatalf("terminal state = %#v", terminal)
	}

	rerun, err := module.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rerun.Actions) != 0 {
		t.Fatalf("idempotent already-retired actions = %q", actionDescriptions(rerun))
	}
	if err := module.Retire(context.Background(), rerun); err != nil {
		t.Fatalf("idempotent already-retired External Resolution: %v", err)
	}
	if len(store.saved) != 2 {
		t.Fatalf("idempotent already-retired External Resolution persisted %d states, want 2", len(store.saved))
	}
}

func TestRetirePersistsProgressBeforeFullReinspectionCanFail(t *testing.T) {
	run := scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint}
	lease := scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: run.RunID}
	store := &policyStateStore{current: state.State{Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease}}}
	policy := testPolicy()
	policy.MarkProgressBeforeMutation = true
	policy.SelectRun = func(current state.State) (scheduler.Run, scheduler.Lease, error) {
		return current.Runs[0], current.Leases[0], nil
	}
	service := Service{
		store: store, github: ghadapter.Client{Executable: filepath.Join(t.TempDir(), "missing-gh")},
		policy: policy,
	}
	approved := Plan{
		Snapshot: Snapshot{Run: run, Lease: lease},
		Actions: []Action{
			plannedAction(actionMarkProgress, "mark progress"),
			plannedAction(actionFinalize, "finalize"),
		},
	}

	err := service.Retire(context.Background(), approved)
	if err == nil || !strings.Contains(err.Error(), "discover repository") {
		t.Fatalf("reinspection error = %v", err)
	}
	if len(store.saved) != 1 || store.current.Runs[0].Status != scheduler.StatusResetting || store.current.Leases[0] != lease {
		t.Fatalf("inspection failure did not retain durable progress and Lease: %#v", store.current)
	}
}

func TestFinalStateRequiresExplanationOnClosedUnmergedPullRequest(t *testing.T) {
	policy := testPolicy()
	policy.RequireClosedExplanation = true
	service := Service{policy: policy}
	err := service.verifyOwnedFinalState(Snapshot{PullRequests: []PullRequest{{Number: 7, State: PullRequestClosed}}})
	if err == nil || !strings.Contains(err.Error(), "explanation") {
		t.Fatalf("unexplained final pull request error = %v", err)
	}
}

func TestPolicyValidationRefusesEveryIncompletePolicyShape(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Policy)
		want   string
	}{
		{name: "core behavior", mutate: func(policy *Policy) { policy.Explanation = nil }, want: "policy is incomplete"},
		{name: "transition policy", mutate: func(policy *Policy) { policy.CanTransition = nil }, want: "policy is incomplete"},
		{name: "lifecycle states", mutate: func(policy *Policy) { policy.ProgressStatus = "" }, want: "incomplete lifecycle states"},
		{name: "equal lifecycle states", mutate: func(policy *Policy) { policy.TerminalStatus = policy.ProgressStatus }, want: "distinct progress and terminal states"},
		{name: "waiting for merge progress", mutate: func(policy *Policy) {
			policy.EligibleStatuses = append(policy.EligibleStatuses, scheduler.StatusWaitingForMerge)
			policy.ProgressStatus = scheduler.StatusWaitingForMerge
		}, want: "cannot use waiting-for-merge as its progress state"},
		{name: "progress cannot become terminal", mutate: func(policy *Policy) {
			policy.CanTransition = func(scheduler.Status, scheduler.Status) bool { return false }
		}, want: "cannot transition from progress state resetting to terminal state reset"},
		{name: "eligible state cannot enter progress", mutate: func(policy *Policy) {
			policy.CanTransition = func(from, to scheduler.Status) bool {
				return from == policy.ProgressStatus && to == policy.TerminalStatus
			}
		}, want: "cannot transition from eligible state failed to progress state resetting"},
		{name: "label outcome", mutate: func(policy *Policy) { policy.Labels = LabelOutcome{} }, want: "no label outcome"},
		{name: "empty add label", mutate: func(policy *Policy) { policy.Labels.Add = append(policy.Labels.Add, " ") }, want: "empty label to add"},
		{name: "empty remove label", mutate: func(policy *Policy) { policy.Labels.Remove = append(policy.Labels.Remove, "") }, want: "empty label to remove"},
		{name: "duplicate add label", mutate: func(policy *Policy) { policy.Labels.Add = append(policy.Labels.Add, "AVAILABLE") }, want: "duplicate label"},
		{name: "Unicode duplicate add label", mutate: func(policy *Policy) { policy.Labels.Add = []string{"Σ", "ς"} }, want: "duplicate label"},
		{name: "duplicate remove label", mutate: func(policy *Policy) { policy.Labels.Remove = append(policy.Labels.Remove, "OWNED") }, want: "duplicate label"},
		{name: "Unicode duplicate remove label", mutate: func(policy *Policy) { policy.Labels.Remove = []string{"Σ", "ς"} }, want: "duplicate label"},
		{name: "overlapping label", mutate: func(policy *Policy) { policy.Labels.Remove = append(policy.Labels.Remove, "AVAILABLE") }, want: "both add and remove"},
		{name: "Unicode overlapping label", mutate: func(policy *Policy) {
			policy.Labels = LabelOutcome{Add: []string{"Σ"}, Remove: []string{"ς"}}
		}, want: "both add and remove"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := testPolicy()
			test.mutate(&policy)
			if err := policy.validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("policy error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestNewRefusesInvalidLifecycleTransitionsBeforeMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Policy)
		want   string
	}{
		{name: "waiting for merge progress", mutate: func(policy *Policy) {
			policy.EligibleStatuses = append(policy.EligibleStatuses, scheduler.StatusWaitingForMerge)
			policy.ProgressStatus = scheduler.StatusWaitingForMerge
		}, want: "cannot use waiting-for-merge as its progress state"},
		{name: "progress cannot become terminal", mutate: func(policy *Policy) {
			policy.CanTransition = func(scheduler.Status, scheduler.Status) bool { return false }
		}, want: "cannot transition from progress state resetting to terminal state reset"},
		{name: "eligible state cannot enter progress", mutate: func(policy *Policy) {
			policy.CanTransition = func(from, to scheduler.Status) bool {
				return from == policy.ProgressStatus && to == policy.TerminalStatus
			}
		}, want: "cannot transition from eligible state failed to progress state resetting"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &policyStateStore{}
			policy := testPolicy()
			test.mutate(&policy)

			module, err := New(Config{
				Store: store, RepositoryRoot: "repository", CommonDirectory: "common",
				StateDirectory: "state", GitExecutable: "git",
			}, policy)
			if module != nil || err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("constructed module, error = %v, %v; want %q", module, err, test.want)
			}
			if store.saves != 0 {
				t.Fatalf("state mutations during refused construction = %d, want 0", store.saves)
			}
		})
	}
}

func TestBuildRefusesWaitingForMergeProgressBeforePlanning(t *testing.T) {
	policy := testPolicy()
	policy.EligibleStatuses = append(policy.EligibleStatuses, scheduler.StatusWaitingForMerge)
	policy.ProgressStatus = scheduler.StatusWaitingForMerge

	_, err := Build(policy, Snapshot{Run: scheduler.Run{Status: scheduler.StatusWaitingForMerge}})
	if err == nil || !strings.Contains(err.Error(), "cannot use waiting-for-merge as its progress state") {
		t.Fatalf("planning error = %v", err)
	}
}

func TestExecutablePlansEqualRejectsRenderedEqualPrivateActionIdentity(t *testing.T) {
	base := Plan{Actions: []Action{{
		kind: actionRemoveIssueLabel, description: "same rendered action", label: "owned",
	}}}
	tests := []struct {
		name   string
		mutate func(*Action)
	}{
		{name: "kind", mutate: func(action *Action) { action.kind = actionAddIssueLabel }},
		{name: "pull request", mutate: func(action *Action) { action.pullRequest = 99 }},
		{name: "label", mutate: func(action *Action) { action.label = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			changed.Actions = append([]Action(nil), base.Actions...)
			test.mutate(&changed.Actions[0])
			if !PlansEqual(base, changed) {
				t.Fatal("private action identity unexpectedly changed rendered plan")
			}
			if executablePlansEqual(base, changed) {
				t.Fatal("rendered-equal private action identity was executable")
			}
		})
	}
}

func TestRetireRefusesRenderedEqualPrivateActionIdentityBeforeMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Action)
	}{
		{name: "kind", mutate: func(action *Action) { action.kind = actionFinalize }},
		{name: "pull request", mutate: func(action *Action) { action.pullRequest = 99 }},
		{name: "label", mutate: func(action *Action) { action.label = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			mutationLog := filepath.Join(root, "mutations")
			git := writeRetirementExecutable(t, `#!/bin/sh
case "$*" in
  *" remote get-url origin") printf '%s\n' 'https://github.com/acme/widgets.git' ;;
  *) echo "unexpected git: $*" >&2; exit 9 ;;
esac
`)
			gh := writeRetirementExecutable(t, `#!/bin/sh
case "$*" in
  "repo view --json nameWithOwner,defaultBranchRef")
    printf '%s\n' '{"nameWithOwner":"acme/widgets","defaultBranchRef":{"name":"main"}}' ;;
  "issue view 42 --repo acme/widgets --json number,url,state,labels")
    printf '%s\n' '{"number":42,"url":"https://github.com/acme/widgets/issues/42","state":"OPEN","labels":[{"name":"owned"}]}' ;;
  issue\ edit\ *) printf '%s\n' "$*" >> `+shellQuote(mutationLog)+` ;;
  *) echo "unexpected gh: $*" >&2; exit 9 ;;
esac
`)
			run := scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint}
			lease := scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"}
			store := &policyStateStore{current: state.State{Repo: "acme/widgets", DefaultBranch: "main"}}
			policy := testPolicy()
			policy.SelectRun = func(state.State) (scheduler.Run, scheduler.Lease, error) { return run, lease, nil }
			module, err := New(Config{
				Store: store, GitHub: ghadapter.Client{Executable: gh, Dir: root}, RepositoryRoot: root,
				CommonDirectory: root, StateDirectory: root, GitExecutable: git,
			}, policy)
			if err != nil {
				t.Fatal(err)
			}
			approved, err := module.Inspect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			changed := approved
			changed.Actions = append([]Action(nil), approved.Actions...)
			test.mutate(&changed.Actions[0])
			if !PlansEqual(approved, changed) {
				t.Fatal("private action identity unexpectedly changed rendered plan")
			}
			err = module.Retire(context.Background(), changed)
			if err == nil || !strings.Contains(err.Error(), "Plan changed after confirmation") {
				t.Fatalf("authorization error = %v", err)
			}
			if store.saves != 0 {
				t.Fatalf("state mutations = %d, want 0", store.saves)
			}
			if _, err := os.Stat(mutationLog); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("GitHub mutation occurred before refusal: %v", err)
			}
		})
	}
}

func TestNewRefusesIncompleteConfiguration(t *testing.T) {
	if _, err := New(Config{}, testPolicy()); err == nil || !strings.Contains(err.Error(), "configuration is incomplete") {
		t.Fatalf("incomplete configuration error = %v", err)
	}
}

func TestValidateRefusesStatusOutsideLifecyclePolicy(t *testing.T) {
	service := Service{policy: testPolicy()}
	err := service.Validate(Plan{Snapshot: Snapshot{Run: scheduler.Run{Status: scheduler.StatusRunning}}})
	if err == nil || !strings.Contains(err.Error(), "not eligible for Reset") {
		t.Fatalf("status eligibility error = %v", err)
	}
}

func TestBuildReturnsLifecycleEligibilityFailure(t *testing.T) {
	policy := testPolicy()
	policy.ValidateSnapshot = func(Snapshot) error { return errTestEligibility }
	snapshot := Snapshot{
		Run:   scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed},
		Lease: scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"},
		Issue: Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42"},
	}
	if _, err := Build(policy, snapshot); err != errTestEligibility {
		t.Fatalf("eligibility error = %v", err)
	}
}

var errTestEligibility = &testError{"ineligible"}

type testError struct{ message string }

func (e *testError) Error() string { return e.message }

type policyStateStore struct {
	current state.State
	saves   int
	saved   []state.State
}

func (s *policyStateStore) Preview() (state.State, bool, error) {
	return s.current, true, nil
}

func (s *policyStateStore) Save(current state.State) error {
	s.current = current
	s.saves++
	snapshot := current
	snapshot.Runs = append([]scheduler.Run(nil), current.Runs...)
	snapshot.Leases = append([]scheduler.Lease(nil), current.Leases...)
	s.saved = append(s.saved, snapshot)
	return nil
}

func actionDescriptions(plan Plan) []string {
	descriptions := make([]string, len(plan.Actions))
	for index, action := range plan.Actions {
		descriptions[index] = action.String()
	}
	return descriptions
}

func testPolicy() Policy {
	return Policy{
		Operation: "Reset",
		SelectRun: func(state.State) (scheduler.Run, scheduler.Lease, error) {
			return scheduler.Run{}, scheduler.Lease{}, nil
		},
		ValidateSnapshot:  func(Snapshot) error { return nil },
		EligibleStatuses:  []scheduler.Status{scheduler.StatusFailed, scheduler.StatusResetting, scheduler.StatusReset},
		CanTransition:     scheduler.CanTransition,
		Explanation:       func(scheduler.Run) string { return "explanation" },
		ExplanationAction: "explain retirement",
		Labels:            LabelOutcome{Remove: []string{"owned"}, Add: []string{"available"}},
		ProgressStatus:    scheduler.StatusResetting,
		TerminalStatus:    scheduler.StatusReset,
	}
}
