package retirement

import (
	"bytes"
	"strings"
	"testing"

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

func TestModuleAppliesDistinctExternalResolutionPolicy(t *testing.T) {
	const (
		statusResolvingExternally scheduler.Status = "resolving-externally"
		statusResolvedExternally  scheduler.Status = "resolved-externally"
	)
	policy := testPolicy()
	policy.Operation = "External Resolution"
	policy.EligibleStatuses = []scheduler.Status{
		scheduler.StatusFailed, statusResolvingExternally, statusResolvedExternally,
	}
	policy.Labels = LabelOutcome{Remove: []string{"ready-for-agent", "in-progress"}}
	policy.ProgressStatus = statusResolvingExternally
	policy.TerminalStatus = statusResolvedExternally

	run := scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed}
	lease := scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"}
	snapshot := Snapshot{
		Run: run, Lease: lease,
		Issue: Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", Labels: []string{"ready-for-agent", "in-progress"}},
	}
	plan, err := Build(policy, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"mark Run run-42 resolving-externally while retaining Lease lease-42",
		"remove issue label ready-for-agent from https://github.com/acme/widgets/issues/42",
		"remove issue label in-progress from https://github.com/acme/widgets/issues/42",
		"mark Run run-42 resolved-externally and release Lease lease-42",
	}
	if strings.Join(actionDescriptions(plan), "\n") != strings.Join(want, "\n") {
		t.Fatalf("External Resolution actions = %q, want %q", actionDescriptions(plan), want)
	}
	var output bytes.Buffer
	WritePlan(&output, plan)
	if !strings.Contains(output.String(), "External Resolution Plan for issue #42") {
		t.Fatalf("External Resolution operation missing from plan output: %q", output.String())
	}
	if !strings.Contains(output.String(), "Issue: https://github.com/acme/widgets/issues/42 (closed; labels: in-progress, ready-for-agent)") {
		t.Fatalf("External Resolution issue state missing from plan output: %q", output.String())
	}

	module, err := New(Config{
		Store: &policyStateStore{}, RepositoryRoot: "/repo", CommonDirectory: "/repo/.git",
		StateDirectory: "/state", GitExecutable: "git",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Validate(plan); err != nil {
		t.Fatalf("External Resolution plan validation: %v", err)
	}
	terminal := plan
	terminal.Snapshot.Run.Status = statusResolvedExternally
	terminal.Snapshot.Lease = scheduler.Lease{}
	if err := module.Validate(terminal); err != nil {
		t.Fatalf("External Resolution terminal validation: %v", err)
	}
	outsidePolicy := plan
	outsidePolicy.Snapshot.Run.Status = scheduler.StatusResetting
	if err := module.Validate(outsidePolicy); err == nil || !strings.Contains(err.Error(), "not eligible for External Resolution") {
		t.Fatalf("External Resolution eligibility error = %v", err)
	}
}

func TestPolicyValidationRefusesEveryIncompletePolicyShape(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Policy)
		want   string
	}{
		{name: "core behavior", mutate: func(policy *Policy) { policy.Explanation = nil }, want: "policy is incomplete"},
		{name: "lifecycle states", mutate: func(policy *Policy) { policy.ProgressStatus = "" }, want: "incomplete lifecycle states"},
		{name: "equal lifecycle states", mutate: func(policy *Policy) { policy.TerminalStatus = policy.ProgressStatus }, want: "distinct progress and terminal states"},
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
}

func (s *policyStateStore) Preview() (state.State, bool, error) {
	return s.current, true, nil
}

func (s *policyStateStore) Save(current state.State) error {
	s.current = current
	s.saves++
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
		Explanation:       func(scheduler.Run) string { return "explanation" },
		ExplanationAction: "explain retirement",
		Labels:            LabelOutcome{Remove: []string{"owned"}, Add: []string{"available"}},
		ProgressStatus:    scheduler.StatusResetting,
		TerminalStatus:    scheduler.StatusReset,
	}
}
