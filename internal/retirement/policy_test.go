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
	if strings.Join(plan.Actions, "\n") != strings.Join(want, "\n") {
		t.Fatalf("actions = %q, want %q", plan.Actions, want)
	}
	var output bytes.Buffer
	WritePlan(&output, plan)
	if !strings.Contains(output.String(), "Reset Plan for issue #42") {
		t.Fatalf("policy operation missing from plan output: %q", output.String())
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
		{name: "label outcome", mutate: func(policy *Policy) { policy.Labels = LabelOutcome{} }, want: "no label outcome"},
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
