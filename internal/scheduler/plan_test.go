package scheduler

import (
	"testing"
	"time"
)

func TestPlanStartsOldestEligibleCandidatesWithinCapacity(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	plan := Plan(Snapshot{
		Candidates: []Candidate{
			{Number: 30, CreatedAt: base.Add(2 * time.Hour)},
			{Number: 10, CreatedAt: base},
			{Number: 20, CreatedAt: base.Add(time.Hour)},
			{Number: 5, CreatedAt: base.Add(-time.Hour), Blockers: []Blocker{{Number: 4}}},
		},
		Runs:   []Run{{Issue: 20, RunID: "run-20", Status: StatusRunning}},
		Leases: []Lease{{LeaseID: "run-20", Issue: 20, RunID: "run-20"}},
	}, 3)

	if len(plan.Starts) != 2 {
		t.Fatalf("got %d starts, want 2", len(plan.Starts))
	}
	if plan.Starts[0].Number != 10 || plan.Starts[1].Number != 30 {
		t.Fatalf("got starts %v, want issues [10 30]", issueNumbers(plan.Starts))
	}
}

func TestPlanDoesNotStartAnIssueWithAnActiveLease(t *testing.T) {
	t.Parallel()

	plan := Plan(Snapshot{
		Candidates: []Candidate{{Number: 10, CreatedAt: time.Now()}},
		Runs:       []Run{{Issue: 10, RunID: "run-10", Status: StatusFailed}},
		Leases:     []Lease{{LeaseID: "run-10", Issue: 10, RunID: "run-10"}},
	}, 3)

	if len(plan.Starts) != 0 {
		t.Fatalf("got starts %v, want none", issueNumbers(plan.Starts))
	}
}

func TestPlanCountsOnlyActiveRunsAgainstCapacity(t *testing.T) {
	t.Parallel()

	plan := Plan(Snapshot{
		Candidates: []Candidate{
			{Number: 10, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
			{Number: 20, CreatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		},
		Runs: []Run{
			{Issue: 1, RunID: "run-1", Status: StatusMerged},
			{Issue: 2, RunID: "run-2", Status: StatusRunning},
		},
		Leases: []Lease{{LeaseID: "run-2", Issue: 2, RunID: "run-2"}},
	}, 2)

	if len(plan.Starts) != 1 || plan.Starts[0].Number != 10 {
		t.Fatalf("got starts %v, want [10]", issueNumbers(plan.Starts))
	}
}

func TestPlanCountsNeedsHumanRunWithRetainedWorkerIdentityAgainstCapacity(t *testing.T) {
	t.Parallel()

	plan := Plan(Snapshot{
		Candidates: []Candidate{{Number: 2, CreatedAt: time.Now()}},
		Runs:       []Run{{Issue: 1, RunID: "run-1", Status: StatusNeedsHuman, PID: 1234}},
		Leases:     []Lease{{LeaseID: "run-1", Issue: 1, RunID: "run-1"}},
	}, 1)
	if len(plan.Starts) != 0 {
		t.Fatalf("got starts %v, want retained live Worker to consume capacity", issueNumbers(plan.Starts))
	}
}

func TestPlanWaitingForMergeLeaseDoesNotConsumeWorkerCapacity(t *testing.T) {
	t.Parallel()

	plan := Plan(Snapshot{
		Candidates: []Candidate{{Number: 2, CreatedAt: time.Now()}},
		Runs:       []Run{{Issue: 1, RunID: "run-1", Status: StatusWaitingForMerge}},
		Leases:     []Lease{{LeaseID: "run-1", Issue: 1, RunID: "run-1"}},
	}, 1)
	if len(plan.Starts) != 1 || plan.Starts[0].Number != 2 {
		t.Fatalf("got starts %v, want [2]", issueNumbers(plan.Starts))
	}
}

func TestPlanAllowsNewRunWhenOnlyHistoricalRunsRemain(t *testing.T) {
	t.Parallel()

	plan := Plan(Snapshot{
		Candidates: []Candidate{{Number: 10, CreatedAt: time.Now()}},
		Runs: []Run{
			{Issue: 10, RunID: "merged", Status: StatusMerged},
			{Issue: 10, RunID: "failed", Status: StatusFailed},
		},
	}, 1)

	if len(plan.Starts) != 1 || plan.Starts[0].Number != 10 {
		t.Fatalf("got starts %v, want [10]", issueNumbers(plan.Starts))
	}
}

func issueNumbers(candidates []Candidate) []int {
	numbers := make([]int, len(candidates))
	for i, candidate := range candidates {
		numbers[i] = candidate.Number
	}
	return numbers
}
