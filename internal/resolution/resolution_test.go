package resolution

import (
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestPolicyAcceptsSupportedClosureReasonsAndPreservesHumanLabels(t *testing.T) {
	for _, reason := range []string{"completed", "not-planned"} {
		snapshot := retirement.Snapshot{
			Repository: "acme/widgets",
			Run:        scheduler.Run{Issue: 42, RunID: "run-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
			Lease:      scheduler.Lease{LeaseID: "lease-42", Issue: 42, RunID: "run-42"},
			Issue: retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: reason,
				Labels: []string{"in-progress", "ready-for-agent", "needs-info", "spec"}},
		}
		plan, err := retirement.Build(Policy("run-42"), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		joined := make([]string, len(plan.Actions))
		for index, action := range plan.Actions {
			joined[index] = action.String()
		}
		text := strings.Join(joined, "\n")
		for _, want := range []string{"resolving-externally", "remove issue label in-progress", "remove issue label ready-for-agent", "resolved-externally"} {
			if !strings.Contains(text, want) {
				t.Fatalf("%s plan missing %q: %s", reason, want, text)
			}
		}
		if strings.Contains(text, "needs-info") || strings.Contains(text, "spec") {
			t.Fatalf("human/unrelated label mutated: %s", text)
		}
	}
}

func TestPolicyRefusesOpenAndUnsupportedClosureState(t *testing.T) {
	for _, issue := range []retirement.Issue{
		{Number: 42, URL: "https://example.test/42", Open: true},
		{Number: 42, URL: "https://example.test/42"},
		{Number: 42, URL: "https://example.test/42", ClosureReason: "future"},
	} {
		snapshot := retirement.Snapshot{Run: scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusFailed}, Lease: scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"}, Issue: issue}
		if _, err := retirement.Build(Policy("run"), snapshot); err == nil {
			t.Fatalf("accepted issue %#v", issue)
		}
	}
}

func TestMergedExpectedPullRequestPlansCompletion(t *testing.T) {
	snapshot := retirement.Snapshot{
		Run:          scheduler.Run{Issue: 42, RunID: "run", Status: scheduler.StatusWaitingForMerge, PullRequest: "https://github.com/acme/widgets/pull/9", Branch: "agent/run"},
		Lease:        scheduler.Lease{LeaseID: "lease", Issue: 42, RunID: "run"},
		Issue:        retirement.Issue{Number: 42, URL: "https://github.com/acme/widgets/issues/42", ClosureReason: "completed"},
		PullRequests: []retirement.PullRequest{{Number: 9, URL: "https://github.com/acme/widgets/pull/9", Branch: "agent/run", Commit: strings.Repeat("a", 40), State: retirement.PullRequestMerged}},
	}
	plan, err := retirement.Build(Policy("run"), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if plan.TerminalState != scheduler.StatusMerged || len(plan.Actions) != 1 || !strings.Contains(plan.Actions[0].String(), "record Completion") {
		t.Fatalf("Completion plan = %#v", plan)
	}
}

func TestSelectorUsesLeaseAndHistoricalRerunPreservesResolutionMetadata(t *testing.T) {
	resolvedAt := time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC)
	current := state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 42, RunID: "old", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 42, RunID: "active", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 7, RunID: "historical", Status: scheduler.StatusResolvedExternally, WorkerMode: scheduler.WorkerModePrint, ResolvedExternallyAt: &resolvedAt, ClosureReason: "completed"},
	}, Leases: []scheduler.Lease{{LeaseID: "active", Issue: 42, RunID: "active"}}}
	run, lease, err := Policy("42").SelectRun(current)
	if err != nil || run.RunID != "active" || lease.RunID != "active" {
		t.Fatalf("issue selection = %#v %#v %v", run, lease, err)
	}
	run, lease, err = Policy("historical").SelectRun(current)
	if err != nil || run.ResolvedExternallyAt != &resolvedAt || lease.LeaseID != "" {
		t.Fatalf("historical selection = %#v %#v %v", run, lease, err)
	}
}
