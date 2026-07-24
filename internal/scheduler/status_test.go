package scheduler

import "testing"

func TestRequiresLeaseCoversEveryStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status Status
		want   bool
	}{
		{StatusClaimed, true},
		{StatusWorktreeReady, true},
		{StatusRunning, true},
		{StatusWaitingForMerge, true},
		{StatusSuspended, true},
		{StatusResetting, true},
		{StatusReset, false},
		{StatusMerged, false},
		{StatusFailed, false},
		{StatusNeedsHuman, false},
	}
	for _, test := range tests {
		if got := RequiresLease(test.status); got != test.want {
			t.Errorf("RequiresLease(%q) = %t, want %t", test.status, got, test.want)
		}
	}
}

func TestRunStateTransitionsAreExplicit(t *testing.T) {
	t.Parallel()

	allowed := [][2]Status{
		{StatusClaimed, StatusWorktreeReady},
		{StatusClaimed, StatusFailed},
		{StatusClaimed, StatusMerged},
		{StatusWorktreeReady, StatusRunning},
		{StatusRunning, StatusWaitingForMerge},
		{StatusRunning, StatusSuspended},
		{StatusRunning, StatusMerged},
		{StatusRunning, StatusFailed},
		{StatusRunning, StatusNeedsHuman},
		{StatusWaitingForMerge, StatusWaitingForMerge},
		{StatusWaitingForMerge, StatusMerged},
		{StatusWaitingForMerge, StatusNeedsHuman},
		{StatusSuspended, StatusRunning},
		{StatusSuspended, StatusWaitingForMerge},
		{StatusSuspended, StatusMerged},
		{StatusSuspended, StatusNeedsHuman},
		{StatusFailed, StatusResetting},
		{StatusNeedsHuman, StatusResetting},
		{StatusSuspended, StatusResetting},
		{StatusFailed, StatusReset},
		{StatusSuspended, StatusReset},
		{StatusResetting, StatusReset},
	}
	for _, transition := range allowed {
		if !CanTransition(transition[0], transition[1]) {
			t.Errorf("transition %s -> %s was rejected", transition[0], transition[1])
		}
	}

	rejected := [][2]Status{
		{StatusMerged, StatusRunning},
		{StatusFailed, StatusClaimed},
		{StatusNeedsHuman, StatusRunning},
		{StatusClaimed, StatusWaitingForMerge},
		{StatusReset, StatusRunning},
		{StatusReset, StatusResetting},
	}
	for _, transition := range rejected {
		if CanTransition(transition[0], transition[1]) {
			t.Errorf("transition %s -> %s was allowed", transition[0], transition[1])
		}
	}
}
