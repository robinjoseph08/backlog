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

func TestActiveAndInterventionRequiredPartitionEveryStatus(t *testing.T) {
	t.Parallel()

	active := map[Status]bool{
		StatusClaimed: true, StatusWorktreeReady: true, StatusRunning: true,
		StatusWaitingForMerge: true, StatusSuspended: true,
	}
	for _, status := range []Status{
		StatusClaimed, StatusWorktreeReady, StatusRunning, StatusWaitingForMerge, StatusSuspended,
		StatusResetting, StatusReset, StatusMerged, StatusFailed, StatusNeedsHuman,
	} {
		if got := IsActive(status); got != active[status] {
			t.Errorf("IsActive(%q) = %t, want %t", status, got, active[status])
		}
		if got := RequiresIntervention(status); got == active[status] {
			t.Errorf("RequiresIntervention(%q) = %t, want %t", status, got, !active[status])
		}
	}
}

func TestIsTerminalCoversEveryStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status Status
		want   bool
	}{
		{StatusClaimed, false},
		{StatusWorktreeReady, false},
		{StatusRunning, false},
		{StatusWaitingForMerge, false},
		{StatusSuspended, false},
		{StatusResetting, false},
		{StatusReset, true},
		{StatusMerged, true},
		{StatusFailed, true},
		{StatusNeedsHuman, true},
	}
	for _, test := range tests {
		if got := IsTerminal(test.status); got != test.want {
			t.Errorf("IsTerminal(%q) = %t, want %t", test.status, got, test.want)
		}
	}
}

func TestRunStateTransitionsAreExplicit(t *testing.T) {
	t.Parallel()

	allowed := [][2]Status{
		{StatusClaimed, StatusWorktreeReady},
		{StatusClaimed, StatusResetting},
		{StatusClaimed, StatusReset},
		{StatusClaimed, StatusFailed},
		{StatusClaimed, StatusMerged},
		{StatusWorktreeReady, StatusRunning},
		{StatusWorktreeReady, StatusResetting},
		{StatusWorktreeReady, StatusReset},
		{StatusRunning, StatusWaitingForMerge},
		{StatusRunning, StatusSuspended},
		{StatusRunning, StatusResetting},
		{StatusRunning, StatusReset},
		{StatusRunning, StatusMerged},
		{StatusRunning, StatusFailed},
		{StatusRunning, StatusNeedsHuman},
		{StatusWaitingForMerge, StatusWaitingForMerge},
		{StatusWaitingForMerge, StatusMerged},
		{StatusWaitingForMerge, StatusNeedsHuman},
		{StatusWaitingForMerge, StatusResetting},
		{StatusSuspended, StatusRunning},
		{StatusSuspended, StatusWaitingForMerge},
		{StatusSuspended, StatusMerged},
		{StatusSuspended, StatusNeedsHuman},
		{StatusFailed, StatusResetting},
		{StatusNeedsHuman, StatusResetting},
		{StatusSuspended, StatusResetting},
		{StatusFailed, StatusReset},
		{StatusNeedsHuman, StatusReset},
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
