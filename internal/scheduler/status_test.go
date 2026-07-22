package scheduler

import "testing"

func TestRunStateTransitionsAreExplicit(t *testing.T) {
	t.Parallel()

	allowed := [][2]Status{
		{StatusClaimed, StatusWorktreeReady},
		{StatusClaimed, StatusFailed},
		{StatusClaimed, StatusMerged},
		{StatusWorktreeReady, StatusRunning},
		{StatusRunning, StatusWaitingForMerge},
		{StatusRunning, StatusMerged},
		{StatusRunning, StatusFailed},
		{StatusRunning, StatusNeedsHuman},
		{StatusWaitingForMerge, StatusWaitingForMerge},
		{StatusWaitingForMerge, StatusMerged},
		{StatusWaitingForMerge, StatusNeedsHuman},
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
	}
	for _, transition := range rejected {
		if CanTransition(transition[0], transition[1]) {
			t.Errorf("transition %s -> %s was allowed", transition[0], transition[1])
		}
	}
}
