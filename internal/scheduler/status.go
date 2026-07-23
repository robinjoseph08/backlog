package scheduler

// RequiresLease reports whether a Run status represents unfinished work that
// must retain active ownership of its issue.
func RequiresLease(status Status) bool {
	switch status {
	case StatusClaimed, StatusWorktreeReady, StatusRunning, StatusWaitingForMerge, StatusSuspended:
		return true
	default:
		return false
	}
}

// CanTransition defines every persisted run-state transition. Failed,
// needs-human, and merged Runs are terminal. Retry releases an active Lease,
// and any later scheduler admission creates a new Run.
func CanTransition(from, to Status) bool {
	if from == to {
		return from == StatusWaitingForMerge
	}
	switch from {
	case StatusClaimed:
		return to == StatusWorktreeReady || to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	case StatusWorktreeReady:
		return to == StatusRunning || to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	case StatusRunning:
		return to == StatusWaitingForMerge || to == StatusSuspended || to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	case StatusSuspended:
		return to == StatusRunning || to == StatusMerged || to == StatusNeedsHuman
	case StatusWaitingForMerge:
		return to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	default:
		return false
	}
}
