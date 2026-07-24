package scheduler

// RequiresLease reports whether a Run status represents unfinished work that
// must retain active ownership of its issue.
func RequiresLease(status Status) bool {
	switch status {
	case StatusClaimed, StatusWorktreeReady, StatusRunning, StatusWaitingForMerge, StatusSuspended, StatusResetting:
		return true
	default:
		return false
	}
}

// CanTransition defines every persisted Run-state transition. Reset may move
// an eligible unfinished Run through resetting before it becomes reset.
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
		return to == StatusRunning || to == StatusWaitingForMerge || to == StatusMerged || to == StatusNeedsHuman || to == StatusResetting || to == StatusReset
	case StatusWaitingForMerge:
		return to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	case StatusFailed, StatusNeedsHuman:
		return to == StatusResetting || to == StatusReset
	case StatusResetting:
		return to == StatusReset
	default:
		return false
	}
}
