package scheduler

// RequiresLease reports whether a Run status represents unfinished work that
// must retain active ownership of its issue.
func RequiresLease(status Status) bool {
	switch status {
	case StatusClaimed, StatusWorktreeReady, StatusRunning, StatusWaitingForMerge, StatusSuspended, StatusResetting, StatusResolvingExternally:
		return true
	default:
		return false
	}
}

// IsActive reports whether a leased Run can still advance autonomously or be
// reconciled by the Runner. Every other status with a retained Lease requires
// intervention.
func IsActive(status Status) bool {
	switch status {
	case StatusClaimed, StatusWorktreeReady, StatusRunning, StatusWaitingForMerge, StatusSuspended:
		return true
	default:
		return false
	}
}

// RequiresIntervention reports whether a retained Lease represents work that
// the Runner cannot advance autonomously.
func RequiresIntervention(status Status) bool {
	return !IsActive(status)
}

// IsTerminal reports whether a Run status stops producing autonomous work.
func IsTerminal(status Status) bool {
	switch status {
	case StatusReset, StatusResolvedExternally, StatusMerged, StatusFailed, StatusNeedsHuman:
		return true
	default:
		return false
	}
}

// CanTransition defines every persisted Run-state transition. Retirement may
// move an eligible unfinished Run through its durable progress state.
func CanTransition(from, to Status) bool {
	if from == to {
		return from == StatusWaitingForMerge
	}
	switch from {
	case StatusClaimed:
		return to == StatusWorktreeReady || to == StatusResetting || to == StatusReset || to == StatusResolvingExternally || to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	case StatusWorktreeReady:
		return to == StatusRunning || to == StatusResetting || to == StatusReset || to == StatusResolvingExternally || to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	case StatusRunning:
		return to == StatusWaitingForMerge || to == StatusSuspended || to == StatusResetting || to == StatusReset || to == StatusResolvingExternally || to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman
	case StatusSuspended:
		return to == StatusRunning || to == StatusWaitingForMerge || to == StatusMerged || to == StatusNeedsHuman || to == StatusResetting || to == StatusReset || to == StatusResolvingExternally
	case StatusWaitingForMerge:
		return to == StatusMerged || to == StatusFailed || to == StatusNeedsHuman || to == StatusResetting || to == StatusResolvingExternally
	case StatusFailed, StatusNeedsHuman:
		return to == StatusResetting || to == StatusReset || to == StatusResolvingExternally || to == StatusMerged
	case StatusResetting:
		return to == StatusReset || to == StatusResolvingExternally || to == StatusMerged
	case StatusResolvingExternally:
		return to == StatusResolvedExternally || to == StatusMerged
	default:
		return false
	}
}
