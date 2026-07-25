package scheduler

import (
	"sort"
	"time"
)

type Status string

const (
	StatusClaimed         Status = "claimed"
	StatusWorktreeReady   Status = "worktree-ready"
	StatusRunning         Status = "running"
	StatusWaitingForMerge Status = "waiting-for-merge"
	StatusSuspended       Status = "suspended"
	StatusResetting       Status = "resetting"
	StatusReset           Status = "reset"
	StatusMerged          Status = "merged"
	StatusFailed          Status = "failed"
	StatusNeedsHuman      Status = "needs-human"
)

type WorkerMode string

const (
	WorkerModePrint WorkerMode = "print"
	WorkerModeRPC   WorkerMode = "rpc"
)

type Blocker struct {
	Owner  string `json:"owner,omitempty"`
	Repo   string `json:"repo,omitempty"`
	Number int    `json:"number"`
	Title  string `json:"title,omitempty"`
	URL    string `json:"url,omitempty"`
}

type Candidate struct {
	Number    int
	Title     string
	URL       string
	CreatedAt time.Time
	Blockers  []Blocker
}

type ContinuationBoundary struct {
	SessionID   string    `json:"sessionId"`
	SessionFile string    `json:"sessionFile"`
	Worktree    string    `json:"worktree"`
	LeafID      string    `json:"leafId"`
	EntryCount  int       `json:"entryCount"`
	SHA256      string    `json:"sha256"`
	VerifiedAt  time.Time `json:"verifiedAt"`
}

type Run struct {
	Issue           int                   `json:"issue"`
	IssueTitle      string                `json:"issueTitle,omitempty"`
	IssueURL        string                `json:"issueUrl,omitempty"`
	RunID           string                `json:"runId"`
	Status          Status                `json:"status"`
	WorkerMode      WorkerMode            `json:"workerMode"`
	PID             int                   `json:"pid,omitempty"`
	ProcessIdentity string                `json:"processIdentity,omitempty"`
	Branch          string                `json:"branch,omitempty"`
	Worktree        string                `json:"worktree,omitempty"`
	SessionName     string                `json:"sessionName,omitempty"`
	SessionID       string                `json:"sessionId,omitempty"`
	SessionDir      string                `json:"sessionDir,omitempty"`
	Continuation    *ContinuationBoundary `json:"continuation,omitempty"`
	ResumePending   bool                  `json:"resumePending,omitempty"`
	LogPath         string                `json:"logPath,omitempty"`
	StderrPath      string                `json:"stderrPath,omitempty"`
	WorkerLogOpen   bool                  `json:"workerLogOpen,omitempty"`
	PullRequest     string                `json:"pullRequest,omitempty"`
	Error           string                `json:"error,omitempty"`
	StartedAt       time.Time             `json:"startedAt"`
	WorkerStartedAt time.Time             `json:"workerStartedAt,omitempty"`
	UpdatedAt       time.Time             `json:"updatedAt"`
	CompletedAt     *time.Time            `json:"completedAt,omitempty"`
}

type Lease struct {
	LeaseID string `json:"leaseId"`
	Issue   int    `json:"issue"`
	RunID   string `json:"runId"`
}

type Snapshot struct {
	Candidates []Candidate
	Runs       []Run
	Leases     []Lease
}

type Schedule struct {
	Starts []Candidate
}

// Plan selects the oldest eligible candidates that fit in the available worker
// capacity. Active Leases prevent overlapping Runs, while historical Runs do
// not prevent a reopened Candidate from being admitted again.
func Plan(snapshot Snapshot, maxConcurrentIssues int) Schedule {
	if maxConcurrentIssues <= 0 {
		return Schedule{}
	}

	runsByID := make(map[string]Run, len(snapshot.Runs))
	for _, run := range snapshot.Runs {
		runsByID[run.RunID] = run
	}
	leased := make(map[int]struct{}, len(snapshot.Leases))
	workerCount := 0
	for _, lease := range snapshot.Leases {
		if run, exists := runsByID[lease.RunID]; exists && consumesWorkerCapacity(run) {
			workerCount++
		}
		leased[lease.Issue] = struct{}{}
	}

	available := maxConcurrentIssues - workerCount
	if available <= 0 {
		return Schedule{}
	}

	eligible := make([]Candidate, 0, len(snapshot.Candidates))
	for _, candidate := range snapshot.Candidates {
		if len(candidate.Blockers) != 0 {
			continue
		}
		if _, exists := leased[candidate.Number]; exists {
			continue
		}
		eligible = append(eligible, candidate)
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].CreatedAt.Equal(eligible[j].CreatedAt) {
			return eligible[i].Number < eligible[j].Number
		}
		return eligible[i].CreatedAt.Before(eligible[j].CreatedAt)
	})
	if len(eligible) > available {
		eligible = eligible[:available]
	}
	return Schedule{Starts: eligible}
}

func consumesWorkerCapacity(run Run) bool {
	switch run.Status {
	case StatusClaimed, StatusWorktreeReady, StatusRunning, StatusSuspended:
		return true
	case StatusNeedsHuman:
		return run.PID > 0
	default:
		return false
	}
}
