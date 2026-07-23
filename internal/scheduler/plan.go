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
	StatusMerged          Status = "merged"
	StatusFailed          Status = "failed"
	StatusNeedsHuman      Status = "needs-human"
)

type WorkerMode string

const WorkerModePrint WorkerMode = "print"

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

type Run struct {
	Issue           int        `json:"issue"`
	RunID           string     `json:"runId"`
	Status          Status     `json:"status"`
	WorkerMode      WorkerMode `json:"workerMode"`
	PID             int        `json:"pid,omitempty"`
	ProcessIdentity string     `json:"processIdentity,omitempty"`
	Branch          string     `json:"branch,omitempty"`
	Worktree        string     `json:"worktree,omitempty"`
	SessionName     string     `json:"sessionName,omitempty"`
	LogPath         string     `json:"logPath,omitempty"`
	StderrPath      string     `json:"stderrPath,omitempty"`
	PullRequest     string     `json:"pullRequest,omitempty"`
	Error           string     `json:"error,omitempty"`
	StartedAt       time.Time  `json:"startedAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
	CompletedAt     *time.Time `json:"completedAt,omitempty"`
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
// capacity. Active Leases prevent overlapping Runs, and verified merged history
// prevents a completed issue from being admitted again.
func Plan(snapshot Snapshot, maxConcurrentIssues int) Schedule {
	if maxConcurrentIssues <= 0 {
		return Schedule{}
	}

	runsByID := make(map[string]Run, len(snapshot.Runs))
	completedIssues := make(map[int]struct{})
	for _, run := range snapshot.Runs {
		runsByID[run.RunID] = run
		if run.Status == StatusMerged {
			completedIssues[run.Issue] = struct{}{}
		}
	}
	leased := make(map[int]struct{}, len(snapshot.Leases))
	active := 0
	for _, lease := range snapshot.Leases {
		if run, exists := runsByID[lease.RunID]; exists && isActive(run.Status) {
			active++
		}
		leased[lease.Issue] = struct{}{}
	}

	available := maxConcurrentIssues - active
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
		if _, completed := completedIssues[candidate.Number]; completed {
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

func isActive(status Status) bool {
	switch status {
	case StatusClaimed, StatusWorktreeReady, StatusRunning:
		return true
	default:
		return false
	}
}
