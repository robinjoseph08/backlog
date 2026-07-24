// Package reset derives read-only Reset Plans from conclusively inspected Run resources.
package reset

import (
	"fmt"
	"sort"
	"strings"

	"github.com/robinjoseph08/backlog/internal/scheduler"
)

type PullRequestState string

const (
	PullRequestOpen   PullRequestState = "open"
	PullRequestClosed PullRequestState = "closed"
	PullRequestMerged PullRequestState = "merged"
)

type Issue struct {
	Number int
	URL    string
	Open   bool
	Labels []string
}

type PullRequest struct {
	Number         int
	URL            string
	State          PullRequestState
	AutoMergeArmed bool
}

type Branch struct {
	Name    string
	Commit  string
	Present bool
}

type Worktree struct {
	Path    string
	Branch  string
	Commit  string
	Present bool
}

type Session struct {
	ID      string
	Dir     string
	Present bool
}

// Snapshot contains only known-present or known-absent resources. Inspectors
// return an error rather than constructing a Snapshot when any state is unknown.
type Snapshot struct {
	Run           scheduler.Run
	Lease         scheduler.Lease
	Issue         Issue
	PullRequests  []PullRequest
	RemoteBranch  Branch
	LocalBranch   Branch
	Worktree      Worktree
	Session       Session
	WorkerSummary string
}

type Plan struct {
	Snapshot Snapshot
	Actions  []string
}

var humanWorkflowLabels = map[string]struct{}{
	"needs-triage":    {},
	"needs-info":      {},
	"ready-for-human": {},
	"wontfix":         {},
}

// Build refuses unsafe resource combinations and orders only mutations whose
// postconditions are not already satisfied.
func Build(snapshot Snapshot) (Plan, error) {
	if err := validateIdentity(snapshot); err != nil {
		return Plan{}, err
	}
	if snapshot.Run.Status == scheduler.StatusMerged {
		return Plan{}, fmt.Errorf("Run %s is merged; merged work cannot be Reset", snapshot.Run.RunID)
	}
	labels := make(map[string]struct{}, len(snapshot.Issue.Labels))
	for _, label := range snapshot.Issue.Labels {
		normalized := strings.ToLower(label)
		labels[normalized] = struct{}{}
		if _, blocks := humanWorkflowLabels[normalized]; blocks {
			return Plan{}, fmt.Errorf("issue #%d has human workflow label %q", snapshot.Issue.Number, label)
		}
	}

	pullRequests := append([]PullRequest(nil), snapshot.PullRequests...)
	sort.Slice(pullRequests, func(i, j int) bool { return pullRequests[i].Number < pullRequests[j].Number })
	foundRecorded := snapshot.Run.PullRequest == ""
	pullNumbers := make(map[int]struct{}, len(pullRequests))
	pullURLs := make(map[string]struct{}, len(pullRequests))
	for _, pull := range pullRequests {
		if pull.Number <= 0 || pull.URL == "" {
			return Plan{}, fmt.Errorf("Run %s has a pull request with incomplete identity", snapshot.Run.RunID)
		}
		if _, duplicate := pullNumbers[pull.Number]; duplicate {
			return Plan{}, fmt.Errorf("Run %s has duplicate pull request number #%d", snapshot.Run.RunID, pull.Number)
		}
		if _, duplicate := pullURLs[pull.URL]; duplicate {
			return Plan{}, fmt.Errorf("Run %s has duplicate pull request URL %s", snapshot.Run.RunID, pull.URL)
		}
		pullNumbers[pull.Number] = struct{}{}
		pullURLs[pull.URL] = struct{}{}
		if pull.URL == snapshot.Run.PullRequest {
			foundRecorded = true
		}
		if pull.State == PullRequestMerged {
			return Plan{}, fmt.Errorf("pull request #%d is merged; merged work cannot be Reset", pull.Number)
		}
		if pull.State != PullRequestOpen && pull.State != PullRequestClosed {
			return Plan{}, fmt.Errorf("pull request #%d has unknown state %q", pull.Number, pull.State)
		}
		if pull.State == PullRequestClosed && pull.AutoMergeArmed {
			return Plan{}, fmt.Errorf("closed pull request #%d has uncertain auto-merge state", pull.Number)
		}
	}
	if !foundRecorded {
		return Plan{}, fmt.Errorf("recorded pull request %s was not found for Run branch %s", snapshot.Run.PullRequest, snapshot.Run.Branch)
	}
	if !snapshot.Issue.Open {
		return Plan{}, fmt.Errorf("issue #%d is closed without verified Completion; refusing unexplained closure", snapshot.Issue.Number)
	}

	plan := Plan{Snapshot: snapshot}
	for _, pull := range pullRequests {
		if pull.State != PullRequestOpen {
			continue
		}
		if pull.AutoMergeArmed {
			plan.Actions = append(plan.Actions, fmt.Sprintf("disable auto-merge for pull request #%d (%s)", pull.Number, pull.URL))
		}
		plan.Actions = append(plan.Actions, fmt.Sprintf("close unmerged pull request #%d (%s)", pull.Number, pull.URL))
	}
	if snapshot.RemoteBranch.Present {
		plan.Actions = append(plan.Actions, fmt.Sprintf("delete remote branch %s at %s", snapshot.RemoteBranch.Name, snapshot.RemoteBranch.Commit))
	}
	if snapshot.Worktree.Present {
		plan.Actions = append(plan.Actions, fmt.Sprintf("remove local worktree %s for %s at %s", snapshot.Worktree.Path, snapshot.Worktree.Branch, snapshot.Worktree.Commit))
	}
	if snapshot.LocalBranch.Present {
		plan.Actions = append(plan.Actions, fmt.Sprintf("delete local branch %s at %s", snapshot.LocalBranch.Name, snapshot.LocalBranch.Commit))
	}
	if snapshot.Session.Present {
		plan.Actions = append(plan.Actions, fmt.Sprintf("retire Pi session %s in %s", snapshot.Session.ID, snapshot.Session.Dir))
	}
	if _, present := labels["in-progress"]; present {
		plan.Actions = append(plan.Actions, fmt.Sprintf("remove issue label in-progress from %s", snapshot.Issue.URL))
	}
	if _, present := labels["ready-for-agent"]; !present {
		plan.Actions = append(plan.Actions, fmt.Sprintf("add issue label ready-for-agent to %s", snapshot.Issue.URL))
	}
	if snapshot.Run.Status != scheduler.StatusReset {
		plan.Actions = append(plan.Actions, fmt.Sprintf("mark Run %s reset and release Lease %s", snapshot.Run.RunID, snapshot.Lease.LeaseID))
	}
	return plan, nil
}

func validateIdentity(snapshot Snapshot) error {
	run, lease, issue := snapshot.Run, snapshot.Lease, snapshot.Issue
	if run.Issue <= 0 || run.RunID == "" || issue.URL == "" {
		return fmt.Errorf("Reset Run or issue identity is incomplete")
	}
	if issue.Number != run.Issue {
		return fmt.Errorf("Run %s and issue identity do not match", run.RunID)
	}
	if run.Status == scheduler.StatusReset {
		if lease.LeaseID != "" || lease.Issue != 0 || lease.RunID != "" {
			return fmt.Errorf("reset Run %s still has an active Lease", run.RunID)
		}
	} else if lease.LeaseID == "" || lease.Issue != run.Issue || lease.RunID != run.RunID {
		return fmt.Errorf("Run %s, Lease %s, and issue identity do not match", run.RunID, lease.LeaseID)
	}
	if err := validateBranch(snapshot.RemoteBranch, run.Branch, "remote"); err != nil {
		return err
	}
	if err := validateBranch(snapshot.LocalBranch, run.Branch, "local"); err != nil {
		return err
	}
	if snapshot.Worktree.Present && (snapshot.Worktree.Path != run.Worktree || snapshot.Worktree.Branch != run.Branch || snapshot.Worktree.Commit == "") {
		return fmt.Errorf("local worktree identity does not match Run %s", run.RunID)
	}
	if snapshot.Session.Present && (snapshot.Session.ID != run.SessionID || snapshot.Session.Dir != run.SessionDir) {
		return fmt.Errorf("Pi session identity does not match Run %s", run.RunID)
	}
	return nil
}

func validateBranch(branch Branch, expected, location string) error {
	if !branch.Present {
		return nil
	}
	if branch.Name == "" || branch.Name != expected || strings.TrimSpace(branch.Commit) == "" {
		return fmt.Errorf("%s branch identity does not match Run branch %q", location, expected)
	}
	return nil
}
