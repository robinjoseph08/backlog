// Package retirement conclusively inspects and retires artifacts owned by a Run.
package retirement

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
	Number        int
	URL           string
	Open          bool
	ClosureReason string
	Labels        []string
}

type PullRequest struct {
	Number         int
	URL            string
	Branch         string
	Commit         string
	State          PullRequestState
	AutoMergeArmed bool
	Explained      bool
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
	ID         string
	Dir        string
	ArchiveDir string
	Present    bool
	Archived   bool
}

// Snapshot contains only known-present or known-absent resources. Inspectors
// return an error rather than constructing a Snapshot when any state is unknown.
type Snapshot struct {
	Repository    string
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

type actionKind uint8

const (
	actionMarkProgress actionKind = iota + 1
	actionDisablePullRequestAutoMerge
	actionExplainPullRequest
	actionClosePullRequest
	actionDeleteRemoteBranch
	actionRemoveLocalWorktree
	actionDeleteLocalBranch
	actionArchiveSession
	actionRemoveIssueLabel
	actionAddIssueLabel
	actionFinalize
	actionFinalizeCompletion
)

// Action is one ordered mutation authorized by a Plan. Its executable identity
// is private so callers can approve and render actions without constructing a
// mutation that did not come from Build.
type Action struct {
	kind        actionKind
	description string
	pullRequest int
	label       string
}

func (a Action) String() string {
	return a.description
}

type Plan struct {
	Snapshot      Snapshot
	Actions       []Action
	Operation     string
	TerminalState scheduler.Status
}

func plannedAction(kind actionKind, description string) Action {
	return Action{kind: kind, description: description}
}

func plannedPullRequestAction(kind actionKind, pull PullRequest, description string) Action {
	return Action{kind: kind, description: description, pullRequest: pull.Number}
}

func plannedLabelAction(kind actionKind, label, description string) Action {
	return Action{kind: kind, description: description, label: label}
}

// Build refuses unsafe resource combinations and orders only mutations whose
// postconditions are not already satisfied.
func Build(policy Policy, snapshot Snapshot) (Plan, error) {
	if err := policy.validate(); err != nil {
		return Plan{}, err
	}
	if err := validateIdentity(policy, snapshot); err != nil {
		return Plan{}, err
	}
	if snapshot.Run.Status == scheduler.StatusMerged && snapshot.Run.Status != policy.TerminalStatus {
		return Plan{}, fmt.Errorf("Run %s is merged; merged work cannot be %s", snapshot.Run.RunID, policy.Operation)
	}
	pullRequests := append([]PullRequest(nil), snapshot.PullRequests...)
	sort.Slice(pullRequests, func(i, j int) bool { return pullRequests[i].Number < pullRequests[j].Number })
	foundRecorded := snapshot.Run.PullRequest == ""
	var mergedPulls []PullRequest
	pullNumbers := make(map[int]struct{}, len(pullRequests))
	pullURLs := make(map[string]struct{}, len(pullRequests))
	for _, pull := range pullRequests {
		if pull.Number <= 0 || pull.URL == "" || pull.Branch != snapshot.Run.Branch || strings.TrimSpace(pull.Commit) == "" {
			return Plan{}, fmt.Errorf("Run %s has a pull request with incomplete or mismatched branch identity", snapshot.Run.RunID)
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
			mergedPulls = append(mergedPulls, pull)
			continue
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
	if len(mergedPulls) != 0 && snapshot.Run.Status != policy.TerminalStatus && policy.AllowMergedCompletion {
		if (snapshot.Run.RecoveredRetirementRequired || snapshot.Run.RecoveryCount > 0) && !policy.RetireMergedCompletionArtifacts {
			return Plan{}, fmt.Errorf("recovered Completion requires the full recovered retirement policy")
		}
		if snapshot.Issue.Open {
			return Plan{}, fmt.Errorf("issue #%d is open; Completion requires a verified GitHub closure", snapshot.Issue.Number)
		}
		if snapshot.Run.PullRequest == "" && len(mergedPulls) != 1 {
			return Plan{}, fmt.Errorf("Run %s has multiple merged pull requests on expected branch %s; Completion requires an unambiguous pull request", snapshot.Run.RunID, snapshot.Run.Branch)
		}
		merged := mergedPulls[len(mergedPulls)-1]
		if snapshot.Run.PullRequest != "" {
			found := false
			for _, pull := range mergedPulls {
				if pull.URL == snapshot.Run.PullRequest {
					merged, found = pull, true
					break
				}
			}
			if !found {
				return Plan{}, fmt.Errorf("merged pull request is not the expected pull request %s", snapshot.Run.PullRequest)
			}
		}
		plan := Plan{Snapshot: snapshot, Operation: policy.Operation, TerminalState: scheduler.StatusMerged}
		if policy.RetireMergedCompletionArtifacts {
			if policy.ValidateMergedCompletionSnapshot != nil {
				if err := policy.ValidateMergedCompletionSnapshot(snapshot, merged); err != nil {
					return Plan{}, err
				}
			}
			_, removeLabels := policy.desiredLabels(snapshot.Issue.Labels)
			needsCleanup := snapshot.RemoteBranch.Present || snapshot.Worktree.Present || snapshot.LocalBranch.Present ||
				snapshot.Session.Present || len(removeLabels) > 0
			if needsCleanup && policy.MarkProgressBeforeMutation && snapshot.Run.Status != policy.ProgressStatus {
				plan.Actions = append(plan.Actions, plannedAction(actionMarkProgress, fmt.Sprintf("mark Run %s %s while retaining Lease %s", snapshot.Run.RunID, policy.ProgressStatus, snapshot.Lease.LeaseID)))
			}
			if snapshot.RemoteBranch.Present {
				plan.Actions = append(plan.Actions, plannedAction(actionDeleteRemoteBranch, fmt.Sprintf("delete remote branch %s at %s", snapshot.RemoteBranch.Name, snapshot.RemoteBranch.Commit)))
			}
			if snapshot.Worktree.Present {
				plan.Actions = append(plan.Actions, plannedAction(actionRemoveLocalWorktree, fmt.Sprintf("remove local worktree %s for %s at %s", snapshot.Worktree.Path, snapshot.Worktree.Branch, snapshot.Worktree.Commit)))
			}
			if snapshot.LocalBranch.Present {
				plan.Actions = append(plan.Actions, plannedAction(actionDeleteLocalBranch, fmt.Sprintf("delete local branch %s at %s", snapshot.LocalBranch.Name, snapshot.LocalBranch.Commit)))
			}
			if snapshot.Session.Present {
				plan.Actions = append(plan.Actions, plannedAction(actionArchiveSession, fmt.Sprintf("archive Pi session %s from %s to %s", snapshot.Session.ID, snapshot.Session.Dir, snapshot.Session.ArchiveDir)))
			}
			for _, label := range removeLabels {
				plan.Actions = append(plan.Actions, plannedLabelAction(actionRemoveIssueLabel, label, fmt.Sprintf("remove issue label %s from %s", label, snapshot.Issue.URL)))
			}
		}
		plan.Actions = append(plan.Actions, plannedPullRequestAction(actionFinalizeCompletion, merged,
			fmt.Sprintf("record Completion from merged expected pull request #%d (%s) and release Lease %s", merged.Number, merged.URL, snapshot.Lease.LeaseID)))
		return plan, nil
	}
	if err := policy.ValidateSnapshot(snapshot); err != nil {
		return Plan{}, err
	}
	if len(mergedPulls) != 0 && snapshot.Run.Status != policy.TerminalStatus {
		return Plan{}, fmt.Errorf("pull request #%d is merged; merged work cannot be %s", mergedPulls[0].Number, policy.Operation)
	}
	if snapshot.Run.Status == policy.TerminalStatus && policy.VerifyHistoricalOnly {
		if !policy.labelsSatisfied(snapshot.Issue.Labels) {
			return Plan{}, fmt.Errorf("historical %s Run %s has managed issue label drift; verification-only rerun will not mutate without a Lease", policy.TerminalStatus, snapshot.Run.RunID)
		}
		return Plan{Snapshot: snapshot, Operation: policy.Operation, TerminalState: policy.TerminalStatus}, nil
	}

	plan := Plan{Snapshot: snapshot, Operation: policy.Operation, TerminalState: policy.TerminalStatus}
	planning := snapshot
	planning.PullRequests = pullRequests
	hasPullRequestAction := false
	for _, pull := range pullRequests {
		if pull.State == PullRequestOpen || policy.RequireClosedExplanation && pull.State == PullRequestClosed && !pull.Explained {
			hasPullRequestAction = true
			break
		}
	}
	addLabels, removeLabels := policy.desiredLabels(snapshot.Issue.Labels)
	requiresProgressTransition := planning.Run.Status != policy.ProgressStatus && planning.Run.Status != policy.TerminalStatus &&
		!policy.CanTransition(planning.Run.Status, policy.TerminalStatus)
	needsProgress := requiresProgressTransition || hasPullRequestAction || snapshot.RemoteBranch.Present || snapshot.LocalBranch.Present ||
		snapshot.Worktree.Present || snapshot.Session.Present || len(addLabels) > 0 || len(removeLabels) > 0
	if needsProgress && planning.Run.Status != policy.ProgressStatus &&
		(planning.Run.Status != scheduler.StatusWaitingForMerge || policy.MarkProgressBeforeMutation) && planning.Run.Status != policy.TerminalStatus {
		plan.Actions = append(plan.Actions, plannedAction(actionMarkProgress, fmt.Sprintf("mark Run %s %s while retaining Lease %s", snapshot.Run.RunID, policy.ProgressStatus, snapshot.Lease.LeaseID)))
		planning.Run.Status = policy.ProgressStatus
	}
	for {
		pull, found := nextPullRequest(planning, policy.RequireClosedExplanation)
		if planning.Run.Status == scheduler.StatusWaitingForMerge && (!found || !pull.AutoMergeArmed) {
			plan.Actions = append(plan.Actions, plannedAction(actionMarkProgress, fmt.Sprintf("mark Run %s %s while retaining Lease %s", snapshot.Run.RunID, policy.ProgressStatus, snapshot.Lease.LeaseID)))
			planning.Run.Status = policy.ProgressStatus
			continue
		}
		if !found {
			break
		}
		for index := range planning.PullRequests {
			if planning.PullRequests[index].Number != pull.Number {
				continue
			}
			switch {
			case pull.AutoMergeArmed:
				plan.Actions = append(plan.Actions, plannedPullRequestAction(actionDisablePullRequestAutoMerge, pull, fmt.Sprintf("disable auto-merge for pull request #%d (%s)", pull.Number, pull.URL)))
				planning.PullRequests[index].AutoMergeArmed = false
				if planning.Run.Status == scheduler.StatusWaitingForMerge {
					plan.Actions = append(plan.Actions, plannedAction(actionMarkProgress, fmt.Sprintf("mark Run %s %s while retaining Lease %s", snapshot.Run.RunID, policy.ProgressStatus, snapshot.Lease.LeaseID)))
					planning.Run.Status = policy.ProgressStatus
				}
			case !pull.Explained:
				plan.Actions = append(plan.Actions, plannedPullRequestAction(actionExplainPullRequest, pull, fmt.Sprintf("%s on pull request #%d (%s)", policy.ExplanationAction, pull.Number, pull.URL)))
				planning.PullRequests[index].Explained = true
			default:
				plan.Actions = append(plan.Actions, plannedPullRequestAction(actionClosePullRequest, pull, fmt.Sprintf("close unmerged pull request #%d (%s)", pull.Number, pull.URL)))
				planning.PullRequests[index].State = PullRequestClosed
			}
			break
		}
	}
	if snapshot.RemoteBranch.Present {
		plan.Actions = append(plan.Actions, plannedAction(actionDeleteRemoteBranch, fmt.Sprintf("delete remote branch %s at %s", snapshot.RemoteBranch.Name, snapshot.RemoteBranch.Commit)))
	}
	if snapshot.Worktree.Present {
		plan.Actions = append(plan.Actions, plannedAction(actionRemoveLocalWorktree, fmt.Sprintf("remove local worktree %s for %s at %s", snapshot.Worktree.Path, snapshot.Worktree.Branch, snapshot.Worktree.Commit)))
	}
	if snapshot.LocalBranch.Present {
		plan.Actions = append(plan.Actions, plannedAction(actionDeleteLocalBranch, fmt.Sprintf("delete local branch %s at %s", snapshot.LocalBranch.Name, snapshot.LocalBranch.Commit)))
	}
	if snapshot.Session.Present {
		plan.Actions = append(plan.Actions, plannedAction(actionArchiveSession, fmt.Sprintf("archive Pi session %s from %s to %s", snapshot.Session.ID, snapshot.Session.Dir, snapshot.Session.ArchiveDir)))
	}
	for _, label := range removeLabels {
		plan.Actions = append(plan.Actions, plannedLabelAction(actionRemoveIssueLabel, label, fmt.Sprintf("remove issue label %s from %s", label, snapshot.Issue.URL)))
	}
	for _, label := range addLabels {
		plan.Actions = append(plan.Actions, plannedLabelAction(actionAddIssueLabel, label, fmt.Sprintf("add issue label %s to %s", label, snapshot.Issue.URL)))
	}
	if snapshot.Run.Status != policy.TerminalStatus {
		plan.Actions = append(plan.Actions, plannedAction(actionFinalize, fmt.Sprintf("mark Run %s %s and release Lease %s", snapshot.Run.RunID, policy.TerminalStatus, snapshot.Lease.LeaseID)))
	}
	return plan, nil
}

// NextPullRequest returns the owned open pull request targeted by the next
// retirement action. A waiting-for-merge Run must handle its recorded pull
// request before retirement can advance to other pull requests for its branch.
func NextPullRequest(snapshot Snapshot) (PullRequest, bool) {
	return nextPullRequest(snapshot, false)
}

func nextPullRequest(snapshot Snapshot, requireClosedExplanation bool) (PullRequest, bool) {
	if snapshot.Run.Status == scheduler.StatusWaitingForMerge {
		for _, pull := range snapshot.PullRequests {
			if pull.URL == snapshot.Run.PullRequest {
				return pull, pull.State == PullRequestOpen || requireClosedExplanation && pull.State == PullRequestClosed && !pull.Explained
			}
		}
		return PullRequest{}, false
	}

	result := PullRequest{}
	found := false
	for _, pull := range snapshot.PullRequests {
		if pull.State != PullRequestOpen && (!requireClosedExplanation || pull.State != PullRequestClosed || pull.Explained) {
			continue
		}
		if !found || (pull.AutoMergeArmed && !result.AutoMergeArmed) || (pull.AutoMergeArmed == result.AutoMergeArmed && pull.Number < result.Number) {
			result, found = pull, true
		}
	}
	return result, found
}

func validateIdentity(policy Policy, snapshot Snapshot) error {
	run, lease, issue := snapshot.Run, snapshot.Lease, snapshot.Issue
	if run.Issue <= 0 || run.RunID == "" || issue.URL == "" {
		return fmt.Errorf("%s Run or issue identity is incomplete", policy.Operation)
	}
	if issue.Number != run.Issue {
		return fmt.Errorf("Run %s and issue identity do not match", run.RunID)
	}
	if run.Status == policy.TerminalStatus {
		if lease.LeaseID != "" || lease.Issue != 0 || lease.RunID != "" {
			return fmt.Errorf("%s Run %s still has an active Lease", policy.TerminalStatus, run.RunID)
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
	if (snapshot.Session.Present || snapshot.Session.Archived) && (snapshot.Session.ID != run.SessionID || snapshot.Session.Dir != run.SessionDir || snapshot.Session.ArchiveDir == "") {
		return fmt.Errorf("Pi session identity does not match Run %s", run.RunID)
	}
	if snapshot.Session.Present && snapshot.Session.Archived {
		return fmt.Errorf("Pi session %s is present in both active and historical storage", run.SessionID)
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
