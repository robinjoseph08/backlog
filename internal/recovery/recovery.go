// Package recovery verifies and establishes durable continuation for an
// Intervention-required Run without creating new ownership identities.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	ghadapter "github.com/robinjoseph08/backlog/internal/github"
	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
	"github.com/robinjoseph08/backlog/internal/worker"
	"github.com/robinjoseph08/backlog/internal/worktree"
)

type Store interface {
	Load() (state.State, error)
	Save(state.State) error
}

type GitHub interface {
	OwnedRunResources(context.Context, string, int, string) (ghadapter.OwnedRunIssue, []ghadapter.OwnedRunPullRequest, error)
}

type Worktrees interface {
	Verify(context.Context, worktree.Assignment) error
	Cleanup(context.Context, worktree.Assignment) error
}

type GitIdentity struct {
	LocalCommit   string
	RemotePresent bool
	RemoteCommit  string
}

type GitVerifier interface {
	Verify(context.Context, scheduler.Run) (GitIdentity, error)
}

type Config struct {
	Store             Store
	GitHub            GitHub
	Worktrees         Worktrees
	Git               GitVerifier
	Repo              string
	Now               func() time.Time
	ProcessAlive      func(int) (bool, error)
	ProcessGroupAlive func(int) (bool, error)
}

type Outcome string

const (
	OutcomeSuspend    Outcome = "suspend"
	OutcomeAlready    Outcome = "already-recovered"
	OutcomeWaiting    Outcome = "waiting-for-merge"
	OutcomeCompletion Outcome = "completion"
)

type Plan struct {
	Run                scheduler.Run
	Lease              scheduler.Lease
	Boundary           scheduler.ContinuationBoundary
	DerivedOffline     bool
	Outcome            Outcome
	PullRequest        string
	PullRequestHead    string
	OriginalDiagnostic string
}

type Module struct{ config Config }

func New(config Config) (*Module, error) {
	if config.Store == nil || config.GitHub == nil || config.Worktrees == nil || config.Git == nil || config.Repo == "" || config.ProcessAlive == nil || config.ProcessGroupAlive == nil {
		return nil, errors.New("Recovery configuration is incomplete")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Module{config: config}, nil
}

func (m *Module) Inspect(ctx context.Context, selector string) (Plan, error) {
	current, err := m.config.Store.Load()
	if err != nil {
		return Plan{}, fmt.Errorf("load Recovery state: %w", err)
	}
	run, err := selectRun(current, selector)
	if err != nil {
		return Plan{}, err
	}
	lease, leased := findLease(current, run)
	if !leased {
		if run.Status != scheduler.StatusMerged || run.PullRequest == "" {
			return Plan{}, fmt.Errorf("Run %q no longer owns its Lease", run.RunID)
		}
		issue, pulls, inspectErr := m.config.GitHub.OwnedRunResources(ctx, m.config.Repo, run.Issue, run.Branch)
		if inspectErr != nil {
			return Plan{}, fmt.Errorf("verify historical Recovery Completion: %w", inspectErr)
		}
		if issue.State != "closed" || len(pulls) != 1 || !pulls[0].Merged || pulls[0].URL != run.PullRequest {
			return Plan{}, errors.New("historical Recovery Completion no longer matches one merged expected pull request and closed issue")
		}
		return Plan{Run: run, Outcome: OutcomeCompletion, PullRequest: run.PullRequest, PullRequestHead: pulls[0].Commit}, nil
	}
	already := run.Status == scheduler.StatusSuspended && run.RecoveryCount > 0
	if run.Status != scheduler.StatusFailed && run.Status != scheduler.StatusNeedsHuman && !already {
		return Plan{}, fmt.Errorf("Run %q is not eligible for Recovery in state %q", run.RunID, run.Status)
	}
	if already && run.Continuation == nil {
		return Plan{}, fmt.Errorf("Run %q has no persisted Recovery continuation boundary", run.RunID)
	}
	if run.WorkerMode != scheduler.WorkerModeRPC || run.Branch == "" || run.Worktree == "" || run.SessionID == "" || run.SessionDir == "" {
		return Plan{}, fmt.Errorf("Run %q has incomplete RPC continuation identities", run.RunID)
	}
	if err := m.verifyAbsent(run); err != nil {
		return Plan{}, err
	}

	issue, pulls, err := m.config.GitHub.OwnedRunResources(ctx, m.config.Repo, run.Issue, run.Branch)
	if err != nil {
		return Plan{}, fmt.Errorf("verify issue and expected-branch pull request state: %w", err)
	}
	if len(pulls) > 1 {
		return Plan{}, fmt.Errorf("expected branch has %d pull requests; continuation is ambiguous", len(pulls))
	}
	if run.PullRequest != "" && (len(pulls) == 0 || pulls[0].URL != run.PullRequest) {
		return Plan{}, fmt.Errorf("recorded expected pull request %q no longer matches the expected branch", run.PullRequest)
	}
	outcome := OutcomeSuspend
	pullRequest := ""
	if len(pulls) == 1 {
		pull := pulls[0]
		pullRequest = pull.URL
		switch {
		case pull.Merged && issue.State == "closed":
			outcome = OutcomeCompletion
		case pull.Merged:
			return Plan{}, errors.New("expected pull request merged but issue remains open")
		case pull.State == "closed":
			return Plan{}, errors.New("expected pull request is closed without merge; safe continuation is unsupported")
		case pull.AutoMergeArmed:
			outcome = OutcomeWaiting
		}
	}
	if outcome == OutcomeCompletion || outcome == OutcomeWaiting {
		if outcome == OutcomeWaiting && issue.State != "open" {
			return Plan{}, errors.New("issue closed while expected pull request remains armed; External Resolution takes precedence")
		}
		pullHead := ""
		if len(pulls) == 1 {
			pullHead = pulls[0].Commit
		}
		return Plan{Run: run, Lease: lease, Outcome: outcome, PullRequest: pullRequest, PullRequestHead: pullHead, OriginalDiagnostic: run.Error}, nil
	}
	if err := verifyIssue(issue); err != nil {
		return Plan{}, err
	}
	identity, err := m.config.Git.Verify(ctx, run)
	if err != nil {
		return Plan{}, fmt.Errorf("verify expected local and remote branch identities: %w", err)
	}
	if identity.LocalCommit == "" {
		return Plan{}, errors.New("expected local branch has no commit identity")
	}
	if identity.RemotePresent != (identity.RemoteCommit != "") {
		return Plan{}, errors.New("remote branch presence and commit identity are inconsistent")
	}
	if len(pulls) == 1 {
		if !identity.RemotePresent {
			return Plan{}, errors.New("expected pull request exists but its remote branch is absent")
		}
		if pulls[0].Commit != "" && pulls[0].Commit != identity.RemoteCommit {
			return Plan{}, errors.New("expected pull request head changed from the verified remote branch identity")
		}
	}
	if err := m.config.Worktrees.Verify(ctx, worktree.Assignment{Path: run.Worktree, Branch: run.Branch}); err != nil {
		return Plan{}, fmt.Errorf("verify retained branch and worktree: %w", err)
	}

	var continuation scheduler.ContinuationBoundary
	derived := false
	if run.Continuation == nil {
		continuation, err = worker.InspectContinuation(continuationRequest(run))
		derived = true
	} else {
		continuation = *run.Continuation
		if err = worker.VerifyContinuation(continuationRequest(run), continuation); err != nil {
			return Plan{}, fmt.Errorf("verify persisted continuation evidence: %w", err)
		}
		if continuation.WorkerGeneration > 0 && continuation.WorkerGeneration != run.WorkerGeneration {
			return Plan{}, errors.New("persisted continuation boundary belongs to an obsolete Worker generation")
		}
	}
	if err != nil {
		return Plan{}, fmt.Errorf("derive offline continuation evidence: %w", err)
	}
	if continuation.WorkflowStage == "" || (continuation.Workflow != "afk" && continuation.Workflow != "ship-it") {
		return Plan{}, errors.New("continuation evidence has an unsupported workflow checkpoint identity")
	}
	boundary := continuation
	boundary.WorkerGeneration = run.WorkerGeneration
	boundary.LocalCommit = identity.LocalCommit
	boundary.RemoteBranchState = map[bool]string{true: "present", false: "absent"}[identity.RemotePresent]
	boundary.RemoteCommit = identity.RemoteCommit
	boundary.PullRequest = pullRequest
	boundary.VerifiedAt = m.config.Now().UTC()
	pullHead := ""
	if len(pulls) == 1 {
		pullHead = pulls[0].Commit
		boundary.PullRequestHead = pullHead
	}
	if already {
		persisted := run.Continuation
		if persisted.LocalCommit != identity.LocalCommit || persisted.RemoteBranchState != boundary.RemoteBranchState || persisted.RemoteCommit != identity.RemoteCommit || persisted.PullRequest != boundary.PullRequest || persisted.PullRequestHead != boundary.PullRequestHead {
			return Plan{}, errors.New("repository or pull request identity drifted since the persisted Recovery continuation")
		}
		outcome = OutcomeAlready
	}
	return Plan{
		Run: run, Lease: lease, Boundary: boundary, DerivedOffline: derived, Outcome: outcome,
		PullRequest: pullRequest, OriginalDiagnostic: run.Error,
	}, nil
}

func (m *Module) Recover(ctx context.Context, expected Plan) (Plan, error) {
	fresh, err := m.Inspect(ctx, expected.Run.RunID)
	if err != nil {
		return Plan{}, err
	}
	if fresh.Outcome == OutcomeCompletion {
		return fresh, nil
	}
	if !PlansEqual(expected, fresh) {
		return Plan{}, errors.New("Recovery Plan changed after confirmation; inspect and confirm the current plan")
	}
	if fresh.Outcome == OutcomeAlready {
		return fresh, nil
	}
	if fresh.DerivedOffline && fresh.Outcome == OutcomeSuspend {
		if err := worker.SyncContinuation(continuationRequest(fresh.Run), fresh.Boundary); err != nil {
			return Plan{}, fmt.Errorf("synchronize offline continuation evidence: %w", err)
		}
	}
	current, err := m.config.Store.Load()
	if err != nil {
		return Plan{}, err
	}
	run, err := selectRun(current, fresh.Run.RunID)
	lease, leased := findLease(current, run)
	if err != nil || !leased || !reflect.DeepEqual(run, fresh.Run) || lease != fresh.Lease {
		return Plan{}, errors.New("Run or Lease identity changed before Recovery")
	}
	now := m.config.Now().UTC()
	switch fresh.Outcome {
	case OutcomeWaiting:
		run.Status = scheduler.StatusWaitingForMerge
		run.ResumeAfter = nil
		run.PullRequest = fresh.PullRequest
		run.PID, run.ProcessIdentity = 0, ""
		run.RecoveredRetirementRequired = true
		run.UpdatedAt = now
	default:
		if run.PreservedCause == "" {
			run.PreservedCause = run.Error
		}
		run.PullRequest = fresh.PullRequest
		run.Status = scheduler.StatusSuspended
		run.PID, run.ProcessIdentity = 0, ""
		run.WorkerLogOpen = false
		run.Continuation = &fresh.Boundary
		run.Workflow = fresh.Boundary.Workflow
		run.WorkflowStage = fresh.Boundary.WorkflowStage
		run.RecoveredRetirementRequired = true
		if fresh.Boundary.CheckpointFailureClass != "" {
			run.FailureClass = scheduler.FailureClass(fresh.Boundary.CheckpointFailureClass)
		}
		run.BlockerKind = fresh.Boundary.CheckpointBlockerKind
		run.BlockerCause = fresh.Boundary.CheckpointBlockerCause
		run.BlockerFingerprint = fresh.Boundary.CheckpointBlockerFingerprint
		run.ResumeAfter = nil
		run.SuspendedAt = &now
		run.RecoveryCount++
		if run.FirstRecoveredAt == nil {
			run.FirstRecoveredAt = &now
		}
		run.LastRecoveredAt = &now
		run.UpdatedAt = now
	}
	replaceRun(&current, run)
	if err := m.config.Store.Save(current); err != nil {
		return Plan{}, fmt.Errorf("persist Recovery transition: %w", err)
	}
	fresh.Run = run
	if fresh.Outcome == OutcomeSuspend {
		fresh.Outcome = OutcomeAlready
	}
	return fresh, nil
}

func (m *Module) verifyAbsent(run scheduler.Run) error {
	if run.ResumePending {
		return errors.New("Run has a pending replacement Worker launch")
	}
	if run.PID != 0 || run.ProcessIdentity != "" {
		return errors.New("Run still has a current Worker process identity")
	}
	if run.WorkerGeneration <= 0 || run.StoppedWorkerGeneration != run.WorkerGeneration || run.WorkerStoppedAt == nil || run.WorkerStoppedAt.IsZero() {
		return errors.New("Run has no durable proof that its last Worker generation stopped")
	}
	if run.StoppedWorkerProcessIdentity == "" {
		if run.StoppedWorkerPID != 0 {
			return errors.New("stopped Worker PID has no checkable process identity")
		}
		return nil
	}
	identityPIDText, started, found := strings.Cut(run.StoppedWorkerProcessIdentity, ":")
	identityPID, parseErr := strconv.Atoi(identityPIDText)
	if !found || parseErr != nil || identityPID <= 0 || strings.TrimSpace(started) == "" || run.StoppedWorkerPID != identityPID {
		return errors.New("stopped Worker process identity is malformed or mismatched")
	}
	alive, err := m.config.ProcessAlive(identityPID)
	if err != nil {
		return fmt.Errorf("verify Worker absence: %w", err)
	}
	if alive {
		return errors.New("recorded Worker is still live")
	}
	groupAlive, err := m.config.ProcessGroupAlive(identityPID)
	if err != nil {
		return fmt.Errorf("verify Worker process-group absence: %w", err)
	}
	if groupAlive {
		return errors.New("recorded Worker process group is still live")
	}
	return nil
}

func continuationRequest(run scheduler.Run) worker.ContinuationRequest {
	expectedWorkflow := run.Workflow
	if expectedWorkflow == "" && run.Continuation != nil {
		expectedWorkflow = run.Continuation.Workflow
	}
	if expectedWorkflow == "" && run.WorkflowStage != "" && run.WorkflowStage != "afk-coordinator" {
		expectedWorkflow = "ship-it"
	}
	return worker.ContinuationRequest{
		Issue: run.Issue, RunID: run.RunID, Branch: run.Branch,
		SessionID: run.SessionID, SessionDir: run.SessionDir, Worktree: run.Worktree,
		PromptDigest: run.PromptDigest, PromptOwnership: run.PromptOwnership,
		AllowLegacyPromptOwnership: run.LegacyPromptOwnership,
		RequirePromptOwnership:     true, ExpectedWorkflow: expectedWorkflow,
	}
}

func verifyIssue(issue ghadapter.OwnedRunIssue) error {
	if issue.State != "open" {
		return errors.New("issue is not open; verify Completion or External Resolution before Recovery")
	}
	labels := make(map[string]bool, len(issue.Labels))
	for _, label := range issue.Labels {
		labels[label] = true
	}
	for _, human := range []string{"needs-triage", "needs-info", "ready-for-human", "wontfix"} {
		if labels[human] {
			return fmt.Errorf("human workflow label %q blocks Recovery", human)
		}
	}
	if !labels["in-progress"] || labels["ready-for-agent"] {
		return errors.New("managed workflow labels do not identify one active leased Run")
	}
	return nil
}

func selectRun(current state.State, selector string) (scheduler.Run, error) {
	for _, run := range current.Runs {
		if run.RunID == selector {
			return run, nil
		}
	}
	issue, err := strconv.Atoi(selector)
	if err != nil || issue <= 0 {
		return scheduler.Run{}, fmt.Errorf("Run %q was not found", selector)
	}
	var selected scheduler.Run
	for _, lease := range current.Leases {
		if lease.Issue != issue {
			continue
		}
		for _, run := range current.Runs {
			if run.RunID == lease.RunID {
				if selected.RunID != "" {
					return scheduler.Run{}, fmt.Errorf("issue #%d has ambiguous leased Runs", issue)
				}
				selected = run
			}
		}
	}
	if selected.RunID == "" {
		return scheduler.Run{}, fmt.Errorf("issue #%d has no leased Run", issue)
	}
	return selected, nil
}

func findLease(current state.State, run scheduler.Run) (scheduler.Lease, bool) {
	for _, lease := range current.Leases {
		if lease.RunID == run.RunID && lease.Issue == run.Issue {
			return lease, true
		}
	}
	return scheduler.Lease{}, false
}

// PlansEqual compares every safety-relevant identity while ignoring only the
// observation timestamp, which is expected to change on each inspection.
func PlansEqual(left, right Plan) bool {
	left.Boundary.VerifiedAt = time.Time{}
	right.Boundary.VerifiedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func replaceRun(current *state.State, replacement scheduler.Run) {
	for index := range current.Runs {
		if current.Runs[index].RunID == replacement.RunID {
			current.Runs[index] = replacement
			return
		}
	}
}

func WritePlan(writer interface{ Write([]byte) (int, error) }, plan Plan) error {
	remote := plan.Boundary.RemoteCommit
	if remote == "" {
		remote = "absent"
	}
	lines := []string{
		fmt.Sprintf("Recovery Plan for Run %s (issue #%d):", plan.Run.RunID, plan.Run.Issue),
		fmt.Sprintf("  Outcome: %s", plan.Outcome),
	}
	switch plan.Outcome {
	case OutcomeSuspend, OutcomeAlready:
		stateAction := "establish Run as Suspended"
		if plan.Outcome == OutcomeAlready {
			stateAction = "leave Run already Suspended"
		}
		lines = append(lines,
			fmt.Sprintf("  Preserve: existing Lease, branch %s, worktree %s, Pi session %s, diagnostic logs, and original diagnostic", plan.Run.Branch, plan.Run.Worktree, plan.Run.SessionID),
			"  State action: "+stateAction+" with no replacement Worker launch",
			fmt.Sprintf("  Continuation: %s stage %s at leaf %s (%d entries, SHA-256 %s)", plan.Boundary.Workflow, plan.Boundary.WorkflowStage, plan.Boundary.LeafID, plan.Boundary.EntryCount, plan.Boundary.SHA256),
			fmt.Sprintf("  Session file: %s", plan.Boundary.SessionFile),
			"  Launch boundary: existing Resume will freshly recheck GitHub, Git, worktree, session, checkpoint, and process absence before starting a replacement Worker.",
			"  Mutation guard: reassess external state and never repeat a push, pull request, merge, issue, or cleanup mutation whose outcome is uncertain.",
		)
		if plan.Boundary.CheckpointFile != "" {
			lines = append(lines, fmt.Sprintf("  Workflow checkpoint: %s (SHA-256 %s)", plan.Boundary.CheckpointFile, plan.Boundary.CheckpointSHA256))
		}
	case OutcomeWaiting:
		lines = append(lines,
			fmt.Sprintf("  Preserve: existing Lease, branch %s, worktree %s, Pi session %s, diagnostic logs, and original diagnostic", plan.Run.Branch, plan.Run.Worktree, plan.Run.SessionID),
			"  State action: transition Run to waiting-for-merge and clear any cooldown; do not launch a replacement Worker",
			fmt.Sprintf("  Pull request identity: %s at head %s with auto-merge armed", plan.PullRequest, plan.PullRequestHead),
		)
	case OutcomeCompletion:
		lines = append(lines,
			fmt.Sprintf("  Completion identity: merged pull request %s at head %s and closed issue #%d", plan.PullRequest, plan.PullRequestHead, plan.Run.Issue),
			"  Retirement: inspect and approve the exact current owned-artifact actions printed below; no replacement Worker will launch",
		)
	}
	if plan.Boundary.LocalCommit != "" {
		lines = append(lines, fmt.Sprintf("  Git identity: local %s; remote %s", plan.Boundary.LocalCommit, remote))
	}
	if plan.PullRequest != "" && plan.Outcome != OutcomeWaiting && plan.Outcome != OutcomeCompletion {
		lines = append(lines, "  Expected pull request: "+plan.PullRequest)
	}
	if plan.OriginalDiagnostic != "" {
		lines = append(lines, "  Preserved diagnostic: "+plan.OriginalDiagnostic)
	}
	_, err := writer.Write([]byte(strings.Join(lines, "\n") + "\n"))
	return err
}
