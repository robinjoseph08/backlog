// Package recovery verifies and establishes durable continuation for an
// Intervention-required Run without creating new ownership identities.
package recovery

import (
	"context"
	"errors"
	"fmt"
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
	LocalCommit  string
	RemoteCommit string
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
	Boundary           scheduler.ContinuationBoundary
	DerivedOffline     bool
	Outcome            Outcome
	PullRequest        string
	LocalCommit        string
	RemoteCommit       string
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
	if !hasLease(current, run) {
		return Plan{}, fmt.Errorf("Run %q no longer owns its Lease", run.RunID)
	}
	already := run.Status == scheduler.StatusSuspended && run.RecoveryCount > 0
	if run.Status != scheduler.StatusFailed && run.Status != scheduler.StatusNeedsHuman && !already {
		return Plan{}, fmt.Errorf("Run %q is not eligible for Recovery in state %q", run.RunID, run.Status)
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
		return Plan{Run: run, Outcome: outcome, PullRequest: pullRequest, OriginalDiagnostic: run.Error}, nil
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
	if len(pulls) == 1 {
		if identity.RemoteCommit == "" {
			return Plan{}, errors.New("expected pull request exists but its remote branch is absent")
		}
		if pulls[0].Commit != "" && pulls[0].Commit != identity.RemoteCommit {
			return Plan{}, errors.New("expected pull request head changed from the verified remote branch identity")
		}
	}
	if err := m.config.Worktrees.Verify(ctx, worktree.Assignment{Path: run.Worktree, Branch: run.Branch}); err != nil {
		return Plan{}, fmt.Errorf("verify retained branch and worktree: %w", err)
	}

	var continuation worker.Continuation
	derived := false
	if run.Continuation == nil {
		continuation, err = worker.InspectContinuation(worker.ContinuationRequest{SessionID: run.SessionID, SessionDir: run.SessionDir, Worktree: run.Worktree})
		derived = true
	} else {
		boundary := run.Continuation
		continuation = worker.Continuation{
			SessionID: boundary.SessionID, SessionFile: boundary.SessionFile, Worktree: boundary.Worktree,
			LeafID: boundary.LeafID, EntryCount: boundary.EntryCount, SHA256: boundary.SHA256,
			Workflow: boundary.Workflow, WorkflowStage: boundary.WorkflowStage,
			CheckpointFile: boundary.CheckpointFile, CheckpointSHA256: boundary.CheckpointSHA256,
		}
		if err = worker.VerifyContinuation(worker.ContinuationRequest{SessionID: run.SessionID, SessionDir: run.SessionDir, Worktree: run.Worktree}, continuation); err != nil {
			return Plan{}, fmt.Errorf("verify persisted continuation evidence: %w", err)
		}
		if continuation.Workflow == "" {
			continuation.Workflow, continuation.WorkflowStage, continuation.CheckpointFile, continuation.CheckpointSHA256, err = worker.InspectWorkflowCheckpoint(run.Worktree)
		}
	}
	if err != nil {
		return Plan{}, fmt.Errorf("derive offline continuation evidence: %w", err)
	}
	if continuation.WorkflowStage == "" || (continuation.Workflow != "afk" && continuation.Workflow != "ship-it") {
		return Plan{}, errors.New("continuation evidence has an unsupported workflow checkpoint identity")
	}
	boundary := scheduler.ContinuationBoundary{
		SessionID: continuation.SessionID, SessionFile: continuation.SessionFile, Worktree: continuation.Worktree,
		LeafID: continuation.LeafID, EntryCount: continuation.EntryCount, SHA256: continuation.SHA256,
		Workflow: continuation.Workflow, WorkflowStage: continuation.WorkflowStage,
		CheckpointFile: continuation.CheckpointFile, CheckpointSHA256: continuation.CheckpointSHA256,
		VerifiedAt: m.config.Now().UTC(),
	}
	if already {
		outcome = OutcomeAlready
	}
	return Plan{
		Run: run, Boundary: boundary, DerivedOffline: derived, Outcome: outcome, PullRequest: pullRequest,
		LocalCommit: identity.LocalCommit, RemoteCommit: identity.RemoteCommit, OriginalDiagnostic: run.Error,
	}, nil
}

func (m *Module) Recover(ctx context.Context, expected Plan) (Plan, error) {
	fresh, err := m.Inspect(ctx, expected.Run.RunID)
	if err != nil {
		return Plan{}, err
	}
	if !PlansEqual(expected, fresh) {
		return Plan{}, errors.New("Recovery Plan changed after confirmation; inspect and confirm the current plan")
	}
	if fresh.Outcome == OutcomeAlready {
		return fresh, nil
	}
	if fresh.DerivedOffline && fresh.Outcome == OutcomeSuspend {
		continuation := worker.Continuation{
			SessionID: fresh.Boundary.SessionID, SessionFile: fresh.Boundary.SessionFile, Worktree: fresh.Boundary.Worktree,
			LeafID: fresh.Boundary.LeafID, EntryCount: fresh.Boundary.EntryCount, SHA256: fresh.Boundary.SHA256,
			Workflow: fresh.Boundary.Workflow, WorkflowStage: fresh.Boundary.WorkflowStage,
			CheckpointFile: fresh.Boundary.CheckpointFile, CheckpointSHA256: fresh.Boundary.CheckpointSHA256,
		}
		if err := worker.SyncContinuation(worker.ContinuationRequest{SessionID: fresh.Run.SessionID, SessionDir: fresh.Run.SessionDir, Worktree: fresh.Run.Worktree}, continuation); err != nil {
			return Plan{}, fmt.Errorf("synchronize offline continuation evidence: %w", err)
		}
	}
	current, err := m.config.Store.Load()
	if err != nil {
		return Plan{}, err
	}
	run, err := selectRun(current, fresh.Run.RunID)
	if err != nil || !hasLease(current, run) {
		return Plan{}, errors.New("Run or Lease identity changed before Recovery")
	}
	now := m.config.Now().UTC()
	switch fresh.Outcome {
	case OutcomeWaiting:
		run.Status = scheduler.StatusWaitingForMerge
		run.ResumeAfter = nil
		run.PullRequest = fresh.PullRequest
		run.PID, run.ProcessIdentity = 0, ""
		run.UpdatedAt = now
	case OutcomeCompletion:
		if err := m.config.Worktrees.Cleanup(ctx, worktree.Assignment{Path: run.Worktree, Branch: run.Branch}); err != nil {
			return Plan{}, fmt.Errorf("retire completed worktree: %w", err)
		}
		run.Status = scheduler.StatusMerged
		run.ResumeAfter = nil
		run.PullRequest = fresh.PullRequest
		run.PID, run.ProcessIdentity = 0, ""
		run.CompletedAt = &now
		run.Error = ""
		removeLease(&current, run.RunID)
	default:
		if run.PreservedCause == "" {
			run.PreservedCause = run.Error
		}
		run.PullRequest = fresh.PullRequest
		run.Status = scheduler.StatusSuspended
		run.PID, run.ProcessIdentity = 0, ""
		run.WorkerLogOpen = false
		run.Continuation = &fresh.Boundary
		run.WorkflowStage = fresh.Boundary.WorkflowStage
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
	if run.PID == 0 && run.ProcessIdentity == "" {
		return nil
	}
	identityPIDText, started, found := strings.Cut(run.ProcessIdentity, ":")
	identityPID, parseErr := strconv.Atoi(identityPIDText)
	if !found || parseErr != nil || identityPID <= 0 || strings.TrimSpace(started) == "" || run.PID != 0 && run.PID != identityPID {
		return errors.New("recorded Worker process identity is malformed or mismatched")
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

func verifyIssue(issue ghadapter.OwnedRunIssue) error {
	if issue.State != "open" {
		return errors.New("issue is not open; Completion or External Resolution takes precedence over Recovery")
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

func hasLease(current state.State, run scheduler.Run) bool {
	for _, lease := range current.Leases {
		if lease.RunID == run.RunID && lease.Issue == run.Issue {
			return true
		}
	}
	return false
}

// PlansEqual compares every safety-relevant identity while ignoring only the
// observation timestamp, which is expected to change on each inspection.
func PlansEqual(left, right Plan) bool {
	return left.Run.RunID == right.Run.RunID && left.Run.Issue == right.Run.Issue && left.Run.Status == right.Run.Status &&
		left.Run.Branch == right.Run.Branch && left.Run.Worktree == right.Run.Worktree && left.Run.SessionID == right.Run.SessionID &&
		left.Run.SessionDir == right.Run.SessionDir && left.Run.LogPath == right.Run.LogPath && left.Run.StderrPath == right.Run.StderrPath &&
		left.Run.RecoveryCount == right.Run.RecoveryCount && left.DerivedOffline == right.DerivedOffline && left.Outcome == right.Outcome &&
		left.PullRequest == right.PullRequest && left.LocalCommit == right.LocalCommit && left.RemoteCommit == right.RemoteCommit &&
		left.OriginalDiagnostic == right.OriginalDiagnostic && left.Boundary.SessionID == right.Boundary.SessionID &&
		left.Boundary.SessionFile == right.Boundary.SessionFile && left.Boundary.Worktree == right.Boundary.Worktree &&
		left.Boundary.LeafID == right.Boundary.LeafID && left.Boundary.EntryCount == right.Boundary.EntryCount &&
		left.Boundary.SHA256 == right.Boundary.SHA256 && left.Boundary.Workflow == right.Boundary.Workflow &&
		left.Boundary.WorkflowStage == right.Boundary.WorkflowStage && left.Boundary.CheckpointFile == right.Boundary.CheckpointFile &&
		left.Boundary.CheckpointSHA256 == right.Boundary.CheckpointSHA256
}

func replaceRun(current *state.State, replacement scheduler.Run) {
	for index := range current.Runs {
		if current.Runs[index].RunID == replacement.RunID {
			current.Runs[index] = replacement
			return
		}
	}
}

func removeLease(current *state.State, runID string) {
	for index := range current.Leases {
		if current.Leases[index].RunID == runID {
			current.Leases = append(current.Leases[:index], current.Leases[index+1:]...)
			return
		}
	}
}

func WritePlan(writer interface{ Write([]byte) (int, error) }, plan Plan) error {
	remote := plan.RemoteCommit
	if remote == "" {
		remote = "absent"
	}
	lines := []string{
		fmt.Sprintf("Recovery Plan for Run %s (issue #%d):", plan.Run.RunID, plan.Run.Issue),
		fmt.Sprintf("  Outcome: %s", plan.Outcome),
		fmt.Sprintf("  Preserve: Lease, branch %s, worktree %s, Pi session %s, and diagnostic logs", plan.Run.Branch, plan.Run.Worktree, plan.Run.SessionID),
	}
	if plan.LocalCommit != "" {
		lines = append(lines, fmt.Sprintf("  Git identity: local %s; remote %s", plan.LocalCommit, remote))
	}
	if plan.Outcome == OutcomeSuspend || plan.Outcome == OutcomeAlready {
		lines = append(lines,
			fmt.Sprintf("  Continuation: %s stage %s at leaf %s (%d entries, SHA-256 %s)", plan.Boundary.Workflow, plan.Boundary.WorkflowStage, plan.Boundary.LeafID, plan.Boundary.EntryCount, plan.Boundary.SHA256),
			fmt.Sprintf("  Session file: %s", plan.Boundary.SessionFile),
			"  Launch boundary: existing Resume will freshly recheck GitHub, Git, worktree, session, checkpoint, and process absence before starting a replacement Worker.",
			"  Mutation guard: reassess external state and never repeat a push, pull request, merge, issue, or cleanup mutation whose outcome is uncertain.",
		)
		if plan.Boundary.CheckpointFile != "" {
			lines = append(lines, fmt.Sprintf("  Workflow checkpoint: %s (SHA-256 %s)", plan.Boundary.CheckpointFile, plan.Boundary.CheckpointSHA256))
		}
	}
	if plan.PullRequest != "" {
		lines = append(lines, "  Expected pull request: "+plan.PullRequest)
	}
	if plan.OriginalDiagnostic != "" {
		lines = append(lines, "  Preserved diagnostic: "+plan.OriginalDiagnostic)
	}
	_, err := writer.Write([]byte(strings.Join(lines, "\n") + "\n"))
	return err
}
