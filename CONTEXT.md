# Backlog Runner

This context describes how GitHub work moves from an available backlog into isolated autonomous execution.

## Language

**Candidate**:
An open GitHub issue carrying the `ready-for-agent` label that may be considered for autonomous work.
_Avoid_: Ticket, job

**Blocker**:
An open GitHub issue explicitly preventing a candidate from becoming eligible, either through native dependency tracking or an explicit dependency statement.
_Avoid_: Related issue, prerequisite

**Eligible candidate**:
A candidate with no open blockers and no active or intervention-required run.
_Avoid_: Ready issue, runnable ticket

**Admission**:
The transition that creates a Run and its Lease for an eligible candidate.
_Avoid_: Claiming a ticket, starting a job

**Lease**:
A durable reservation preventing more than one run from owning an issue at a time.
_Avoid_: Claim, lock

**Run**:
One isolated attempt to take an eligible candidate through the AFK workflow and verify its GitHub outcome.
_Avoid_: Task, execution

**Historical Run**:
A Run without a Lease that remains available for inspection but no longer reserves its issue.
_Avoid_: Archived Run, deleted Run

**Outcome Acknowledgment**:
A durable indication that an operator has seen a Historical Run's unexpected non-Completion outcome, allowing default status to omit it without deleting history.
_Avoid_: Dismissal, clearance, deletion

**Intervention-required Run**:
An incomplete Run that retains its Lease because Backlog cannot safely continue or release ownership without human judgment.
_Avoid_: Past failed Run, historical failure

**Drain**:
A runner shutdown phase that stops creating Leases while allowing every Owned Worker to finish before the runner exits.
_Avoid_: Pause, cancellation

**Suspending Run**:
A running Run whose Owned Worker is establishing a verified continuation boundary during bounded shutdown.
_Avoid_: Pausing Run, paused Worker

**Suspended Run**:
A Run with no live Worker whose retained continuation state allows a replacement Worker to continue it.
_Avoid_: Paused Worker, failed Run

**Resume**:
Continuation of a Suspended Run by a replacement Worker using the same Run identity and retained artifacts.
_Avoid_: Retry, restart

**Reset**:
Idempotent abandonment of an incomplete Run that retires its active artifacts, restores the issue as a Candidate, and releases its Lease while preserving history.
_Avoid_: Retry, cleanup

**Reset Plan**:
A read-only, current-state-derived list of mutations required to Reset a Run.
_Avoid_: Generic cleanup plan

**Worker**:
The live Pi process belonging to a run.
_Avoid_: Agent, subagent

**Owned Worker**:
A live Worker whose lifecycle is directly supervised by the current runner invocation.
_Avoid_: Any live Worker, leased Run

**Subagent**:
A subordinate autonomous executor launched by a Worker to perform part of its Run.
_Avoid_: Worker, Run

**Activity**:
A semantically meaningful change from a Worker or Subagent that provides evidence a Run may be advancing. Cosmetic refreshes and mere liveness are not Activity.
_Avoid_: Output, heartbeat

**Follow**:
Continuous read-only observation of one specific Run and its Activity.
_Avoid_: Attach, control

**Completion**:
A GitHub-verified outcome in which the expected pull request is merged and the issue is closed when appropriate.
_Avoid_: Successful exit, done
