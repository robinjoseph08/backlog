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

**Lease**:
A durable reservation preventing more than one run from owning an issue at a time.
_Avoid_: Claim, lock

**Run**:
One isolated attempt to take an eligible candidate through the AFK workflow and verify its GitHub outcome.
_Avoid_: Task, execution

**Drain**:
A runner shutdown phase that stops creating Leases while allowing every live Worker to finish before the runner exits.
_Avoid_: Pause, cancellation

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

**Completion**:
A GitHub-verified outcome in which the expected pull request is merged and the issue is closed when appropriate.
_Avoid_: Successful exit, done
