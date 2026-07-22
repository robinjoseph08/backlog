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

**Worker**:
The live Pi process belonging to a run.
_Avoid_: Agent, subagent

**Completion**:
A GitHub-verified outcome in which the expected pull request is merged and the issue is closed when appropriate.
_Avoid_: Successful exit, done
