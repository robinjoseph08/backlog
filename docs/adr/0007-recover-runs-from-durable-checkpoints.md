---
status: accepted
---

# Recover Intervention-required Runs from durable checkpoints

Backlog will provide Recovery as a fail-closed lifecycle operation distinct from Resume and Reset. Recovery verifies durable continuation evidence for the same leased `failed` or `needs-human` Run and records it as Suspended. Resume remains responsible for replacement Worker launch after fresh checks.

## Consequences

- `backlog recover <run-id|positive-issue-number>` supports read-only dry-run, default-negative interactive confirmation, and `--yes`.
- Recovery first requires absent Runner coordination, a retained Lease, conclusive Worker and process-group absence, and verified issue and expected-branch pull request state. Completion and armed auto-merge take precedence; an armed expected pull request is reconciled as waiting for merge before Git, worktree, Pi session, or workflow checkpoint inspection. Transition to Suspended additionally requires exact branch and worktree identities, an open issue with active managed labels, and unchanged Pi session and workflow checkpoint evidence.
- Existing terminal Runs without a continuation marker may derive the Pi session boundary only from one unambiguous owned regular session file with a matching version 3 header, complete entry graph, durable leaf, and no tool call awaiting a durable result. An original AFK invocation proves ownership, not the current workflow stage, so Recovery also requires the exact Backlog-owned AFK stage checkpoint or ship-it checkpoint.
- Successful Recovery preserves the Run, Lease, branch, worktree, session, logs, and original diagnostic. It records explicit Recovery metadata separately from implementation and publication repair budgets.
- Completion and armed expected pull requests take precedence over replacement launch. A late verified Completion retires the owned branch, worktree, active Pi session, and managed workflow labels before recording the merged pull request and releasing the Lease. Changed, malformed, missing, ambiguous, or unsupported evidence refuses Recovery while retaining ownership and artifacts.
- Every settled incomplete RPC Worker attempts the same race-safe synchronized continuation checkpoint used by suspension before Backlog closes the idle process, but does not send `abort`. Capture requires consecutive stable RPC entry and synchronized session-file snapshots and creates the Backlog-owned AFK stage checkpoint when ship-it has not started.
- A replacement prompt names the verified AFK or ship-it checkpoint stage and requires fresh repository, remote branch, pull request, and issue inspection before any external mutation.
- Automatic continuation is limited to a structured top-level Pi `auto_retry_end` event with `success:false`. Backlog permits one continuation after a fixed 30-second cooldown, schedules a wake-up at the durable deadline independently of watch polling, and requires intervention after a second exhaustion.
- State schema version 5 persists Recovery safety metadata. Version 4 migrates without changing Run, Lease, artifact, diagnostic, checkpoint, or Recovery identities. Failure class and ship-it blocker kind, cause, and fingerprint remain separate fields; only Failure class is validated against Backlog's enum.
