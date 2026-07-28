---
status: accepted
---

# Recover Intervention-required Runs from durable checkpoints

Backlog will provide Recovery as a fail-closed lifecycle operation distinct from Resume and Reset. Recovery verifies durable continuation evidence for the same leased `failed` or `needs-human` Run and records it as Suspended. Resume remains responsible for replacement Worker launch after fresh checks.

## Consequences

- `backlog recover <run-id|positive-issue-number>` supports read-only dry-run, default-negative interactive confirmation, and `--yes`.
- Recovery requires absent Runner coordination, a retained Lease, conclusive Worker and process-group absence, exact branch and worktree identities, an open issue with active managed labels, verified expected-branch pull request state, and unchanged Pi session and workflow checkpoint evidence.
- Existing terminal Runs without a continuation marker may derive one only from one unambiguous owned regular session file with a matching version 3 header, complete entry graph, durable leaf, and no tool call awaiting a durable result.
- Successful Recovery preserves the Run, Lease, branch, worktree, session, logs, and original diagnostic. It records explicit Recovery metadata separately from implementation and publication repair budgets.
- Completion and armed expected pull requests take precedence over replacement launch. Changed, malformed, missing, ambiguous, or unsupported evidence refuses Recovery while retaining ownership and artifacts.
- Every settled incomplete RPC Worker attempts the same synchronized continuation checkpoint used by suspension before Backlog closes the idle process, but does not send `abort`.
- A replacement prompt names the verified AFK or ship-it checkpoint stage and requires fresh repository, remote branch, pull request, and issue inspection before any external mutation.
- Automatic continuation is limited to a structured top-level Pi `auto_retry_end` event with `success:false`. Backlog permits one continuation after a fixed 30-second cooldown. A second exhaustion requires intervention.
