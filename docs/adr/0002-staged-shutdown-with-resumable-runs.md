---
status: proposed
---

# Use staged shutdown with resumable runs

Backlog will separate runner shutdown from Run termination so routine maintenance does not discard active work. Pi will run in RPC mode with persisted sessions under Backlog state, allowing Backlog to establish a verified continuation boundary before a replacement Worker resumes the same Run after a Backlog update or computer restart.

## Consequences

- `agent_settled` is the normal Worker completion trigger, not process exit. Backlog reconciles GitHub, persists the resulting Run state, closes the idle RPC process, and waits for its process group to exit.
- The first `SIGINT` starts Drain. The Drain transition and Lease admission are serialized so no Lease can commit after Drain is accepted, while every Run whose Lease was committed by this runner may continue.
- Drain reconciles each settled Owned Worker once and exits when no Owned Worker remains. A Run waiting for merge does not keep Drain alive, but its persisted state and Lease remain intact for a later runner to reconcile.
- The second `SIGINT` starts suspension with one 60-second wall-clock deadline shared by all remaining Workers. Before acting, Backlog rechecks the Run state and the PID and process-start identity so a concurrently completed Run is reconciled rather than suspended.
- An RPC `abort` response alone is not a continuation boundary. Backlog must also observe `agent_settled`, verify through `get_state` that streaming, compaction, and queued messages are idle, validate the exact session ID, path, durable leaf, and complete tool-result tail, and sync the session file.
- Backlog persists the verified continuation boundary before closing the RPC process. After the process group exits, it atomically records the Run as suspended without a live PID. A crash between those steps is recoverable from the persisted boundary.
- Backlog reconciles GitHub after reaching the continuation boundary. A verified Completion or waiting-for-merge outcome is persisted normally; only unfinished Runs become suspended.
- A Suspended Run continuously retains its original Lease. Suspension and Resume never make its issue eligible for another Run.
- At suspension timeout, Backlog verifies process identities, kills remaining Worker process groups, classifies Runs without a persisted continuation boundary as `needs-human`, persists the outcomes, and exits. A third `SIGINT` performs this escalation immediately.
- `SIGTERM` starts the same bounded suspension path directly so operating-system shutdown does not wait for Drain.
- Every stage prints its current action, remaining Worker count, and the effect of the next `SIGINT`.
- Before Resume, Backlog reconciles GitHub and verifies the issue state, workflow labels, exact Pi session identity and durable leaf, branch, worktree, and conclusive absence of the old Worker. Completion and waiting-for-merge outcomes are not resumed; missing or uncertain state becomes `needs-human` and retains the Lease.
- Suspended Runs consume available Worker capacity before Backlog creates new Leases. Each Resume either assigns exactly one replacement Worker to the same Run or fails closed as `needs-human` while retaining the Lease.
- Resume keeps the Run ID, Lease, Pi session, branch, and worktree while assigning a new Worker process.
- Backlog does not provide a scheduler Pause mode. Drain is transient, while Run suspension is durable.
