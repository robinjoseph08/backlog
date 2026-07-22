---
status: proposed
---

# Use staged shutdown with resumable runs

Backlog will separate runner shutdown from Run termination so routine maintenance does not discard active work. Pi Workers will use RPC sessions stored under Backlog state, allowing an acknowledged abort to suspend a Run and a replacement Worker to resume the same Run after a Backlog update or computer restart.

## Consequences

- The first `SIGINT` starts Drain: stop creating Leases, allow already leased Runs to continue, reconcile each exited Worker once, and exit when no Worker remains. A Run waiting for merge does not keep Drain alive.
- The second `SIGINT` suspends remaining Runs by sending Pi's RPC `abort`, waiting for durable idle state, and stopping each Worker. Suspension has a 60-second timeout.
- A third `SIGINT` bypasses the timeout and kills verified Worker process groups. Runs without a trustworthy continuation boundary become `needs-human`.
- `SIGTERM` attempts suspension directly so operating-system shutdown has a bounded path to preserving work.
- Suspended Runs resume before Backlog creates new Leases. Resume keeps the Run ID, Pi session, branch, and worktree while assigning a new Worker process.
- Backlog does not provide a scheduler Pause mode. Drain is transient, while Run suspension is durable.
