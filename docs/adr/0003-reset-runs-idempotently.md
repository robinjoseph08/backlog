---
status: proposed
---

# Reset runs idempotently

Backlog will use Reset, rather than Retry, to abandon an incomplete Run and return its issue to normal scheduling. Reset is an idempotent reconciliation operation because its GitHub, Git, filesystem, session, label, and Lease changes cannot be committed atomically.

## Consequences

- Reset begins with a read-only inspection and prints a Reset Plan tailored to the Run's current state before making any mutation.
- Interactive Reset requires confirmation. Non-interactive use requires `--yes`, and `--dry-run` prints the plan without executing it.
- Reset refuses a merged Run, a verified live Worker, a manually closed issue, or conflicting workflow labels.
- Reset disables auto-merge, closes an unmerged pull request, deletes its remote branch, removes the local worktree and branch, and retires the resumable Pi session only when each artifact exists.
- Label restoration removes `in-progress` and adds `ready-for-agent` only when needed. It preserves unrelated labels and treats the already-restored state as success.
- Reset preserves Run metadata and logs, marks the old Run as reset, and releases its Lease only after required cleanup succeeds.
- An interrupted Reset remains `resetting`; running Reset again recomputes the plan and continues safely. An already reset Run exits successfully without work.
- Reset does not start a replacement Run immediately. Normal Candidate ordering determines when the issue receives a new Run.
- `retry` becomes a temporary deprecated alias for `reset`.
