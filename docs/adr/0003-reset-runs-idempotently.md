---
status: proposed
---

# Reset runs idempotently

Backlog will use Reset, rather than Retry, to abandon an incomplete Run and return its issue to normal scheduling consideration. Reset is an idempotent reconciliation operation because its GitHub, Git, filesystem, session, label, and Lease changes cannot be committed atomically.

## Consequences

- Reset holds the repository coordination lock throughout inspection, confirmation, mutation, and final verification.
- Reset begins with a read-only inspection of the Run ID, Lease identity, Worker liveness, issue state and workflow labels, pull request state and identity, auto-merge state, remote branch and commit, local branch and worktree identity, and Pi session identity.
- Reset prints a Reset Plan in execution order containing every exact resource and action currently required, plus the final Run and Lease transition. Actions whose postconditions are already satisfied are omitted.
- Interactive Reset prints the plan and requires affirmative confirmation, with no as the default. EOF or any non-affirmative response cancels without mutation. Non-interactive mutating Reset requires `--yes`; `--dry-run` never prompts, never requires `--yes`, and performs no writes.
- After confirmation, Reset recomputes the plan. If an interactive plan changed, it prints the replacement and requires confirmation again. With `--yes`, it prints the replacement before continuing. Any newly observed refusal condition aborts.
- Reset revalidates ownership and expected state immediately before every destructive action and immediately before releasing the Lease. An unplanned state change stops Reset with the Lease retained; command success is insufficient without verified final postconditions.
- Reset accepts Runs in `failed`, `suspended`, `needs-human`, or `resetting`. It reconciles stale `claimed`, `worktree-ready`, or `running` state before deciding, and handles `waiting-for-merge` only after disabling auto-merge when armed and rechecking that the pull request did not merge.
- Reset refuses a merged Run; an open issue that cannot be verified; a closed issue without verified Completion; a live Worker; or any failure to prove the recorded PID and process group are absent.
- Every combination of Backlog-managed labels `in-progress` and `ready-for-agent` is reconcilable. The configured human workflow labels `needs-triage`, `needs-info`, `ready-for-human`, and `wontfix` block Reset, while unrelated labels such as `spec` are preserved.
- Reset disables auto-merge only when armed, closes an unmerged pull request, deletes the owned remote branch, removes the owned local worktree and branch, and retires the resumable Pi session only when each action remains required.
- Label restoration removes `in-progress` and adds `ready-for-agent` only when needed. The already-restored label state is success.
- Before finalization, Reset verifies that auto-merge is unarmed, every unmerged pull request it owns is closed, owned remote and local branches and worktrees are absent, the Pi session is non-resumable, workflow labels are restored, and Run metadata and logs remain durable. Unknown state is failure, not absence.
- External progress is persisted as `resetting`, with the Lease held, so another invocation can recompute the remaining plan and continue safely.
- After all postconditions hold, marking the historical Run `reset` and removing its active Lease occur in one atomic state-store transaction. An already reset Run succeeds only after verifying that its old Lease is absent.
- Reset does not start a replacement Run. Normal Candidate ordering and blocker evaluation determine whether and when the issue receives a new Run.
- `retry` is a deprecated alias that invokes the exact Reset code path, including its plan, flags, confirmation, refusal checks, mutations, idempotency, exit statuses, and deprecation warning.
