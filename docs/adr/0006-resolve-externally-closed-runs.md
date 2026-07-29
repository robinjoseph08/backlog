---
status: accepted
---

# Resolve externally closed Runs without claiming Completion

Backlog will recognize External Resolution when GitHub verifies that an issue is closed but the Run's expected pull request does not establish Completion. External Resolution retires the incomplete Run without restoring the issue as a Candidate, without claiming the Run completed the issue, and without reducing the operation to presentation-only Outcome Acknowledgment.

## Rollout status

The accepted end-state architecture is current behavior:

- The explicit `backlog resolve` command provides complete owned-artifact retirement.
- Runner startup and watch reconciliation automatically use that complete retirement behavior for incomplete leased Runs with no Owned Worker.
- After normal Worker settlement, process-group exit, and durable log closure, the Runner rechecks Completion and immediately uses the same retirement behavior when the issue was resolved elsewhere.

## Consequences

- An incomplete Run may enter `resolving-externally` while retaining its Lease, then become a Historical Run in `resolved-externally` after all active artifacts are verified retired. The outcome records when Backlog recognized the resolution and GitHub's issue closure reason.
- A merged expected pull request and closed issue establish Completion only after fail-closed lifecycle and artifact-identity checks pass, even when discovered during External Resolution. Recovered Runs require the full Recovered Completion policy, and changed artifact commits retain the Lease. Explicit resolution retires the Run's owned branches, worktree, active Pi session, and managed `in-progress` and `ready-for-agent` labels before recording Completion and releasing the Lease instead of deferring cleanup to a future Runner startup.
- A supervising Runner checks incomplete leased Runs with no Owned Worker at startup, during watch reconciliation, and immediately after normal Worker settlement. Post-settlement reconciliation first proves process-group exit and durable log closure, then rechecks the expected branch for Completion. Each External Resolution check performs complete retirement automatically only when GitHub verifies that the issue is closed with a supported closure reason.
- Backlog never terminates a Worker because an issue closed. A live or potentially live Worker prevents External Resolution until process-group absence is proven.
- `backlog resolve <run-id|positive-issue-number>` provides explicit dry-run, interactive, and `--yes` operation when no Runner is active. It refuses during active Runner supervision and explains that the supervising Runner handles automatic reconciliation at startup, during watch polling, and after normal Worker settlement. Backlog will not add a separate resolution-request control plane.
- External Resolution uses Reset's fail-closed ownership and idempotency model. It disables auto-merge, explains and closes owned unmerged pull requests, deletes owned remote and local branches, removes owned worktrees, and archives active Pi sessions when those actions remain necessary.
- Conclusively absent branches, worktrees, and active sessions are already retired. Already archived sessions are also satisfied. Changed, mismatched, or unknown artifacts stop resolution with the Lease retained.
- Existing diagnostic logs and Run history are preserved. Missing recorded logs add a historical warning but do not retain active ownership solely to preserve unavailable diagnostics.
- External Resolution removes `in-progress` and `ready-for-agent` from the closed issue while preserving unrelated labels and human workflow labels. Human workflow labels explain rather than block external closure.
- GitHub and artifact state are revalidated before destructive mutations and finalization. A recorded expected pull request merge triggers fail-closed Completion validation before External Resolution. When the Run stopped before recording a pull request, exactly one merged pull request from its expected branch may establish Completion after the same checks; multiple unrecorded merged pull requests remain ambiguous. A pull request from a different branch does not establish Completion. An issue that reopens stops resolution with its Lease retained so the operator can close it again or Reset the Run.
- External Resolution durably marks `resolving-externally` progress before its first external mutation and after each verified partial mutation, retaining the Lease throughout. Finalization atomically records the terminal outcome, resolution metadata, Worker log closure state, and Lease release.
- Successful External Resolution is treated as handled and omitted from default status. Full status retains the outcome, timestamp, closure reason, diagnostics, and any missing-log warning.
- Outcome Acknowledgment remains presentation-only. When a selected Run retains a Lease, its error should guide the operator toward Reset or External Resolution rather than implying it can be acknowledged.
- Persisting the new lifecycle states and metadata requires state schema version 4. Version 3 state migrates without changing existing Run outcomes.
