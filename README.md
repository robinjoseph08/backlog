# backlog

`backlog` continuously drains a GitHub issue backlog with isolated Pi AFK workers. It is a deterministic Go scheduler, not a long-running coordinator conversation.

Each issue receives its own lease, Git branch, Git worktree, Pi process, named Pi session, context window, and logs. GitHub is always consulted before a run is considered complete.

## Requirements

- A Unix-like system with `/bin/sh` and POSIX process groups
- [mise](https://mise.jdx.dev/) for tool installation and project tasks
- `git`
- GitHub CLI `gh`, authenticated for the target repository
- Pi 0.80.4 or newer with the global `afk` skill available
- A Git remote named `origin`
- GitHub API access with Issues write permission

The runner uses GitHub's versioned issue dependency endpoint. A dependency lookup failure stops scheduling rather than treating the issue as unblocked.

## Build

Install the pinned tool versions and build the CLI:

```sh
mise install
mise run build
```

Or install it on your Go binary path:

```sh
mise exec -- go install ./cmd/backlog
```

## Run

From the repository whose backlog should be drained:

```sh
backlog run
```

Useful options:

```text
--max-workers 3     Maximum concurrently active issues
--poll 30s          GitHub and process reconciliation interval
--max-worker-age 168h  Bound trust in recovered PID identity
--watch             Keep waiting for future eligible issues
--repo-dir PATH     Target Git repository, default current directory
--state-dir PATH    Override the external state directory
--approve=false     Do not trust project-local Pi resources
```

The concurrency limit counts top-level issues. Each AFK worker may itself launch implementation and review subagents.

After a complete Candidate snapshot succeeds, the foreground runner exits when no unfinished leased Run remains and no eligible Candidate starts a Worker. Blocked Candidates do not keep it alive unless `--watch` is set.

Candidate discovery fails closed for admission. If a discovery pass fails, the runner creates no Leases from that pass, reports the underlying GitHub error, continues supervising existing Workers, and retries after `--poll` in every runner state. This includes initial startup and idle non-watch invocations. A later successful snapshot restores normal admission or one-shot exit behavior.

## Herdr integration

When `backlog run` is launched in a [Herdr](https://herdr.dev/) pane and acquires the repository lock, it reports itself as a custom `backlog` agent over Herdr's inherited local socket. The entry represents the aggregate Backlog runner, not each isolated Worker.

No Herdr integration install or Backlog flag is required. Outside Herdr, the reporter is disabled. Reporting is best effort: Backlog attempts to mark the runner working after setup and release the entry on orderly exit. Socket failures are bounded and ignored, so they cannot change scheduler decisions or durable state.

## Backlog eligibility

A candidate must be an open issue labeled `ready-for-agent`. Candidates are considered oldest first.

The runner checks both:

1. GitHub native `blocked_by` dependencies
2. Explicit statements in the issue body and chronological comments

Recognized text forms include:

```text
Blocked by #123
Depends on owner/repo#123
Waiting for https://github.com/owner/repo/issues/123
```

Bare issue mentions, related links, checklists, and vague sequencing language are ignored. Later explicit statements such as `No longer blocked by #123`, `The dependency on owner/repo#123 was removed`, or `Blocker #123 is resolved` supersede the earlier text-derived blocker. Native dependencies remain authoritative until removed in GitHub or closed.

AFK repeats its own blocker check inside the worker as a final safety check.

## Worker behavior

For issue `#123`, the runner fetches the latest default branch before creating an isolated worktree. A failed fetch is attempted up to three times, with waits of one and two seconds between attempts. Shutdown cancellation interrupts those waits.

The runner starts Pi in RPC mode in the issue worktree using a deterministic session ID derived from the Run ID and a dedicated session directory under Backlog state. A start gate prevents Pi from opening the session or receiving the AFK prompt until the Worker JSONL and standard-error log paths, PID, and process-start identity are durable.

Backlog submits `/skill:afk 123` as a correlated RPC `prompt` command. Standard output uses strict LF-delimited JSONL and is saved separately from standard error. Malformed or truncated records, mismatched or duplicate responses, and invalid lifecycle ordering fail closed and preserve the worktree.

`agent_settled` is the normal completion trigger. An unexpected process exit triggers fail-closed reconciliation instead. While the idle Pi RPC process is still alive, the runner looks up the pull request by the Run's unique branch, verifies the issue state, and persists the reconciled Run. It then closes RPC input, escalates if orderly shutdown exceeds its grace period, confirms process-group exit, and releases Worker capacity. An armed open pull request becomes `waiting-for-merge`; other unverified outcomes require human attention.

Only verified merged runs have their worktrees and local branches removed. Failed and ambiguous runs are retained.

## State and recovery

State and logs live outside the target repository. By default they are stored under the operating system's user cache directory in a path derived from the absolute repository path. Use `--state-dir` when a stable explicit location is preferable.

State is written with same-directory temporary files, file sync, atomic rename, and directory sync. A repository-level advisory lock in the Git common directory prevents two local runner instances from scheduling the same backlog, even if they request different state paths. The first runner start, or a status command that migrates version 1 state, binds the repository to one state directory; later conflicting `--state-dir` values are rejected.

State keeps historical Runs separate from active Leases. New Runs snapshot the Candidate issue title and URL when their Lease is created, then persist their RPC session identity and dedicated session storage before launch. Worker log paths become durable as soon as the gated Worker starts. Existing Runs without issue snapshots or startup log paths remain valid and are not backfilled through GitHub. Upgrading version 1 state preserves Run metadata and artifacts, removes the obsolete paused setting, and records legacy print-mode Runs as non-resumable. During migration, incomplete and intervention-required Runs retain their Leases, while verified merged Runs remain as history without active ownership.

On restart, the runner reconciles persisted Leases with process liveness and GitHub pull request and issue state. Suspended Runs retain their Lease, branch, worktree, Pi session, and verified continuation boundary without becoming eligible for a duplicate launch. It compares each recovered PID with its persisted operating-system process start identity, so PID reuse becomes `needs-human` instead of being mistaken for the worker. A live matching worker is never launched twice. A recovered live RPC Worker becomes `needs-human` because a replacement runner cannot restore its prompt and event pipes. Its process identity remains durable, consumes Worker capacity, and prevents `retry`; future lifecycle work must verify process-group exit before clearing it. A dead worker is verified against GitHub before being classified. Recovered workers older than `--max-worker-age` also become `needs-human`. Uncertainty becomes `needs-human`, never a new launch.

Inspect state. Plain status includes each snapshotted issue title when available and falls back to its issue number for older history. Reading version 1 state performs the version 2 migration under the repository lock before printing status:

```sh
backlog status
backlog status --json
```

Inspect the exact actions required to Reset an incomplete Run:

```sh
backlog reset 123 --dry-run
```

Reset dry-run holds the repository coordination lock while it inspects the Run, Lease, Worker, GitHub issue and pull requests, remote and local branches, worktree, and Pi session. It prints only actions still required. It refuses live or uncertain Workers, merged work, unknown resource state, unexplained issue closure, and human workflow labels. It never prompts or writes, and it does not require `--yes`.

Mutating Reset supports owned GitHub pull requests and remote branches when the local branch, worktree, and Pi session are already absent. Run it interactively to review and confirm the plan, or pass `--yes` for non-interactive use:

```sh
backlog reset 123
backlog reset 123 --yes
```

Interactive confirmation defaults to no. Reset rechecks pull request identity, merge and auto-merge state, branch name, and expected commit before each GitHub mutation. It disables auto-merge, explains the Reset on the pull request before closing it, and conditionally deletes the owned remote branch at its verified commit. Verified partial progress leaves the Run `resetting` with its Lease held, so a rerun performs only remaining actions. After restoring managed issue labels while preserving unrelated labels, Reset verifies every postcondition, then atomically marks the historical Run `reset` and releases its Lease. It does not create a replacement Run. Runs with remaining local artifacts can be inspected with `--dry-run`, but mutating Reset refuses them until local artifact retirement is implemented.

`retry` is a deprecated alias for the same Reset path, flags, output, mutations, and exit statuses. It adds a deprecation warning:

```sh
backlog retry 123 --yes
```

## Shutdown

The first `SIGINT` enters Drain. Backlog atomically stops admitting new Leases, allows every Run whose Lease it committed to settle and reconcile, reports the stage and remaining Owned Worker count, and exits successfully when no Owned Workers remain. A Run whose Lease committed just before Drain may still finish worktree preparation and Worker launch.

A second `SIGINT`, or the first `SIGTERM`, starts bounded suspension directly. All Owned Workers share one 60-second wall-clock deadline. For each unfinished Run, Backlog requires a correlated successful RPC `abort` response and `agent_settled`, proves streaming, compaction, retry, tool, and message queues are idle, compares the exact RPC session and complete entry tree with the synced session file, and persists the continuation marker while RPC is still open. GitHub Completion and armed auto-merge outcomes take precedence. After verified process-group exit, one atomic state write records `suspended` and clears the PID. The Lease, branch, worktree, and Pi session remain in place.

A third `SIGINT` bypasses the remaining deadline. Third-signal and timeout escalation use the same force-stop path, which reloads the current Run and rechecks Worker liveness, PID, and process-start identity immediately before signaling the process group. A mismatched or unverifiable process is not signaled and its Run becomes `needs-human` with its Lease retained. Verified merged, waiting-for-merge, and suspended outcomes are preserved. A force-stopped Run is classified as suspended only when its continuation marker was already durable; otherwise it becomes `needs-human`. Signal shutdown exits 130 when initiated by `SIGINT` and 143 when initiated by `SIGTERM`, including force escalation.

Non-signal context cancellation retains the immediate failure shutdown behavior. Persisted live Workers discovered from an earlier runner are not killed because the new process does not own them.

## Validation

Run the fast lint, test, and build checks:

```sh
mise run check
```

Run the complete CI suite, including the race detector:

```sh
mise run ci
```

Individual tasks are available through `mise tasks ls`. GitHub Actions runs the complete validation suite for pull requests and pushes to `master`.

Tests use temporary state and fake `gh`, `git`, and `pi` executables. They do not modify real GitHub issues, branches, or pull requests.

## Current scope

This first version is a foreground scheduler. It intentionally does not include daemonization, a web UI, webhooks, cross-machine leases, or a Pi extension. A future TypeScript extension can act as a thin control panel over the standalone Go process without moving scheduling into an LLM context.
