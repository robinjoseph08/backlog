# pi-backlog-runner

`pi-backlog-runner` continuously drains a GitHub issue backlog with isolated Pi AFK workers. It is a deterministic Go scheduler, not a long-running coordinator conversation.

Each issue receives its own lease, Git branch, Git worktree, Pi process, named Pi session, context window, and logs. GitHub is always consulted before a run is considered complete.

## Requirements

- A Unix-like system with `/bin/sh` and POSIX process groups
- Go 1.23 or newer to build
- `git`
- GitHub CLI `gh`, authenticated for the target repository
- Pi with the global `afk` skill available
- A Git remote named `origin`
- GitHub API access with Issues read permission

The runner uses GitHub's versioned issue dependency endpoint. A dependency lookup failure stops scheduling rather than treating the issue as unblocked.

## Build

```sh
go build -o pi-backlog-runner ./cmd/pi-backlog-runner
```

Or install it on your Go binary path:

```sh
go install ./cmd/pi-backlog-runner
```

## Run

From the repository whose backlog should be drained:

```sh
pi-backlog-runner run
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

The foreground runner exits when there are no active workers and no currently eligible candidates. Blocked candidates do not keep it alive unless `--watch` is set.

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

For issue `#123`, the runner invokes Pi in the issue worktree using the equivalent of:

```sh
pi --mode json -p --approve --name "afk #123" "/skill:afk 123"
```

Standard output is validated as JSONL and saved separately from standard error. A malformed event stream fails the run safely and preserves its worktree.

A zero Pi exit code is not completion. The runner looks up the pull request by the run's unique branch and verifies that it merged and that the issue closed. If Pi exits cleanly while an open PR has auto-merge armed, the run waits and reconciles it periodically. Other open-PR outcomes require human attention.

Only verified merged runs have their worktrees and local branches removed. Failed and ambiguous runs are retained.

## State and recovery

State and logs live outside the target repository. By default they are stored under the operating system's user cache directory in a path derived from the absolute repository path. Use `--state-dir` when a stable explicit location is preferable.

State is written with same-directory temporary files, file sync, atomic rename, and directory sync. A repository-level advisory lock in the Git common directory prevents two local runner instances from scheduling the same backlog, even if they request different state paths. The first run binds the repository to one state directory; later conflicting `--state-dir` values are rejected.

On restart, the runner reconciles persisted leases with process liveness and GitHub pull request and issue state. It compares each recovered PID with its persisted operating-system process start identity, so PID reuse becomes `needs-human` instead of being mistaken for the worker. A live matching worker is never launched twice. A dead worker is verified against GitHub before being classified. Recovered workers older than `--max-worker-age` also become `needs-human`. Uncertainty becomes `needs-human`, never a new launch.

Inspect state:

```sh
pi-backlog-runner status
pi-backlog-runner status --json
```

Allow a failed or `needs-human` issue to be scheduled again:

```sh
pi-backlog-runner retry 123
```

Retry removes the old lease but deliberately retains the prior worktree for diagnosis. The next attempt receives a new branch and worktree.

## Shutdown

`SIGINT` and `SIGTERM` stop scheduling and cancel workers started by the current runner. Pi process groups receive `SIGTERM`, followed by `SIGKILL` after a grace period if needed. Their runs are persisted as failed and their worktrees are retained. Persisted live workers discovered from an earlier runner are not killed because the new process does not own them.

## Validation

```sh
go test ./...
go test -race ./...
go vet ./...
```

Tests use temporary state and fake `gh`, `git`, and `pi` executables. They do not modify real GitHub issues, branches, or pull requests.

## Current scope

This first version is a foreground scheduler. It intentionally does not include daemonization, a web UI, webhooks, cross-machine leases, or a Pi extension. A future TypeScript extension can act as a thin control panel over the standalone Go process without moving scheduling into an LLM context.
