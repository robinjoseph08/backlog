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
- GitHub API access with Issues and pull requests write permission, plus authenticated Git push access to delete owned remote branches

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
--plain             Disable the full-screen terminal dashboard
--repo-dir PATH     Target Git repository, default current directory
--state-dir PATH    Override the external state directory
--approve=false     Do not trust project-local Pi resources
```

The concurrency limit counts top-level issues. Each AFK worker may itself launch implementation and review subagents.

When standard output is a terminal, `backlog run` automatically opens a responsive Bubble Tea alternate-screen dashboard. Terminals at least 24 rows high show compact Run rows with details for the selected Run. Terminals from 12 through 23 rows use one-line rows and collapse Recent Completions by default. Smaller terminals prioritize repository and Worker health, the Attention Required count, the selected body content, Runner stage, and navigation status. Compact rows are truncated to terminal cell width, while expanded details wrap inside the viewport. Press Enter to expand or collapse the selected Run or section.

A fixed header and lifecycle footer keep health, Runner stage, next Ctrl-C behavior, and navigation help visible while the middle body scrolls with Up/Down, `j`/`k`, Page Up/Page Down, `f`/`b`, Home/End, `g`/`G`, or the mouse wheel. Live updates preserve the selected section or Run and its screen position. New Attention Required outside the viewport marks the header; press `a` to jump to it. Issue identities backed by a safe stored or synthesized URL and valid pull request labels are terminal hyperlinks. Press `o` to open the selected Run's issue or `p` to open its pull request when available. URL opener failures appear only as temporary dashboard diagnostics and do not affect the Runner. The `q` key is intentionally unassigned and does not detach the dashboard or change the Runner lifecycle. The body contains Admission health, Active Runs and Attention Required from retained Leases across Runner invocations, every unacknowledged `failed` or `needs-human` Historical Run outcome, compact Recent Completions, and operational messages. Running Runs without verified Worker liveness appear first in Active Runs; the remaining Active Runs stay oldest first. Recent Completions contains the latest ten merged Runs across invocations and displays every projected Completion when expanded. Press `d` to toggle bounded Candidate discovery Diagnostics. The drawer opens the latest retained record; `[` and `]` select records, while `,` and `.` page through complete oversized evidence without wrapping offscreen evidence on every dashboard refresh. Compact Active rows omit unavailable and low-value implementation details, promote anomalous Worker liveness or supervision, and use the same normalized Activity observation model as `status` and `follow`. Expanded Completion details remain limited to issue, pull request, elapsed time, and completion time. Interactive styling pairs cyan or blue Active work, green Completions, amber degraded, waiting, or suspended conditions, and red attention or fatal conditions with their existing text labels. Metadata uses a muted foreground, and the palette adapts to terminal background and color capability. If the terminal does not report its background, attribute-only semantic styling preserves contrast until detection succeeds. `NO_COLOR` removes semantic colors and attributes without removing labels or keyboard navigation. Bubble Tea restores the normal screen and cursor during ordinary dashboard teardown, including signal shutdown and Runner failure. If presentation output fails, Backlog directly retries an idempotent restoration sequence. When terminal writes remain possible, restoration completes before Backlog prints a concise static result. This fallback handles transient output failures, but a persistently failing terminal writer can prevent both restoration and the static report. The result names the final outcome and lists only Completions produced during this invocation. Once setup can locate and successfully read repository state, the result also retains remaining Active and Attention Required Runs without reproducing Admission retries or the live dashboard. Earlier setup failures report an uninitialized repository with empty sections. Use `--plain` to disable the full-screen dashboard. Redirected output and plain mode keep append-only lifecycle messages and never emit ANSI, hyperlink, mouse, cursor, or alternate-screen controls.

After a complete Candidate snapshot succeeds, the foreground runner exits when no leased Run remains capable of autonomous progress and no eligible Candidate starts a Worker. Before this natural one-shot exit, it leaves a final static aggregate summary with Active and Attention Required sections. The exit is unsuccessful when any Intervention-required Run retains its Lease, including unresolved Runs loaded at startup or produced during the invocation. Historical failed Runs without Leases do not affect the result. Blocked Candidates do not keep the runner alive unless `--watch` is set, and Attention Required alone never ends watch mode.

Candidate discovery fails closed for Admission. If a discovery pass fails, the runner creates no Leases from that pass, continues supervising existing Workers, and retries after `--poll` while Admission remains active. The terminal dashboard aggregates consecutive failures into one degraded Admission banner with the affected operation, concise cause, failure times, and retry countdown. It retains at most twenty full errors and commands for the current invocation in the `d` Diagnostics drawer. A later complete snapshot restores healthy Admission immediately and shows a ten-second recovery notice. Plain and redirected output continue to report every complete retry diagnostic as append-only output. This behavior includes initial startup and idle non-watch invocations.

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

`agent_settled` is the normal completion trigger. An unexpected process exit triggers fail-closed reconciliation instead. While an incomplete idle Pi RPC process is still alive, the runner attempts to capture the same synchronized session leaf, entry count, hash, and AFK or ship-it checkpoint identity used by suspension, without sending `abort`. It then looks up the pull request by the Run's unique branch, verifies the issue state, and persists the reconciled Run before closing RPC input. Shutdown escalates if orderly closure exceeds its grace period, confirms process-group exit, durably closes the Worker log, and releases Worker capacity. If the Run remains incomplete, the runner rechecks the expected branch for a late Completion before immediately considering External Resolution. Issue closure never controls the Worker. An armed open pull request becomes `waiting-for-merge`; other unverified outcomes require human attention.

Normal completion cleanup removes worktrees and local branches only for verified merged Runs. Incomplete Runs retain artifacts until Reset or External Resolution safely retires them.

## State and recovery

State and logs live outside the target repository. By default they are stored under the operating system's user cache directory in a path derived from the absolute repository path. Use `--state-dir` when a stable explicit location is preferable.

State is written with same-directory temporary files, file sync, atomic rename, and directory sync. A repository-level advisory lock in the Git common directory prevents two local runner instances from scheduling the same backlog, even if they request different state paths. The first runner start or mutating lifecycle command binds the repository to one state directory; later conflicting `--state-dir` values are rejected.

State keeps Historical Runs separate from active Leases. New Runs snapshot the Candidate issue title and URL when their Lease is created, then persist their RPC session identity and dedicated session storage before launch. Worker log paths become durable as soon as the gated Worker starts. Existing Runs without issue snapshots or startup log paths remain valid and are not backfilled through GitHub. Runner startup and successful lifecycle mutations migrate versions 1 through 4 state atomically to schema version 5. Status instead validates and previews the current representation of legacy state versions without persisting migration. Migration preserves Run metadata, Leases, and artifacts. Historical `failed` and `needs-human` outcomes remain unacknowledged, while completed Reset outcomes are already treated as handled. Version 1 migration also removes the obsolete paused setting and records legacy print-mode Runs as non-resumable. Incomplete and intervention-required Runs retain their Leases, while verified merged Runs remain as history without active ownership. Legacy print-mode Runs cannot Resume because they have no verified RPC continuation boundary. Backlog refuses state from newer unsupported versions rather than attempting a downgrade.

On restart, the runner reconciles persisted Leases with process liveness and GitHub pull request and issue state before Candidate admission. It automatically performs complete External Resolution for verified closed issues with no Owned Worker, including resuming remaining actions from `resolving-externally`. Watch polling repeats this check during normal reconciliation. When an issue closes during an Owned Worker's lifecycle, periodic reconciliation skips it and the Worker settles normally. After process-group exit and durable log closure, the runner rechecks Completion and immediately performs External Resolution when the issue was handled elsewhere. Safe Suspended Runs fill Worker capacity before new Candidates. Resume rechecks Completion, the open issue and managed workflow labels, the exact branch and worktree, the Pi session identity, durable leaf, entry count and file hash, and proof that the old Worker stopped. A replacement Worker keeps the Run, Lease, branch, worktree, and Pi session identities while receiving a new PID and process-start identity. Its prompt requires Pi to reassess repository and GitHub state before continuing AFK.

A continuation marker persisted before a crash can recover a dead Worker into Resume. Missing, changed, malformed, or uncertain continuation state on a Suspended Run becomes `needs-human` with its Lease retained. Legacy print-mode Runs never Resume automatically. The runner compares each recovered PID with its persisted operating-system process start identity, so PID reuse becomes `needs-human` instead of being mistaken for the Worker. A live matching Worker is never launched twice. A recovered live RPC Worker becomes `needs-human` because a replacement runner cannot restore its prompt and event pipes. Its process identity remains durable and consumes Worker capacity. Recovered Workers older than `--max-worker-age` also become `needs-human`.

Use Recovery to verify an existing `failed` or `needs-human` leased Run and establish it as Suspended without creating a new Run, Lease, branch, worktree, or Pi session:

```sh
backlog recover <run-id|positive-issue-number> --dry-run
backlog recover <run-id|positive-issue-number>
backlog recover <run-id|positive-issue-number> --yes
```

Recovery first requires absent Runner coordination, a conclusively absent Worker and process group, a retained Lease, and verified issue and expected-branch pull request state. Verified Completion and armed auto-merge outcomes are considered before suspension: an armed expected pull request is reconciled as waiting for merge before Recovery inspects Git, worktree, Pi session, or workflow checkpoint evidence. A Run that can transition to Suspended must additionally have an open issue with active managed labels, the exact expected branch and worktree, one unambiguous Pi session with a complete tool-result tail, unchanged durable leaf and hash, and a supported durable AFK stage checkpoint or ship-it checkpoint. An original `/skill:afk` invocation identifies ownership but not a continuation stage and is never sufficient by itself. Settled checkpoint capture writes the Backlog-owned AFK stage checkpoint when ship-it has not started. Existing terminal Runs without exact stage evidence fail closed. Dry-run is read-only. Mutation confirms by default, revalidates after confirmation, preserves the original diagnostic and every ownership identity, records Recovery metadata separately from implementation repair attempts, and transitions to Suspended. Normal Resume performs fresh launch checks and sends the exact checkpoint stage in the replacement prompt. A late verified Completion requires one merged expected pull request, a closed issue, and matching active artifact commits; it retires the owned branch, worktree, active Pi session, and managed workflow labels before recording the merged pull request and releasing the Lease. Refusal retains the Lease and artifacts.

Ship-it Failure class, Blocker kind, blocker cause, and blocker fingerprint are preserved as separate diagnostics; only Failure class uses Backlog's failure-class enum. A structured top-level Pi `auto_retry_end` event with `success:false` is the only automatic provider-exhaustion signal. After Pi exhausts its own retries, Backlog permits one continuation from a verified durable boundary after a fixed 30-second cooldown. A second provider exhaustion becomes `needs-human`. Provider continuation attempts and explicit Recovery counts are durable and do not consume AFK or ship-it implementation repair budgets.

Inspect state. Default plain status is a concise operational projection with Active, Attention Required, Outcomes to Acknowledge, and Recent Completions sections. Active and Attention Required are classified from retained Leases. Every unacknowledged Historical Run in `failed` or `needs-human` remains visible under Outcomes to Acknowledge without an age or count limit. Recent Completions contains the ten newest merged Runs plus any older merged Run whose Completion cleanup is still pending. Completed Reset and External Resolution outcomes, along with explicitly acknowledged outcomes, are omitted from the default projection.

Every Run displayed by plain status includes elapsed time, using `n/a` when no start time is available. Active running Runs use Follow's observation model to report verified Worker liveness, Activity age, the current deepest Worker or Subagent operation, separate turn counts, and observed tokens. Missing legacy telemetry is shown as `n/a`. Plain status includes each snapshotted issue title and URL when available, falls back to its issue number for older history, and reports `suspending` only while a verified local Runner is supervising bounded suspension. Sections are newest first with deterministic Run ID tie-breaking. The summary distinguishes total and displayed Runs and reports how many acknowledged outcomes the default projection hides.

Use `--all` for every persisted Run exactly once. Explicitly acknowledged outcomes show their acknowledgment timestamp. JSON status is also complete and unfiltered, includes additive Outcome Acknowledgment and External Resolution metadata, and does not include normalized observation counters. Status remains read-only and previews supported legacy state versions without persisting their migration:

```sh
backlog status
backlog status --all
backlog status --json
```

Acknowledge one or more reviewed Historical Run outcomes without deleting history:

```sh
backlog acknowledge <run-id>
backlog acknowledge <positive-issue-number>...
backlog acknowledge --all
```

An exact Run ID takes precedence, including a numeric Run ID. Otherwise, a positive issue number selects every Historical Run for that issue in `failed` or `needs-human` without a Lease. Multiple selectors are validated before one atomic state update, so an unknown or ineligible exact Run refuses the whole operation. `--all` applies to one locked snapshot and cannot be combined with selectors. Repeated acknowledgment is successful and preserves the original timestamp. Acknowledgment is non-interactive and does not use `--yes` or `--dry-run`.

Outcome Acknowledgment means only that an operator has seen an unexpected non-Completion outcome. It does not claim Completion or External Resolution. It does not release a Lease, change Candidate eligibility or GitHub state, retire a branch, worktree, session, or log, or remove diagnostics. Reset abandons incomplete owned work and retires active artifacts. Completion is a GitHub-verified merged outcome. Full status and Follow retain access to acknowledged Runs.

Follow one Run through normalized Worker and Subagent Activity without acquiring scheduling ownership or communicating with its Runner or Worker:

```sh
backlog follow <run-id>
backlog follow <positive-issue-number>
```

An exact Run ID is selected as-is, including a numeric Run ID. Otherwise, a positive issue number resolves once to the Run referenced by its active Lease, or to its latest historical Run by start time and Run ID. Follow never switches to a replacement Run.

Follow immediately prints the resolved Run ID and issue identity, durable state, local Runner supervision, verified Worker liveness, elapsed time, Activity age, current Worker operation, exact Worker turns and tokens, separate Subagent status and durations, and approximate Subagent turns, tool uses, and tokens. Worker liveness is `alive` only when both the PID exists and its process-start identity matches persisted state. A nonterminal Run without detected local Runner coordination is prominently `UNSUPERVISED`; Follow keeps observing it until it becomes terminal or the operator detaches. Activity ages of at least five minutes include a `(quiet)` marker without changing the Run state or calling the Worker stalled. Each Subagent is tracked independently, and the summary shows the active count and deepest current operation. The compact observed-token total is prefixed with `~` whenever Subagent estimates contribute.

Follow shows at most the latest 20 semantic Run Activity entries, then streams new model, tool, turn, retry, compaction, lifecycle, and Subagent Activity. Subagent feed updates are limited to one per second per Subagent, except that status transitions and turn milestones are always retained. Every meaningful update still refreshes Activity age. Spinner frames, durations alone, and repeated snapshots are ignored as Activity. Feed entries include safe Subagent durations from available telemetry and show unavailable durations as `n/a`. Reasoning, full Subagent prompts, tool arguments, and tool results are omitted, while safe descriptions and visible final Worker assistant text may be shown. Missing or malformed telemetry is reported as `n/a`.

Backlog records normalized Activity and its local observation time in an append-only sidecar without rewriting lifecycle state for each event. Projection failures do not affect the Worker result. Follow reports a diagnostic and rebuilds privacy-safe semantic history from raw evidence when possible; replayed Activity without trustworthy observation times has an age of `n/a`.

Use `--raw` to follow the verbatim Worker JSONL instead:

```sh
backlog follow <run-id|positive-issue-number> --raw
```

Both modes retain an unterminated final record until its newline arrives and stream newly completed records in order. Raw mode writes the resolved Run ID and observation summary to standard error so standard output remains verbatim JSONL. Follow exits after a terminal Run reaches `merged`, `failed`, `needs-human`, `reset`, or `resolved-externally`, the Runner records the Worker log as closed, and all complete records are emitted. Historical terminal Runs without a log-open marker are treated as already closed. Ctrl-C only detaches the follower. A missing Run, an issue without Run history, or an unavailable Worker log is reported for the requested selection without changing runner state.

Recognize a closed GitHub issue as External Resolution of its incomplete leased Run, or finish verified cleanup for a Historical merged Run:

```sh
backlog resolve <run-id|positive-issue-number> --dry-run
backlog resolve <run-id|positive-issue-number>
backlog resolve <run-id|positive-issue-number> --yes
```

An exact Run ID takes precedence, including a numeric Run ID. Otherwise, the issue number selects its incomplete leased Run first, then a Historical merged Run with pending Completion cleanup, then the latest Historical merged Run for no-op verification, and finally its latest Historical External Resolution. Resolve refuses an open issue, unavailable or unsupported closure state, a live or uncertain Worker, pending replacement, mismatched resources, or active Runner coordination. During active Runner coordination, every explicit Resolve mode explains that the supervising Runner handles automatic reconciliation at startup, during watch polling, and after normal Worker settlement. It safely disarms, explains, and closes owned unmerged pull requests; conditionally deletes verified remote and local branches; removes the verified worktree; and atomically archives the active Pi session. Conclusively absent artifacts and a matching historical session archive are already retired. Changed commits, ownership drift, conflicting archives, symlink uncertainty, or unknown inspection retain the Lease for intervention. Both GitHub `completed` and `not-planned` closure reasons qualify and remain distinct in history. A merged recorded pull request and closed issue may establish Completion after fail-closed lifecycle and artifact-identity validation. When the Run stopped before recording a pull request, one merged pull request discovered from its expected branch may do the same. Multiple unrecorded merged pull requests remain under intervention because the outcome is ambiguous. Completion retirement removes `in-progress` and `ready-for-agent` while preserving human workflow and unrelated labels before recording the merged outcome.

For a Historical merged Run, Resolve verifies its merged expected pull request and inspects the remaining owned artifacts without recreating a Lease. It retires only verified remote and local branches, worktrees, active Pi sessions, and managed labels, then clears `cleanupPending` while preserving the merged outcome, pull request identity, Completion timestamps, diagnostics, and absent Lease. A fully cleaned Historical merged Run is a verification-only no-op rerun. Changed or uncertain artifact identity fails closed without changing Completion metadata.

Dry-run prints only the remaining actions in safe order without prompting, migration, state binding, or mutation. Interactive confirmation defaults to no, including Enter, EOF, and non-affirmative responses. Interactive operation requires confirmation again when the displayed plan changes; `--yes` prints a changed current plan before continuing. Non-interactive mutation requires `--yes`. External Resolution removes `in-progress` and `ready-for-agent` while preserving human workflow and unrelated labels. It records `resolving-externally` before external mutation and retains the Lease across partial progress, then atomically records `resolved-externally`, the recognition time and closure reason, closes the Worker-log-open marker, and releases the Lease only after every artifact and label postcondition is freshly verified. Existing diagnostics and logs remain recorded. Missing recorded logs produce a durable warning rather than retaining ownership. Repeating Resolve verifies the Historical Run and absent Lease without changing its original metadata.

Inspect the exact actions required to Reset an incomplete Run:

```sh
backlog reset 123 --dry-run
```

Reset dry-run holds the repository coordination lock while it inspects the Run, Lease, Worker, GitHub issue and pull requests, remote and local branches, worktree, and Pi session. It prints only actions still required. It refuses live or uncertain Workers, merged work, unknown resource state, unexplained issue closure, and human workflow labels. It never prompts or writes, and it does not require `--yes`.

Mutating Reset handles the complete owned artifact set, including GitHub pull requests, remote and local branches, the real worktree, and the active Pi session. Run it interactively to review and confirm the plan, or pass `--yes` for non-interactive use:

```sh
backlog reset 123
backlog reset 123 --yes
```

Interactive confirmation defaults to no. Reset rechecks pull request identity, merge and auto-merge state, branch name, commit, and worktree association immediately before each destructive mutation. For each open pull request that Reset closes, it disables auto-merge when needed and posts an explanation before closure. It conditionally deletes the owned remote branch at its verified commit, force-removes the verified worktree, and deletes the local branch with an expected-commit check. The active Pi session is atomically renamed into `history/sessions/<run-id>` under the state directory, outside the active session path used by Resume. Already-absent artifacts and an already-archived session are successful and omitted from later plans.

Verified partial progress leaves the Run `resetting` with its Lease held, so a rerun performs only remaining actions. Changed, reassigned, mismatched, or unknown artifacts stop Reset without releasing the Lease. After restoring managed issue labels while preserving unrelated labels, Reset verifies that all owned artifacts are retired and the session is non-resumable, then atomically marks the historical Run `reset` and releases its Lease. Worker JSONL, standard-error logs, Run diagnostics, issue snapshots, and the recorded pull request URL remain in history. Reset does not create a replacement Run.

`retry` is a deprecated alias for the same Reset path, flags, output, mutations, and exit statuses. It adds a deprecation warning:

```sh
backlog retry 123 --yes
```

## Shutdown

The first `SIGINT` enters Drain. Backlog atomically stops admitting new Leases, allows every Run whose Lease it committed to settle and reconcile, reports the stage and remaining Owned Worker count, and exits successfully when no Owned Workers remain and no operational state or process-control failure occurred. Runs that become intervention-required do not make an otherwise orderly Drain fail. A Run whose Lease committed just before Drain may still finish worktree preparation and Worker launch.

A second `SIGINT`, or the first `SIGTERM`, starts bounded suspension directly. All Owned Workers share one 60-second wall-clock deadline. For each unfinished Run, Backlog requires a correlated successful RPC `abort` response and `agent_settled`, proves streaming, compaction, retry, tool, and message queues are idle, compares the exact RPC session and complete entry tree with the synced session file, and persists the continuation marker while RPC is still open. GitHub Completion and armed auto-merge outcomes take precedence. After verified process-group exit, one atomic state write records `suspended` and clears the PID. The Lease, branch, worktree, and Pi session remain in place.

A third `SIGINT` bypasses the remaining deadline. Third-signal and timeout escalation use the same force-stop path, which reloads the current Run and rechecks Worker liveness, PID, and process-start identity immediately before signaling the process group. A mismatched or unverifiable process is not signaled and its Run becomes `needs-human` with its Lease retained. Verified merged, waiting-for-merge, and suspended outcomes are preserved. If merged worktree cleanup reaches the shared deadline, the merged Run records pending cleanup for the next startup rather than losing the retained artifact. A force-stopped Run is classified as suspended only when its continuation marker was already durable; otherwise it becomes `needs-human`. A completed first-`SIGINT` Drain exits successfully unless an operational state or process-control failure occurred. Suspension initiated by a second `SIGINT`, including later force escalation, exits 130. Suspension initiated by `SIGTERM`, including force escalation, exits 143.

Non-signal context cancellation retains the immediate failure shutdown behavior. Persisted live Workers discovered from an earlier runner are not killed because the new process does not own them.

## Exit statuses

- `0`: command succeeded, an acknowledgment was completed or already satisfied, natural one-shot exhaustion found no unresolved intervention, a first-signal Drain completed, a dry-run completed, or interactive Recovery, Reset, or External Resolution was declined
- `1`: natural one-shot exhaustion left an Intervention-required Run with its Lease, an ownership or safety check refused the command, or an operational command failed
- `2`: the top-level command was missing or unknown
- `130`: `backlog run` suspension was initiated by a second `SIGINT`
- `143`: `backlog run` suspension was initiated by `SIGTERM`

Flag parsing failures currently use status `1`. `retry` has the same status as the equivalent `reset` invocation and additionally prints its deprecation warning.

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
