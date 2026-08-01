---
status: accepted
---

# Bind prompt ownership to the persisted Pi entry

Backlog will preserve two distinct prompt identities for every new Run. `PromptDigest` remains the SHA-256 digest of the exact rendered text submitted through the correlated Pi RPC `prompt` command. Versioned prompt-ownership evidence records the durable entry ID and canonical content digest of the actual initial user entry observed through a separately correlated `get_entries` command after Pi accepts that prompt.

## Consequences

- A new Run persists a version 1 pending ownership marker before its gated Worker receives the initial prompt. Pending evidence never falls back to legacy prompt matching.
- The gated Worker is already bound to the Run PID, process-start identity, session directory, logs, branch, and worktree before prompt submission.
- Backlog waits for the successful correlated prompt response, reads the fresh session through a separately correlated command, and requires exactly one initial user entry. Complete evidence becomes durable before the Worker enters normal supervision.
- Pi may persist content different from the submitted RPC text. This supports the default AFK command, arbitrary `/skill:` commands, plain text, and multiline text without interpreting skill names or arguments.
- Backlog stores only the submitted-text digest, persisted entry ID, and canonical persisted-content digest. It does not store complete rendered prompts or expanded skill bodies.
- Continuation verification requires the recorded entry ID and digest to identify exactly one user entry. Missing, changed, duplicated, ambiguous, malformed, unsupported, or substituted evidence fails closed with the Lease and artifacts retained.
- Backlog does not reconstruct expansion from mutable skill files, trust messages because they resemble expanded skill markup, or match only an issue number.
- Prompt ownership remains independent from workflow-stage evidence. Recovery and Resume continue to require the supported Backlog-owned AFK checkpoint or ship-it checkpoint and every existing session, repository, identity, and process-absence check.
- State schema version 7 persists the versioned evidence. Versions 1 through 6 migrate without invented ownership evidence and receive an explicit legacy-mode marker. Only marked legacy Runs continue using their previous exact submitted-prompt digest rule, or the exact default AFK command rule when they have no digest. Missing evidence on a current Run never enables legacy fallback.
